"""Bounded-batch persistence: unique jobs, raw events, and a recoverable JSONL outbox."""

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def compact(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def has_text(value):
    return value is not None and bool(str(value).strip())


def job_key(record):
    for key in ("id", "job_id", "jobId", "job_url", "link", "jobUrl"):
        if has_text(record.get(key)):
            return str(record[key]).strip()
    identity = [record.get(key) for key in ("title", "company", "location")]
    return "derived-" + hashlib.sha256(compact(identity).encode()).hexdigest()[:32]


class Store:
    def __init__(self, data_dir):
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "collector.sqlite3"
        self.events_path = self.data_dir / "events.jsonl"
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, payload TEXT NOT NULL, title TEXT, company TEXT,
                    location TEXT, has_description INTEGER NOT NULL, first_seen TEXT,
                    last_seen TEXT, last_seq INTEGER, seen_live INTEGER DEFAULT 0,
                    seen_replay INTEGER DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS jobs_recent ON jobs(last_seq DESC);
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE,
                    job_id TEXT, mode TEXT, run_id TEXT, created_at TEXT, payload TEXT
                );
                CREATE INDEX IF NOT EXISTS events_time ON events(created_at);
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY CHECK(id=1), events INTEGER DEFAULT 0,
                    unique_jobs INTEGER DEFAULT 0, duplicates INTEGER DEFAULT 0,
                    descriptions INTEGER DEFAULT 0, live_events INTEGER DEFAULT 0,
                    replay_events INTEGER DEFAULT 0, imported_events INTEGER DEFAULT 0
                );
                INSERT OR IGNORE INTO metrics(id) VALUES(1);
                CREATE TABLE IF NOT EXISTS export_state (
                    id INTEGER PRIMARY KEY CHECK(id=1), seq INTEGER DEFAULT 0,
                    bytes INTEGER DEFAULT 0
                );
                INSERT OR IGNORE INTO export_state(id) VALUES(1);
                CREATE TABLE IF NOT EXISTS batches (
                    id INTEGER PRIMARY KEY, at TEXT, mode TEXT, run_id TEXT,
                    received INTEGER, new_jobs INTEGER, duplicates INTEGER,
                    duration REAL, label TEXT
                );
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY, at TEXT, level TEXT, message TEXT
                );
            """)

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.db_path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        try:
            with db:
                yield db
        finally:
            db.close()

    def get_meta(self, key, default=None):
        with self.connection() as db:
            row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key, value):
        with self.connection() as db:
            self._meta(db, key, value)

    @staticmethod
    def _meta(db, key, value):
        db.execute("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                   (key, compact(value)))

    def log(self, level, message):
        with self.connection() as db:
            db.execute("INSERT INTO logs(at,level,message) VALUES(?,?,?)",
                       (utc_now(), level, str(message)[:4000]))
            db.execute("DELETE FROM logs WHERE id < (SELECT COALESCE(MAX(id),0)-2000 FROM logs)")

    def ingest(self, records, *, mode, run_id, label="", duration=0, checkpoint=None):
        if mode not in {"live", "replay", "import"}:
            raise ValueError("Unknown ingestion mode")
        stamp, accepted, new_jobs, description_delta = utc_now(), 0, 0, 0
        with self.connection() as db:
            for raw in records:
                record = dict(raw)
                identity = job_key(record)
                incoming_time = record.get("timestamp")
                record["id"] = identity
                if mode == "replay" and incoming_time:
                    record.setdefault("source_timestamp", incoming_time)
                record["timestamp"] = stamp
                record["_ingestion_mode"] = mode
                record["_run_id"] = run_id
                event_id = record.pop("_event_id", None) or uuid.uuid4().hex
                record.pop("_sequence", None)
                record["_event_id"] = event_id
                encoded = compact(record)
                event = db.execute(
                    "INSERT OR IGNORE INTO events(event_id,job_id,mode,run_id,created_at,payload) VALUES(?,?,?,?,?,?)",
                    (event_id, identity, mode, run_id, stamp, encoded),
                )
                if event.rowcount == 0:
                    continue
                accepted += 1
                sequence = event.lastrowid
                old = db.execute("SELECT payload,has_description FROM jobs WHERE id=?", (identity,)).fetchone()
                merged = json.loads(old["payload"]) if old else {}
                for key, value in record.items():
                    if value is not None and not (isinstance(value, str) and not value.strip()):
                        merged[key] = value
                # Preserve explicit nulls on a brand new job.
                if old is None:
                    merged = record
                    new_jobs += 1
                described = int(has_text(merged.get("description")))
                description_delta += described - (old["has_description"] if old else 0)
                db.execute("""
                    INSERT INTO jobs(id,payload,title,company,location,has_description,
                                     first_seen,last_seen,last_seq,seen_live,seen_replay)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,
                        title=excluded.title,company=excluded.company,location=excluded.location,
                        has_description=excluded.has_description,last_seen=excluded.last_seen,
                        last_seq=excluded.last_seq,
                        seen_live=MAX(jobs.seen_live,excluded.seen_live),
                        seen_replay=MAX(jobs.seen_replay,excluded.seen_replay)
                """, (identity, compact(merged), str(merged.get("title") or ""),
                      str(merged.get("company") or ""), str(merged.get("location") or ""),
                      described, stamp, stamp, sequence, int(mode == "live"), int(mode == "replay")))
            duplicates = accepted - new_jobs
            db.execute("""UPDATE metrics SET events=events+?,unique_jobs=unique_jobs+?,
                duplicates=duplicates+?,descriptions=descriptions+?,live_events=live_events+?,
                replay_events=replay_events+?,imported_events=imported_events+? WHERE id=1""",
                (accepted, new_jobs, duplicates, description_delta,
                 accepted if mode == "live" else 0, accepted if mode == "replay" else 0,
                 accepted if mode == "import" else 0))
            db.execute("INSERT INTO batches(at,mode,run_id,received,new_jobs,duplicates,duration,label) VALUES(?,?,?,?,?,?,?,?)",
                       (stamp, mode, run_id, accepted, new_jobs, duplicates, duration, label))
            if checkpoint:
                self._meta(db, checkpoint[0], checkpoint[1])
        return {"events": accepted, "new_jobs": new_jobs, "duplicates": duplicates}

    def flush_events(self, chunk_size=500):
        """Single-writer outbox. Recover interrupted appends using a byte checkpoint."""
        with self.connection() as db:
            state = db.execute("SELECT seq,bytes FROM export_state WHERE id=1").fetchone()
            sequence, size = state["seq"], state["bytes"]
            actual = self.events_path.stat().st_size if self.events_path.exists() else 0
            if actual < size:
                sequence, size = 0, 0
                db.execute("UPDATE export_state SET seq=0,bytes=0 WHERE id=1")
        mode = "r+b" if self.events_path.exists() else "w+b"
        with self.events_path.open(mode) as output:
            output.truncate(size)
            output.seek(size)
            while True:
                with self.connection() as db:
                    rows = db.execute("SELECT seq,payload FROM events WHERE seq>? ORDER BY seq LIMIT ?",
                                      (sequence, chunk_size)).fetchall()
                if not rows:
                    break
                for row in rows:
                    payload = json.loads(row["payload"])
                    payload["_sequence"] = row["seq"]
                    output.write((compact(payload) + "\n").encode("utf-8"))
                output.flush()
                os.fsync(output.fileno())
                sequence, size = rows[-1]["seq"], output.tell()
                with self.connection() as db:
                    db.execute("UPDATE export_state SET seq=?,bytes=? WHERE id=1", (sequence, size))
        return sequence

    def stats(self):
        with self.connection() as db:
            result = dict(db.execute("SELECT * FROM metrics WHERE id=1").fetchone())
            exported = db.execute("SELECT seq FROM export_state WHERE id=1").fetchone()[0]
        result["missing_descriptions"] = result["unique_jobs"] - result["descriptions"]
        result["exported_sequence"] = exported
        result["disk_bytes"] = sum(path.stat().st_size for path in
            (self.db_path, Path(str(self.db_path) + "-wal"), self.events_path) if path.exists())
        return result

    def jobs_page(self, page=1, page_size=50, search="", description="All"):
        conditions, parameters = [], []
        if search.strip():
            conditions.append("(title LIKE ? OR company LIKE ? OR location LIKE ?)")
            parameters.extend(["%" + search.strip() + "%"] * 3)
        if description != "All":
            conditions.append("has_description=?")
            parameters.append(int(description == "With description"))
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        with self.connection() as db:
            count = db.execute("SELECT COUNT(*) FROM jobs" + where, parameters).fetchone()[0]
            rows = db.execute("SELECT payload FROM jobs" + where +
                " ORDER BY last_seq DESC LIMIT ? OFFSET ?",
                [*parameters, min(100, page_size), max(0, page - 1) * page_size]).fetchall()
        return [json.loads(row[0]) for row in rows], count

    def recent(self, table, limit=60):
        if table not in {"batches", "logs"}:
            raise ValueError("Unsupported table")
        with self.connection() as db:
            return [dict(row) for row in db.execute(
                f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?", (min(limit, 500),))]

    def export_unique(self, destination):
        """Stream all unique jobs to disk without constructing one large DataFrame."""
        destination = Path(destination).expanduser().resolve()
        if destination in {self.events_path, self.db_path}:
            raise ValueError("Choose a separate export filename")
        destination.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with self.connection() as db, destination.open("w", encoding="utf-8") as output:
            cursor = db.execute("SELECT payload FROM jobs ORDER BY id")
            while rows := cursor.fetchmany(500):
                for row in rows:
                    output.write(row[0] + "\n")
                count += len(rows)
        return count
