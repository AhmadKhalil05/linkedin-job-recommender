"""Configuration, single-worker lifecycle, and migration of the previous local file."""

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

from storage import Store, utc_now


ROOT = Path(__file__).resolve().parent
DEFAULTS = {
    "mode": "live", "keywords": ["Data Engineer", "Software Engineer", "Machine Learning Engineer"],
    "locations": ["United States"], "batch_size": 25, "interval_seconds": 120,
    "hours_old": 168, "fetch_description": False, "pages_per_query": 20,
    "replay_path": str(ROOT / "data" / "jobs.jsonl"), "replay_rate": 250,
    "replay_repeat": True,
}


def get_data_dir():
    return Path(os.getenv("JOBSTREAM_DATA_DIR", str(ROOT / "data"))).expanduser().resolve()


@contextmanager
def worker_lock(data_dir, name="collector.lock"):
    path = Path(data_dir) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def worker_alive(data_dir):
    try:
        with worker_lock(data_dir):
            return False
    except BlockingIOError:
        return True


def get_config(store):
    return {**DEFAULTS, **(store.get_meta("config", {}) or {})}


def validate_config(config):
    config = {**DEFAULTS, **config}
    if config["mode"] not in {"live", "replay"}:
        raise ValueError("Choose Live LinkedIn or Dataset replay.")
    config["keywords"] = list(dict.fromkeys(str(x).strip() for x in config["keywords"] if str(x).strip()))
    config["locations"] = list(dict.fromkeys(str(x).strip() for x in config["locations"] if str(x).strip()))
    if config["mode"] == "live" and (not config["keywords"] or not config["locations"]):
        raise ValueError("Enter at least one keyword and one location.")
    if len(config["keywords"]) * len(config["locations"]) > 100:
        raise ValueError("Use at most 100 keyword/location combinations per collection plan.")
    for name, low, high in [("batch_size", 1, 25), ("interval_seconds", 60, 86400),
                            ("hours_old", 1, 720), ("pages_per_query", 1, 40),
                            ("replay_rate", 1, 5000)]:
        config[name] = int(config[name])
        if not low <= config[name] <= high:
            raise ValueError(f"{name} must be between {low} and {high}.")
    if config["mode"] == "replay":
        path = Path(config["replay_path"]).expanduser().resolve()
        if not path.is_file() or path.stat().st_size == 0:
            raise ValueError("Choose an existing, nonempty JSONL dataset on this computer.")
        config["replay_path"] = str(path)
    return config


def file_fingerprint(path):
    path = Path(path).resolve()
    info = path.stat()
    return hashlib.sha256(f"{path}:{info.st_size}:{info.st_mtime_ns}".encode()).hexdigest()[:24]


def migrate_legacy(store):
    """Caller owns the worker lock. Resume imports at a committed byte offset."""
    path = store.data_dir / "jobs.jsonl"
    if not path.is_file() or path.stat().st_size == 0:
        return
    fingerprint = file_fingerprint(path)
    key = "legacy:" + fingerprint
    state = store.get_meta(key, {"offset": 0, "done": False})
    if state.get("done"):
        return
    batch = []
    with path.open("rb") as source:
        source.seek(state["offset"])
        while True:
            position = source.tell()
            line = source.readline(2 * 1024 * 1024 + 1)
            if not line:
                break
            if len(line) > 2 * 1024 * 1024:
                while line and not line.endswith(b"\n"):
                    line = source.readline(2 * 1024 * 1024 + 1)
                continue
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    continue
            except (ValueError, UnicodeDecodeError):
                continue
            record["_event_id"] = f"import:{fingerprint}:{position}"
            batch.append(record)
            if len(batch) >= 500:
                store.ingest(batch, mode="import", run_id="legacy-import", label="Previous saved jobs",
                             checkpoint=(key, {"offset": source.tell(), "done": False}))
                batch = []
        if batch:
            store.ingest(batch, mode="import", run_id="legacy-import", label="Previous saved jobs",
                         checkpoint=(key, {"offset": source.tell(), "done": False}))
    store.flush_events()
    store.set_meta(key, {"offset": path.stat().st_size, "done": True})
    store.log("INFO", "Imported the previous jobs.jsonl file; the original file was preserved.")


def prepare_store(data_dir=None):
    store = Store(data_dir or get_data_dir())
    try:
        with worker_lock(store.data_dir):
            migrate_legacy(store)
    except BlockingIOError:
        pass
    return store


def status(store):
    state = store.get_meta("runtime", {}) or {}
    alive = worker_alive(store.data_dir)
    starting = state.get("status") == "starting" and time.time() - state.get("heartbeat", 0) < 30
    state["alive"] = alive or starting
    if state["alive"] and store.get_meta("stop_requested", False):
        state["status"] = "stopping"
        state["message"] = "Stopping the active request and saving progress…"
    if not state.get("status"):
        state["status"] = "idle"
    elif not state["alive"] and state["status"] in {"collecting", "replaying", "waiting", "backoff", "starting", "stopping", "importing"}:
        state["status"] = "interrupted"
        state["message"] = "The worker stopped unexpectedly. Start again to resume saved progress."
    return state


def start_collector(store, config=None):
    with worker_lock(store.data_dir, "launcher.lock"):
        if status(store)["alive"]:
            return False
        config = validate_config(config or get_config(store))
        store.set_meta("config", config)
        store.set_meta("stop_requested", False)
        run_id = uuid.uuid4().hex[:16]
        store.set_meta("runtime", {
            "status": "starting", "mode": config["mode"], "run_id": run_id,
            "started_at": time.time(), "heartbeat": time.time(),
            "message": "Starting background collector…", "events": 0, "new_jobs": 0,
            "duplicates": 0, "next_run_at": None,
        })
        try:
            with (store.data_dir / "worker.log").open("ab") as log:
                process = subprocess.Popen(
                    [sys.executable, str(ROOT / "worker.py"), "run", "--data-dir", str(store.data_dir), "--run-id", run_id],
                    cwd=ROOT, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            # Reap the detached process when it exits; never call Streamlit from this thread.
            threading.Thread(target=process.wait, daemon=True).start()
        except Exception:
            store.set_meta("runtime", {"status": "error", "message": "Could not start the worker."})
            raise
        return True


def stop_collector(store):
    store.set_meta("stop_requested", True)
    store.log("INFO", "Stop requested from the dashboard.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Control the local job collector")
    parser.add_argument("command", choices=["start", "stop", "status", "export"])
    parser.add_argument("--data-dir", default=str(get_data_dir()))
    parser.add_argument("--output", default="unique_jobs.jsonl")
    args = parser.parse_args()
    store = prepare_store(args.data_dir)
    if args.command == "start":
        print("Started" if start_collector(store) else "Already running")
    elif args.command == "stop":
        stop_collector(store)
        print("Stop requested")
    elif args.command == "status":
        print(json.dumps({"worker": status(store), "totals": store.stats()}, indent=2))
    else:
        print(f"Exported {store.export_unique(args.output):,} unique jobs to {Path(args.output).resolve()}")
