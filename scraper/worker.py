"""Independent continuous collector. Only this process writes the JSONL outbox."""

import argparse
import hashlib
import itertools
import json
from pathlib import Path
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time

from collector import ROOT, file_fingerprint, get_config, migrate_legacy, validate_config, worker_lock
from storage import Store, compact, utc_now


class StopRequested(Exception):
    pass


class Worker:
    def __init__(self, store, run_id):
        self.store, self.run_id = store, run_id
        self.state = {
            "status": "starting", "run_id": run_id, "pid": __import__("os").getpid(),
            "started_at": time.time(), "events": 0, "new_jobs": 0, "duplicates": 0,
        }
        self.stopping = False
        self.last_control_check = 0.0

    def update(self, **values):
        self.state.update(values)
        self.state["heartbeat"] = time.time()
        self.store.set_meta("runtime", self.state)

    def check_stop(self):
        if self.stopping:
            raise StopRequested()
        now = time.monotonic()
        if now - self.last_control_check >= 0.1:
            self.last_control_check = now
            if self.store.get_meta("stop_requested", False):
                raise StopRequested()

    def pause(self, seconds):
        deadline = time.monotonic() + max(0, seconds)
        self.update(next_run_at=time.time() + max(0, seconds))
        while time.monotonic() < deadline:
            self.check_stop()
            if time.time() - self.state.get("heartbeat", 0) >= 3:
                self.update()
            time.sleep(min(0.25, max(0, deadline - time.monotonic())))
        self.check_stop()

    def accept(self, records, mode, *, label="", duration=0, checkpoint=None):
        if shutil.disk_usage(self.store.data_dir).free < 256 * 1024 * 1024:
            raise OSError("Less than 256 MB of free disk space remains. Free space, then restart collection.")
        result = self.store.ingest(records, mode=mode, run_id=self.run_id, label=label,
                                   duration=duration, checkpoint=checkpoint)
        self.store.flush_events()
        for key in ("events", "new_jobs", "duplicates"):
            self.state[key] += result[key]
        self.update(last_batch_at=utc_now())
        return result

    def request(self, settings):
        """A cancellable child prevents a slow HTTP request from blocking Stop."""
        with tempfile.TemporaryDirectory(prefix="request-", dir=self.store.data_dir) as directory:
            request_file = Path(directory) / "request.json"
            output_file = Path(directory) / "response.json"
            request_file.write_text(compact(settings), encoding="utf-8")
            process = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "fetch", "--request", str(request_file), "--output", str(output_file)],
                cwd=ROOT, stdin=subprocess.DEVNULL,
            )
            started = time.monotonic()
            timeout = 900 if settings["fetch_description"] else 120
            try:
                while process.poll() is None:
                    self.check_stop()
                    if time.monotonic() - started > timeout:
                        raise TimeoutError("The source request exceeded its time limit.")
                    if time.time() - self.state.get("heartbeat", 0) >= 3:
                        self.update()
                    time.sleep(0.2)
                if not output_file.exists():
                    raise RuntimeError(f"Request process exited with code {process.returncode}.")
                result = json.loads(output_file.read_text(encoding="utf-8"))
                if result.get("error"):
                    raise RuntimeError(result["error"])
                return result
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=3)

    def live(self, config):
        combinations = list(itertools.product(config["keywords"], config["locations"]))
        rotation_key = "rotation:" + hashlib.sha256(compact(combinations).encode()).hexdigest()[:20]
        rotation = int(self.store.get_meta(rotation_key, 0))
        failures = 0
        while True:
            self.check_stop()
            remaining = float(self.store.get_meta("source_cooldown_until", 0)) - time.time()
            if remaining > 0:
                self.update(status="backoff", message="Resuming the saved source cooldown")
                self.pause(remaining)
            keyword, location = combinations[rotation % len(combinations)]
            cursor_key = "query:" + hashlib.sha256(compact([keyword, location, config["hours_old"], config["batch_size"]]).encode()).hexdigest()[:24]
            offset = int(self.store.get_meta(cursor_key, 0))
            self.update(status="collecting", keyword=keyword, location=location, offset=offset,
                        next_run_at=None, message=f"Collecting {keyword} · {location}")
            started = time.monotonic()
            try:
                response = self.request({**config, "keyword": keyword, "location": location, "offset": offset})
                records = response.get("records", [])
                next_offset = offset + config["batch_size"] if len(records) >= config["batch_size"] else 0
                if next_offset >= config["pages_per_query"] * config["batch_size"]:
                    next_offset = 0
                result = self.accept(records, "live", label=f"{keyword} · {location}",
                    duration=time.monotonic() - started, checkpoint=(cursor_key, next_offset))
                rotation += 1
                self.store.set_meta(rotation_key, rotation)
                self.store.log("INFO", f"{keyword} / {location}: {result['events']} events, {result['new_jobs']} new unique jobs.")
                if response.get("rate_limited"):
                    failures += 1
                    delay = min(3600, 300 * 2 ** min(failures - 1, 4)) + random.uniform(0, 15)
                    self.store.set_meta("source_cooldown_until", time.time() + delay)
                    self.store.log("WARNING", f"LinkedIn returned 429. Cooling down for {int(delay)} seconds.")
                    self.update(status="backoff", message="Source rate limit · waiting before the next request")
                elif response.get("errors"):
                    raise RuntimeError(compact(response["errors"]))
                else:
                    failures = 0
                    delay = config["interval_seconds"]
                    for source, warnings in response.get("warnings", {}).items():
                        self.store.log("WARNING", f"{source}: {warnings}")
                    self.update(status="waiting", message="Batch saved · waiting for the next query")
            except StopRequested:
                raise
            except OSError:
                raise
            except Exception as error:
                failures += 1
                delay = min(3600, max(120, config["interval_seconds"]) * 2 ** min(failures - 1, 4))
                self.store.log("ERROR", f"Source request failed: {error}. Retry in {int(delay)} seconds.")
                self.update(status="backoff", message="Source unavailable · retry scheduled", last_error=str(error))
            self.pause(delay)

    def replay(self, config):
        path = Path(config["replay_path"])
        fingerprint = file_fingerprint(path)
        if path.resolve() in {self.store.events_path.resolve(), self.store.db_path.resolve()}:
            raise ValueError("Replay a separate dataset, not the active event archive or database.")
        key = "replay:" + fingerprint
        progress = self.store.get_meta(key, {"offset": 0, "cycle": 0})
        if progress["offset"] >= path.stat().st_size:
            progress = {"offset": 0, "cycle": progress["cycle"] + 1}
            self.store.set_meta(key, progress)
        batch_size = min(500, config["replay_rate"])
        self.update(status="replaying", message="Replaying a local dataset · these are not new live jobs")
        with path.open("rb") as source:
            source.seek(min(progress["offset"], path.stat().st_size))
            valid_in_cycle = 0
            while True:
                self.check_stop()
                started = time.monotonic()
                records, invalid = [], 0
                eof = False
                while len(records) < batch_size:
                    self.check_stop()
                    position = source.tell()
                    line = source.readline(2 * 1024 * 1024 + 1)
                    if not line:
                        eof = True
                        break
                    if len(line) > 2 * 1024 * 1024:
                        while line and not line.endswith(b"\n"):
                            self.check_stop()
                            line = source.readline(2 * 1024 * 1024 + 1)
                        invalid += 1
                        continue
                    if not line.strip():
                        continue
                    try:
                        def invalid_constant(value):
                            raise ValueError(f"Invalid JSON constant: {value}")
                        record = json.loads(line, parse_constant=invalid_constant)
                        if not isinstance(record, dict):
                            raise ValueError("Expected one object per line")
                        record["_event_id"] = f"replay:{fingerprint}:{progress['cycle']}:{position}"
                        records.append(record)
                    except (ValueError, UnicodeDecodeError):
                        invalid += 1
                progress["offset"] = source.tell()
                if records:
                    self.accept(records, "replay", label=path.name, duration=time.monotonic() - started,
                                checkpoint=(key, dict(progress)))
                    valid_in_cycle += len(records)
                else:
                    self.store.set_meta(key, dict(progress))
                if invalid:
                    self.store.log("WARNING", f"Skipped {invalid} invalid JSONL records.")
                if records:
                    self.pause(max(0, len(records) / config["replay_rate"] - (time.monotonic() - started)))
                if eof:
                    if valid_in_cycle == 0:
                        raise ValueError("No valid JSON objects remained in this dataset cycle.")
                    if not config["replay_repeat"]:
                        self.update(message="Dataset replay completed")
                        return
                    progress = {"offset": 0, "cycle": progress["cycle"] + 1}
                    self.store.set_meta(key, progress)
                    source.seek(0)
                    valid_in_cycle = 0

    def run(self):
        with worker_lock(self.store.data_dir):
            try:
                config = validate_config(get_config(self.store))
                self.state["mode"] = config["mode"]
                self.update(status="importing", message="Checking saved data and pending events")
                migrate_legacy(self.store)
                self.store.flush_events()
                self.store.log("INFO", f"Started {config['mode']} collection · run {self.run_id}.")
                if config["mode"] == "live":
                    self.live(config)
                else:
                    self.replay(config)
                self.update(status="completed", ended_at=time.time(), next_run_at=None)
            except StopRequested:
                self.update(status="stopping", message="Saving progress…")
                self.store.flush_events()
                self.update(status="stopped", ended_at=time.time(), next_run_at=None, message="Collector stopped · progress saved")
                self.store.log("INFO", "Collector stopped cleanly.")
            except Exception as error:
                self.store.log("ERROR", str(error))
                self.update(status="error", ended_at=time.time(), next_run_at=None, message=str(error))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["run", "fetch"])
    parser.add_argument("--data-dir")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--request")
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.command == "fetch":
        from scraper import fetch_jobs
        try:
            result = fetch_jobs(json.loads(Path(args.request).read_text()))
        except Exception as error:
            result = {"error": str(error)}
        Path(args.output).write_text(compact(result), encoding="utf-8")
        return
    worker = Worker(Store(args.data_dir), args.run_id)
    def stop_signal(*_):
        worker.stopping = True
    signal.signal(signal.SIGTERM, stop_signal)
    signal.signal(signal.SIGINT, stop_signal)
    try:
        worker.run()
    except BlockingIOError:
        print("A collector is already running; duplicate worker was not started.")


if __name__ == "__main__":
    main()
