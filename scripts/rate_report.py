"""Read existing local rate measurements. This command never contacts LinkedIn."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"scraper"))
from collector import get_data_dir
from storage import Store


def report(store, hours=2):
    now=time.time()
    rows=[row for row in store.get_meta("live_rate_observations",[]) if row["started_at"]>=now-hours*3600]
    if not rows:
        return {"note":"No measured live batches yet. Leave live collection running, then run this local report again."}
    observed=[row for row in rows if row.get("http_calls") is not None]
    http_calls=sum(row["http_calls"] for row in observed)
    http_429=sum(row.get("http_429") or 0 for row in observed)
    limited=sum(bool(row.get("rate_limited")) for row in rows)
    errors=sum(bool(row.get("source_error")) for row in rows)
    started=min(row["started_at"] for row in rows)
    seconds=max(1,now-started)
    enough=len(rows)>=10 and seconds>=3600
    if limited:
        advice="Rate limits were observed. Keep or increase the minimum interval; do not accelerate this window."
    elif not enough:
        advice="Insufficient evidence of a stable rate. Collect at least 10 batches across an hour or longer."
    elif errors:
        advice="No measured 429s, but other source errors occurred. Inspect those before changing the interval."
    else:
        advice="This observed window was clean. Maintaining it is reasonable; any lower interval needs a new measured window, not a guarantee."
    return {"window_started_utc":datetime.fromtimestamp(started,timezone.utc).isoformat(),
        "window_minutes":round(seconds/60,1),"batches":len(rows),"unmeasured_batches":len(rows)-len(observed),
        "measured_http_calls":http_calls,"measured_http_429_responses":http_429,
        "measured_http_429_percent":round(100*http_429/http_calls,2) if http_calls else None,
        "limited_batches":limited,"source_error_batches":errors,
        "received_events":sum(row["events"] for row in rows),
        "new_unique_jobs":sum(row["new_jobs"] for row in rows),
        "new_unique_jobs_per_hour":round(sum(row["new_jobs"] for row in rows)*3600/seconds,1),
        "adaptive_policy":store.get_meta("adaptive_live_rate_v1",{}),"assessment":advice}


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir",default=str(get_data_dir()))
    parser.add_argument("--hours",type=float,default=2)
    args=parser.parse_args()
    if args.hours<=0:
        parser.error("--hours must be positive")
    print(json.dumps(report(Store(args.data_dir),args.hours),indent=2,ensure_ascii=False))
