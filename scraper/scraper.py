"""One LinkedIn request. No UI state and no implicit execution on import."""

import json
from importlib.metadata import version
from unittest.mock import patch

from rate_control import HttpObserver, PAGE_DEFERRED


def fetch_jobs(request):
    # Import in the request subprocess, keeping the persistent worker lightweight.
    from jobspy import scrape_jobs
    import jobspy.linkedin as linkedin
    if version("python-jobspy") != "1.5.0":
        raise RuntimeError("This measured transport supports python-jobspy==1.5.0. Install the pinned requirements.")
    limit = min(10, int(request["batch_size"]))
    observer = HttpObserver()
    original_create = linkedin.create_session
    def measured_session(*args, **kwargs):
        return observer.attach(original_create(*args, **kwargs))
    # The hook lives only inside this dedicated request process and adds pacing/measurement.
    with patch.object(linkedin, "create_session", measured_session):
        jobs = scrape_jobs(
            site_name=["linkedin"], search_term=request["keyword"],
            location=request["location"], results_wanted=limit,
            offset=request["offset"], hours_old=request["hours_old"],
            fetch_description=request["fetch_description"], proxies=None, verbose=2,
        )
    records = json.loads(jobs.to_json(orient="records", date_format="iso", force_ascii=False))
    for record in records:
        record["search_keyword"] = request["keyword"]
        record["target_location"] = request["location"]
    errors = {key:value for key,value in (jobs.attrs.get("errors", {}) or {}).items()
              if str(value) != PAGE_DEFERRED}
    warnings = jobs.attrs.get("warnings", {}) or {}
    messages = json.dumps([errors, warnings]).lower()
    return {
        "records": records, "errors": errors, "warnings": warnings,
        "rate_limited": observer.limited or "429" in messages or "too many requests" in messages,
        "effective_batch_size": limit, **observer.summary(),
    }


if __name__ == "__main__":
    print("Open the dashboard with: python -m streamlit run scraper/app.py")
