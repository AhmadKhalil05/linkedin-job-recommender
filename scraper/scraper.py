"""One LinkedIn request. No UI state and no implicit execution on import."""

import json


def fetch_jobs(request):
    # Import in the request subprocess, keeping the persistent worker lightweight.
    from jobspy import scrape_jobs
    jobs = scrape_jobs(
        site_name=["linkedin"], search_term=request["keyword"],
        location=request["location"], results_wanted=request["batch_size"],
        offset=request["offset"], hours_old=request["hours_old"],
        fetch_description=request["fetch_description"], proxies=None, verbose=2,
    )
    records = json.loads(jobs.to_json(orient="records", date_format="iso", force_ascii=False))
    for record in records:
        record["search_keyword"] = request["keyword"]
        record["target_location"] = request["location"]
    errors = jobs.attrs.get("errors", {}) or {}
    warnings = jobs.attrs.get("warnings", {}) or {}
    messages = json.dumps([errors, warnings]).lower()
    return {
        "records": records, "errors": errors, "warnings": warnings,
        "rate_limited": "429" in messages or "too many requests" in messages,
    }


if __name__ == "__main__":
    print("Open the dashboard with: python -m streamlit run scraper/app.py")
