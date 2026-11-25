from datetime import datetime, timezone
from pathlib import Path

from jobspy import scrape_jobs


# 1. Collect jobs using your internet connection
jobs = scrape_jobs(
    site_name=["linkedin"],
    search_term="Data Engineer",
    location="United States",
    results_wanted=5,
    hours_old=72,
    fetch_description=True,
    proxies=None,
    verbose=2,
)

# 2. Display the results
print(f"\nCollected {len(jobs)} jobs.")

if jobs.empty:
    print("No jobs returned.")
else:
    print(
        jobs[["title", "company", "location"]]
        .to_string(index=False)
    )

    # 3. Add the collection time in UTC
    jobs["timestamp"] = datetime.now(timezone.utc).isoformat()

    # 4. Count jobs with missing descriptions
    missing_description = (
        jobs["description"]
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("")
    )

    print(
        f"\nJobs missing description: "
        f"{missing_description.sum()}"
    )

    # 5. Create the output folder beside this script
    output_dir = Path(__file__).resolve().parent / "data"
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / "jobs.jsonl"

    # 6. Save one job per JSON line
    # Each run replaces the previous file
    jobs.to_json(
        output_file,
        orient="records",
        lines=True,
        date_format="iso",
        force_ascii=False,
    )

    print(f"\nSaved to: {output_file}")