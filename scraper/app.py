"""Native, idiomatic Streamlit dashboard adhering strictly to Streamlit's design standards."""

import html
import json
import math
import time

import pandas as pd
import streamlit as st

from collector import get_config, get_data_dir, prepare_store, start_collector, status, stop_collector, validate_config


LINKEDIN_LOGO_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="32" height="32" fill="#0a66c2">
  <path d="M19 3a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h14m-.5 15.5v-5.3a3.26 3.26 0 0 0-3.26-3.26c-.85 0-1.84.52-2.28 1.3v-1.11h-2.79v8.37h2.79v-4.93c0-.77.62-1.4 1.39-1.4a1.4 1.4 0 0 1 1.4 1.4v4.93h2.75M6.88 8.56a1.68 1.68 0 0 0 1.68-1.68c0-.93-.75-1.69-1.68-1.69a1.69 1.69 0 0 0-1.69 1.69c0 .93.76 1.68 1.69 1.68m1.39 9.94v-8.37H5.5v8.37h2.77z"/>
</svg>"""

st.set_page_config(
    page_title="LinkedIn Job Streamer",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def open_store(path):
    return prepare_store(path)


store = open_store(str(get_data_dir()))
settings = get_config(store)
current = status(store)
st.session_state["worker_was_alive"] = current["alive"]

# Sidebar - Pure native Streamlit
with st.sidebar:
    st.markdown(
        f'''<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;">
            {LINKEDIN_LOGO_SVG}
            <span style="font-size:1.2rem;font-weight:700;">LinkedIn Streamer</span>
        </div>''',
        unsafe_allow_html=True,
    )
    st.caption("Data Ingestion Pipeline")
    st.divider()

    st.subheader("Collection Settings")
    with st.form("settings", enter_to_submit=False):
        mode = st.selectbox(
            "Source",
            ["live", "replay"],
            index=0 if settings["mode"] == "live" else 1,
            format_func=lambda x: "Live LinkedIn" if x == "live" else "Dataset replay",
            disabled=current["alive"],
            key="mode",
        )
        with st.expander("Live search", expanded=settings["mode"] == "live"):
            keywords = st.text_area(
                "Search keywords (one per line)",
                "\n".join(settings["keywords"]),
                height=110,
                disabled=current["alive"],
                key="keywords",
            )
            locations = st.text_area(
                "Locations (one per line)",
                "\n".join(settings["locations"]),
                height=80,
                disabled=current["alive"],
                key="locations",
            )
            batch_size = st.number_input(
                "Jobs per request",
                1,
                25,
                settings["batch_size"],
                disabled=current["alive"],
                key="batch_size",
            )
            interval = st.number_input(
                "Pause between requests (seconds)",
                10,
                86400,
                settings["interval_seconds"],
                step=10,
                disabled=current["alive"],
                key="interval",
            )
            hours = st.selectbox(
                "Publication age",
                [24, 72, 168, 720],
                index=[24, 72, 168, 720].index(settings["hours_old"]) if settings["hours_old"] in [24, 72, 168, 720] else 2,
                format_func=lambda x: {24: "Past 24 hours", 72: "Past 3 days", 168: "Past 7 days", 720: "Past 30 days"}[x],
                disabled=current["alive"],
                key="hours",
            )
            descriptions = st.checkbox(
                "Fetch full descriptions",
                settings["fetch_description"],
                disabled=current["alive"],
                key="descriptions",
                help="Adds a request for each job. Leave off for initial bulk collection.",
            )
            pages = st.number_input(
                "Pages per query cycle",
                1,
                40,
                settings["pages_per_query"],
                disabled=current["alive"],
                key="pages",
                help="The collector advances through pages, rotates queries, then checks again for fresh postings.",
            )
        with st.expander("Replay dataset", expanded=settings["mode"] == "replay"):
            replay_path = st.text_input(
                "Local JSONL file",
                settings["replay_path"],
                disabled=current["alive"],
                key="replay_path",
            )
            replay_rate = st.number_input(
                "Target events / second",
                1,
                5000,
                settings["replay_rate"],
                disabled=current["alive"],
                key="replay_rate",
            )
            repeat = st.checkbox(
                "Repeat dataset continuously",
                settings["replay_repeat"],
                disabled=current["alive"],
                key="repeat",
            )
            st.caption("Replayed records are labeled. Repeating a dataset increases event count, not unique jobs.")
        saved = st.form_submit_button("Save settings", use_container_width=True, disabled=current["alive"])
    if saved:
        try:
            candidate = validate_config({
                "mode": mode,
                "keywords": keywords.splitlines(),
                "locations": locations.splitlines(),
                "batch_size": batch_size,
                "interval_seconds": interval,
                "hours_old": hours,
                "fetch_description": descriptions,
                "pages_per_query": pages,
                "replay_path": replay_path,
                "replay_rate": replay_rate,
                "replay_repeat": repeat,
            })
            store.set_meta("config", candidate)
            st.success("Settings saved. Press Start collection.")
        except (ValueError, TypeError) as error:
            st.error(str(error))
    if current["alive"]:
        st.caption("Stop the collector to edit its settings.")
    st.divider()
    st.caption("The collector runs independently of this browser tab. Your computer must remain awake.")

# Main Header - Native Streamlit typography with LinkedIn logo
st.markdown(
    f'''<div style="display:flex;align-items:center;gap:12px;margin-bottom:2px;">
        {LINKEDIN_LOGO_SVG}
        <h1 style="margin:0;padding:0;font-size:2.2rem;font-weight:700;">LinkedIn Job Streamer</h1>
    </div>''',
    unsafe_allow_html=True,
)
st.caption("Real-time data ingestion and deduplication pipeline feeding Kafka and recommendation services.")
st.divider()


def readable_size(value):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}"
        value /= 1024


@st.fragment(run_every=2)
def dashboard():
    state = status(store)
    if state["alive"] != st.session_state.get("worker_was_alive"):
        st.rerun()
    totals = store.stats()
    label = state["status"].replace("_", " ").upper()
    message = state.get("message", "Ready when you are. Save a collection plan, then press Start.")
    next_run = state.get("next_run_at")
    if next_run and state["alive"]:
        message += f" - next batch in {max(0, math.ceil(next_run - time.time()))}s"
    active_mode = state.get("mode", get_config(store)["mode"])
    source_name = "Live LinkedIn" if active_mode == "live" else "Dataset Replay"

    # Status Row
    info_col, state_col = st.columns([3, 1])
    with info_col:
        st.write(f"**Source:** {source_name} &nbsp;·&nbsp; {message}")
    with state_col:
        st.write(f"**Status:** `{label}`")

    # Controls Row
    start_col, stop_col, stats_col = st.columns([1.2, 1.2, 2.6])
    if start_col.button("Start Collection", type="primary", disabled=state["alive"], use_container_width=True, key="start"):
        try:
            start_collector(store)
            st.rerun()
        except (ValueError, OSError) as error:
            st.error(str(error))
    if stop_col.button("Stop", type="secondary", disabled=not state["alive"] or state["status"] == "stopping", use_container_width=True, key="stop"):
        stop_collector(store)
        st.rerun()

    elapsed = max(1, state.get("ended_at", time.time()) - state.get("started_at", time.time()))
    measured_rate = state.get("events", 0) / elapsed
    with stats_col:
        st.write(f"Run rate: **{measured_rate:,.1f} events/s** &nbsp;|&nbsp; Disk usage: **{readable_size(totals['disk_bytes'])}**")

    st.write("")

    # Native Metrics Grid
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Unique Jobs", f"{totals['unique_jobs']:,}")
    m2.metric("Events Captured", f"{totals['events']:,}")
    m3.metric("Repeat Observations", f"{totals['duplicates']:,}")
    m4.metric("Missing Descriptions", f"{totals['missing_descriptions']:,}")

    st.caption(f"Live events: **{totals['live_events']:,}** · Replay events: **{totals['replay_events']:,}** · Imported events: **{totals['imported_events']:,}**")

    st.divider()

    overview, explorer, activity = st.tabs(["Overview", "Job Explorer", "Activity Log"])
    with overview:
        chart_col, run_col = st.columns([2, 1])
        with chart_col:
            st.subheader("Collection Activity")
            st.caption("Recent batches (events received vs new unique jobs)")
            batches = store.recent("batches", 60)
            if batches:
                frame = pd.DataFrame(batches[::-1])
                frame["at"] = pd.to_datetime(frame["at"], utc=True)
                frame = frame.set_index("at")[["received", "new_jobs"]].rename(columns={"received": "Events", "new_jobs": "New jobs"})
                st.line_chart(frame, height=250)
            else:
                st.info("No batch activity recorded yet.")
        with run_col:
            st.subheader("Current Run")
            st.metric("New Unique Jobs", f"{state.get('new_jobs', 0):,}")
            st.metric("Events Received", f"{state.get('events', 0):,}")
            st.caption("Live collection pauses between requests and backs off automatically when the source limits traffic.")

        if active_mode == "replay":
            st.info("Dataset replay is active. These events come from a local file.")

        st.subheader("Latest Jobs")
        rows, _ = store.jobs_page(page_size=8)
        show_jobs(rows, details=False)

        with st.expander("Data files and export"):
            st.write("Every event is saved to an append-only JSONL archive. Unique jobs are indexed separately.")
            st.code(str(store.events_path))
            st.caption(f"Archive synced through sequence {totals['exported_sequence']:,}.")
            st.write("Export the complete unique-job collection to disk:")
            st.code('python scraper/collector.py export --output unique_jobs.jsonl', language="bash")

    with explorer:
        f_col, d_col, s_col = st.columns([2, 1.4, 1])
        query = f_col.text_input("Search collected jobs", key="job_search", placeholder="Title, company, or location")
        description_filter = d_col.selectbox("Description", ["All", "With description", "Missing description"], key="description_filter")
        page_size = s_col.selectbox("Rows per page", [25, 50, 100], index=1, key="page_size")
        page = st.number_input("Page", min_value=1, value=1, step=1, key="job_page")
        rows, count = store.jobs_page(int(page), int(page_size), query, description_filter)
        st.caption(f"{count:,} matching unique jobs - page {page:,} of {max(1, math.ceil(count / page_size)):,}")
        show_jobs(rows, details=True)

    with activity:
        st.subheader("Worker Activity")
        logs = store.recent("logs", 100)
        if logs:
            st.dataframe(
                pd.DataFrame(logs)[["at", "level", "message"]],
                hide_index=True,
                use_container_width=True,
                height=450,
                column_config={
                    "at": "Time (UTC)",
                    "level": "Level",
                    "message": st.column_config.TextColumn("Message", width="large"),
                },
            )
        else:
            st.info("No worker activity logs recorded yet.")


def show_jobs(rows, details):
    if not rows:
        st.info("No jobs to display yet.")
        return
    frame = pd.DataFrame(rows)
    columns = [key for key in ["title", "company", "location", "_ingestion_mode", "job_type", "is_remote", "job_url"] if key in frame.columns]
    st.dataframe(
        frame[columns],
        hide_index=True,
        use_container_width=True,
        height=min(420, 40 + 36 * len(rows)),
        column_config={
            "title": st.column_config.TextColumn("Job title", width="large"),
            "company": "Company",
            "location": "Location",
            "_ingestion_mode": "Source",
            "job_type": "Type",
            "is_remote": st.column_config.CheckboxColumn("Remote"),
            "job_url": st.column_config.LinkColumn("Listing", display_text="View job"),
        },
    )
    if details:
        left, right = st.columns(2)
        payload = "".join(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n" for row in rows)
        left.download_button(
            "Download this page (JSONL)",
            payload.encode(),
            "jobs_page.jsonl",
            "application/x-ndjson",
            use_container_width=True,
            key="json_download",
        )
        right.download_button(
            "Download this page (CSV)",
            frame.to_csv(index=False).encode("utf-8-sig"),
            "jobs_page.csv",
            "text/csv",
            use_container_width=True,
            key="csv_download",
        )
        with st.expander("Read a job description"):
            index = st.selectbox(
                "Job",
                list(range(len(rows))),
                format_func=lambda i: f"{rows[i].get('title', 'Untitled')} - {rows[i].get('company', '')}",
                key="job_detail",
            )
            description = rows[index].get("description")
            if description:
                st.markdown(str(description))
            else:
                st.info("The source did not provide a description for this observation.")


dashboard()
