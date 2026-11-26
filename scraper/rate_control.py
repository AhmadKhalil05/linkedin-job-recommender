"""Measured HTTP pacing and persistent, conservative live-source scheduling."""

from dataclasses import dataclass, field
from datetime import timezone
from email.utils import parsedate_to_datetime
import math
import time
from urllib.parse import urlsplit


PAGE_DEFERRED = "Local search-page budget reached; continue in the next scheduled batch"
RATE_DEFERRED = "Further HTTP calls deferred after an observed 429"


def retry_after_seconds(value, now=None):
    if value is None:
        return 0
    now = time.time() if now is None else now
    try:
        return max(0, math.ceil(float(str(value).strip())))
    except (ValueError, OverflowError):
        try:
            date = parsedate_to_datetime(str(value))
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            return max(0, math.ceil(date.timestamp() - now))
        except (ValueError, TypeError, OverflowError):
            return 0


@dataclass
class HttpObserver:
    """Scoped to one JobSpy request subprocess; not a global network patch."""
    min_gap: float = 30.0
    clock: object = time.monotonic
    sleeper: object = time.sleep
    wall_clock: object = time.time
    calls: int = 0
    search_calls: int = 0
    search_cards: int = 0
    search_succeeded: bool = False
    limited: bool = False
    retry_after: int = 0
    budget_deferred: bool = False
    next_send_at: float = 0
    status_counts: dict = field(default_factory=dict)
    first_http_at: float | None = None
    last_http_at: float | None = None
    _responses: list = field(default_factory=list)

    def attach(self, session):
        if not callable(getattr(session, "send", None)):
            raise RuntimeError("Unsupported JobSpy transport; keep python-jobspy pinned to 1.5.0.")
        original_request, original_send = session.request, session.send

        def guarded_request(method, url, *args, **kwargs):
            if self.limited:
                raise ValueError(RATE_DEFERRED)
            if "/jobs-guest/jobs/api/seeMoreJobPostings/search" in urlsplit(str(url)).path:
                if self.search_calls >= 1:
                    self.budget_deferred = True
                    raise ValueError(PAGE_DEFERRED)
                self.search_calls += 1
            return original_request(method, url, *args, **kwargs)

        def guarded_send(request, **kwargs):
            if self.limited:
                raise ValueError(RATE_DEFERRED)
            remaining = self.next_send_at - self.clock()
            if remaining > 0:
                self.sleeper(remaining)
            self.next_send_at = self.clock() + self.min_gap
            self.calls += 1
            now = self.wall_clock()
            if self.first_http_at is None:
                self.first_http_at = now
            self.last_http_at = now
            response = original_send(request, **kwargs)
            # Redirect handling can return the same response through nested send calls.
            if not any(response is old for old in self._responses):
                self._responses.append(response)
                code = str(response.status_code)
                self.status_counts[code] = self.status_counts.get(code, 0) + 1
                if response.status_code == 429:
                    self.limited = True
                    self.retry_after = max(self.retry_after,
                        retry_after_seconds(response.headers.get("Retry-After"), now=self.wall_clock()))
                response_request = getattr(response, "request", None)
                url = getattr(response_request, "url", None) or request.url
                if "/jobs-guest/jobs/api/seeMoreJobPostings/search" in urlsplit(str(url)).path and response.status_code == 200:
                    from bs4 import BeautifulSoup
                    self.search_succeeded = True
                    self.search_cards = len(BeautifulSoup(response.text, "html.parser").find_all(class_="base-search-card"))
            return response

        session.request, session.send = guarded_request, guarded_send
        return session

    def summary(self):
        return {"http_calls":self.calls,"http_status_counts":self.status_counts,
                "http_429":self.status_counts.get("429",0),"retry_after_seconds":self.retry_after,
                "search_cards":self.search_cards,"search_succeeded":self.search_succeeded,
                "budget_deferred":self.budget_deferred,"first_http_at":self.first_http_at,
                "last_http_at":self.last_http_at}


class AdaptiveRate:
    KEY = "adaptive_live_rate_v1"

    def __init__(self, store, minimum_interval):
        self.store = store
        self.minimum = int(minimum_interval)
        # Always adhere strictly to the interval chosen in the UI
        self.interval = float(minimum_interval)
        self.state = store.get_meta(self.KEY, {}) or {}
        self.clean = int(self.state.get("clean", 0))
        self.limits = int(self.state.get("limits", 0))
        self.errors = int(self.state.get("errors", 0))

    def save(self):
        self.state = {"interval":self.interval,"clean":self.clean,"limits":self.limits,
                      "errors":self.errors,"minimum":self.minimum,"updated_at":time.time()}
        self.store.set_meta(self.KEY, self.state)

    def rate_limited(self, server_wait=0):
        self.clean, self.errors = 0, 0
        self.limits += 1
        # Keep interval strictly as chosen by user in UI, respecting Retry-After header only if supplied
        delay = max(self.interval, float(server_wait or 0))
        self.store.set_meta("source_cooldown_until", time.time() + delay)
        self.save()
        return delay

    def failed(self):
        self.clean = 0
        self.errors += 1
        delay = self.interval
        self.save()
        return delay

    def succeeded(self):
        self.errors = 0
        self.clean += 1
        self.save()
        return self.interval


def next_offset(offset, response, page_limit, pages_per_query):
    """A failed or throttled request is not evidence of the end of search results."""
    received = len(response.get("records", []))
    incomplete = response.get("rate_limited") or bool(response.get("errors"))
    if incomplete:
        return offset + min(received, page_limit) if received else offset
    cards = response.get("search_cards")
    if cards is None:
        cards = received
    if cards < page_limit:
        return 0
    value = offset + page_limit
    return 0 if value >= pages_per_query * page_limit else value


def record_observation(store, observation):
    history = store.get_meta("live_rate_observations", [])
    history.append(observation)
    store.set_meta("live_rate_observations", history[-2000:])
