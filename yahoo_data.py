"""Resilient access to Yahoo Finance for the MCP server.

Why this module exists
----------------------
Yahoo throttles some endpoints per client IP. From a shared cloud IP (such as
a Render instance) the ``quoteSummary`` endpoint behind ``Ticker.info`` and the
news endpoint are often answered with HTTP 429 "Too Many Requests", while the
chart endpoint keeps working. The original server also called yfinance
synchronously inside async handlers, so one slow Yahoo request froze every
other request on the server.

What it provides
----------------
* A bounded worker pool with a time budget per call, so nothing blocks the
  event loop and no tool call waits forever.
* Single-flight de-duplication: concurrent identical requests share one fetch.
* An in-memory cache that serves fresh data, and stale data when Yahoo fails.
* Per-endpoint circuit breakers with exponential back-off after a 429, so a
  throttled endpoint is not hammered (hammering prolongs the block).
* A direct, crumb-free client for the chart, search and RSS endpoints. It is
  used to build a live quote even while ``quoteSummary`` is blocked, and as a
  fallback for price history and news.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import datetime as dt
import html
import json
import logging
import math
import os
import random
import re
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Callable
from urllib.parse import quote as _urlquote
from zoneinfo import ZoneInfo

log = logging.getLogger("yahoo")


# --------------------------------------------------------------------------
# Configuration (environment variables, all optional)
# --------------------------------------------------------------------------


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


MAX_WORKERS = max(2, _env_int("YF_MAX_WORKERS", 8))
UPSTREAM_CONCURRENCY = max(1, _env_int("YF_UPSTREAM_CONCURRENCY", 4))
HTTP_TIMEOUT = max(2.0, _env_float("YF_HTTP_TIMEOUT", 8.0))
GATE_WAIT = 20.0  # seconds a worker waits for an upstream slot before giving up

# Volatile quote fields: only trusted from a quoteSummary response that is
# younger than this many seconds; otherwise the live chart values are used.
ENRICHMENT_VOLATILE_MAX_AGE = 90.0


# --------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------


class UpstreamError(Exception):
    """A Yahoo request failed."""


class RateLimited(UpstreamError):
    """Yahoo answered 429 / Too Many Requests for this endpoint."""

    def __init__(self, message: str = "Too Many Requests. Rate limited. Try after a while."):
        super().__init__(message)


class NotFound(UpstreamError):
    """Yahoo has no data for this symbol (or rejected the parameters)."""


class Unavailable(UpstreamError):
    """No upstream attempt was possible (breaker open, server busy)."""


def classify_exception(exc: BaseException) -> BaseException:
    """Map yfinance / HTTP-library exceptions onto this module's error types."""
    if isinstance(exc, UpstreamError):
        return exc
    name = type(exc).__name__
    text = str(exc)
    low = text.lower()
    if name == "YFRateLimitError" or "too many requests" in low or "rate limit" in low:
        return RateLimited()
    if (
        name in ("YFTickerMissingError", "YFPricesMissingError", "YFInvalidPeriodError")
        or "no data found" in low
        or "delisted" in low
        or "not found" in low
    ):
        return NotFound(text or name)
    return UpstreamError(f"{name}: {text}" if text else name)


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------


def num(value: Any) -> float | None:
    """Return a finite float, or None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def utc_iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean_text(value: Any) -> str:
    """Single-line plain text: strip HTML tags and entities, collapse whitespace."""
    if value is None:
        return ""
    s = html.unescape(str(value))
    s = re.sub(r"<[^>]+>", " ", s)
    s = s.replace("\xa0", " ")
    return re.sub(r"\s+", " ", s).strip()


def _tz(name: str | None) -> dt.tzinfo:
    if name:
        try:
            return ZoneInfo(name)
        except Exception:  # unknown zone name
            pass
    return dt.timezone.utc


# --------------------------------------------------------------------------
# Cache
# --------------------------------------------------------------------------


@dataclass
class CacheHit:
    value: Any
    stored_at: float

    @property
    def age(self) -> float:
        return max(0.0, time.time() - self.stored_at)


class TTLCache:
    """Thread-safe store of (value, stored_at). Freshness is decided by callers.

    Memory is bounded (the free Render instance has 512 MB): entries older than
    ``max_age`` are swept periodically, and the oldest tenth is dropped when
    ``max_entries`` is exceeded.
    """

    def __init__(self, max_entries: int = 2500, max_age: float = 8 * 86400.0):
        self._data: dict[str, tuple[Any, float]] = {}
        self._lock = threading.Lock()
        self._max = max_entries
        self._max_age = max_age
        self._writes = 0

    def get(self, key: str) -> CacheHit | None:
        with self._lock:
            item = self._data.get(key)
        return CacheHit(item[0], item[1]) if item else None

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            now = time.time()
            self._data[key] = (value, now)
            self._writes += 1
            if self._writes % 256 == 0:
                expired = [k for k, (_, at) in self._data.items() if now - at > self._max_age]
                for k in expired:
                    self._data.pop(k, None)
            if len(self._data) > self._max:
                oldest = sorted(self._data.items(), key=lambda kv: kv[1][1])
                for k, _ in oldest[: max(1, self._max // 10)]:
                    self._data.pop(k, None)

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


CACHE = TTLCache()


# --------------------------------------------------------------------------
# Circuit breakers
# --------------------------------------------------------------------------


class Breaker:
    """Per-endpoint circuit breaker.

    * A 429 opens the breaker at once; each further 429 doubles the cooldown
      (up to ``max_cooldown``).
    * Other failures open it after ``threshold`` consecutive failures.
    * After the cooldown one probe request is let through; success closes the
      breaker and resets the cooldown.
    """

    PROBE_TIMEOUT = 90.0

    def __init__(self, name: str, base_cooldown: float, max_cooldown: float, threshold: int = 3):
        self.name = name
        self.base = base_cooldown
        self.max = max_cooldown
        self.threshold = threshold
        self._lock = threading.Lock()
        self._open_until = 0.0
        self._tripped = False
        self._next_cooldown = base_cooldown
        self._streak = 0
        self._probe_started = 0.0
        self._rate_limited = False
        self.last_error: str | None = None
        self.last_error_at: float | None = None
        self.last_success_at: float | None = None
        self.trips = 0

    def allow(self) -> bool:
        with self._lock:
            now = time.time()
            if not self._tripped:
                return True
            if now < self._open_until:
                return False
            if self._probe_started and now - self._probe_started < self.PROBE_TIMEOUT:
                return False  # one probe at a time
            self._probe_started = now
            return True

    def success(self) -> None:
        with self._lock:
            if self._tripped:
                log.info("breaker %s closed (upstream recovered)", self.name)
            self._tripped = False
            self._open_until = 0.0
            self._next_cooldown = self.base
            self._streak = 0
            self._probe_started = 0.0
            self._rate_limited = False
            self.last_success_at = time.time()

    def failure(self, exc: BaseException) -> None:
        rate_limited = isinstance(exc, RateLimited)
        with self._lock:
            now = time.time()
            self.last_error = str(exc)[:300]
            self.last_error_at = now
            if self._tripped and now < self._open_until:
                return  # a request that started before the breaker opened
            self._probe_started = 0.0
            self._streak += 1
            if rate_limited or self._tripped or self._streak >= self.threshold:
                cooldown = self._next_cooldown
                self._open_until = now + cooldown * random.uniform(0.9, 1.1)
                self._tripped = True
                self._rate_limited = rate_limited
                self._next_cooldown = min(self.max, cooldown * 2)
                self.trips += 1
                log.warning(
                    "breaker %s open for %.0fs (%s)",
                    self.name,
                    self._open_until - now,
                    "rate limited" if rate_limited else self.last_error,
                )

    def unavailable_error(self) -> UpstreamError:
        with self._lock:
            until = utc_iso(self._open_until) if self._open_until else None
            if self._rate_limited:
                return RateLimited(
                    "Too Many Requests. Rate limited by Yahoo for this server; "
                    f"retrying automatically after {until}."
                )
            return Unavailable(
                f"Yahoo {self.name} endpoint is failing ({self.last_error}); "
                f"retrying automatically after {until}."
            )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            now = time.time()
            if not self._tripped:
                state = "closed"
            elif now < self._open_until:
                state = "open"
            else:
                state = "half-open"
            return {
                "state": state,
                "rateLimited": self._rate_limited if self._tripped else False,
                "retryAt": utc_iso(self._open_until) if self._tripped else None,
                "trips": self.trips,
                "lastError": self.last_error,
                "lastErrorAt": utc_iso(self.last_error_at),
                "lastSuccessAt": utc_iso(self.last_success_at),
            }


BREAKERS: dict[str, Breaker] = {
    # crumb-free endpoints (direct client)
    "chart": Breaker("chart", 20, 300, threshold=3),
    "chart-direct": Breaker("chart-direct", 60, 1800, threshold=3),
    "rss": Breaker("rss", 120, 1800, threshold=2),
    "search": Breaker("search", 120, 1800, threshold=2),
    # yfinance (cookie + crumb) endpoints
    "history": Breaker("history", 30, 600, threshold=3),
    "quoteSummary": Breaker("quoteSummary", 300, 3600, threshold=2),
    "news": Breaker("news", 300, 3600, threshold=2),
    "fundamentals": Breaker("fundamentals", 120, 1800, threshold=3),
    "options": Breaker("options", 60, 900, threshold=3),
    "screener": Breaker("screener", 120, 1800, threshold=2),
    # third-party Sharia filter sites
    "yaqeen": Breaker("yaqeen", 120, 3600, threshold=2),
}


# --------------------------------------------------------------------------
# Worker pool, upstream gate, single-flight
# --------------------------------------------------------------------------

_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=MAX_WORKERS, thread_name_prefix="yahoo"
)
_GATE = threading.BoundedSemaphore(UPSTREAM_CONCURRENCY)
_INFLIGHT: dict[str, concurrent.futures.Future] = {}
_INFLIGHT_LOCK = threading.Lock()


class upstream_slot:
    """Limit simultaneous upstream requests. Never nest two gated sections."""

    def __enter__(self):
        if not _GATE.acquire(timeout=GATE_WAIT):
            raise Unavailable("server busy: too many upstream requests in flight")
        return self

    def __exit__(self, *exc):
        _GATE.release()
        return False


def _forget(key: str, fut: concurrent.futures.Future) -> None:
    with _INFLIGHT_LOCK:
        if _INFLIGHT.get(key) is fut:
            _INFLIGHT.pop(key, None)


def _submit(key: str, fn: Callable[[], Any]) -> concurrent.futures.Future:
    with _INFLIGHT_LOCK:
        fut = _INFLIGHT.get(key)
        if fut is not None and not fut.done():
            return fut
        fut = _EXECUTOR.submit(fn)
        _INFLIGHT[key] = fut
    # Registered outside the lock: a future that is already done runs the
    # callback immediately in this thread, which would deadlock inside it.
    fut.add_done_callback(lambda f, k=key: _forget(k, f))
    return fut


async def run_shared(key: str, fn: Callable[[], Any], budget: float) -> Any:
    """Run ``fn`` in the worker pool, shared by concurrent callers of ``key``.

    Waits at most ``budget`` seconds. On timeout the work keeps running in the
    background (and fills the cache when it finishes); the caller gets
    ``asyncio.TimeoutError``.
    """
    fut = _submit(key, fn)
    return await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(fut)), timeout=budget)


@dataclass
class Result:
    value: Any
    age: float = 0.0
    stale: bool = False
    note: str | None = None


async def cached_call(
    *,
    key: str,
    family: str,
    fn: Callable[[], Any],
    fresh_ttl: float,
    stale_ttl: float,
    budget: float,
    work_key: str | None = None,
) -> Result:
    """Serve ``key`` from cache while fresh, otherwise fetch through ``fn``.

    ``fn`` runs in the worker pool and its return value is cached. When the
    fetch fails, times out, or the endpoint's breaker is open, a cached value
    younger than ``stale_ttl`` is returned with ``stale=True``. ``NotFound`` is
    never masked by stale data.
    """
    hit = CACHE.get(key)
    if hit is not None and hit.age < fresh_ttl:
        return Result(hit.value, hit.age)

    breaker = BREAKERS[family]

    def stale_or_raise(exc: BaseException) -> Result:
        if hit is not None and hit.age < stale_ttl:
            return Result(hit.value, hit.age, stale=True, note=str(exc)[:200])
        raise exc

    if not breaker.allow():
        return stale_or_raise(breaker.unavailable_error())

    def work() -> Any:
        try:
            value = fn()
        except BaseException as raw:  # noqa: BLE001 - classify everything
            exc = classify_exception(raw)
            if isinstance(exc, NotFound):
                breaker.success()  # Yahoo answered properly
            elif isinstance(exc, Unavailable):
                pass  # local back-pressure, says nothing about Yahoo
            else:
                breaker.failure(exc)
                log.warning("%s %s failed: %s", family, key, exc)
            if exc is raw:
                raise
            raise exc from raw
        breaker.success()
        CACHE.set(key, value)
        return value

    try:
        value = await run_shared(work_key or f"{family}|{key}", work, budget)
    except NotFound:
        raise
    except asyncio.TimeoutError:
        return stale_or_raise(
            Unavailable(f"Yahoo did not answer within {budget:.0f}s (request continues in background)")
        )
    except UpstreamError as exc:
        return stale_or_raise(exc)
    return Result(value, 0.0)


# --------------------------------------------------------------------------
# Direct Yahoo client (no cookie / crumb needed)
# --------------------------------------------------------------------------

try:  # curl_cffi gives a real browser TLS fingerprint; it ships with yfinance
    from curl_cffi import requests as _cffi_requests  # type: ignore
except Exception:  # pragma: no cover - fallback path
    _cffi_requests = None

import requests as _plain_requests  # noqa: E402  (always installed with yfinance)

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)
_HOSTS = ("https://query1.finance.yahoo.com", "https://query2.finance.yahoo.com")
_THREAD = threading.local()


def _session():
    s = getattr(_THREAD, "session", None)
    if s is None:
        if _cffi_requests is not None:
            s = _cffi_requests.Session(impersonate="chrome")
        else:
            s = _plain_requests.Session()
            s.headers.update(
                {
                    "User-Agent": _BROWSER_UA,
                    "Accept": "application/json,text/html,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                }
            )
        _THREAD.session = s
    return s


def _http_get(url: str, params: dict[str, Any] | None = None, timeout: float = HTTP_TIMEOUT):
    with upstream_slot():
        try:
            resp = _session().get(url, params=params, timeout=timeout, allow_redirects=True)
        except Exception as exc:  # network / TLS / timeout
            # a broken session can stay broken; start a fresh one next time
            _THREAD.session = None
            raise UpstreamError(f"{type(exc).__name__}: {exc}") from exc
    text = resp.text or ""
    if (
        resp.status_code == 429
        or text.lstrip().startswith("Too Many Requests")
        or (resp.status_code >= 400 and "Too Many Requests" in text[:300])
    ):
        raise RateLimited()
    return resp.status_code, text


def _yahoo_error(payload: Any) -> tuple[str | None, str | None]:
    """Extract (code, description) from a Yahoo JSON error envelope."""
    if not isinstance(payload, dict):
        return None, None
    for top in payload.values():
        if isinstance(top, dict) and isinstance(top.get("error"), dict):
            err = top["error"]
            return err.get("code"), err.get("description")
    if isinstance(payload.get("error"), dict):
        err = payload["error"]
        return err.get("code"), err.get("description")
    return None, None


def _payload_or_raise(status: int, text: str) -> Any:
    """Parse a Yahoo JSON reply; raise NotFound / UpstreamError on errors."""
    try:
        payload = json.loads(text) if text else None
    except ValueError:
        payload = None
    if 400 <= status < 500:
        code, desc = _yahoo_error(payload)
        message = desc or code or f"HTTP {status}"
        if status in (400, 404, 422) or (code and "not found" in code.lower()):
            raise NotFound(message)  # unknown symbol, or parameters rejected
        raise UpstreamError(f"HTTP {status}: {message}")
    if status >= 500:
        raise UpstreamError(f"HTTP {status} from Yahoo")
    if payload is None:
        raise UpstreamError("Yahoo returned a non-JSON response")
    return payload


def yf_session_json(url: str, params: dict[str, Any]) -> Any:
    """GET through yfinance's own session (cookie + crumb handled by yfinance)."""
    try:
        from yfinance.data import YfData
    except Exception as exc:  # pragma: no cover - yfinance internals moved
        raise UpstreamError(f"yfinance session unavailable: {exc}") from exc
    with upstream_slot():
        try:
            resp = YfData().get(url=url, params=params, timeout=HTTP_TIMEOUT)
        except Exception as exc:
            raise classify_exception(exc) from exc
    text = resp.text or ""
    if resp.status_code == 429 or text.lstrip().startswith("Too Many Requests"):
        raise RateLimited()
    return _payload_or_raise(resp.status_code, text)


def yahoo_json(path: str, params: dict[str, Any]) -> Any:
    """GET a Yahoo JSON endpoint directly, trying both API hosts."""
    hosts = list(_HOSTS)
    random.shuffle(hosts)
    last: UpstreamError = UpstreamError("no Yahoo host tried")
    for base in hosts:
        try:
            status, text = _http_get(base + path, params)
            return _payload_or_raise(status, text)
        except NotFound:
            raise
        except UpstreamError as exc:
            last = exc
    raise last


# --------------------------------------------------------------------------
# Chart endpoint
# --------------------------------------------------------------------------


def fetch_chart(
    symbol: str,
    *,
    range_: str,
    interval: str,
    prepost: bool = False,
    events: str = "div,splits",
) -> dict[str, Any]:
    params = {
        "range": range_,
        "interval": interval,
        "includePrePost": "true" if prepost else "false",
        "events": events,
        "lang": "en-US",
        "region": "US",
    }
    path = f"/v8/finance/chart/{_urlquote(symbol, safe='')}"
    payload = None
    direct = BREAKERS["chart-direct"]
    if direct.allow():
        try:
            payload = yahoo_json(path, params)
            direct.success()
        except NotFound:
            direct.success()
            raise
        except RateLimited:
            raise  # same server IP on either route; let the caller back off
        except UpstreamError as exc:
            direct.failure(exc)
            log.info("direct chart request failed (%s); retrying through yfinance", exc)
    if payload is None:
        payload = yf_session_json(_HOSTS[1] + path, params)
    chart = payload.get("chart") if isinstance(payload, dict) else None
    if not isinstance(chart, dict):
        raise UpstreamError("unexpected chart response")
    err = chart.get("error")
    if isinstance(err, dict) and (err.get("code") or err.get("description")):
        raise NotFound(err.get("description") or err.get("code"))
    results = chart.get("result") or []
    if not results or not isinstance(results[0], dict):
        raise NotFound("No data found, symbol may be delisted")
    return results[0]


@dataclass
class Bar:
    ts: int
    open: float | None
    high: float | None
    low: float | None
    close: float
    volume: float | None
    adjclose: float | None


def chart_bars(res: dict[str, Any] | None) -> list[Bar]:
    if not res:
        return []
    stamps = res.get("timestamp") or []
    indicators = res.get("indicators") or {}
    quote = (indicators.get("quote") or [{}])[0] or {}
    adj_list = indicators.get("adjclose") or []
    adj = (adj_list[0] or {}).get("adjclose") if adj_list else None

    def col(name: str) -> list:
        values = quote.get(name) or []
        return values if isinstance(values, list) else []

    opens, highs, lows, closes, vols = (col(n) for n in ("open", "high", "low", "close", "volume"))
    out: list[Bar] = []
    for i, ts in enumerate(stamps):
        close = num(closes[i]) if i < len(closes) else None
        if close is None or not isinstance(ts, (int, float)):
            continue
        out.append(
            Bar(
                ts=int(ts),
                open=num(opens[i]) if i < len(opens) else None,
                high=num(highs[i]) if i < len(highs) else None,
                low=num(lows[i]) if i < len(lows) else None,
                close=close,
                volume=num(vols[i]) if i < len(vols) else None,
                adjclose=num(adj[i]) if isinstance(adj, list) and i < len(adj) else None,
            )
        )
    out.sort(key=lambda b: b.ts)
    return out


def _windows(meta: dict[str, Any], kind: str) -> list[tuple[int, int]]:
    """Session windows of ``kind`` (pre/regular/post) from meta.tradingPeriods."""
    tps = meta.get("tradingPeriods")
    if isinstance(tps, dict):
        raw = tps.get(kind) or []
    elif isinstance(tps, list) and kind == "regular":
        raw = tps
    else:
        raw = []
    out: list[tuple[int, int]] = []
    stack = list(raw)
    while stack:
        item = stack.pop()
        if isinstance(item, list):
            stack.extend(item)
        elif isinstance(item, dict):
            s, e = item.get("start"), item.get("end")
            if isinstance(s, (int, float)) and isinstance(e, (int, float)):
                out.append((int(s), int(e)))
    ctp = (meta.get("currentTradingPeriod") or {}).get(kind) or {}
    s, e = ctp.get("start"), ctp.get("end")
    if isinstance(s, (int, float)) and isinstance(e, (int, float)):
        out.append((int(s), int(e)))
    return sorted(set(out))


def _in_any(ts: int, windows: list[tuple[int, int]]) -> bool:
    return any(s <= ts < e for s, e in windows)


def market_state(meta: dict[str, Any], now: float) -> str:
    ctp = meta.get("currentTradingPeriod") or {}

    def inside(kind: str) -> bool:
        p = ctp.get(kind) or {}
        s, e = p.get("start"), p.get("end")
        return isinstance(s, (int, float)) and isinstance(e, (int, float)) and s <= now < e

    if inside("regular"):
        return "REGULAR"
    if inside("pre"):
        return "PRE"
    if inside("post"):
        return "POST"
    return "CLOSED"


def build_live_quote(
    symbol: str,
    intraday: dict[str, Any],
    daily: dict[str, Any] | None = None,
    splits: dict[str, Any] | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Build yfinance-style ``info`` quote fields from chart responses.

    ``intraday``: range=1d, interval=1m, includePrePost=true (required).
    ``daily``: range=3mo, interval=1d (optional; previous close in pre-market,
    average volume). ``splits``: long range with split events (optional).
    """
    now = time.time() if now is None else now
    meta = intraday.get("meta") or {}
    tz = _tz(meta.get("exchangeTimezoneName"))

    def day(ts: float | None) -> dt.date | None:
        if ts is None:
            return None
        return dt.datetime.fromtimestamp(ts, tz=tz).date()

    q: dict[str, Any] = {}
    sym = meta.get("symbol") or symbol
    q["symbol"] = sym
    if meta.get("instrumentType"):
        q["quoteType"] = meta["instrumentType"]
    for src, dst in (
        ("shortName", "shortName"),
        ("longName", "longName"),
        ("fullExchangeName", "fullExchangeName"),
        ("exchangeName", "exchange"),
        ("currency", "currency"),
        ("exchangeTimezoneName", "exchangeTimezoneName"),
        ("timezone", "exchangeTimezoneShortName"),
        ("priceHint", "priceHint"),
        ("hasPrePostMarketData", "hasPrePostMarketData"),
    ):
        if meta.get(src) is not None:
            q[dst] = meta[src]
    if isinstance(meta.get("gmtoffset"), (int, float)):
        q["gmtOffSetMilliseconds"] = int(meta["gmtoffset"]) * 1000
    if isinstance(meta.get("firstTradeDate"), (int, float)):
        q["firstTradeDateMilliseconds"] = int(meta["firstTradeDate"]) * 1000

    state = market_state(meta, now)
    q["marketState"] = state

    price = num(meta.get("regularMarketPrice"))
    rmt = meta.get("regularMarketTime")
    rmt = int(rmt) if isinstance(rmt, (int, float)) else None
    bars = chart_bars(intraday)
    pre_w, reg_w, post_w = (_windows(meta, k) for k in ("pre", "regular", "post"))
    reg_bars = [b for b in bars if _in_any(b.ts, reg_w)]
    pre_bars = [b for b in bars if _in_any(b.ts, pre_w)]
    post_bars = [b for b in bars if _in_any(b.ts, post_w)]

    if price is None and reg_bars:
        price = reg_bars[-1].close
        rmt = rmt or reg_bars[-1].ts
    last_day = day(rmt)
    daily_bars = chart_bars(daily) if daily else []

    # Previous close of the session that produced regularMarketPrice.
    prev = None
    reg_days = {day(s) for s, _ in reg_w}
    chart_pc = num(meta.get("chartPreviousClose"))
    if chart_pc is None:
        chart_pc = num(meta.get("previousClose"))
    if last_day is not None and daily_bars:
        earlier = [b for b in daily_bars if day(b.ts) is not None and day(b.ts) < last_day]
        if earlier:
            prev = earlier[-1].close
    if last_day is not None and chart_pc is not None and reg_bars and day(reg_bars[0].ts) == last_day:
        # the 1-day chart covers that session, so its previous close is exact
        prev = chart_pc
    if prev is None and chart_pc is not None and (not reg_days or last_day in reg_days):
        prev = chart_pc

    if price is not None:
        q["regularMarketPrice"] = price
        q["currentPrice"] = price
    if rmt is not None:
        q["regularMarketTime"] = rmt
    if prev is not None:
        q["regularMarketPreviousClose"] = prev
        q["previousClose"] = prev
        if price is not None and prev:
            q["regularMarketChange"] = price - prev
            q["regularMarketChangePercent"] = (price / prev - 1.0) * 100.0

    # Day statistics of that session.
    session_bars = [b for b in reg_bars if day(b.ts) == last_day] if last_day else []
    day_bar = next((b for b in reversed(daily_bars) if day(b.ts) == last_day), None)
    open_ = session_bars[0].open if session_bars else None
    if open_ is None and day_bar is not None:
        open_ = day_bar.open
    high = num(meta.get("regularMarketDayHigh"))
    low = num(meta.get("regularMarketDayLow"))
    volume = num(meta.get("regularMarketVolume"))
    if session_bars:
        highs = [b.high for b in session_bars if b.high is not None]
        lows = [b.low for b in session_bars if b.low is not None]
        if high is None and highs:
            high = max(highs)
        if low is None and lows:
            low = min(lows)
        if volume is None:
            volume = sum(b.volume or 0 for b in session_bars)
    if open_ is not None:
        q["regularMarketOpen"] = open_
        q["open"] = open_
    if high is not None:
        q["regularMarketDayHigh"] = high
        q["dayHigh"] = high
    if low is not None:
        q["regularMarketDayLow"] = low
        q["dayLow"] = low
    if high is not None and low is not None:
        q["regularMarketDayRange"] = f"{low} - {high}"
    if volume is not None:
        q["regularMarketVolume"] = int(volume)
        q["volume"] = int(volume)

    # Extended hours.
    if pre_bars and price is not None:
        last_pre = pre_bars[-1]
        regular_after = any(b.ts > last_pre.ts for b in reg_bars)
        if not regular_after:  # today's regular session has not started yet
            q["preMarketPrice"] = last_pre.close
            q["preMarketTime"] = last_pre.ts
            q["preMarketChange"] = last_pre.close - price
            if price:
                q["preMarketChangePercent"] = (last_pre.close / price - 1.0) * 100.0
    if post_bars and price is not None:
        last_post = post_bars[-1]
        if rmt is None or last_post.ts >= rmt:
            q["postMarketPrice"] = last_post.close
            q["postMarketTime"] = last_post.ts
            q["postMarketChange"] = last_post.close - price
            if price:
                q["postMarketChangePercent"] = (last_post.close / price - 1.0) * 100.0

    # 52-week range.
    hi52, lo52 = num(meta.get("fiftyTwoWeekHigh")), num(meta.get("fiftyTwoWeekLow"))
    if hi52 is not None:
        q["fiftyTwoWeekHigh"] = hi52
    if lo52 is not None:
        q["fiftyTwoWeekLow"] = lo52
    if hi52 is not None and lo52 is not None:
        q["fiftyTwoWeekRange"] = f"{lo52} - {hi52}"

    # Average volume from completed daily sessions (today's counts only once closed).
    if daily_bars:
        today = day(now)
        done = [
            b
            for b in daily_bars
            if b.volume is not None and (day(b.ts) < today or state in ("POST", "CLOSED"))
        ]
        if len(done) >= 5:
            avg3m = sum(b.volume for b in done[-63:]) / len(done[-63:])
            avg10 = sum(b.volume for b in done[-10:]) / len(done[-10:])
            q["averageVolume"] = int(round(avg3m))
            q["averageDailyVolume3Month"] = int(round(avg3m))
            q["averageVolume10days"] = int(round(avg10))
            q["averageDailyVolume10Day"] = int(round(avg10))

    # Last split.
    split_events = ((splits or {}).get("events") or {}).get("splits") or {}
    if isinstance(split_events, dict) and split_events:
        latest = max(
            (e for e in split_events.values() if isinstance(e, dict) and e.get("date")),
            key=lambda e: e["date"],
            default=None,
        )
        if latest:
            ratio = latest.get("splitRatio")
            if not ratio and latest.get("numerator") and latest.get("denominator"):
                ratio = f"{latest['numerator']:g}:{latest['denominator']:g}"
            if ratio:
                q["lastSplitFactor"] = ratio
                q["lastSplitDate"] = int(latest["date"])
    return q


VOLATILE_INFO_KEYS = frozenset(
    {
        "currentPrice", "regularMarketPrice", "regularMarketChange", "regularMarketChangePercent",
        "regularMarketTime", "regularMarketDayHigh", "regularMarketDayLow", "regularMarketDayRange",
        "regularMarketOpen", "regularMarketVolume", "regularMarketPreviousClose", "previousClose",
        "open", "dayHigh", "dayLow", "volume", "bid", "ask", "bidSize", "askSize",
        "preMarketPrice", "preMarketChange", "preMarketChangePercent", "preMarketTime",
        "postMarketPrice", "postMarketChange", "postMarketChangePercent", "postMarketTime",
        "marketState", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "fiftyTwoWeekRange",
    }
)


def merge_quote(
    live: dict[str, Any] | None,
    enrichment: dict[str, Any] | None,
    enrichment_age: float | None,
) -> dict[str, Any]:
    """Combine live chart fields with (possibly cached) quoteSummary fields.

    Price-like fields from a quoteSummary response older than
    ``ENRICHMENT_VOLATILE_MAX_AGE`` are dropped in favour of the live chart,
    unless no live quote exists at all (then the caller flags staleness).
    """
    if not live:
        return dict(enrichment or {})
    base: dict[str, Any] = {}
    if enrichment:
        fresh = enrichment_age is not None and enrichment_age <= ENRICHMENT_VOLATILE_MAX_AGE
        base = {k: v for k, v in enrichment.items() if fresh or k not in VOLATILE_INFO_KEYS}
    extras = {
        "averageVolume", "averageDailyVolume3Month", "averageVolume10days",
        "averageDailyVolume10Day", "lastSplitFactor", "lastSplitDate",
    }
    for k, v in live.items():
        if v is None:
            continue
        if k in extras and base.get(k) is not None:
            continue  # Yahoo's own figure wins over our estimate
        base[k] = v
    price = num(base.get("regularMarketPrice"))
    shares = num(base.get("sharesOutstanding"))
    if price is not None and shares and base.get("quoteType") in (None, "EQUITY"):
        base["marketCap"] = int(round(price * shares))
    return base


# --------------------------------------------------------------------------
# News sources
# --------------------------------------------------------------------------


@dataclass
class NewsItem:
    title: str
    summary: str = ""
    description: str = ""
    url: str = ""
    published: float | None = None
    publisher: str = ""


def news_from_yfinance(articles: list[dict[str, Any]] | None) -> list[NewsItem]:
    items: list[NewsItem] = []
    for art in articles or []:
        content = (art or {}).get("content") or {}
        if content.get("contentType", "") != "STORY":
            continue
        url = ((content.get("canonicalUrl") or {}).get("url")) or (
            (content.get("clickThroughUrl") or {}).get("url")
        ) or ""
        published = None
        pub = content.get("pubDate") or content.get("displayTime")
        if isinstance(pub, str):
            try:
                published = dt.datetime.fromisoformat(pub.replace("Z", "+00:00")).timestamp()
            except ValueError:
                published = None
        items.append(
            NewsItem(
                title=clean_text(content.get("title")),
                summary=clean_text(content.get("summary")),
                description=clean_text(content.get("description")),
                url=str(url or ""),
                published=published,
                publisher=clean_text((content.get("provider") or {}).get("displayName")),
            )
        )
    return [i for i in items if i.title]


def fetch_news_rss(symbol: str) -> list[NewsItem]:
    status, text = _http_get(
        "https://feeds.finance.yahoo.com/rss/2.0/headline",
        {"s": symbol, "region": "US", "lang": "en-US"},
    )
    if status >= 400:
        raise UpstreamError(f"HTTP {status} from Yahoo RSS")
    try:
        root = ET.fromstring(text.encode("utf-8") if isinstance(text, str) else text)
    except ET.ParseError as exc:
        raise UpstreamError(f"unreadable RSS feed: {exc}") from exc
    items: list[NewsItem] = []
    for node in root.iter("item"):
        title = clean_text(node.findtext("title"))
        if not title:
            continue
        published = None
        pub = node.findtext("pubDate")
        if pub:
            try:
                published = parsedate_to_datetime(pub).timestamp()
            except (TypeError, ValueError):
                published = None
        items.append(
            NewsItem(
                title=title,
                summary=clean_text(node.findtext("description")),
                url=(node.findtext("link") or "").strip(),
                published=published,
                publisher=clean_text(node.findtext("source")),
            )
        )
    return items


def fetch_news_search(symbol: str) -> list[NewsItem]:
    payload = yahoo_json(
        "/v1/finance/search",
        {
            "q": symbol,
            "quotesCount": 0,
            "newsCount": 12,
            "enableFuzzyQuery": "false",
            "lang": "en-US",
            "region": "US",
        },
    )
    items: list[NewsItem] = []
    for art in (payload or {}).get("news") or []:
        if not isinstance(art, dict):
            continue
        related = [str(t).upper() for t in art.get("relatedTickers") or []]
        if related and symbol.upper() not in related:
            continue  # keyword hit about another company
        items.append(
            NewsItem(
                title=clean_text(art.get("title")),
                url=str(art.get("link") or ""),
                published=num(art.get("providerPublishTime")),
                publisher=clean_text(art.get("publisher")),
            )
        )
    return [i for i in items if i.title]


def format_news(items: list[NewsItem]) -> str:
    blocks = []
    for it in items:
        lines = [
            f"Title: {it.title}",
            f"Summary: {it.summary}",
            f"Description: {it.description}",
            f"URL: {it.url}",
        ]
        if it.published:
            lines.append(f"Published: {utc_iso(it.published)}")
        if it.publisher:
            lines.append(f"Publisher: {it.publisher}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


# --------------------------------------------------------------------------
# History fallback built from the chart endpoint
# --------------------------------------------------------------------------


def chart_history_json(res: dict[str, Any], interval: str) -> str:
    """Records in the same shape yfinance ``history()`` produces (auto-adjusted)."""
    import pandas as pd  # local import keeps module import light

    meta = res.get("meta") or {}
    tzname = meta.get("exchangeTimezoneName") or "UTC"
    bars = chart_bars(res)
    if not bars:
        return "[]"
    intraday = interval[-1:] in ("m", "h")
    events = res.get("events") or {}

    def event_frame(kind: str, value: Callable[[dict], float | None]) -> dict[Any, float]:
        out: dict[Any, float] = {}
        for ev in (events.get(kind) or {}).values():
            if isinstance(ev, dict) and isinstance(ev.get("date"), (int, float)):
                v = value(ev)
                if v is not None:
                    out[int(ev["date"])] = v
        return out

    dividends = event_frame("dividends", lambda e: num(e.get("amount")))
    splits = event_frame(
        "splits",
        lambda e: (num(e.get("numerator")) or 0) / num(e.get("denominator"))
        if num(e.get("denominator"))
        else None,
    )
    gains = event_frame("capitalGains", lambda e: num(e.get("amount")))
    with_gains = meta.get("instrumentType") in ("ETF", "MUTUALFUND")

    rows = []
    for b in bars:
        ratio = (b.adjclose / b.close) if (not intraday and b.adjclose and b.close) else 1.0
        rows.append(
            {
                "ts": b.ts,
                "Open": b.open * ratio if b.open is not None else None,
                "High": b.high * ratio if b.high is not None else None,
                "Low": b.low * ratio if b.low is not None else None,
                "Close": b.close * ratio,
                "Volume": int(b.volume or 0),
            }
        )
    df = pd.DataFrame(rows)
    idx = pd.to_datetime(df.pop("ts"), unit="s", utc=True).dt.tz_convert(tzname)
    if not intraday:
        idx = pd.to_datetime(idx.dt.date).dt.tz_localize(tzname, ambiguous=True, nonexistent="shift_forward")
    df.index = pd.DatetimeIndex(idx)

    def event_column(mapping: dict[Any, float]) -> list[float]:
        if not mapping:
            return [0.0] * len(df)
        ev_idx = pd.to_datetime(list(mapping.keys()), unit="s", utc=True).tz_convert(tzname)
        if not intraday:
            ev_days = {d: v for d, v in zip(ev_idx.date, mapping.values())}
            return [float(ev_days.get(ts.date(), 0.0)) for ts in df.index]
        out = [0.0] * len(df)
        positions = df.index.searchsorted(ev_idx)
        for pos, v in zip(positions, mapping.values()):
            if 0 <= pos < len(out):
                out[pos] = float(v)
        return out

    df["Dividends"] = event_column(dividends)
    df["Stock Splits"] = event_column(splits)
    if with_gains:
        df["Capital Gains"] = event_column(gains)
    df = df[~df.index.duplicated(keep="first")]
    return df.reset_index(names="Date").to_json(orient="records", date_format="iso")


# --------------------------------------------------------------------------
# Diagnostics
# --------------------------------------------------------------------------


def status_snapshot() -> dict[str, Any]:
    return {
        "httpBackend": "curl_cffi" if _cffi_requests is not None else "requests",
        "workers": MAX_WORKERS,
        "upstreamConcurrency": UPSTREAM_CONCURRENCY,
        "cacheEntries": len(CACHE),
        "inflight": len(_INFLIGHT),
        "endpoints": {name: b.snapshot() for name, b in BREAKERS.items()},
    }
