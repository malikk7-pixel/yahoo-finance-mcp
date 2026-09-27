"""Offline tests for the resilience layer (no network access needed).

Yahoo is replaced by fakes, so these tests pin down behaviour that the live
integration tests cannot: quote building from chart data, caching, stale
fallbacks, circuit breakers and the news / history fallbacks.
"""

import asyncio
import datetime as dt
import json
import time
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import server
import yahoo_data as yd

NY = ZoneInfo("America/New_York")


def ts(day: dt.date, hh: int, mm: int) -> int:
    return int(dt.datetime(day.year, day.month, day.day, hh, mm, tzinfo=NY).timestamp())


def windows(day: dt.date) -> dict:
    def w(a, b):
        return {"timezone": "EDT", "start": ts(day, *a), "end": ts(day, *b), "gmtoffset": -14400}

    return {"pre": w((4, 0), (9, 30)), "regular": w((9, 30), (16, 0)), "post": w((16, 0), (20, 0))}


def intraday(day, bars, price, rmt, chart_prev, ctp_day=None, **meta_extra):
    """A range=1d / 1m / includePrePost chart result. bars: (ts, close, volume)."""
    sessions = windows(day)
    meta = {
        "currency": "USD",
        "symbol": "MSGY",
        "exchangeName": "NCM",
        "fullExchangeName": "NasdaqCM",
        "instrumentType": "EQUITY",
        "firstTradeDate": 1735300000,
        "regularMarketTime": rmt,
        "hasPrePostMarketData": True,
        "gmtoffset": -14400,
        "timezone": "EDT",
        "exchangeTimezoneName": "America/New_York",
        "regularMarketPrice": price,
        "fiftyTwoWeekHigh": 170.0,
        "fiftyTwoWeekLow": 1.7,
        "longName": "Masonglory Limited",
        "shortName": "Masonglory Ltd",
        "chartPreviousClose": chart_prev,
        "previousClose": chart_prev,
        "priceHint": 4,
        "currentTradingPeriod": windows(ctp_day or day),
        "tradingPeriods": {k: [[v]] for k, v in sessions.items()},
        "dataGranularity": "1m",
        "range": "1d",
    }
    meta.update(meta_extra)
    return {
        "meta": meta,
        "timestamp": [b[0] for b in bars],
        "indicators": {
            "quote": [
                {
                    "open": [b[1] for b in bars],
                    "high": [b[1] + 0.05 for b in bars],
                    "low": [b[1] - 0.05 for b in bars],
                    "close": [b[1] for b in bars],
                    "volume": [b[2] for b in bars],
                }
            ]
        },
    }


def daily(days_closes_vols, adj_factor=1.0):
    """A range=3mo / 1d chart result. days_closes_vols: (date, close, volume)."""
    stamps = [ts(d, 9, 30) for d, _, _ in days_closes_vols]
    closes = [c for _, c, _ in days_closes_vols]
    return {
        "meta": {"symbol": "MSGY", "exchangeTimezoneName": "America/New_York", "instrumentType": "EQUITY"},
        "timestamp": stamps,
        "indicators": {
            "quote": [
                {
                    "open": [c - 0.1 for c in closes],
                    "high": [c + 0.2 for c in closes],
                    "low": [c - 0.2 for c in closes],
                    "close": closes,
                    "volume": [v for _, _, v in days_closes_vols],
                }
            ],
            "adjclose": [{"adjclose": [c * adj_factor for c in closes]}],
        },
    }


TUE = dt.date(2026, 9, 29)
MON = dt.date(2026, 9, 28)
FRI = dt.date(2026, 9, 25)


def trading_days(end: dt.date, n: int) -> list:
    out, d = [], end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= dt.timedelta(days=1)
    return sorted(out)


@pytest.fixture(autouse=True)
def fresh_state():
    yd.CACHE._data.clear()
    for b in yd.BREAKERS.values():
        b.success()
        b.trips = 0
    yield
    yd.CACHE._data.clear()


# --------------------------------------------------------------------------
# build_live_quote
# --------------------------------------------------------------------------


def test_regular_session_quote():
    bars = [(ts(TUE, 8, 0), 9.8, 500), (ts(TUE, 9, 30), 10.0, 1000), (ts(TUE, 10, 59), 10.5, 2000)]
    res = intraday(TUE, bars, price=10.5, rmt=ts(TUE, 10, 59), chart_prev=9.5,
                   regularMarketVolume=3000, regularMarketDayHigh=10.6, regularMarketDayLow=9.9)
    q = yd.build_live_quote("MSGY", res, now=ts(TUE, 11, 0))
    assert q["marketState"] == "REGULAR"
    assert q["regularMarketPrice"] == 10.5 and q["currentPrice"] == 10.5
    assert q["regularMarketPreviousClose"] == 9.5
    assert q["regularMarketChangePercent"] == pytest.approx((10.5 / 9.5 - 1) * 100)
    assert q["regularMarketOpen"] == 10.0
    assert q["regularMarketVolume"] == 3000
    assert "preMarketPrice" not in q  # the regular session has started
    assert "postMarketPrice" not in q
    assert q["quoteType"] == "EQUITY" and q["symbol"] == "MSGY"
    assert q["fullExchangeName"] == "NasdaqCM"


def test_pre_market_quote_uses_daily_bars_for_previous_close():
    bars = [(ts(TUE, 7, 0), 9.9, 100), (ts(TUE, 8, 10), 10.2, 300)]
    res = intraday(TUE, bars, price=9.5, rmt=ts(MON, 16, 0), chart_prev=9.5)
    days = trading_days(MON, 30)
    d = daily([(day, 9.0 if day == FRI else (9.5 if day == MON else 8.0), 1000) for day in days])
    q = yd.build_live_quote("MSGY", res, daily=d, now=ts(TUE, 8, 15))
    assert q["marketState"] == "PRE"
    assert q["regularMarketPrice"] == 9.5
    assert q["regularMarketPreviousClose"] == 9.0  # Friday, not Monday
    assert q["regularMarketChangePercent"] == pytest.approx((9.5 / 9.0 - 1) * 100)
    assert q["preMarketPrice"] == 10.2
    assert q["preMarketTime"] == ts(TUE, 8, 10)
    assert q["preMarketChangePercent"] == pytest.approx((10.2 / 9.5 - 1) * 100)
    assert q["averageVolume"] == 1000


def test_pre_market_without_daily_bars_does_not_invent_a_change():
    bars = [(ts(TUE, 8, 10), 10.2, 300)]
    res = intraday(TUE, bars, price=9.5, rmt=ts(MON, 16, 0), chart_prev=9.5)
    q = yd.build_live_quote("MSGY", res, now=ts(TUE, 8, 15))
    assert "regularMarketChangePercent" not in q  # Monday's change is unknown here
    assert q["preMarketPrice"] == 10.2


def test_post_market_quote():
    bars = [
        (ts(TUE, 9, 30), 10.0, 1000),
        (ts(TUE, 15, 59), 10.8, 5000),
        (ts(TUE, 16, 5), 11.0, 700),
        (ts(TUE, 17, 20), 11.2, 900),
    ]
    res = intraday(TUE, bars, price=10.8, rmt=ts(TUE, 16, 0), chart_prev=9.5)
    q = yd.build_live_quote("MSGY", res, now=ts(TUE, 17, 30))
    assert q["marketState"] == "POST"
    assert q["regularMarketChangePercent"] == pytest.approx((10.8 / 9.5 - 1) * 100)
    assert q["postMarketPrice"] == 11.2
    assert q["postMarketChangePercent"] == pytest.approx((11.2 / 10.8 - 1) * 100)


def test_weekend_quote_is_closed_with_friday_after_hours():
    sat, mon_next = dt.date(2026, 10, 3), dt.date(2026, 10, 5)
    fri = dt.date(2026, 10, 2)
    bars = [(ts(fri, 9, 30), 11.6, 800), (ts(fri, 15, 59), 12.0, 900), (ts(fri, 19, 50), 12.1, 50)]
    res = intraday(fri, bars, price=12.0, rmt=ts(fri, 16, 0), chart_prev=11.5, ctp_day=mon_next)
    q = yd.build_live_quote("MSGY", res, now=ts(sat, 12, 0))
    assert q["marketState"] == "CLOSED"
    assert q["regularMarketPreviousClose"] == 11.5
    assert q["postMarketPrice"] == 12.1


def test_average_volume_excludes_the_session_in_progress_and_split_is_reported():
    bars = [(ts(TUE, 9, 30), 10.0, 1000)]
    res = intraday(TUE, bars, price=10.0, rmt=ts(TUE, 9, 31), chart_prev=9.5)
    days = trading_days(TUE, 20)
    d = daily([(day, 10.0, 9_000_000 if day == TUE else 1000) for day in days])
    splits = {"events": {"splits": {"1786406400": {"date": 1786406400, "numerator": 1,
                                                   "denominator": 8, "splitRatio": "1:8"}}}}
    q = yd.build_live_quote("MSGY", res, daily=d, splits=splits, now=ts(TUE, 9, 40))
    assert q["averageVolume"] == 1000
    assert q["averageVolume10days"] == 1000
    assert q["lastSplitFactor"] == "1:8" and q["lastSplitDate"] == 1786406400


def test_merge_quote_prefers_live_prices_and_keeps_fundamentals():
    live = {"symbol": "MSGY", "quoteType": "EQUITY", "regularMarketPrice": 10.0, "averageVolume": 5}
    enrichment = {"symbol": "MSGY", "quoteType": "EQUITY", "regularMarketPrice": 4.0, "bid": 3.9,
                  "floatShares": 800_000, "sharesOutstanding": 2_000_000, "averageVolume": 7}
    old = yd.merge_quote(live, enrichment, enrichment_age=3600)
    assert old["regularMarketPrice"] == 10.0
    assert "bid" not in old  # an hour-old bid is misleading
    assert old["floatShares"] == 800_000
    assert old["averageVolume"] == 7  # Yahoo's own figure wins over our estimate
    assert old["marketCap"] == 20_000_000
    fresh = yd.merge_quote(live, enrichment, enrichment_age=10)
    assert fresh["bid"] == 3.9
    only_enrichment = yd.merge_quote(None, enrichment, enrichment_age=3600)
    assert only_enrichment["regularMarketPrice"] == 4.0


# --------------------------------------------------------------------------
# cached_call, breakers, single-flight
# --------------------------------------------------------------------------


def test_cached_call_serves_fresh_then_stale_on_failure():
    calls = []

    def ok():
        calls.append(1)
        return "v1"

    async def scenario():
        r1 = await yd.cached_call(key="k", family="chart", fn=ok, fresh_ttl=60, stale_ttl=600, budget=5)
        r2 = await yd.cached_call(key="k", family="chart", fn=ok, fresh_ttl=60, stale_ttl=600, budget=5)
        assert (r1.value, r1.stale, r2.value) == ("v1", False, "v1")
        assert len(calls) == 1  # second call served from cache
        yd.CACHE._data["k"] = ("v1", time.time() - 120)  # make it expire

        def fail():
            raise yd.UpstreamError("boom")

        r3 = await yd.cached_call(key="k", family="chart", fn=fail, fresh_ttl=60, stale_ttl=600, budget=5)
        assert r3.value == "v1" and r3.stale

    asyncio.run(scenario())


def test_rate_limit_opens_breaker_and_stops_calling_yahoo():
    attempts = []

    def limited():
        attempts.append(1)
        raise yd.RateLimited()

    async def scenario():
        with pytest.raises(yd.RateLimited):
            await yd.cached_call(key="q", family="quoteSummary", fn=limited, fresh_ttl=1, stale_ttl=1, budget=5)
        with pytest.raises(yd.RateLimited) as err:
            await yd.cached_call(key="q", family="quoteSummary", fn=limited, fresh_ttl=1, stale_ttl=1, budget=5)
        assert "retrying automatically" in str(err.value)
        assert len(attempts) == 1  # the open breaker blocked the second attempt

    asyncio.run(scenario())
    snap = yd.BREAKERS["quoteSummary"].snapshot()
    assert snap["state"] == "open" and snap["rateLimited"]


def test_breaker_lets_one_probe_through_after_cooldown():
    b = yd.Breaker("t", base_cooldown=0.05, max_cooldown=1)
    b.failure(yd.RateLimited())
    assert not b.allow()
    time.sleep(0.08)
    assert b.allow()  # the probe
    assert not b.allow()  # nobody else while it runs
    b.success()
    assert b.allow() and b.snapshot()["state"] == "closed"


def test_not_found_is_not_masked_and_does_not_trip_breaker():
    def missing():
        raise yd.NotFound("No data found, symbol may be delisted")

    async def scenario():
        for _ in range(5):
            with pytest.raises(yd.NotFound):
                await yd.cached_call(key="nf", family="chart", fn=missing, fresh_ttl=1, stale_ttl=1, budget=5)

    asyncio.run(scenario())
    assert yd.BREAKERS["chart"].snapshot()["state"] == "closed"


def test_concurrent_identical_requests_share_one_fetch():
    calls = []

    def slow():
        calls.append(1)
        time.sleep(0.3)
        return "shared"

    async def scenario():
        results = await asyncio.gather(
            *[yd.cached_call(key="s", family="chart", fn=slow, fresh_ttl=60, stale_ttl=60, budget=5)
              for _ in range(6)]
        )
        assert {r.value for r in results} == {"shared"}

    asyncio.run(scenario())
    assert len(calls) == 1


def test_timeout_returns_quickly_and_background_fetch_fills_cache():
    def slow():
        time.sleep(0.6)
        return "late"

    async def scenario():
        t0 = time.monotonic()
        with pytest.raises(yd.Unavailable):
            await yd.cached_call(key="t", family="chart", fn=slow, fresh_ttl=60, stale_ttl=60, budget=0.1)
        assert time.monotonic() - t0 < 0.5
        await asyncio.sleep(0.8)
        r = await yd.cached_call(key="t", family="chart", fn=slow, fresh_ttl=60, stale_ttl=60, budget=0.1)
        assert r.value == "late"

    asyncio.run(scenario())


# --------------------------------------------------------------------------
# HTTP helpers and parsers
# --------------------------------------------------------------------------


def test_yahoo_json_error_mapping(monkeypatch):
    replies = []

    def fake_get(url, params=None, timeout=None):
        return replies.pop(0)

    monkeypatch.setattr(yd, "_http_get", fake_get)
    replies[:] = [(404, json.dumps({"chart": {"result": None, "error": {
        "code": "Not Found", "description": "No data found, symbol may be delisted"}}}))]
    with pytest.raises(yd.NotFound):
        yd.yahoo_json("/v8/finance/chart/XXXX", {})
    replies[:] = [(500, "oops"), (200, json.dumps({"ok": 1}))]
    assert yd.yahoo_json("/x", {}) == {"ok": 1}  # second host answered

    def limited(url, params=None, timeout=None):
        raise yd.RateLimited()

    monkeypatch.setattr(yd, "_http_get", limited)
    with pytest.raises(yd.RateLimited):
        yd.yahoo_json("/x", {})


RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Yahoo! Finance: MSGY News</title>
<item><title>Masonglory wins &amp; expands</title>
<description>&lt;p&gt;The company signed a
 new contract.&lt;/p&gt;</description>
<link>https://finance.yahoo.com/news/masonglory-1.html</link>
<pubDate>Fri, 25 Sep 2026 13:05:00 +0000</pubDate></item>
<item><title>Second headline</title><link>https://example.com/b</link></item>
</channel></rss>"""


def test_rss_parser(monkeypatch):
    monkeypatch.setattr(yd, "_http_get", lambda url, params=None, timeout=None: (200, RSS))
    items = yd.fetch_news_rss("MSGY")
    assert [i.title for i in items] == ["Masonglory wins & expands", "Second headline"]
    assert items[0].summary == "The company signed a new contract."
    assert items[0].published == dt.datetime(2026, 9, 25, 13, 5, tzinfo=dt.timezone.utc).timestamp()
    text = yd.format_news(items)
    assert "Title: Masonglory wins & expands\nSummary: The company signed a new contract." in text
    assert "URL: https://finance.yahoo.com/news/masonglory-1.html\nPublished: 2026-09-25T13:05:00Z" in text


def test_search_news_parser_skips_other_companies(monkeypatch):
    payload = {"news": [
        {"title": "MSGY jumps", "publisher": "Wire", "link": "https://x/1",
         "providerPublishTime": 1790000000, "relatedTickers": ["MSGY"]},
        {"title": "Unrelated", "link": "https://x/2", "relatedTickers": ["AAPL"]},
    ]}
    monkeypatch.setattr(yd, "yahoo_json", lambda path, params: payload)
    items = yd.fetch_news_search("MSGY")
    assert [i.title for i in items] == ["MSGY jumps"]
    assert items[0].publisher == "Wire"


def test_chart_history_json_matches_yfinance_shape():
    days = trading_days(TUE, 3)
    res = daily([(d, 10.0 + i, 100 * (i + 1)) for i, d in enumerate(days)], adj_factor=0.5)
    res["events"] = {"dividends": {str(ts(days[1], 9, 30)): {"amount": 0.25, "date": ts(days[1], 9, 30)}}}
    rows = json.loads(yd.chart_history_json(res, "1d"))
    assert [set(r) for r in rows][0] == {"Date", "Open", "High", "Low", "Close", "Volume",
                                         "Dividends", "Stock Splits"}
    assert rows[0]["Date"] == "2026-09-25T04:00:00.000Z"  # midnight New York, as yfinance
    assert rows[0]["Close"] == pytest.approx(5.0)  # auto-adjusted by adjclose/close
    assert rows[1]["Dividends"] == 0.25 and rows[2]["Volume"] == 300


# --------------------------------------------------------------------------
# Tools end-to-end with a fake Yahoo
# --------------------------------------------------------------------------


def fake_charts(monkeypatch, now):
    bars = [(ts(TUE, 9, 30), 10.0, 1000), (ts(TUE, 10, 59), 10.5, 2000)]
    one_day = intraday(TUE, bars, price=10.5, rmt=ts(TUE, 10, 59), chart_prev=9.5)
    three_mo = daily([(d, 9.5, 1500) for d in trading_days(TUE, 30)])
    calls = []

    def fetch_chart(symbol, *, range_, interval, prepost=False, events="div,splits"):
        calls.append((symbol, range_, interval))
        if symbol == "NOPE":
            raise yd.NotFound("No data found, symbol may be delisted")
        return {"1d": one_day, "3mo": three_mo}.get(range_, {"meta": {}, "events": {}})

    monkeypatch.setattr(yd, "fetch_chart", fetch_chart)
    monkeypatch.setattr(yd.time, "time", lambda: now)
    return calls


def test_stock_info_survives_quote_summary_rate_limit(monkeypatch):
    calls = fake_charts(monkeypatch, now=ts(TUE, 11, 0))
    monkeypatch.setattr(server, "_fetch_info", lambda s: (_ for _ in ()).throw(yd.RateLimited()))
    text = asyncio.run(server.get_stock_info("msgy"))
    assert "Error" not in text
    data = json.loads(text)
    assert data["symbol"] == "MSGY" and data["quoteType"] == "EQUITY"
    assert data["regularMarketPrice"] == 10.5
    assert data["marketState"] in ("REGULAR", "CLOSED", "PRE", "POST")
    assert data["averageVolume"] == 1500
    assert data["dataStatus"]["quote"] == "live"
    assert data["dataStatus"]["fundamentals"] == "unavailable"
    assert any("rate-limiting" in n for n in data["dataStatus"]["notes"])
    # second call: chart served from cache, quoteSummary not retried (breaker open)
    asyncio.run(server.get_stock_info("MSGY"))
    assert sum(1 for c in calls if c[1] == "1d") == 1


def test_stock_info_merges_fundamentals_when_available(monkeypatch):
    fake_charts(monkeypatch, now=ts(TUE, 11, 0))
    monkeypatch.setattr(server, "_fetch_info", lambda s: {
        "symbol": "MSGY", "quoteType": "EQUITY", "floatShares": 800000, "shortPercentOfFloat": 0.12,
        "sharesOutstanding": 2000000, "regularMarketPrice": 3.0})
    data = json.loads(asyncio.run(server.get_stock_info("MSGY")))
    assert data["floatShares"] == 800000 and data["shortPercentOfFloat"] == 0.12
    assert data["regularMarketPrice"] == 10.5
    assert data["dataStatus"]["fundamentals"] == "fresh"


def test_stock_info_unknown_symbol(monkeypatch):
    fake_charts(monkeypatch, now=ts(TUE, 11, 0))
    monkeypatch.setattr(server, "_fetch_info", lambda s: (_ for _ in ()).throw(yd.NotFound("404")))
    assert asyncio.run(server.get_stock_info("NOPE")) == "No stock info found for ticker NOPE."


def test_history_falls_back_to_chart_api(monkeypatch):
    fake_charts(monkeypatch, now=ts(TUE, 11, 0))
    monkeypatch.setattr(server, "_yf_history", lambda *a: (_ for _ in ()).throw(yd.RateLimited()))
    rows = json.loads(asyncio.run(server.get_historical_stock_prices("MSGY", "3mo", "1d")))
    assert len(rows) == 30 and {"Date", "Close", "Volume"} <= set(rows[0])


def test_history_unknown_symbol_returns_empty_list(monkeypatch):
    fake_charts(monkeypatch, now=ts(TUE, 11, 0))
    monkeypatch.setattr(server, "_yf_history", lambda *a: (_ for _ in ()).throw(yd.NotFound("no rows")))
    assert asyncio.run(server.get_historical_stock_prices("NOPE")) == "[]"


def test_news_falls_back_to_rss(monkeypatch):
    monkeypatch.setattr(server, "_yf_news", lambda s: (_ for _ in ()).throw(yd.RateLimited()))
    monkeypatch.setattr(yd, "_http_get", lambda url, params=None, timeout=None: (200, RSS))
    text = asyncio.run(server.get_yahoo_finance_news("MSGY"))
    for field in ("Title:", "Summary:", "Description:", "URL:"):
        assert field in text
    assert "Masonglory wins & expands" in text
    # cached: no second RSS request
    monkeypatch.setattr(yd, "_http_get", lambda *a, **k: (_ for _ in ()).throw(AssertionError("refetched")))
    assert asyncio.run(server.get_yahoo_finance_news("MSGY")) == text


def test_news_reports_rate_limit_when_every_source_fails(monkeypatch):
    monkeypatch.setattr(server, "_yf_news", lambda s: (_ for _ in ()).throw(yd.RateLimited()))
    monkeypatch.setattr(yd, "_http_get", lambda *a, **k: (_ for _ in ()).throw(yd.RateLimited()))
    text = asyncio.run(server.get_yahoo_finance_news("MSGY"))
    assert text.startswith("Error: getting news for MSGY:")
    assert "Too Many Requests" in text  # the dashboard classifies this as a rate limit


def test_validation_messages_are_unchanged():
    run = asyncio.run
    assert "Error: invalid financial type bogus" in run(server.get_financial_statement("AAPL", "bogus"))
    assert "Error: invalid holder type bogus" in run(server.get_holder_info("AAPL", "bogus"))
    assert "Error: invalid recommendation type bogus" in run(server.get_recommendations("AAPL", "bogus"))


def test_us_extended_hours_window():
    assert server._us_extended_hours(ts(TUE, 3, 45))
    assert server._us_extended_hours(ts(TUE, 20, 15))
    assert not server._us_extended_hours(ts(TUE, 21, 0))
    assert not server._us_extended_hours(ts(dt.date(2026, 10, 3), 12, 0))  # Saturday


def test_chart_uses_yfinance_session_when_direct_route_fails(monkeypatch):
    payload = {"chart": {"result": [{"meta": {"symbol": "MSGY"}, "timestamp": []}], "error": None}}
    monkeypatch.setattr(yd, "yahoo_json", lambda path, params: (_ for _ in ()).throw(yd.UpstreamError("TLS")))
    seen = []
    monkeypatch.setattr(yd, "yf_session_json", lambda url, params: seen.append(url) or payload)
    assert yd.fetch_chart("MSGY", range_="1d", interval="1m")["meta"]["symbol"] == "MSGY"
    assert seen and seen[0].endswith("/v8/finance/chart/MSGY")


def test_chart_rate_limit_is_not_retried_on_the_second_route(monkeypatch):
    monkeypatch.setattr(yd, "yahoo_json", lambda path, params: (_ for _ in ()).throw(yd.RateLimited()))
    monkeypatch.setattr(yd, "yf_session_json", lambda url, params: pytest.fail("same IP, no retry"))
    with pytest.raises(yd.RateLimited):
        yd.fetch_chart("MSGY", range_="1d", interval="1m")
    assert yd.BREAKERS["chart-direct"].snapshot()["state"] == "closed"


def test_breaker_does_not_escalate_for_requests_already_in_flight():
    b = yd.Breaker("t", base_cooldown=100, max_cooldown=10_000)
    b.failure(yd.RateLimited())
    first_retry = b.snapshot()["retryAt"]
    for _ in range(5):  # stragglers that started before the trip
        b.failure(yd.RateLimited())
    assert b.snapshot()["retryAt"] == first_retry and b.trips == 1


def test_local_back_pressure_does_not_trip_the_breaker():
    def busy():
        raise yd.Unavailable("server busy: too many upstream requests in flight")

    async def scenario():
        for _ in range(5):
            with pytest.raises(yd.Unavailable):
                await yd.cached_call(key="b", family="chart", fn=busy, fresh_ttl=1, stale_ttl=1, budget=5)

    asyncio.run(scenario())
    assert yd.BREAKERS["chart"].snapshot()["state"] == "closed"


class FakeTicker:
    """Stands in for yfinance.Ticker in the remaining tools."""

    calls: list = []

    def __init__(self, symbol):
        self.symbol = symbol
        FakeTicker.calls.append(symbol)

    @property
    def actions(self):
        idx = pd.DatetimeIndex([pd.Timestamp("2026-08-01", tz="America/New_York")], name="Date")
        return pd.DataFrame({"Dividends": [0.0], "Stock Splits": [0.125]}, index=idx)

    @property
    def quarterly_income_stmt(self):
        return pd.DataFrame({pd.Timestamp("2026-06-30"): [1.5e6, float("nan")]},
                            index=["TotalRevenue", "NetIncome"])

    @property
    def major_holders(self):
        return pd.DataFrame({"Value": [0.35]}, index=["insidersPercentHeld"])

    @property
    def institutional_holders(self):
        return pd.DataFrame({"Holder": ["Fund A"], "Shares": [1000]})

    @property
    def options(self):
        return ("2026-10-16", "2026-10-23")

    def option_chain(self, date):
        chain = pd.DataFrame({"strike": [5.0, 10.0, 15.0], "bid": [5.1, 0.9, 0.1],
                              "ask": [5.3, 1.0, 0.2], "impliedVolatility": [1.1, 0.9, 1.3]})

        class OC:
            calls = chain
            puts = chain

        return OC()

    @property
    def recommendations(self):
        return pd.DataFrame({"period": ["0m"], "strongBuy": [1]})

    @property
    def upgrades_downgrades(self):
        idx = pd.DatetimeIndex([pd.Timestamp.now() - pd.Timedelta(days=30),
                                pd.Timestamp.now() - pd.Timedelta(days=900)], name="GradeDate")
        return pd.DataFrame({"Firm": ["A", "B"], "ToGrade": ["Buy", "Hold"]}, index=idx)



def test_remaining_tools_keep_their_output_format(monkeypatch):
    monkeypatch.setattr(server.yf, "Ticker", FakeTicker)
    run = asyncio.run
    actions = json.loads(run(server.get_stock_actions("msgy")))
    assert actions[0]["Stock Splits"] == 0.125 and "Date" in actions[0]
    stmt = json.loads(run(server.get_financial_statement("MSGY", "quarterly_income_stmt")))
    assert stmt == [{"date": "2026-06-30", "TotalRevenue": 1.5e6, "NetIncome": None}]
    major = json.loads(run(server.get_holder_info("MSGY", "major_holders")))
    assert major[0]["metric"] == "insidersPercentHeld"
    assert json.loads(run(server.get_holder_info("MSGY", "institutional_holders")))[0]["Holder"] == "Fund A"
    assert json.loads(run(server.get_option_expiration_dates("MSGY"))) == ["2026-10-16", "2026-10-23"]
    assert "No options available" in run(server.get_option_chain("MSGY", "1999-01-01", "calls"))
    assert "Invalid option type" in run(server.get_option_chain("MSGY", "2026-10-16", "straddles"))
    monkeypatch.setattr(server, "_spot_price", lambda s: asyncio.sleep(0, result=10.0))
    windowed = json.loads(run(server.get_option_chain("MSGY", "2026-10-16", "calls", 0.2, ["bid"])))
    assert windowed == [{"strike": 10.0, "bid": 0.9}]
    assert json.loads(run(server.get_recommendations("MSGY", "recommendations")))[0]["strongBuy"] == 1
    grades = json.loads(run(server.get_recommendations("MSGY", "upgrades_downgrades", 12)))
    assert [g["Firm"] for g in grades] == ["A"]
    calls_before = len(FakeTicker.calls)
    run(server.get_financial_statement("MSGY", "quarterly_income_stmt"))  # cached
    assert len(FakeTicker.calls) == calls_before


def test_cache_is_bounded():
    c = yd.TTLCache(max_entries=50, max_age=100)
    for i in range(200):
        c.set(f"k{i}", i)
    assert len(c) <= 50 and c.get("k199").value == 199
    c._data["old"] = ("x", time.time() - 1000)
    for i in range(256):
        c.set(f"n{i}", i)
    assert c.get("old") is None
