"""Offline tests for compact quotes, levels and market movers (no network)."""

import asyncio
import datetime as dt
import json

import pytest

import market
import server
import yahoo_data as yd
from test_resilience import TUE, intraday, trading_days, ts

WED = dt.date(2026, 9, 30)


@pytest.fixture(autouse=True)
def fresh_state():
    yd.CACHE._data.clear()
    for b in yd.BREAKERS.values():
        b.success()
        b.trips = 0
    yield
    yd.CACHE._data.clear()


def daily_hl(rows):
    """Daily chart with explicit (date, open, high, low, close, volume)."""
    return {
        "meta": {"symbol": "MSGY", "exchangeTimezoneName": "America/New_York"},
        "timestamp": [ts(d, 9, 30) for d, *_ in rows],
        "indicators": {"quote": [{
            "open": [r[1] for r in rows], "high": [r[2] for r in rows], "low": [r[3] for r in rows],
            "close": [r[4] for r in rows], "volume": [r[5] for r in rows]}]},
    }


def history_until(day, n=20):
    rows = []
    for i, d in enumerate(trading_days(day, n)):
        c = 10.0 + i * 0.1
        rows.append((d, c - 0.05, c + 0.5, c - 0.5, c, 1_000_000))
    return rows


INFO = {"symbol": "MSGY", "quoteType": "EQUITY", "floatShares": 800_000, "sharesOutstanding": 2_000_000,
        "marketCap": 9_000_000, "shortPercentOfFloat": 0.15, "quoteSourceName": "Nasdaq Real Time Price"}


def test_pre_market_quote_has_pre_levels_rotation_and_no_regular_rvol():
    # yesterday (Tuesday) closed at 11.9; Wednesday pre-market trades 12.5 then 13.0
    hist = history_until(TUE)
    bars = [(ts(WED, 4, 0), 12.5, 100_000), (ts(WED, 4, 31), 13.0, 300_000)]
    chart = intraday(WED, bars, price=11.9, rmt=ts(TUE, 16, 0), chart_prev=11.8)
    q = market.compact_quote("MSGY", chart, daily_hl(hist), INFO, 30.0, now=ts(WED, 4, 32))
    assert q["session"] == "pre" and q["priceSession"] == "pre"
    assert q["price"] == 13.0 and q["reference"] == 11.9
    assert q["changePct"] == pytest.approx((13.0 / 11.9 - 1) * 100, abs=1e-3)
    lv = q["levels"]
    assert lv["preHigh"] == pytest.approx(13.05) and lv["preLow"] == pytest.approx(12.45)
    # previous session = last daily bar before Wednesday = Tuesday's bar
    assert lv["prevDate"] == str(TUE) and lv["prevHigh"] == pytest.approx(hist[-1][2])
    assert lv["atr14"] == pytest.approx(1.0, abs=1e-6)  # every true range is high-low = 1.0
    assert "vwap" not in lv and "rvol" not in q
    assert q["preVolume"] == 400_000
    assert q["floatRotation"] == pytest.approx(0.5)  # 400k / 800k float
    assert q["preVolumeVsAvg"] == pytest.approx(0.4)
    assert q["spark"]["step"] == 600 and q["spark"]["c"][-1] == 13.0
    assert q["quoteSource"] == "Nasdaq Real Time Price"


def test_pre_market_without_trades_reports_no_change():
    chart = intraday(WED, [], price=11.9, rmt=ts(TUE, 16, 0), chart_prev=11.8)
    q = market.compact_quote("MSGY", chart, daily_hl(history_until(TUE)), None, None, now=ts(WED, 4, 5))
    assert q["session"] == "pre" and q["price"] == 11.9 and q["changePct"] is None


def test_regular_session_vwap_opening_range_and_rvol():
    hist = history_until(TUE)
    bars = [
        (ts(WED, 8, 0), 10.0, 50_000),  # pre-market
        (ts(WED, 9, 30), 10.0, 100_000),
        (ts(WED, 9, 33), 11.0, 100_000),
        (ts(WED, 9, 40), 12.0, 200_000),
    ]
    chart = intraday(WED, bars, price=12.0, rmt=ts(WED, 9, 40), chart_prev=9.8, regularMarketVolume=400_000)
    q = market.compact_quote("MSGY", chart, daily_hl(hist), INFO, 30.0, now=ts(WED, 9, 41))
    assert q["session"] == "regular" and q["price"] == 12.0
    lv = q["levels"]
    # typical price = close (high/low are close +/- 0.05): (10*1 + 11*1 + 12*2) / 4 = 11.25
    assert lv["vwap"] == pytest.approx(11.25)
    assert lv["orHigh"] == pytest.approx(11.05) and lv["orLow"] == pytest.approx(9.95)
    assert q["rvol"] == pytest.approx(0.4)  # 400k regular volume / 1M average
    assert q["volumeAllSessions"] == 450_000
    assert q["floatRotation"] == pytest.approx(450_000 / 800_000, abs=1e-3)
    assert q["vsVwapPct"] == pytest.approx((12.0 / 11.25 - 1) * 100, abs=1e-3)
    assert q["dollarVolume"] == int(400_000 * 11.25)


def test_post_market_price_and_levels():
    hist = history_until(TUE)
    bars = [(ts(WED, 15, 59), 12.0, 100_000), (ts(WED, 16, 30), 12.6, 20_000), (ts(WED, 17, 0), 12.4, 10_000)]
    chart = intraday(WED, bars, price=12.0, rmt=ts(WED, 16, 0), chart_prev=11.0)
    q = market.compact_quote("MSGY", chart, daily_hl(hist + [(WED, 11, 12.2, 10.9, 12.0, 900_000)]),
                             None, None, now=ts(WED, 17, 5))
    assert q["session"] == "post" and q["price"] == 12.4 and q["priceSession"] == "post"
    assert q["changePct"] == pytest.approx((12.4 / 11.0 - 1) * 100, abs=1e-3)
    assert q["postMarket"]["price"] == 12.4
    assert q["levels"]["postHigh"] == pytest.approx(12.65)
    # the Wednesday daily bar is the session in progress, so "previous" is still Tuesday
    assert q["levels"]["prevDate"] == str(TUE)


def test_paced_rvol_during_the_session_and_after_the_close():
    hist = history_until(TUE)  # every completed day traded 1,000,000 shares
    bars = [(ts(WED, 9, 30), 10.0, 100_000), (ts(WED, 9, 59), 11.0, 140_000)]
    chart = intraday(WED, bars, price=11.0, rmt=ts(WED, 9, 59), chart_prev=9.8, regularMarketVolume=240_000)
    q = market.compact_quote("MSGY", chart, daily_hl(hist), INFO, 30.0, now=ts(WED, 10, 0))
    # 30 minutes after the first regular bar a typical day has traded 15% of its volume
    assert market.volume_share(30) == pytest.approx(0.15)
    assert q["rvol"] == pytest.approx(0.24)
    assert q["rvolPace"] == pytest.approx(240_000 / (1_000_000 * 0.15), abs=1e-3)
    # the first minutes are too few to pace
    early = market.compact_quote("MSGY", chart, daily_hl(hist), INFO, 30.0, now=ts(WED, 9, 33))
    assert "rvolPace" not in early or early["session"] != "regular"
    # after the close the paced figure is the plain relative volume
    closed = [(ts(WED, 15, 59), 12.0, 100_000), (ts(WED, 16, 30), 12.6, 20_000)]
    post = market.compact_quote("MSGY", intraday(WED, closed, price=12.0, rmt=ts(WED, 16, 0), chart_prev=11.0),
                                daily_hl(hist), None, None, now=ts(WED, 17, 5))
    assert post["rvolPace"] == post["rvol"]


def test_volume_profile_is_monotonic_and_complete():
    shares = [market.volume_share(m) for m in range(0, 400, 5)]
    assert shares == sorted(shares)
    assert market.volume_share(0) == 0.0 and market.volume_share(390) == 1.0 and market.volume_share(500) == 1.0


def test_category_fields_come_from_the_chart_and_quote_summary():
    info = dict(INFO, sector="Healthcare", industry="Biotechnology", country="Hong Kong",
                lastSplitFactor="1:8", lastSplitDate=1786406400, earningsTimestamp=1790798400,
                isEarningsDateEstimate=False)
    chart = intraday(WED, [(ts(WED, 9, 30), 10.0, 100_000)], price=10.0, rmt=ts(WED, 9, 30), chart_prev=9.8)
    q = market.compact_quote("MSGY", chart, daily_hl(history_until(TUE)), info, 30.0, now=ts(WED, 9, 45))
    assert (q["sector"], q["industry"], q["country"]) == ("Healthcare", "Biotechnology", "Hong Kong")
    assert q["lastSplit"] == {"factor": "1:8", "date": "2026-08-11"}
    assert q["earningsDate"] == "2026-09-30T20:00:00Z" and q["earningsDateEstimate"] is False
    assert q["firstTradeDate"] == "2024-12-27"  # the chart's firstTradeDate
    # without quoteSummary only the chart's first trade date is known
    bare = market.compact_quote("MSGY", chart, daily_hl(history_until(TUE)), None, None, now=ts(WED, 9, 45))
    assert bare["firstTradeDate"] == "2024-12-27"
    assert not {"sector", "industry", "country", "lastSplit", "earningsDate"} & set(bare)


def test_atr14_matches_hand_computation():
    rows = []
    days = trading_days(TUE, 15)
    for i, d in enumerate(days):
        rows.append((d, 10, 10 + (i % 3), 9, 9.5 + (i % 2), 100))
    bars = yd.chart_bars(daily_hl(rows))
    trs = []
    for prev, cur in zip(bars[:-1], bars[1:]):
        trs.append(max(cur.high - cur.low, abs(cur.high - prev.close), abs(cur.low - prev.close)))
    assert market.atr14(bars) == pytest.approx(sum(trs[-14:]) / 14)
    assert market.atr14(bars[:14]) is None


SCREEN = {
    "finance": {
        "result": [{
            "id": "day_gainers", "title": "Day Gainers", "total": 2,
            "quotes": [
                {"symbol": "AAA", "shortName": "Alpha", "fullExchangeName": "NasdaqCM",
                 "regularMarketPrice": {"raw": 2.5, "fmt": "2.50"}, "regularMarketChangePercent": {"raw": 40.0},
                 "regularMarketVolume": 3_000_000, "averageDailyVolume3Month": 1_000_000, "marketCap": 50_000_000,
                 "preMarketPrice": 2.7, "preMarketChangePercent": 8.0},
                {"symbol": "BBB", "shortName": "Beta", "fullExchangeName": "NYSE",
                 "regularMarketPrice": 10.0, "regularMarketChangePercent": 12.5, "regularMarketVolume": 500_000},
            ],
        }],
        "error": None,
    }
}


def test_parse_screener_handles_raw_and_formatted_numbers():
    data = market.parse_screener(SCREEN)
    assert data["title"] == "Day Gainers"
    a, b = data["quotes"]
    assert a["symbol"] == "AAA" and a["price"] == 2.5 and a["changePct"] == 40.0 and a["rvol"] == 3.0
    assert a["preMarketPrice"] == 2.7 and b["rvol"] is None
    with pytest.raises(yd.UpstreamError):
        market.parse_screener({"finance": {"result": None, "error": {"code": "Unauthorized", "description": "Invalid Crumb"}}})


def test_parse_trending_and_symbol_filters():
    payload = {"finance": {"result": [{"quotes": [{"symbol": "NVDA"}, {"symbol": "ES=F"}, {"symbol": "BTC-USD"},
                                                   {"symbol": "NVDA"}, {"symbol": "^VIX"}]}], "error": None}}
    syms = market.parse_trending(payload)
    assert syms == ["NVDA", "ES=F", "BTC-USD", "^VIX"]
    assert [s for s in syms if market.is_plain_us_stock(s)] == ["NVDA"]
    assert market.is_nasdaq("NasdaqGS") and market.is_nasdaq("NMS") and not market.is_nasdaq("NYSE")


def fake_market(monkeypatch, now):
    hist = history_until(TUE, 30)
    pre = intraday(WED, [(ts(WED, 4, 0), 12.5, 100_000), (ts(WED, 4, 31), 13.0, 300_000)],
                   price=11.9, rmt=ts(TUE, 16, 0), chart_prev=11.8)
    calls = []

    def fetch_chart(symbol, *, range_, interval, prepost=False, events="div,splits"):
        calls.append((symbol, range_))
        if symbol == "NOPE":
            raise yd.NotFound("No data found, symbol may be delisted")
        if range_ == "1d":
            chart = json.loads(json.dumps(pre))
            chart["meta"]["symbol"] = symbol
            return chart
        return daily_hl(hist)

    monkeypatch.setattr(yd, "fetch_chart", fetch_chart)
    monkeypatch.setattr(server, "_fetch_info", lambda s: dict(INFO, symbol=s))
    monkeypatch.setattr(yd.time, "time", lambda: now)
    monkeypatch.setattr(market.time, "time", lambda: now)
    return calls


def test_get_quotes_batches_symbols_and_reports_unknown_ones(monkeypatch):
    calls = fake_market(monkeypatch, now=ts(WED, 4, 32))
    out = json.loads(asyncio.run(server.get_quotes("msgy, QQQ nope,MSGY", spark=False)))
    assert out["count"] == 2
    assert [q["symbol"] for q in out["quotes"]] == ["MSGY", "QQQ"]
    assert out["errors"] == {"NOPE": "not found on Yahoo"}
    q = out["quotes"][0]
    assert q["session"] == "pre" and q["price"] == 13.0 and "spark" not in q
    assert q["levels"]["preHigh"] == pytest.approx(13.05)
    assert q["dataStatus"]["quote"] == "live" and q["dataStatus"]["fundamentals"] == "fresh"
    assert not any(r == "max" for _, r in calls)  # no split-history request for batch quotes
    # a second call inside the freshness window is served from cache
    before = len(calls)
    asyncio.run(server.get_quotes("MSGY"))
    assert len(calls) == before


def test_get_quotes_rejects_empty_input():
    assert asyncio.run(server.get_quotes("  ,, ")).startswith("Error")


def test_market_movers_screen_premarket_and_filters(monkeypatch):
    fake_market(monkeypatch, now=ts(WED, 4, 32))
    monkeypatch.setattr(market, "fetch_screener", lambda scr, n: market.parse_screener(SCREEN))
    monkeypatch.setattr(market, "fetch_trending", lambda n: ["ZZZ", "ES=F", "AAA"])

    out = json.loads(asyncio.run(server.get_market_movers("day_gainers", 10)))
    assert out["title"] == "Day Gainers" and [r["symbol"] for r in out["quotes"]] == ["AAA", "BBB"]
    out = json.loads(asyncio.run(server.get_market_movers("day_gainers", 10, nasdaq_only=True)))
    assert [r["symbol"] for r in out["quotes"]] == ["AAA"]

    out = json.loads(asyncio.run(server.get_market_movers("premarket", 10)))
    assert out["universe"] == 3  # ZZZ, AAA, BBB (futures dropped, duplicates merged)
    assert {r["symbol"] for r in out["quotes"]} == {"ZZZ", "AAA", "BBB"}
    assert all(r["session"] == "pre" and r["changePct"] is not None for r in out["quotes"])
    assert "levels" in out["quotes"][0]

    assert asyncio.run(server.get_market_movers("nonsense")).startswith("Error: unknown screen")


def test_market_movers_reports_upstream_failure(monkeypatch):
    def broken(scr, n):
        raise yd.RateLimited()

    monkeypatch.setattr(market, "fetch_screener", broken)
    text = asyncio.run(server.get_market_movers("most_actives"))
    assert text.startswith("Error") and "Too Many Requests" in text


def test_parse_tickers():
    assert server._parse_tickers("aapl, brk.b;QQQ  qqq|^VIX") == ["AAPL", "BRK-B", "QQQ", "^VIX"]
