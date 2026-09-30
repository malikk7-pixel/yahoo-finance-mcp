"""Compact quotes with intraday levels, and market movers.

A dashboard that watches 25 symbols used to make 50 MCP calls a minute (a
quote and a candle request per symbol). ``compact_quote`` turns the chart data
that ``get_stock_info`` already caches into one small record per symbol, with
the levels a day trader reads first:

* session-aware price and change (pre-market, regular, after-hours);
* pre-market high / low / volume and VWAP of the regular session;
* opening range (first five minutes), previous session high / low;
* ATR(14) from completed daily sessions;
* relative volume, paced relative volume (against what a typical day has
  traded by this time) and float rotation (volume / free float);
* the fields that place a stock in a category: sector, industry, country,
  first trade date, last split and the earnings date;
* a 10-minute sparkline of the whole trading day.

Everything here is a pure function of chart / quoteSummary payloads, so it is
tested offline; network access stays in ``yahoo_data``.
"""

from __future__ import annotations

import datetime as dt
import time
from typing import Any, Iterable

import yahoo_data as yd

SPARK_STEP = 600  # seconds per sparkline bucket
SPARK_MAX = 120  # at most 20 hours of 10-minute buckets
OPENING_RANGE_SECONDS = 300

# Share of a typical regular session's volume that has traded N minutes after
# the open: a smoothed U-shaped profile of US equities (a heavy first hour, a
# quiet midday, a heavy last half hour with the closing auction). It paces
# relative volume during the day; it is an estimate, not a measured curve.
VOLUME_PROFILE = (
    (0, 0.0), (15, 0.09), (30, 0.15), (60, 0.24), (90, 0.31), (150, 0.42),
    (210, 0.51), (270, 0.60), (330, 0.71), (360, 0.79), (380, 0.88), (390, 1.0),
)
PACE_MIN_MINUTES = 5  # too few minutes make the paced figure meaningless


def volume_share(minutes: float) -> float:
    """Expected share of a full regular session's volume after ``minutes`` of trading."""
    if minutes <= 0:
        return 0.0
    for (m0, s0), (m1, s1) in zip(VOLUME_PROFILE, VOLUME_PROFILE[1:]):
        if minutes <= m1:
            return s0 + (s1 - s0) * (minutes - m0) / (m1 - m0)
    return 1.0


def _iso_day(seconds: float | None) -> str | None:
    if not seconds:
        return None
    return dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc).date().isoformat()

# Yahoo's predefined screeners that make sense for a US day trader.
SCREENS = {
    "day_gainers": "day_gainers",
    "day_losers": "day_losers",
    "most_actives": "most_actives",
    "small_cap_gainers": "small_cap_gainers",
    "aggressive_small_caps": "aggressive_small_caps",
    "most_shorted_stocks": "most_shorted_stocks",
}
COMPOSITE_SCREENS = ("trending", "premarket")


def rnd(x: float | None) -> float | None:
    """Round prices for transport: 3 decimals from $1, 5 below."""
    if x is None:
        return None
    return round(x, 3 if abs(x) >= 1 else 5)


def _day(ts: float | None, tz: dt.tzinfo) -> dt.date | None:
    if ts is None:
        return None
    return dt.datetime.fromtimestamp(ts, tz=tz).date()


def _vwap(bars: Iterable[yd.Bar]) -> tuple[float | None, int]:
    pv = vol = 0.0
    for b in bars:
        v = b.volume or 0
        if v <= 0:
            continue
        hi = b.high if b.high is not None else b.close
        lo = b.low if b.low is not None else b.close
        pv += (hi + lo + b.close) / 3.0 * v
        vol += v
    return (pv / vol if vol else None), int(vol)


def _hi_lo(bars: list[yd.Bar]) -> tuple[float | None, float | None]:
    highs = [b.high if b.high is not None else b.close for b in bars]
    lows = [b.low if b.low is not None else b.close for b in bars]
    return (max(highs) if highs else None), (min(lows) if lows else None)


def atr14(daily_bars: list[yd.Bar]) -> float | None:
    """Simple average of the last 14 true ranges (needs 15 bars)."""
    usable = [b for b in daily_bars if b.high is not None and b.low is not None]
    if len(usable) < 15:
        return None
    trs = []
    for prev, cur in zip(usable[-15:-1], usable[-14:]):
        trs.append(max(cur.high - cur.low, abs(cur.high - prev.close), abs(cur.low - prev.close)))
    return sum(trs) / len(trs)


def spark(bars: list[yd.Bar], step: int = SPARK_STEP) -> dict[str, Any] | None:
    """Last close in each ``step``-second bucket, gaps as null."""
    if not bars:
        return None
    start = bars[0].ts - bars[0].ts % step
    closes: list[float | None] = []
    for b in bars:
        idx = (b.ts - start) // step
        if idx >= SPARK_MAX:
            break
        while len(closes) <= idx:
            closes.append(None)
        closes[idx] = rnd(b.close)
    return {"start": start, "step": step, "c": closes}


def compact_quote(
    symbol: str,
    intraday: dict[str, Any] | None,
    daily: dict[str, Any] | None,
    info: dict[str, Any] | None = None,
    info_age: float | None = None,
    now: float | None = None,
    with_spark: bool = True,
) -> dict[str, Any]:
    """One small quote record with the day's levels (see module docstring)."""
    now = time.time() if now is None else now
    live = yd.build_live_quote(symbol, intraday, daily, None, now=now) if intraday else None
    q = yd.merge_quote(live, info, info_age)
    meta = (intraday or {}).get("meta") or {}
    tz = yd._tz(meta.get("exchangeTimezoneName") or q.get("exchangeTimezoneName"))

    state = str(q.get("marketState") or "CLOSED").upper()
    session = {"PRE": "pre", "REGULAR": "regular", "POST": "post"}.get(state, "closed")
    reg = yd.num(q.get("regularMarketPrice"))
    prev = yd.num(q.get("regularMarketPreviousClose"))
    pre, post = yd.num(q.get("preMarketPrice")), yd.num(q.get("postMarketPrice"))
    reg_t = yd.num(q.get("regularMarketTime"))
    pre_t, post_t = yd.num(q.get("preMarketTime")), yd.num(q.get("postMarketTime"))

    # Headline price and change, as a trader reads them in each session.
    price, ref, quote_time, label = reg, prev, reg_t, "regular"
    if session == "pre":
        ref = reg  # the last close is the reference before the open
        if pre is not None:
            price, quote_time, label = pre, pre_t, "pre"
    elif session == "post" and post is not None and (reg_t is None or post_t is None or post_t >= reg_t):
        price, quote_time, label = post, post_t, "post"
    change_pct = (price / ref - 1.0) * 100.0 if price is not None and ref else None
    if session == "pre" and pre is None:
        change_pct = None  # no pre-market trade yet: do not invent a move

    out: dict[str, Any] = {
        "symbol": q.get("symbol") or symbol,
        "name": q.get("shortName") or q.get("longName") or "",
        "exchange": q.get("fullExchangeName") or q.get("exchange") or "",
        "quoteType": q.get("quoteType"),
        "marketState": state,
        "session": session,
        "price": rnd(price),
        "priceSession": label,
        "reference": rnd(ref),
        "changePct": round(change_pct, 3) if change_pct is not None else None,
        "quoteTime": int(quote_time) if quote_time else None,
        "regularPrice": rnd(reg),
        "prevClose": rnd(prev),
        "regularChangePct": round((reg / prev - 1.0) * 100.0, 3) if reg is not None and prev else None,
        "open": rnd(yd.num(q.get("regularMarketOpen"))),
        "dayHigh": rnd(yd.num(q.get("regularMarketDayHigh"))),
        "dayLow": rnd(yd.num(q.get("regularMarketDayLow"))),
        "volume": int(yd.num(q.get("regularMarketVolume")) or 0) or None,
        "avgVolume": int(yd.num(q.get("averageVolume")) or 0) or None,
        "avgVolume10d": int(yd.num(q.get("averageVolume10days")) or 0) or None,
        "high52": rnd(yd.num(q.get("fiftyTwoWeekHigh"))),
        "low52": rnd(yd.num(q.get("fiftyTwoWeekLow"))),
        "floatShares": int(yd.num(q.get("floatShares")) or 0) or None,
        "sharesOutstanding": int(yd.num(q.get("sharesOutstanding")) or 0) or None,
        "marketCap": int(yd.num(q.get("marketCap")) or 0) or None,
        "shortPctFloat": yd.num(q.get("shortPercentOfFloat")),
        "quoteSource": q.get("quoteSourceName"),
    }
    if session == "post" or (session == "closed" and post is not None):
        out["postMarket"] = {"price": rnd(post), "time": int(post_t) if post_t else None,
                             "changePct": round((post / reg - 1.0) * 100.0, 3) if post and reg else None}
    if pre is not None:
        out["preMarket"] = {"price": rnd(pre), "time": int(pre_t) if pre_t else None,
                            "changePct": round((pre / reg - 1.0) * 100.0, 3) if reg else None}

    # Intraday levels from the 1-minute bars of the chart's trading day.
    bars = yd.chart_bars(intraday) if intraday else []
    session_day = _day(bars[-1].ts, tz) if bars else None
    day_bars = [b for b in bars if _day(b.ts, tz) == session_day]
    pre_w, reg_w, post_w = (yd._windows(meta, k) for k in ("pre", "regular", "post"))
    pre_b = [b for b in day_bars if yd._in_any(b.ts, pre_w)]
    reg_b = [b for b in day_bars if yd._in_any(b.ts, reg_w)]
    post_b = [b for b in day_bars if yd._in_any(b.ts, post_w)]
    levels: dict[str, Any] = {}
    if pre_b:
        hi, lo = _hi_lo(pre_b)
        vw, vol = _vwap(pre_b)
        levels.update(preHigh=rnd(hi), preLow=rnd(lo), preVwap=rnd(vw))
        out["preVolume"] = vol
    if reg_b:
        vw, _ = _vwap(reg_b)
        levels["vwap"] = rnd(vw)
        first = reg_b[0].ts
        opening = [b for b in reg_b if b.ts < first + OPENING_RANGE_SECONDS]
        hi, lo = _hi_lo(opening)
        levels.update(orHigh=rnd(hi), orLow=rnd(lo))
    if post_b:
        hi, lo = _hi_lo(post_b)
        _, vol = _vwap(post_b)
        levels.update(postHigh=rnd(hi), postLow=rnd(lo))
        out["postVolume"] = vol

    # Previous completed session and ATR from the daily bars.
    daily_bars = yd.chart_bars(daily) if daily else []
    if session_day is not None:
        done = [b for b in daily_bars if (_day(b.ts, tz) or session_day) < session_day]
    else:
        done = daily_bars[:-1]
    if done:
        last = done[-1]
        levels.update(prevDate=str(_day(last.ts, tz)), prevHigh=rnd(last.high), prevLow=rnd(last.low),
                      prevDayClose=rnd(last.close))
        a = atr14(done)
        if a is not None:
            levels["atr14"] = rnd(a)
            base = price if price is not None else last.close
            if base:
                levels["atr14Pct"] = round(a / base * 100.0, 2)
        recent = done[-20:]
        if len(recent) >= 5:
            hi, lo = _hi_lo(recent)
            levels.update(high20=rnd(hi), low20=rnd(lo))
    out["levels"] = levels
    out["sessionDate"] = str(session_day) if session_day else None

    # Relative volume and float rotation for the chart's trading day.
    reg_vol_today = 0
    if reg_b:
        meta_vol = yd.num(meta.get("regularMarketVolume"))
        reg_vol_today = int(meta_vol) if meta_vol and _day(reg_t, tz) == session_day else _vwap(reg_b)[1]
    all_vol = (out.get("preVolume") or 0) + reg_vol_today + (out.get("postVolume") or 0)
    out["volumeAllSessions"] = all_vol or None
    avg = out.get("avgVolume")
    if avg:
        if reg_b:
            out["rvol"] = round(reg_vol_today / avg, 3)
            # Paced relative volume: today's regular volume against what a typical
            # day has traded by this time. After the close it equals rvol.
            if session == "regular":
                minutes = (now - reg_b[0].ts) / 60.0
                share = volume_share(minutes)
                if minutes >= PACE_MIN_MINUTES and share > 0:
                    out["rvolPace"] = round(reg_vol_today / (avg * share), 3)
            else:
                out["rvolPace"] = out["rvol"]
        if out.get("preVolume"):
            out["preVolumeVsAvg"] = round(out["preVolume"] / avg, 4)
    flt = out.get("floatShares")
    if flt and all_vol:
        out["floatRotation"] = round(all_vol / flt, 3)
    px = price if price is not None else reg
    if px and reg_vol_today:
        out["dollarVolume"] = int(reg_vol_today * (levels.get("vwap") or px))
    vwap = levels.get("vwap")
    if vwap and price is not None and session in ("regular", "post"):
        out["vsVwapPct"] = round((price / vwap - 1.0) * 100.0, 3)

    # What a dashboard needs to place the stock in a category (small biotech,
    # recent IPO, foreign small cap, reverse split, earnings day, ...). Sector,
    # industry, country and the earnings date come from quoteSummary, so they
    # are missing while Yahoo rate-limits that endpoint.
    out["firstTradeDate"] = _iso_day((yd.num(q.get("firstTradeDateMilliseconds")) or 0) / 1000.0)
    for key in ("sector", "industry", "country"):
        if q.get(key):
            out[key] = str(q[key])
    split_t = yd.num(q.get("lastSplitDate"))
    if q.get("lastSplitFactor") and split_t:
        out["lastSplit"] = {"factor": str(q["lastSplitFactor"]), "date": _iso_day(split_t)}
    earn = yd.num(q.get("earningsTimestamp")) or yd.num(q.get("earningsTimestampStart"))
    if earn:
        out["earningsDate"] = dt.datetime.fromtimestamp(earn, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        out["earningsDateEstimate"] = bool(q.get("isEarningsDateEstimate"))

    if with_spark:
        out["spark"] = spark(day_bars)
    return out


# --------------------------------------------------------------------------
# Market movers
# --------------------------------------------------------------------------


def _raw(value: Any) -> Any:
    """Values from formatted=true responses come as {"raw": ..., "fmt": ...}."""
    if isinstance(value, dict) and "raw" in value:
        return value.get("raw")
    return value


def _finance_result(payload: Any) -> dict[str, Any]:
    fin = payload.get("finance") if isinstance(payload, dict) else None
    if not isinstance(fin, dict):
        raise yd.UpstreamError("unexpected screener response")
    err = fin.get("error")
    if isinstance(err, dict) and (err.get("code") or err.get("description")):
        raise yd.UpstreamError(f"{err.get('code')}: {err.get('description')}")
    result = fin.get("result") or []
    if not result or not isinstance(result[0], dict):
        raise yd.UpstreamError("empty screener result")
    return result[0]


def parse_screener(payload: Any) -> dict[str, Any]:
    """Quotes of a predefined Yahoo screener, as compact rows."""
    res = _finance_result(payload)
    rows = []
    for item in res.get("quotes") or []:
        if not isinstance(item, dict) or not item.get("symbol"):
            continue
        g = lambda k: yd.num(_raw(item.get(k)))  # noqa: E731
        vol, avg = g("regularMarketVolume"), g("averageDailyVolume3Month")
        rows.append({
            "symbol": item.get("symbol"),
            "name": item.get("shortName") or item.get("longName") or "",
            "exchange": item.get("fullExchangeName") or item.get("exchange") or "",
            "price": rnd(g("regularMarketPrice")),
            "changePct": round(g("regularMarketChangePercent"), 3) if g("regularMarketChangePercent") is not None else None,
            "volume": int(vol) if vol else None,
            "avgVolume": int(avg) if avg else None,
            "rvol": round(vol / avg, 3) if vol and avg else None,
            "marketCap": int(g("marketCap")) if g("marketCap") else None,
            "preMarketPrice": rnd(g("preMarketPrice")),
            "preMarketChangePct": round(g("preMarketChangePercent"), 3) if g("preMarketChangePercent") is not None else None,
            "postMarketPrice": rnd(g("postMarketPrice")),
            "postMarketChangePct": round(g("postMarketChangePercent"), 3) if g("postMarketChangePercent") is not None else None,
            "quoteTime": int(g("regularMarketTime")) if g("regularMarketTime") else None,
        })
    return {"title": res.get("title") or res.get("id") or "", "total": res.get("total"), "quotes": rows}


def parse_trending(payload: Any) -> list[str]:
    res = _finance_result(payload)
    out = []
    for item in res.get("quotes") or []:
        sym = item.get("symbol") if isinstance(item, dict) else None
        if isinstance(sym, str) and sym and sym not in out:
            out.append(sym)
    return out


def is_nasdaq(exchange: str | None) -> bool:
    e = (exchange or "").lower()
    return "nasdaq" in e or e in ("nms", "ngm", "ncm", "nas", "nyq_nas")


def is_plain_us_stock(symbol: str) -> bool:
    """Skip futures, indexes, currencies and crypto pairs in trending lists."""
    return bool(symbol) and not any(c in symbol for c in "=^") and not symbol.endswith("-USD")


def fetch_screener(scr_id: str, count: int) -> dict[str, Any]:
    """Predefined screener: crumb-free route first, yfinance session second."""
    path = "/v1/finance/screener/predefined/saved"
    params = {"scrIds": scr_id, "count": count, "formatted": "false", "lang": "en-US", "region": "US"}
    try:
        return parse_screener(yd.yahoo_json(path, params))
    except yd.RateLimited:
        raise
    except yd.UpstreamError as exc:
        yd.log.info("direct screener request failed (%s); retrying through yfinance", exc)
    return parse_screener(yd.yf_session_json(yd._HOSTS[0] + path, params))


def fetch_trending(count: int) -> list[str]:
    path = "/v1/finance/trending/US"
    params = {"count": count, "lang": "en-US", "region": "US"}
    try:
        return parse_trending(yd.yahoo_json(path, params))
    except yd.RateLimited:
        raise
    except yd.UpstreamError as exc:
        yd.log.info("direct trending request failed (%s); retrying through yfinance", exc)
    return parse_trending(yd.yf_session_json(yd._HOSTS[0] + path, params))
