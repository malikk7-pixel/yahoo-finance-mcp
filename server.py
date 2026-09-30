"""Yahoo Finance MCP server.

Tools, argument names and output formats are unchanged from the original
server. What changed is how data is fetched (see ``yahoo_data.py``):

* every Yahoo call runs in a worker pool with a time budget, so one slow
  request no longer freezes the server;
* results are cached, concurrent identical requests share one fetch, and the
  last good result is served when Yahoo fails;
* ``get_stock_info`` builds the live quote from Yahoo's chart API, which keeps
  working when the ``quoteSummary`` endpoint behind ``Ticker.info`` answers
  "Too Many Requests"; fundamentals from ``quoteSummary`` are merged in when
  available and cached for hours;
* news falls back from yfinance to Yahoo's RSS feed and search API;
* endpoints that answer 429 are backed off automatically (circuit breakers).

Transports: stdio (default when run locally), or HTTP when ``PORT`` is set
(Render). Over HTTP the server speaks both legacy SSE (``GET /sse`` +
``POST /messages/``) and Streamable HTTP (``POST /mcp``, and ``POST /sse`` so
existing connector URLs keep working). ``GET /health`` reports status.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
import math
import os
import re
import time
import urllib.request
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
from mcp.server import MCPServer

import market
import sharia
import yahoo_data as yd

SERVER_VERSION = "2.1.0"

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("server")


# Define an enum for the type of financial statement
class FinancialType(str, Enum):
    income_stmt = "income_stmt"
    quarterly_income_stmt = "quarterly_income_stmt"
    balance_sheet = "balance_sheet"
    quarterly_balance_sheet = "quarterly_balance_sheet"
    cashflow = "cashflow"
    quarterly_cashflow = "quarterly_cashflow"


class HolderType(str, Enum):
    major_holders = "major_holders"
    institutional_holders = "institutional_holders"
    mutualfund_holders = "mutualfund_holders"
    insider_transactions = "insider_transactions"
    insider_purchases = "insider_purchases"
    insider_roster_holders = "insider_roster_holders"


class RecommendationType(str, Enum):
    recommendations = "recommendations"
    upgrades_downgrades = "upgrades_downgrades"


# --- Ticker normalization ------------------------------------------------
# Yahoo Finance represents US class shares with a hyphen (e.g. "BRK-B"), but
# users and LLMs routinely type them with a dot or slash ("BRK.B", "BRK/B").
# For the dotted form yfinance raises nothing and returns *empty* price data,
# so the bad request fails silently. Normalize the known single-letter US
# share-class suffixes to the hyphen form.
#
# Only a single trailing "A"/"B" class letter is converted. Exchange suffixes
# such as .TO, .L, .HK, .T, .AX are legitimate yfinance tickers and are left
# untouched (e.g. "SHOP.TO", "RIO.L", "7203.T" are returned unchanged).
_CLASS_SHARE_SUFFIXES = {"A", "B"}


def normalize_ticker(ticker: str) -> str:
    """Normalize US class-share tickers to the hyphen form yfinance expects.

    "BRK.B" / "BRK/B" -> "BRK-B", "BF.B" -> "BF-B". Any symbol that is not a
    plain <root><separator><class-letter> class share (including
    exchange-suffixed tickers like "SHOP.TO" or "RIO.L") is returned unchanged.
    """
    if not ticker:
        return ticker
    match = re.fullmatch(r"\s*([A-Za-z]{1,6})[./-]([A-Za-z])\s*", ticker)
    if match and match.group(2).upper() in _CLASS_SHARE_SUFFIXES:
        return f"{match.group(1).upper()}-{match.group(2).upper()}"
    return ticker


def _symbol(ticker: str) -> str:
    return normalize_ticker((ticker or "").strip()).strip().upper()


# --- Time budgets (seconds a tool call may wait) and cache lifetimes --------
QUOTE_BUDGET = 12.0
ENRICH_WAIT = 4.0  # first quoteSummary attempt; it keeps running in background
HISTORY_BUDGET = 20.0
NEWS_BUDGET = 18.0
FUND_BUDGET = 25.0

HOUR = 3600.0
DAY = 86400.0
QUOTE_FRESH, QUOTE_STALE = 10.0, 30 * 60.0
DAILY_FRESH, DAILY_STALE = 30 * 60.0, 3 * DAY
SPLITS_FRESH, SPLITS_STALE = DAY, 7 * DAY
INFO_FRESH, INFO_STALE = 6 * HOUR, 7 * DAY
NEWS_FRESH, NEWS_STALE = 180.0, DAY
FUND_FRESH, FUND_STALE = 6 * HOUR, 7 * DAY
OPT_DATES_FRESH, OPT_DATES_STALE = 15 * 60.0, DAY
OPT_CHAIN_FRESH, OPT_CHAIN_STALE = 60.0, HOUR

_HISTORY_FRESH = {
    "1m": 20, "2m": 20, "5m": 30, "15m": 60, "30m": 60, "60m": 120, "90m": 120,
    "1h": 120, "1d": 120, "5d": 900, "1wk": 900, "1mo": HOUR, "3mo": HOUR,
}


def _history_ttl(interval: str) -> tuple[float, float]:
    fresh = float(_HISTORY_FRESH.get(interval, 120))
    stale = 6 * HOUR if interval[-1:] in ("m", "h") else 7 * DAY
    return fresh, stale


def _json_default(value: Any) -> Any:
    """json.dumps fallback for numpy / pandas scalars and timestamps."""
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, (pd.Timestamp, dt.datetime, dt.date)):
        return value.isoformat()
    return str(value)


def _describe(exc: BaseException) -> str:
    """Short, user-facing reason for an upstream failure."""
    if isinstance(exc, yd.RateLimited):
        return str(exc)
    if isinstance(exc, asyncio.TimeoutError):
        return "Yahoo did not answer in time"
    return str(exc) or type(exc).__name__


# Initialize MCP server
yfinance_server = MCPServer(
    "yfinance",
    version=SERVER_VERSION,
    instructions="""
# Yahoo Finance MCP Server

This server is used to get information about a given ticker symbol from yahoo finance.

Available tools:
- get_historical_stock_prices: Get historical stock prices for a given ticker symbol from yahoo finance. Include the following information: Date, Open, High, Low, Close, Volume, Adj Close.
- get_stock_info: Get stock information for a given ticker symbol from yahoo finance. Include the following information: Stock Price & Trading Info, Company Information, Financial Metrics, Earnings & Revenue, Margins & Returns, Dividends, Balance Sheet, Ownership, Analyst Coverage, Risk Metrics, Other.
- get_yahoo_finance_news: Get news for a given ticker symbol from yahoo finance.
- get_stock_actions: Get stock dividends and stock splits for a given ticker symbol from yahoo finance.
- get_financial_statement: Get financial statement for a given ticker symbol from yahoo finance. You can choose from the following financial statement types: income_stmt, quarterly_income_stmt, balance_sheet, quarterly_balance_sheet, cashflow, quarterly_cashflow.
- get_holder_info: Get holder information for a given ticker symbol from yahoo finance. You can choose from the following holder types: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.
- get_option_expiration_dates: Fetch the available options expiration dates for a given ticker symbol.
- get_option_chain: Fetch the option chain for a given ticker symbol, expiration date, and option type.
- get_recommendations: Get recommendations or upgrades/downgrades for a given ticker symbol from yahoo finance. You can also specify the number of months back to get upgrades/downgrades for, default is 12.
- get_sharia_status: Get the Sharia compliance classification of a stock (Yaqeen first; Chart Idea link and indicative ratios when Yaqeen says محل نظر).
- get_quotes: Compact live quotes for up to 40 tickers in one call, with the day's levels (pre-market high/low, VWAP, opening range, previous session high/low, ATR14), relative volume and float rotation. Prefer it over repeated get_stock_info calls when watching several symbols.
- get_market_movers: Yahoo screeners (day_gainers, day_losers, most_actives, small_cap_gainers, aggressive_small_caps, most_shorted_stocks), Yahoo trending tickers, or "premarket" (a session-aware scan of the trending and screener names ranked by their move).

Data freshness: prices in get_stock_info come live from Yahoo's chart feed.
Fundamentals (float, short interest, ownership, ratios) can be cached for up
to 6 hours. The "dataStatus" field of get_stock_info says how old each part
is and whether anything was unavailable.
""",
)


# ==========================================================================
# get_historical_stock_prices
# ==========================================================================


def _yf_history(symbol: str, period: str, interval: str) -> str:
    with yd.upstream_slot():
        df = yf.Ticker(symbol).history(period=period, interval=interval, timeout=10)
    if df is None or df.empty:
        # yfinance hides the reason; the chart fallback will report it
        raise yd.NotFound("yfinance returned no rows")
    df = df.reset_index(names="Date")
    return df.to_json(orient="records", date_format="iso")


def _chart_history(symbol: str, period: str, interval: str) -> str:
    res = yd.fetch_chart(
        symbol, range_=period, interval=interval, prepost=False, events="div,splits,capitalGains"
    )
    return yd.chart_history_json(res, interval)


def _symbol_missing(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "delisted" in text or "no data found" in text or "not found" in text


@yfinance_server.tool(
    name="get_historical_stock_prices",
    description="""Get historical stock prices for a given ticker symbol from yahoo finance. Include the following information: Date, Open, High, Low, Close, Volume, Adj Close.
Args:
    ticker: str
        The ticker symbol of the stock to get historical prices for, e.g. "AAPL"
    period : str
        Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        Either Use period parameter or use start and end
        Default is "1mo"
    interval : str
        Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        Intraday data cannot extend last 60 days
        Default is "1d"
""",
)
async def get_historical_stock_prices(
    ticker: str, period: str = "1mo", interval: str = "1d"
) -> str:
    """Get historical stock prices for a given ticker symbol

    Args:
        ticker: str
            The ticker symbol of the stock to get historical prices for, e.g. "AAPL"
        period : str
            Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            Either Use period parameter or use start and end
            Default is "1mo"
        interval : str
            Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            Intraday data cannot extend last 60 days
            Default is "1d"
    """
    symbol = _symbol(ticker)
    if not symbol:
        return f"Error: getting historical stock prices for {ticker}: empty ticker symbol"
    period = (period or "1mo").strip().lower()
    interval = (interval or "1d").strip().lower()
    key = f"hist|{symbol}|{period}|{interval}"
    fresh, stale = _history_ttl(interval)

    stale_value = None
    first_error: BaseException | None = None
    try:
        res = await yd.cached_call(
            key=key,
            family="history",
            fn=lambda: _yf_history(symbol, period, interval),
            fresh_ttl=fresh,
            stale_ttl=stale,
            budget=HISTORY_BUDGET,
        )
        if not res.stale:
            return res.value
        stale_value = res.value
    except yd.UpstreamError as exc:  # includes NotFound ("no rows")
        first_error = exc

    # Fallback: Yahoo chart API directly (no cookie/crumb involved).
    try:
        res = await yd.cached_call(
            key=key,
            family="chart",
            fn=lambda: _chart_history(symbol, period, interval),
            fresh_ttl=fresh,
            stale_ttl=stale,
            budget=HISTORY_BUDGET,
        )
        return res.value
    except yd.NotFound as exc:
        if _symbol_missing(exc):
            log.info(f"No price data for {ticker}: {exc}")
            return "[]"
        return f"Error: getting historical stock prices for {ticker}: {exc}"
    except yd.UpstreamError as exc:
        if stale_value is not None:
            return stale_value
        reason = _describe(first_error if isinstance(first_error, yd.RateLimited) else exc)
        log.warning(f"Error: getting historical stock prices for {ticker}: {reason}")
        return f"Error: getting historical stock prices for {ticker}: {reason}"


# ==========================================================================
# get_stock_info
# ==========================================================================


def _fetch_info(symbol: str) -> dict[str, Any]:
    with yd.upstream_slot():
        info = yf.Ticker(symbol).get_info()
    if not info or "symbol" not in info or "quoteType" not in info:
        raise yd.UpstreamError("empty quoteSummary response")
    return info


def _quote_calls(symbol: str, splits: bool = True) -> list:
    """Concurrent fetches that make up a quote: (name, awaitable).

    ``splits=False`` leaves out the long split-history chart (get_quotes does
    not report splits). Coroutines are only created for calls that are made.
    """
    specs = [
        ("intraday", f"chart1d|{symbol}", "chart", QUOTE_FRESH, QUOTE_STALE, QUOTE_BUDGET,
         lambda: yd.fetch_chart(symbol, range_="1d", interval="1m", prepost=True)),
        ("daily", f"chart3mo|{symbol}", "chart", DAILY_FRESH, DAILY_STALE, QUOTE_BUDGET,
         lambda: yd.fetch_chart(symbol, range_="3mo", interval="1d")),
        ("splits", f"splits|{symbol}", "chart", SPLITS_FRESH, SPLITS_STALE, QUOTE_BUDGET,
         lambda: yd.fetch_chart(symbol, range_="max", interval="3mo", events="splits")),
        ("info", f"info|{symbol}", "quoteSummary", INFO_FRESH, INFO_STALE, ENRICH_WAIT,
         lambda: _fetch_info(symbol)),
    ]
    return [
        (name, yd.cached_call(key=key, family=family, fn=fn, fresh_ttl=fresh, stale_ttl=stale, budget=budget))
        for name, key, family, fresh, stale, budget, fn in specs
        if splits or name != "splits"
    ]


def _age_text(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 90:
        return f"{seconds}s"
    if seconds < 2 * 3600:
        return f"{seconds // 60} min"
    if seconds < 2 * 86400:
        return f"{seconds // 3600} h"
    return f"{seconds // 86400} days"


def _friendly(exc: BaseException) -> str:
    """Reason text for dataStatus notes (kept free of exception class names)."""
    if isinstance(exc, yd.RateLimited):
        return "Yahoo is rate-limiting this endpoint for the server"
    if isinstance(exc, yd.NotFound):
        return "not available for this symbol"
    if isinstance(exc, asyncio.TimeoutError):
        return "Yahoo did not answer in time"
    text = str(exc) or type(exc).__name__
    return re.sub(r"\b\w*Error\b:?\s*", "", text).strip() or "upstream failure"


async def build_stock_info(symbol: str) -> tuple[dict[str, Any] | None, BaseException | None]:
    names, calls = zip(*_quote_calls(symbol))
    outcomes = dict(zip(names, await asyncio.gather(*calls, return_exceptions=True)))

    def ok(name: str) -> yd.Result | None:
        value = outcomes.get(name)
        return value if isinstance(value, yd.Result) else None

    intraday, daily, splits, info = ok("intraday"), ok("daily"), ok("splits"), ok("info")
    live = None
    notes: list[str] = []
    if intraday is not None:
        try:
            live = yd.build_live_quote(
                symbol,
                intraday.value,
                daily.value if daily else None,
                splits.value if splits else None,
            )
        except Exception as exc:  # malformed payload: fall back to quoteSummary
            log.exception("could not build quote for %s", symbol)
            notes.append(f"live quote could not be parsed ({_friendly(exc)})")
            live = None
    enrichment = info.value if info is not None and isinstance(info.value, dict) else None

    if live is None and not enrichment:
        err = outcomes.get("intraday")
        if not isinstance(err, BaseException):
            err = outcomes.get("info")
        return None, err if isinstance(err, BaseException) else yd.UpstreamError("no data")

    merged = yd.merge_quote(live, enrichment, info.age if info is not None else None)
    merged.setdefault("symbol", symbol)

    status: dict[str, Any] = {"asOf": yd.utc_iso(time.time())}
    if intraday is not None and live is not None:
        status["quote"] = "stale" if intraday.stale else "live"
        status["quoteAgeSeconds"] = int(intraday.age)
        if intraday.stale:
            notes.append(
                f"live price unavailable ({_friendly(yd.Unavailable(intraday.note or ''))}); "
                f"price is {_age_text(intraday.age)} old"
            )
    else:
        status["quote"] = "from quoteSummary"
        reason = outcomes.get("intraday")
        if isinstance(reason, BaseException):
            notes.append(f"live chart price unavailable ({_friendly(reason)})")
    if info is not None:
        status["fundamentals"] = "stale" if info.stale else ("fresh" if info.age < 60 else "cached")
        status["fundamentalsAgeSeconds"] = int(info.age)
    else:
        status["fundamentals"] = "unavailable"
        reason = outcomes.get("info")
        if isinstance(reason, BaseException):
            if isinstance(reason, (yd.Unavailable, asyncio.TimeoutError)) and "background" in str(reason):
                notes.append("fundamentals (float, short interest, ratios) are loading; ask again shortly")
            else:
                notes.append(f"fundamentals (float, short interest, ratios) unavailable: {_friendly(reason)}")
    if notes:
        status["notes"] = notes
    merged["dataStatus"] = status
    return merged, None


@yfinance_server.tool(
    name="get_stock_info",
    description="""Get stock information for a given ticker symbol from yahoo finance. Include the following information:
Stock Price & Trading Info, Company Information, Financial Metrics, Earnings & Revenue, Margins & Returns, Dividends, Balance Sheet, Ownership, Analyst Coverage, Risk Metrics, Other.

Args:
    ticker: str
        The ticker symbol of the stock to get information for, e.g. "AAPL"
""",
)
async def get_stock_info(ticker: str) -> str:
    """Get stock information for a given ticker symbol"""
    symbol = _symbol(ticker)
    if not symbol:
        return f"Error: getting stock information for {ticker}: empty ticker symbol"
    try:
        info, err = await build_stock_info(symbol)
    except Exception as exc:  # never let one symbol break the tool
        log.exception("get_stock_info failed for %s", symbol)
        return f"Error: getting stock information for {ticker}: {_describe(exc)}"
    if info is None:
        if isinstance(err, yd.NotFound):
            log.info(f"No stock info found for ticker {ticker}.")
            return f"No stock info found for ticker {ticker}."
        log.warning(f"Error: getting stock information for {ticker}: {_describe(err)}")
        return f"Error: getting stock information for {ticker}: {_describe(err)}"
    return json.dumps(info, default=_json_default)


# ==========================================================================
# get_yahoo_finance_news
# ==========================================================================


def _yf_news(symbol: str) -> list[yd.NewsItem]:
    with yd.upstream_slot():
        articles = yf.Ticker(symbol).get_news(count=15, tab="news")
    return yd.news_from_yfinance(articles)


@yfinance_server.tool(
    name="get_yahoo_finance_news",
    description="""Get news for a given ticker symbol from yahoo finance.

Args:
    ticker: str
        The ticker symbol of the stock to get news for, e.g. "AAPL"
""",
)
async def get_yahoo_finance_news(ticker: str) -> str:
    """Get news for a given ticker symbol

    Args:
        ticker: str
            The ticker symbol of the stock to get news for, e.g. "AAPL"
    """
    symbol = _symbol(ticker)
    if not symbol:
        return f"Error: getting news for {ticker}: empty ticker symbol"
    key = f"news|{symbol}"
    hit = yd.CACHE.get(key)
    if hit is not None and hit.age < NEWS_FRESH:
        return hit.value

    sources = (
        ("news", lambda: _yf_news(symbol)),
        ("rss", lambda: yd.fetch_news_rss(symbol)),
        ("search", lambda: yd.fetch_news_search(symbol)),
    )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + NEWS_BUDGET
    errors: list[str] = []
    answered_empty = False
    for family, fn in sources:
        remaining = deadline - loop.time()
        if remaining < 1.0:
            errors.append(f"{family}: skipped (time budget used)")
            continue
        try:
            res = await yd.cached_call(
                key=f"{key}|{family}",
                family=family,
                fn=fn,
                fresh_ttl=NEWS_FRESH,
                stale_ttl=0.0,
                budget=min(8.0, remaining),
            )
        except yd.NotFound:
            answered_empty = True
            continue
        except yd.UpstreamError as exc:
            errors.append(f"{family}: {_describe(exc)}")
            continue
        if res.value:
            text = yd.format_news(res.value)
            yd.CACHE.set(key, text)
            return text
        answered_empty = True

    if hit is not None and hit.age < NEWS_STALE:
        return hit.value
    if answered_empty:
        log.info(f"No news found for ticker {ticker}.")
        return f"No news found for ticker {ticker}."
    reason = "; ".join(errors) or "no source answered"
    log.warning(f"Error: getting news for {ticker}: {reason}")
    return f"Error: getting news for {ticker}: {reason}"


# ==========================================================================
# Fundamentals: actions, statements, holders, recommendations
# ==========================================================================


async def _cached_fundamental(key: str, fn, family: str = "fundamentals",
                              fresh: float = FUND_FRESH, stale: float = FUND_STALE) -> Any:
    res = await yd.cached_call(
        key=key, family=family, fn=fn, fresh_ttl=fresh, stale_ttl=stale, budget=FUND_BUDGET
    )
    return res.value


def _gated(fn):
    def run():
        with yd.upstream_slot():
            return fn()

    return run


@yfinance_server.tool(
    name="get_stock_actions",
    description="""Get stock dividends and stock splits for a given ticker symbol from yahoo finance.

Args:
    ticker: str
        The ticker symbol of the stock to get stock actions for, e.g. "AAPL"
""",
)
async def get_stock_actions(ticker: str) -> str:
    """Get stock dividends and stock splits for a given ticker symbol"""
    symbol = _symbol(ticker)

    def fetch() -> str:
        actions_df = yf.Ticker(symbol).actions
        actions_df = actions_df.reset_index(names="Date")
        return actions_df.to_json(orient="records", date_format="iso")

    try:
        return await _cached_fundamental(f"actions|{symbol}", _gated(fetch))
    except Exception as e:
        log.warning(f"Error: getting stock actions for {ticker}: {_describe(e)}")
        return f"Error: getting stock actions for {ticker}: {_describe(e)}"


def _statement_to_json(financial_statement: pd.DataFrame) -> str:
    # Create a list to store all the json objects
    result = []
    # Loop through each column (date)
    for column in financial_statement.columns:
        if isinstance(column, pd.Timestamp):
            date_str = column.strftime("%Y-%m-%d")  # Format as YYYY-MM-DD
        else:
            date_str = str(column)
        # Create a dictionary for each date
        date_obj = {"date": date_str}
        # Add each metric as a key-value pair
        for index, value in financial_statement[column].items():
            # Add the value, handling NaN values
            date_obj[index] = None if pd.isna(value) else value
        result.append(date_obj)
    return json.dumps(result, default=_json_default)


@yfinance_server.tool(
    name="get_financial_statement",
    description="""Get financial statement for a given ticker symbol from yahoo finance. You can choose from the following financial statement types: income_stmt, quarterly_income_stmt, balance_sheet, quarterly_balance_sheet, cashflow, quarterly_cashflow.

Args:
    ticker: str
        The ticker symbol of the stock to get financial statement for, e.g. "AAPL"
    financial_type: str
        The type of financial statement to get. You can choose from the following financial statement types: income_stmt, quarterly_income_stmt, balance_sheet, quarterly_balance_sheet, cashflow, quarterly_cashflow.
""",
)
async def get_financial_statement(ticker: str, financial_type: str) -> str:
    """Get financial statement for a given ticker symbol"""
    valid = {t.value for t in FinancialType}
    if financial_type not in valid:
        return f"Error: invalid financial type {financial_type}. Please use one of the following: {FinancialType.income_stmt}, {FinancialType.quarterly_income_stmt}, {FinancialType.balance_sheet}, {FinancialType.quarterly_balance_sheet}, {FinancialType.cashflow}, {FinancialType.quarterly_cashflow}."
    symbol = _symbol(ticker)

    def fetch() -> str:
        # e.g. Ticker.income_stmt, Ticker.quarterly_cashflow
        financial_statement = getattr(yf.Ticker(symbol), financial_type)
        if financial_statement is None or financial_statement.empty:
            return ""
        return _statement_to_json(financial_statement)

    try:
        text = await _cached_fundamental(f"stmt|{symbol}|{financial_type}", _gated(fetch))
    except Exception as e:
        log.warning(f"Error: getting financial statement for {ticker}: {_describe(e)}")
        return f"Error: getting financial statement for {ticker}: {_describe(e)}"
    if not text:
        log.info(f"No financial statement data found for ticker {ticker}.")
        return f"No financial statement data found for ticker {ticker}."
    return text


@yfinance_server.tool(
    name="get_holder_info",
    description="""Get holder information for a given ticker symbol from yahoo finance. You can choose from the following holder types: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.

Args:
    ticker: str
        The ticker symbol of the stock to get holder information for, e.g. "AAPL"
    holder_type: str
        The type of holder information to get. You can choose from the following holder types: major_holders, institutional_holders, mutualfund_holders, insider_transactions, insider_purchases, insider_roster_holders.
""",
)
async def get_holder_info(ticker: str, holder_type: str) -> str:
    """Get holder information for a given ticker symbol"""
    valid = {t.value for t in HolderType}
    if holder_type not in valid:
        return f"Error: invalid holder type {holder_type}. Please use one of the following: {HolderType.major_holders}, {HolderType.institutional_holders}, {HolderType.mutualfund_holders}, {HolderType.insider_transactions}, {HolderType.insider_purchases}, {HolderType.insider_roster_holders}."
    symbol = _symbol(ticker)

    def fetch() -> str:
        company = yf.Ticker(symbol)
        if holder_type == HolderType.major_holders:
            return company.major_holders.reset_index(names="metric").to_json(orient="records")
        if holder_type == HolderType.institutional_holders:
            return company.institutional_holders.to_json(orient="records")
        frame = getattr(company, holder_type)
        return frame.to_json(orient="records", date_format="iso")

    try:
        return await _cached_fundamental(f"holders|{symbol}|{holder_type}", _gated(fetch))
    except Exception as e:
        log.warning(f"Error: getting holder info for {ticker}: {_describe(e)}")
        return f"Error: getting holder info for {ticker}: {_describe(e)}"


# ==========================================================================
# Options
# ==========================================================================


async def _option_dates(symbol: str) -> list[str]:
    def fetch() -> list[str]:
        return list(yf.Ticker(symbol).options or [])

    return await _cached_fundamental(
        f"optdates|{symbol}", _gated(fetch), family="options",
        fresh=OPT_DATES_FRESH, stale=OPT_DATES_STALE,
    )


@yfinance_server.tool(
    name="get_option_expiration_dates",
    description="""Fetch the available options expiration dates for a given ticker symbol.

Args:
    ticker: str
        The ticker symbol of the stock to get option expiration dates for, e.g. "AAPL"
""",
)
async def get_option_expiration_dates(ticker: str) -> str:
    """Fetch the available options expiration dates for a given ticker symbol."""
    symbol = _symbol(ticker)
    try:
        options = await _option_dates(symbol)
    except Exception as e:
        log.warning(f"Error: getting option expiration dates for {ticker}: {_describe(e)}")
        return f"Error: getting option expiration dates for {ticker}: {_describe(e)}"
    if not options:
        log.info(f"No options expiration dates found for ticker {ticker}.")
        return f"No options expiration dates found for ticker {ticker}."
    return json.dumps(options)


async def _spot_price(symbol: str) -> float | None:
    """Current underlying price from the (cached) live chart, or None."""
    try:
        res = await yd.cached_call(
            key=f"chart1d|{symbol}",
            family="chart",
            fn=lambda: yd.fetch_chart(symbol, range_="1d", interval="1m", prepost=True),
            fresh_ttl=QUOTE_FRESH,
            stale_ttl=QUOTE_STALE,
            budget=QUOTE_BUDGET,
        )
    except Exception:
        return None
    price = yd.num(((res.value or {}).get("meta") or {}).get("regularMarketPrice"))
    return price if price and price > 0 else None


def _window_strikes(
    chain: pd.DataFrame, strike_window_pct: float | None, spot: float | None
) -> pd.DataFrame:
    """Restrict `chain` to strikes within +/- `strike_window_pct` of spot.

    Returns the chain unchanged when no window is requested, the window is not a
    positive number, spot cannot be resolved, or the window would select nothing
    — a caller asking for a narrower view must never receive an empty chain when
    a wider one exists.
    """
    if strike_window_pct is None or "strike" not in chain.columns:
        return chain

    try:
        window = float(strike_window_pct)
    except (TypeError, ValueError):
        return chain
    if not math.isfinite(window) or window <= 0:
        return chain

    if spot is None:
        return chain

    windowed = chain[
        (chain["strike"] >= spot * (1 - window))
        & (chain["strike"] <= spot * (1 + window))
    ]
    return windowed if not windowed.empty else chain


def _project_fields(chain: pd.DataFrame, fields: list[str] | None) -> pd.DataFrame:
    """Restrict `chain` to `fields`, always retaining `strike`.

    Projection is all-or-nothing: if *any* requested name is not a real column,
    the full chain is returned unchanged.

    A partial match is the dangerous case. Dropping just the unrecognized names
    yields a chain that parses as valid data while silently missing a column the
    caller asked for and believes it has — e.g. requesting
    ``["bid", "implied volatility"]`` would hand back bids with no volatility at
    all. Returning everything is wasteful but never wrong, and the caller can
    see its projection did not apply.
    """
    if not fields:
        return chain

    available = set(chain.columns)
    if any(field not in available for field in fields):
        return chain

    keep = [column for column in chain.columns if column in set(fields)]
    if "strike" in chain.columns and "strike" not in keep:
        keep.insert(0, "strike")
    return chain[keep]


@yfinance_server.tool(
    name="get_option_chain",
    description="""Fetch the option chain for a given ticker symbol, expiration date, and option type.

A full chain is large (a liquid US name runs 40-90 strikes and ~17KB of JSON per
expiration). Prefer `strike_window_pct` and `fields` to request only the strikes
and columns you actually need — pulling several full chains into one analysis is
the main driver of oversized requests.

Args:
    ticker: str
        The ticker symbol of the stock to get option chain for, e.g. "AAPL"
    expiration_date: str
        The expiration date for the options chain (format: 'YYYY-MM-DD')
    option_type: str
        The type of option to fetch ('calls' or 'puts')
    strike_window_pct: float | None
        Keep only strikes within +/- this fraction of the current spot price
        (e.g. 0.15 keeps strikes from 85% to 115% of spot). Omit for every strike.
    fields: list[str] | None
        Only return these columns. `strike` is always included. Omit for every column.
        Names must match these columns EXACTLY (they are case-sensitive, and none
        contain spaces or underscores):
            contractSymbol, lastTradeDate, strike, lastPrice, bid, ask, change,
            percentChange, volume, openInterest, impliedVolatility, inTheMoney,
            contractSize, currency
        Note "lastPrice" (not "last"), "impliedVolatility" (not "implied volatility"),
        "openInterest" (not "open interest"). If ANY name is not in that list the
        projection is dropped and the full chain is returned, so a typo costs
        payload rather than silently omitting a column you asked for.
        A good pricing set: ["strike", "bid", "ask", "lastPrice",
        "impliedVolatility", "openInterest", "volume"].
""",
)
async def get_option_chain(
    ticker: str,
    expiration_date: str,
    option_type: str,
    strike_window_pct: float | None = None,
    fields: list[str] | None = None,
) -> str:
    """Fetch the option chain for a given ticker symbol, expiration date, and option type.

    Args:
        ticker: The ticker symbol of the stock
        expiration_date: The expiration date for the options chain (format: 'YYYY-MM-DD')
        option_type: The type of option to fetch ('calls' or 'puts')
        strike_window_pct: Keep only strikes within +/- this fraction of spot.
            None returns every strike (the historical behavior).
        fields: Only return these columns, matched exactly against the chain's
            own column names. None returns every column (the historical
            behavior). `strike` is always retained so rows stay identifiable.
            If any name is unrecognized the projection is dropped entirely —
            see :func:`_project_fields`.

    Returns:
        str: JSON string containing the option chain data
    """
    symbol = _symbol(ticker)
    try:
        # Check if the expiration date is valid
        if expiration_date not in await _option_dates(symbol):
            return f"Error: No options available for the date {expiration_date}. You can use `get_option_expiration_dates` to get the available expiration dates."

        # Check if the option type is valid
        if option_type not in ["calls", "puts"]:
            return "Error: Invalid option type. Please use 'calls' or 'puts'."

        def fetch() -> dict[str, pd.DataFrame]:
            option_chain = yf.Ticker(symbol).option_chain(expiration_date)
            return {"calls": option_chain.calls, "puts": option_chain.puts}

        chains = await _cached_fundamental(
            f"optchain|{symbol}|{expiration_date}", _gated(fetch), family="options",
            fresh=OPT_CHAIN_FRESH, stale=OPT_CHAIN_STALE,
        )
        chain = chains[option_type]
        spot = await _spot_price(symbol) if strike_window_pct is not None else None
        chain = _window_strikes(chain, strike_window_pct, spot)
        chain = _project_fields(chain, fields)
        return chain.to_json(orient="records", date_format="iso")
    except Exception as e:
        log.warning(f"Error: getting option chain for {ticker}: {_describe(e)}")
        return f"Error: getting option chain for {ticker}: {_describe(e)}"


@yfinance_server.tool(
    name="get_recommendations",
    description="""Get recommendations or upgrades/downgrades for a given ticker symbol from yahoo finance. You can also specify the number of months back to get upgrades/downgrades for, default is 12.

Args:
    ticker: str
        The ticker symbol of the stock to get recommendations for, e.g. "AAPL"
    recommendation_type: str
        The type of recommendation to get. You can choose from the following recommendation types: recommendations, upgrades_downgrades.
    months_back: int
        The number of months back to get upgrades/downgrades for, default is 12.
""",
)
async def get_recommendations(ticker: str, recommendation_type: str, months_back: int = 12) -> str:
    """Get recommendations or upgrades/downgrades for a given ticker symbol"""
    if recommendation_type not in {t.value for t in RecommendationType}:
        return f"Error: invalid recommendation type {recommendation_type}. Please use one of the following: {RecommendationType.recommendations}, {RecommendationType.upgrades_downgrades}."
    symbol = _symbol(ticker)
    try:
        if recommendation_type == RecommendationType.recommendations:

            def fetch_recs() -> str:
                recommendations = yf.Ticker(symbol).recommendations
                if recommendations is None or recommendations.empty:
                    return "[]"
                return recommendations.to_json(orient="records")

            return await _cached_fundamental(f"recs|{symbol}", _gated(fetch_recs))

        def fetch_grades() -> pd.DataFrame:
            frame = yf.Ticker(symbol).upgrades_downgrades
            return frame if frame is not None else pd.DataFrame()

        upgrades_downgrades = await _cached_fundamental(f"grades|{symbol}", _gated(fetch_grades))
        if upgrades_downgrades.empty:
            return "[]"
        # Get the upgrades/downgrades based on the cutoff date
        upgrades_downgrades = upgrades_downgrades.reset_index()
        cutoff_date = pd.Timestamp.now() - pd.DateOffset(months=months_back)
        upgrades_downgrades = upgrades_downgrades[upgrades_downgrades["GradeDate"] >= cutoff_date]
        upgrades_downgrades = upgrades_downgrades.sort_values("GradeDate", ascending=False)
        # Get the first occurrence (most recent) for each firm
        latest_by_firm = upgrades_downgrades.drop_duplicates(subset=["Firm"])
        return latest_by_firm.to_json(orient="records", date_format="iso")
    except Exception as e:
        log.warning(f"Error: getting recommendations for {ticker}: {_describe(e)}")
        return f"Error: getting recommendations for {ticker}: {_describe(e)}"


# ==========================================================================
# get_sharia_status
# ==========================================================================

SHARIA_FRESH, SHARIA_STALE = HOUR, 7 * DAY
SHARIA_BUDGET = 20.0


async def _sharia_indicators(symbol: str) -> dict[str, Any]:
    """Indicative ratios against Al-Rajhi decision 485 thresholds (not a ruling)."""
    out: dict[str, Any] = {}
    try:
        res = await yd.cached_call(
            key=f"info|{symbol}",
            family="quoteSummary",
            fn=lambda: _fetch_info(symbol),
            fresh_ttl=INFO_FRESH,
            stale_ttl=INFO_STALE,
            budget=ENRICH_WAIT,
        )
        info = res.value if isinstance(res.value, dict) else {}
    except yd.UpstreamError:
        info = {}
    market_cap, debt = yd.num(info.get("marketCap")), yd.num(info.get("totalDebt"))
    if market_cap and debt is not None:
        out["debtToMarketCapPct"] = round(debt / market_cap * 100, 2)
    hint = sharia.activity_hint(info)
    if hint:
        out["activityNote"] = hint
    try:
        rows = json.loads(await get_financial_statement(symbol, "income_stmt"))
        latest = rows[0] if isinstance(rows, list) and rows else {}
    except (ValueError, TypeError):
        latest = {}
    revenue = yd.num(latest.get("Total Revenue"))
    interest = yd.num(latest.get("Interest Income"))
    if interest is None:
        interest = yd.num(latest.get("Interest Income Non Operating"))
    if revenue and revenue > 0 and interest is not None:
        out["interestIncomeToRevenuePct"] = round(interest / revenue * 100, 2)
        out["incomePeriod"] = latest.get("date")
    if out:
        out["thresholds"] = {"debtToMarketCapPct": 30, "impermissibleIncomePct": 5}
        out["note"] = (
            "مؤشرات مساعدة محسوبة من بيانات Yahoo بحدود قرار الهيئة الشرعية للراجحي رقم 485. "
            "دخل الفوائد جزء من الدخل المحرم لا كله، وهذه المؤشرات ليست حكمًا شرعيًّا."
        )
    return out


@yfinance_server.tool(
    name="get_sharia_status",
    description="""Get the Sharia (Islamic) compliance classification of a US-listed stock or ETF.

Primary source: Yaqeen (yaaqen.com), a free filter that applies the standards of Al-Rajhi's
Sharia committee and shows each stock's last update date. Labels (Arabic): شرعي (compliant),
غير شرعي (non-compliant), محل نظر (questionable).

When Yaqeen rates the stock محل نظر or has no rating, the result also links the stock's page on
Chart Idea (chart-idea.com, same standards) for a manual check, because that site blocks
automated queries, and adds indicative ratios computed from Yahoo data (debt to market cap,
interest income to revenue) against the 30% / 5% thresholds. The ratios are not a ruling.

Args:
    ticker: str
        The ticker symbol, e.g. "AAPL"
""",
)
async def get_sharia_status(ticker: str) -> str:
    """Sharia classification: Yaqeen first, then a Chart Idea link and indicative ratios."""
    symbol = _symbol(ticker)
    if not symbol:
        return f"Error: getting sharia status for {ticker}: empty ticker symbol"
    try:
        res = await yd.cached_call(
            key=f"sharia|yaqeen|{symbol}",
            family="yaqeen",
            fn=lambda: sharia.fetch_yaqeen(symbol),
            fresh_ttl=SHARIA_FRESH,
            stale_ttl=SHARIA_STALE,
            budget=SHARIA_BUDGET,
        )
        yaqeen = dict(res.value)
        yaqeen["checkedAt"] = yd.utc_iso(time.time() - res.age)
        if res.stale:
            yaqeen["stale"] = True
            yaqeen["staleReason"] = _friendly(yd.Unavailable(res.note or ""))
    except yd.UpstreamError as exc:
        yaqeen = {
            "source": "يقين",
            "url": f"{sharia.YAQEEN_BASE}/stocks/{symbol}",
            "available": False,
            "reason": _describe(exc),
        }

    result: dict[str, Any] = {"symbol": symbol}
    definitive = yaqeen.get("available") and yaqeen.get("code") in ("compliant", "non_compliant", "mixed")
    if definitive:
        result["verdict"] = yaqeen["label"]
        result["verdictSource"] = "يقين"
        result["sources"] = [yaqeen]
    else:
        result["verdict"] = yaqeen.get("label") or "غير متاح"
        result["verdictSource"] = "يقين" if yaqeen.get("label") else None
        result["sources"] = [yaqeen, sharia.chart_idea_link(symbol), sharia.stock_hunter_link(symbol)]
        result["nextStep"] = "تحقق من صفحة السهم في شارت آيديا (الرابط في المصادر)، ثم في صائد الأسهم إن أردت رأيًا ثالثًا."
        indicators = await _sharia_indicators(symbol)
        if indicators:
            result["indicators"] = indicators
    result["disclaimer"] = "التصنيف منقول عن الجهة المذكورة وفق معاييرها، وليس فتوى."
    return json.dumps(result, ensure_ascii=False, default=_json_default)


# ==========================================================================
# get_quotes / get_market_movers: compact data for dashboards and scans
# ==========================================================================

QUOTES_BUDGET = 20.0
QUOTES_MAX = 40
QUOTES_PARALLEL = 8
MOVERS_FRESH, MOVERS_STALE = 60.0, 30 * 60.0
MOVERS_BUDGET = 15.0
PREMARKET_UNIVERSE = 40


def _parse_tickers(text: str) -> list[str]:
    out: list[str] = []
    for part in re.split(r"[\s,;|]+", text or ""):
        sym = _symbol(part)
        if sym and sym not in out and re.fullmatch(r"[A-Z0-9^=.\-]{1,15}", sym):
            out.append(sym)
    return out


async def _compact(symbol: str, sem: asyncio.Semaphore, with_spark: bool) -> dict[str, Any]:
    async with sem:
        names, calls = zip(*_quote_calls(symbol, splits=False))
        outcomes = dict(zip(names, await asyncio.gather(*calls, return_exceptions=True)))

    def ok(name: str) -> yd.Result | None:
        value = outcomes.get(name)
        return value if isinstance(value, yd.Result) else None

    intraday, daily, info = ok("intraday"), ok("daily"), ok("info")
    enrichment = info.value if info is not None and isinstance(info.value, dict) else None
    err = outcomes.get("intraday")
    if isinstance(err, yd.NotFound):
        raise err  # the chart endpoint is authoritative about unknown symbols
    if intraday is None and not enrichment:
        raise err if isinstance(err, BaseException) else yd.UpstreamError("no data")
    rec = market.compact_quote(
        symbol,
        intraday.value if intraday else None,
        daily.value if daily else None,
        enrichment,
        info.age if info is not None else None,
        with_spark=with_spark,
    )
    status: dict[str, Any] = {}
    if intraday is not None:
        status["quote"] = "stale" if intraday.stale else "live"
        status["quoteAgeSeconds"] = int(intraday.age)
    else:
        status["quote"] = "from quoteSummary"
    status["fundamentals"] = (
        "unavailable" if info is None else "stale" if info.stale else "fresh" if info.age < 60 else "cached"
    )
    if info is not None:
        status["fundamentalsAgeSeconds"] = int(info.age)
    rec["dataStatus"] = status
    return rec


async def _compact_many(symbols: list[str], with_spark: bool, budget: float) -> tuple[list, dict]:
    sem = asyncio.Semaphore(QUOTES_PARALLEL)
    tasks = {s: asyncio.ensure_future(_compact(s, sem, with_spark)) for s in symbols}
    _, pending = await asyncio.wait(tasks.values(), timeout=budget)
    quotes: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    for sym, task in tasks.items():
        if task in pending:
            task.cancel()  # the Yahoo requests keep running and fill the cache
            errors[sym] = "still loading; ask again in a few seconds"
            continue
        exc = task.exception()
        if exc is None:
            quotes.append(task.result())
        elif isinstance(exc, yd.NotFound):
            errors[sym] = "not found on Yahoo"
        else:
            errors[sym] = _friendly(exc)
    return quotes, errors


@yfinance_server.tool(
    name="get_quotes",
    description="""Compact live quotes for several tickers in one call (up to 40), for watchlists and scans.

Each quote has: session-aware price and change (pre-market price against the last close before the
open, regular price against the previous close, after-hours price), open, day high/low, volume,
average volume, relative volume (rvol), float shares, float rotation (today's volume across all
sessions / float), market cap, short % of float, 52-week range, and "levels": pre-market high/low
and VWAP, regular-session VWAP, opening range (first 5 minutes), previous session high/low/close,
ATR(14) and the 20-session high/low. With spark=true it adds a 10-minute sparkline of the day.
Symbols that are not ready within the time budget are listed under "errors" and can be asked again.

Args:
    tickers: str
        Ticker symbols separated by commas or spaces, e.g. "AAPL, MSFT, QQQ"
    spark: bool
        Include the 10-minute sparkline (default true)
""",
)
async def get_quotes(tickers: str, spark: bool = True) -> str:
    symbols = _parse_tickers(tickers)
    if not symbols:
        return f"Error: getting quotes for {tickers}: no valid ticker symbols"
    dropped = symbols[QUOTES_MAX:]
    symbols = symbols[:QUOTES_MAX]
    try:
        quotes, errors = await _compact_many(symbols, bool(spark), QUOTES_BUDGET)
    except Exception as exc:  # never let one batch break the server
        log.exception("get_quotes failed")
        return f"Error: getting quotes for {tickers}: {_describe(exc)}"
    for sym in dropped:
        errors[sym] = f"skipped: at most {QUOTES_MAX} symbols per call"
    payload: dict[str, Any] = {"asOf": yd.utc_iso(time.time()), "count": len(quotes), "quotes": quotes}
    if errors:
        payload["errors"] = errors
    payload["notes"] = [
        "Prices come from Yahoo's chart feed; quoteSource says whether Yahoo marks them real-time or delayed.",
        "floatRotation counts pre-market, regular and after-hours volume of sessionDate.",
        "atr14 is the simple average of the last 14 true ranges of completed daily sessions.",
    ]
    return json.dumps(payload, default=_json_default)


async def _screen(screen: str, count: int) -> dict[str, Any]:
    res = await yd.cached_call(
        key=f"screen|{screen}|{count}",
        family="screener",
        fn=lambda: market.fetch_screener(market.SCREENS[screen], count),
        fresh_ttl=MOVERS_FRESH,
        stale_ttl=MOVERS_STALE,
        budget=MOVERS_BUDGET,
    )
    data = dict(res.value)
    data["ageSeconds"] = int(res.age)
    if res.stale:
        data["stale"] = True
    return data


async def _trending(count: int) -> list[str]:
    res = await yd.cached_call(
        key=f"trending|{count}",
        family="screener",
        fn=lambda: market.fetch_trending(count),
        fresh_ttl=MOVERS_FRESH,
        stale_ttl=MOVERS_STALE,
        budget=MOVERS_BUDGET,
    )
    return [s for s in res.value if market.is_plain_us_stock(s)]


def _mover_row(q: dict[str, Any]) -> dict[str, Any]:
    keep = ("symbol", "name", "exchange", "session", "price", "priceSession", "reference", "changePct",
            "quoteTime", "regularPrice", "prevClose", "regularChangePct", "volume", "avgVolume", "rvol",
            "preVolume", "floatShares", "floatRotation", "marketCap", "shortPctFloat", "quoteSource",
            "levels", "sessionDate")
    return {k: q.get(k) for k in keep if q.get(k) is not None}


@yfinance_server.tool(
    name="get_market_movers",
    description="""US market movers from Yahoo Finance.

screen:
    day_gainers | day_losers | most_actives | small_cap_gainers | aggressive_small_caps |
    most_shorted_stocks  -> Yahoo's predefined screeners (regular-session change and volume)
    trending             -> Yahoo's trending US tickers, with compact quotes
    premarket            -> scan of the trending, gainers, most-active and small-cap-gainer names,
                            ranked by their session-aware move (pre-market price against the last
                            close before the open), with levels and float rotation
count: how many rows (1-50, default 25)
nasdaq_only: keep only Nasdaq-listed names (default false)

These lists are what Yahoo publishes; they are not a complete market scan.
""",
)
async def get_market_movers(screen: str = "day_gainers", count: int = 25, nasdaq_only: bool = False) -> str:
    screen = (screen or "day_gainers").strip().lower()
    try:
        count = max(1, min(int(count or 25), 50))
    except (TypeError, ValueError):
        count = 25
    if screen not in market.SCREENS and screen not in market.COMPOSITE_SCREENS:
        options = ", ".join(list(market.SCREENS) + list(market.COMPOSITE_SCREENS))
        return f"Error: unknown screen {screen!r}. Use one of: {options}"
    payload: dict[str, Any] = {"screen": screen, "asOf": yd.utc_iso(time.time())}
    try:
        if screen in market.SCREENS:
            data = await _screen(screen, count)
            rows = data["quotes"]
            payload.update(title=data.get("title"), ageSeconds=data.get("ageSeconds"), source="Yahoo predefined screener")
            if data.get("stale"):
                payload["stale"] = True
        else:
            if screen == "trending":
                symbols = (await _trending(max(count, 20)))[:count]
                sources = ["trending"]
            else:
                symbols, sources = [], []
                for name in ("trending", "small_cap_gainers", "day_gainers", "most_actives"):
                    try:
                        found = await _trending(25) if name == "trending" else [
                            r["symbol"] for r in (await _screen(name, 25))["quotes"]]
                        sources.append(name)
                    except yd.UpstreamError as exc:
                        log.info("premarket scan: %s unavailable (%s)", name, exc)
                        continue
                    for s in found:
                        if market.is_plain_us_stock(s) and s not in symbols:
                            symbols.append(s)
                symbols = symbols[:PREMARKET_UNIVERSE]
            if not symbols:
                return f"Error: getting market movers ({screen}): Yahoo returned no symbols"
            quotes, errors = await _compact_many(symbols, False, QUOTES_BUDGET)
            rows = [_mover_row(q) for q in quotes]
            if screen == "premarket":
                rows = [r for r in rows if r.get("changePct") is not None]
                rows.sort(key=lambda r: abs(r["changePct"]), reverse=True)
            payload.update(source="Yahoo " + " + ".join(sources), universe=len(symbols))
            if errors:
                payload["errors"] = errors
    except yd.UpstreamError as exc:
        return f"Error: getting market movers ({screen}): {_describe(exc)}"
    if nasdaq_only:
        rows = [r for r in rows if market.is_nasdaq(r.get("exchange"))]
    payload["count"] = len(rows[:count])
    payload["quotes"] = rows[:count]
    return json.dumps(payload, default=_json_default)


# ==========================================================================
# HTTP app: SSE + Streamable HTTP + health, keep-alive
# ==========================================================================

STARTED_AT = time.time()
KEEPALIVE_MODE = (os.environ.get("KEEPALIVE") or "always").strip().lower()  # always|market|off
KEEPALIVE_URL = os.environ.get("KEEPALIVE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or ""
KEEPALIVE_INTERVAL = max(60, int(os.environ.get("KEEPALIVE_INTERVAL", "600") or 600))
STREAMABLE_ON_SSE_PATH = (os.environ.get("STREAMABLE_ON_SSE_PATH") or "1").strip() not in ("0", "false", "no")
KEEPALIVE_STATE: dict[str, Any] = {}
_NEW_YORK = ZoneInfo("America/New_York")


def _us_extended_hours(now: float | None = None) -> bool:
    """Weekday 03:30-20:30 New York time (pre-market to after-hours, with margin)."""
    t = dt.datetime.fromtimestamp(time.time() if now is None else now, tz=_NEW_YORK)
    minutes = t.hour * 60 + t.minute
    return t.weekday() < 5 and 3 * 60 + 30 <= minutes < 20 * 60 + 30


def _ping(url: str) -> int:
    req = urllib.request.Request(url, headers={"User-Agent": "yahoo-finance-mcp-keepalive"})
    with urllib.request.urlopen(req, timeout=20) as resp:  # noqa: S310 - own service URL
        return resp.status


async def _keepalive_loop() -> None:
    """Render's free plan sleeps after 15 idle minutes; ping ourselves."""
    if KEEPALIVE_MODE == "off" or not KEEPALIVE_URL:
        return
    url = KEEPALIVE_URL.rstrip("/") + "/health?source=keepalive"
    KEEPALIVE_STATE.update(mode=KEEPALIVE_MODE, intervalSeconds=KEEPALIVE_INTERVAL)
    await asyncio.sleep(45)
    while True:
        if KEEPALIVE_MODE == "always" or _us_extended_hours():
            try:
                code = await asyncio.to_thread(_ping, url)
                KEEPALIVE_STATE.update(lastPingAt=yd.utc_iso(time.time()), lastStatus=code)
            except Exception as exc:
                KEEPALIVE_STATE.update(lastErrorAt=yd.utc_iso(time.time()), lastError=str(exc)[:200])
                log.warning("keep-alive ping failed: %s", exc)
        await asyncio.sleep(KEEPALIVE_INTERVAL)


# One Sharia lookup shortly after start: proves from the live server that the
# Yaqeen page still parses, and warns early if its layout changes.
SHARIA_SELF_CHECK = (os.environ.get("SHARIA_SELF_CHECK") or "AAPL").strip().upper()
SHARIA_CHECK_STATE: dict[str, Any] = {}


async def _sharia_self_check() -> None:
    if not SHARIA_SELF_CHECK or SHARIA_SELF_CHECK in ("0", "OFF", "NO"):
        return
    await asyncio.sleep(20)
    try:
        out = json.loads(await get_sharia_status(SHARIA_SELF_CHECK))
        yaqeen = (out.get("sources") or [{}])[0]
        SHARIA_CHECK_STATE.update(
            symbol=SHARIA_SELF_CHECK,
            at=yd.utc_iso(time.time()),
            verdict=out.get("verdict"),
            yaqeenAvailable=bool(yaqeen.get("available")),
            yaqeenUpdated=yaqeen.get("updated"),
            reason=yaqeen.get("reason"),
        )
        if yaqeen.get("available"):
            log.info("sharia self-check %s: %s (Yaqeen, updated %s)",
                     SHARIA_SELF_CHECK, out.get("verdict"), yaqeen.get("updated"))
        else:
            log.warning("sharia self-check %s: Yaqeen unavailable (%s)",
                        SHARIA_SELF_CHECK, yaqeen.get("reason"))
    except Exception as exc:  # never let the check affect the server
        SHARIA_CHECK_STATE.update(symbol=SHARIA_SELF_CHECK, at=yd.utc_iso(time.time()), error=str(exc)[:200])
        log.warning("sharia self-check %s failed: %s", SHARIA_SELF_CHECK, exc)


# One batch quote and one screener request after start: the logs and /health
# then show from the live server that get_quotes and get_market_movers work.
DATA_SELF_CHECK = (os.environ.get("DATA_SELF_CHECK") or "QQQ,AAPL").strip().upper()
DATA_CHECK_STATE: dict[str, Any] = {}


async def _data_self_check() -> None:
    if not DATA_SELF_CHECK or DATA_SELF_CHECK in ("0", "OFF", "NO"):
        return
    await asyncio.sleep(35)
    state: dict[str, Any] = {"at": yd.utc_iso(time.time())}
    try:
        out = json.loads(await get_quotes(DATA_SELF_CHECK, spark=False))
        state["quotes"] = {
            q["symbol"]: {"price": q.get("price"), "session": q.get("session"),
                          "levels": sorted((q.get("levels") or {}).keys())}
            for q in out.get("quotes", [])
        }
        if out.get("errors"):
            state["quoteErrors"] = out["errors"]
        log.info("data self-check get_quotes: %s", json.dumps(state["quotes"], default=_json_default)[:500])
    except Exception as exc:  # never let the check affect the server
        state["quotesError"] = str(exc)[:200]
        log.warning("data self-check get_quotes failed: %s", exc)
    for screen in ("day_gainers", "trending"):
        try:
            text = await get_market_movers(screen, 5)
            if text.startswith("Error"):
                state[f"movers_{screen}"] = text[:200]
                log.warning("data self-check %s: %s", screen, text[:200])
            else:
                out = json.loads(text)
                state[f"movers_{screen}"] = [r.get("symbol") for r in out.get("quotes", [])]
                log.info("data self-check %s: %s", screen, state[f"movers_{screen}"])
        except Exception as exc:
            state[f"movers_{screen}"] = f"failed: {str(exc)[:200]}"
            log.warning("data self-check %s failed: %s", screen, exc)
    DATA_CHECK_STATE.clear()
    DATA_CHECK_STATE.update(state)


def health_payload() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "yahoo-finance-mcp",
        "version": SERVER_VERSION,
        "yfinance": getattr(yf, "__version__", "?"),
        "startedAt": yd.utc_iso(STARTED_AT),
        "uptimeSeconds": int(time.time() - STARTED_AT),
        "transports": {
            "sse": "GET /sse + POST /messages/",
            "streamableHttp": ["POST /mcp"] + (["POST /sse"] if STREAMABLE_ON_SSE_PATH else []),
        },
        "keepalive": {"mode": KEEPALIVE_MODE, "target": bool(KEEPALIVE_URL), **KEEPALIVE_STATE},
        "shariaSelfCheck": SHARIA_CHECK_STATE,
        "dataSelfCheck": DATA_CHECK_STATE,
        "upstream": yd.status_snapshot(),
    }


async def _send_json(send, status: int, payload: Any, head: bool = False, headers=()) -> None:
    body = json.dumps(payload, default=_json_default).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"cache-control", b"no-store"),
                (b"content-length", str(len(body)).encode()),
                *headers,
            ],
        }
    )
    await send({"type": "http.response.body", "body": b"" if head else body})


def build_web_app():
    """ASGI app serving legacy SSE, Streamable HTTP (stateless) and /health."""
    from mcp.server.streamable_http_manager import StreamableHTTPASGIApp

    sse_app = yfinance_server.sse_app(sse_path="/sse", message_path="/messages/", host="0.0.0.0")
    yfinance_server.streamable_http_app(
        streamable_http_path="/mcp", stateless_http=True, json_response=True, host="0.0.0.0"
    )
    session_manager = yfinance_server.session_manager
    streamable = StreamableHTTPASGIApp(session_manager)

    async def lifespan(receive, send) -> None:
        started = False
        try:
            await receive()  # lifespan.startup
            async with session_manager.run():
                keepalive = asyncio.create_task(_keepalive_loop())
                self_check = asyncio.create_task(_sharia_self_check())
                data_check = asyncio.create_task(_data_self_check())
                try:
                    await send({"type": "lifespan.startup.complete"})
                    started = True
                    log.info(
                        "Yahoo Finance MCP %s ready (keep-alive: %s)",
                        SERVER_VERSION,
                        KEEPALIVE_MODE if KEEPALIVE_URL else "off (no public URL)",
                    )
                    while (await receive())["type"] != "lifespan.shutdown":
                        pass
                finally:
                    keepalive.cancel()
                    self_check.cancel()
                    data_check.cancel()
        except BaseException as exc:  # noqa: BLE001 - report to the server
            log.exception("lifespan failure")
            kind = "lifespan.shutdown.failed" if started else "lifespan.startup.failed"
            await send({"type": kind, "message": str(exc)})
            return
        await send({"type": "lifespan.shutdown.complete"})

    async def app(scope, receive, send) -> None:
        kind = scope["type"]
        if kind == "lifespan":
            await lifespan(receive, send)
            return
        if kind != "http":
            if kind == "websocket":
                await receive()
                await send({"type": "websocket.close", "code": 1003})
            return
        path = scope.get("path") or "/"
        method = scope.get("method", "GET")
        if path in ("/", "/health", "/healthz"):
            if method in ("GET", "HEAD"):
                await _send_json(send, 200, health_payload(), head=method == "HEAD")
            else:
                await _send_json(send, 405, {"error": "method not allowed"}, headers=[(b"allow", b"GET, HEAD")])
            return
        if path in ("/mcp", "/mcp/"):
            await streamable(scope, receive, send)
            return
        if path in ("/sse", "/sse/") and STREAMABLE_ON_SSE_PATH and method in ("POST", "DELETE"):
            # Clients that try Streamable HTTP first are served directly instead
            # of falling back to legacy SSE. GET always opens the legacy SSE
            # stream: claude.ai's SSE client sends MCP headers on that GET too.
            await streamable(scope, receive, send)
            return
        await sse_app(scope, receive, send)

    return app


def main() -> None:
    transport = (os.environ.get("MCP_TRANSPORT") or "").strip().lower()
    if not transport:
        transport = "http" if os.environ.get("PORT") else "stdio"
    if transport == "stdio":
        yfinance_server.run()
        return

    import uvicorn

    port = int(os.environ.get("PORT", 8080))
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"Starting Yahoo Finance MCP server {SERVER_VERSION} on {host}:{port} ...", flush=True)
    uvicorn.run(
        build_web_app(),
        host=host,
        port=port,
        log_level="info",
        proxy_headers=True,
        forwarded_allow_ips="*",
        timeout_keep_alive=65,
        lifespan="on",
    )


if __name__ == "__main__":
    main()
