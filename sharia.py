"""Sharia screening lookups for the MCP server.

Primary source: Yaqeen (yaaqen.com), a free US-stock filter that applies the
standards of Al-Rajhi's Sharia committee and shows, for each ticker, a public
classification and its last update date.

Access is deliberately polite: robots.txt is honoured (checked at run time),
requests are spaced out, results are cached for an hour, and an honest
User-Agent is sent. Chart Idea (chart-idea.com), the user's second choice,
answers automated requests with a bot-verification page, so it is linked for a
manual check and never fetched.
"""

from __future__ import annotations

import html as _html
import re
import threading
import time
import urllib.robotparser
from typing import Any
from urllib.parse import quote as _urlquote

import requests

import yahoo_data as yd

YAQEEN_BASE = "https://yaaqen.com"
CHART_IDEA_PAGE = "https://chart-idea.com/filter/company_detail/{symbol}/"
# Stock Hunter's filter ("نبض الأسهم") shows per-stock results only after login,
# its pages carry reCAPTCHA and its robots.txt could not be read (checked
# 30 Sep 2026), so it is linked for a manual check and never fetched.
STOCK_HUNTER_PAGE = "https://usastockhunteracademy.com/halal-stocks-usa/"
USER_AGENT = "Mozilla/5.0 (compatible; YahooFinanceMCP-ShariaCheck/1.0; personal use)"
MIN_INTERVAL = 1.5  # seconds between two requests to yaaqen.com
ROBOTS_TTL = 24 * 3600.0

# Labels Yaqeen shows under "توافق الشريعة"; normalised codes for callers.
LABELS = {
    "غير شرعي": "non_compliant",
    "غير متوافق": "non_compliant",
    "محل نظر": "questionable",
    "مختلط": "mixed",
    "شرعي": "compliant",
    "متوافق": "compliant",
}

_lock = threading.Lock()
_last_request = 0.0
_robots: dict[str, Any] = {"parser": None, "at": 0.0, "allow_all": None, "unreachable": None}


def _get(url: str, timeout: float = 10.0) -> requests.Response:
    """GET with spacing between requests (called from worker threads)."""
    global _last_request
    with _lock:
        wait = MIN_INTERVAL - (time.monotonic() - _last_request)
        if wait > 0:
            time.sleep(wait)
        _last_request = time.monotonic()
    with yd.upstream_slot():
        try:
            return requests.get(
                url,
                headers={"User-Agent": USER_AGENT, "Accept-Language": "ar,en;q=0.8"},
                timeout=timeout,
            )
        except requests.RequestException as exc:
            raise yd.UpstreamError(f"{type(exc).__name__}: {exc}") from exc


def robots_allows(url: str) -> bool:
    """RFC 9309: a 4xx robots.txt means allow; a 5xx or unreachable one means
    "do not crawl for now". That second case is a temporary failure, not a
    rule, so it raises UpstreamError: the caller then backs off, serves its last
    good result, and caches nothing."""
    now = time.time()
    if now - _robots["at"] > ROBOTS_TTL:
        parser, allow_all, unreachable = None, None, None
        try:
            resp = _get(YAQEEN_BASE + "/robots.txt")
            if resp.status_code == 200:
                parser = urllib.robotparser.RobotFileParser()
                parser.parse(resp.text.splitlines())
            elif 400 <= resp.status_code < 500:
                allow_all = True
            else:
                unreachable = f"HTTP {resp.status_code}"
        except yd.UpstreamError as exc:
            unreachable = str(exc)
        # An unreachable robots.txt is retried after 10 minutes, not 24 hours.
        checked_at = now if unreachable is None else now - ROBOTS_TTL + 600
        _robots.update(parser=parser, allow_all=allow_all, unreachable=unreachable, at=checked_at)
    if _robots.get("unreachable"):
        raise yd.UpstreamError(f"تعذّر الوصول إلى موقع يقين ({_robots['unreachable']})")
    if _robots["parser"] is not None:
        return _robots["parser"].can_fetch(USER_AGENT, url)
    return bool(_robots["allow_all"])


def html_to_lines(page: str) -> list[str]:
    """Visible text of an HTML page, one element per line."""
    page = re.sub(r"(?is)<(script|style|noscript|svg)[^>]*>.*?</\1>", " ", page)
    page = re.sub(r"(?s)<!--.*?-->", " ", page)
    page = re.sub(r"<[^>]+>", "\n", page)
    text = _html.unescape(page).replace("\xa0", " ")
    return [re.sub(r"\s+", " ", line).strip() for line in text.split("\n") if line.strip()]


def _arabic_words(line: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^؀-ۿ\s]", " ", line)).strip()


def parse_yaqeen(page: str) -> dict[str, Any]:
    """Extract the Sharia label and update date from a Yaqeen stock page."""
    lines = html_to_lines(page)
    head = next((i for i, line in enumerate(lines) if "توافق الشريعة" in line), None)
    if head is None:
        return {"label": None}
    label = None
    # The heading itself contains "الشرعية"; only whole lines count as labels.
    for line in lines[head + 1 : head + 15]:
        words = _arabic_words(line)
        if words in LABELS:
            label = words
            break
    if label is None:  # heading and label rendered on one line
        tail = _arabic_words(lines[head]).split("للراجحي", 1)[-1].strip()
        label = tail if tail in LABELS else None
    updated = None
    match = re.search(r"تم التحديث بتاريخ\s*(\d{1,2})[-/](\d{1,2})[-/](\d{4})", "\n".join(lines))
    if match:
        day, month, year = match.groups()
        updated = f"{year}-{int(month):02d}-{int(day):02d}"
    purification = None
    for line in lines[head : head + 15]:
        pct = re.search(r"نسبة التطهير\D{0,20}(\d+(?:[.,]\d+)?)\s*%", line)
        if pct:
            purification = float(pct.group(1).replace(",", "."))
            break
    h1 = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", page)
    title = " ".join(html_to_lines(h1.group(1))) if h1 else None
    return {"label": label, "updated": updated, "purificationPct": purification, "title": title}


def fetch_yaqeen(symbol: str) -> dict[str, Any]:
    """Classification of one ticker on Yaqeen (runs in a worker thread)."""
    # Class shares are "BRK-B" on Yahoo but may be "BRK.B" elsewhere.
    candidates = [symbol] + ([symbol.replace("-", ".")] if "-" in symbol else [])
    first_url = f"{YAQEEN_BASE}/stocks/{_urlquote(symbol, safe='')}"
    for candidate in candidates:
        url = f"{YAQEEN_BASE}/stocks/{_urlquote(candidate, safe='')}"
        base = {"source": "يقين", "standard": "معايير الهيئة الشرعية لشركة الراجحي", "url": url}
        if not robots_allows(url):
            return {**base, "available": False, "reason": "robots.txt في موقع يقين لا يسمح بالاستعلام الآلي"}
        resp = _get(url)
        if resp.status_code == 404:
            continue
        if resp.status_code == 429:
            raise yd.RateLimited()
        if resp.status_code >= 400:
            raise yd.UpstreamError(f"HTTP {resp.status_code} from yaaqen.com")
        parsed = parse_yaqeen(resp.text)
        if not parsed["label"]:
            return {**base, "available": False, "reason": "لم يظهر تصنيف شرعي في صفحة السهم"}
        return {
            **base,
            "available": True,
            "label": parsed["label"],
            "code": LABELS[parsed["label"]],
            "updated": parsed["updated"],
            "purificationPct": parsed["purificationPct"],
            "title": parsed["title"],
        }
    return {
        "source": "يقين",
        "standard": "معايير الهيئة الشرعية لشركة الراجحي",
        "url": first_url,
        "available": False,
        "reason": "السهم غير موجود في يقين",
    }


def chart_idea_link(symbol: str) -> dict[str, Any]:
    return {
        "source": "شارت آيديا",
        "standard": "معايير الهيئة الشرعية لشركة الراجحي",
        "url": CHART_IDEA_PAGE.format(symbol=_urlquote(symbol, safe="")),
        "available": False,
        "reason": "يحجب الموقع الاستعلام الآلي بصفحة تحقق أمني؛ افتح الرابط للتحقق يدويًّا",
    }


def stock_hunter_link(symbol: str) -> dict[str, Any]:
    return {
        "source": "صائد الأسهم (نبض الأسهم)",
        "standard": "الراجحي والبلاد ودار الإفتاء المصرية",
        "url": STOCK_HUNTER_PAGE,
        "symbol": symbol,
        "available": False,
        "reason": "نتيجة السهم تظهر بعد تسجيل الدخول في الموقع؛ ابحث عن الرمز يدويًّا",
    }


# Industries whose core activity is impermissible under the usual screens.
PROHIBITED_INDUSTRY_HINTS = (
    "bank", "insurance", "credit services", "mortgage",
    "brewer", "wineries", "distiller", "tobacco", "gambling", "casino",
)


def activity_hint(info: dict[str, Any]) -> str | None:
    industry = str(info.get("industry") or info.get("industryDisp") or "").lower()
    if any(hint in industry for hint in PROHIBITED_INDUSTRY_HINTS):
        return f"نشاط الشركة ({info.get('industry')}) من الأنشطة التي تمنعها الضوابط عادة"
    return None
