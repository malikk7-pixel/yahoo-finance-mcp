"""Offline tests for the Sharia lookup (Yaqeen first, Chart Idea link as fallback)."""

import asyncio
import json

import pandas as pd
import pytest

import server
import sharia
import yahoo_data as yd


def page(label_block: str, title: str = "سهم Applied Opt") -> str:
    return f"""<html><head><title>سهم (AAOI) Applied Opt: الشرعية، الأداء وتوزيع الأرباح - موقع يقين</title>
<script>window.labels = ["شرعي", "غير شرعي"];</script><style>.x{{content:"شرعي"}}</style></head>
<body><nav><a href="/">الرئيسية</a><a href="/stocks">فلتر الأسهم</a></nav>
<h1>{title}</h1><span>AAOI-NASDAQ</span>
<div class="card">{label_block}
<div>نسبة التطهير <a href="/register">فتح</a></div><hr>
<p>تم التحديث بتاريخ 20-09-2026</p>
<a href="/register">عرض سجل التوافق الشرعي</a></div></body></html>"""


HEADING = "<h3>توافق الشريعة<span>وفقاً للمعايير الشرعية للراجحي</span></h3>"


@pytest.mark.parametrize("label", ["شرعي", "غير شرعي", "محل نظر"])
def test_parse_yaqeen_labels(label):
    parsed = sharia.parse_yaqeen(page(HEADING + f'<div class="badge"> {label} </div>'))
    assert parsed["label"] == label
    assert parsed["updated"] == "2026-09-20"
    assert parsed["title"] == "سهم Applied Opt"


def test_parse_yaqeen_label_on_heading_line_and_missing_section():
    inline = "<p>توافق الشريعة وفقاً للمعايير الشرعية للراجحي محل نظر</p>"
    assert sharia.parse_yaqeen(page(inline))["label"] == "محل نظر"
    assert sharia.parse_yaqeen("<html><body><h1>صفحة أخرى</h1></body></html>")["label"] is None


class Resp:
    def __init__(self, status, text=""):
        self.status_code, self.text = status, text


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    yd.CACHE._data.clear()
    for b in yd.BREAKERS.values():
        b.success()
    sharia._robots.update(parser=None, allow_all=None, unreachable=None, at=0.0)
    monkeypatch.setattr(sharia, "MIN_INTERVAL", 0)
    yield
    yd.CACHE._data.clear()


def test_fetch_yaqeen_respects_robots(monkeypatch):
    seen = []

    def fake_get(url, timeout=10.0):
        seen.append(url)
        if url.endswith("/robots.txt"):
            return Resp(200, "User-agent: *\nDisallow: /stocks/\n")
        raise AssertionError("must not fetch a disallowed page")

    monkeypatch.setattr(sharia, "_get", fake_get)
    out = sharia.fetch_yaqeen("AAPL")
    assert out["available"] is False and "robots.txt" in out["reason"]
    assert seen == ["https://yaaqen.com/robots.txt"]


def test_fetch_yaqeen_parses_page_and_handles_404(monkeypatch):
    def fake_get(url, timeout=10.0):
        if url.endswith("/robots.txt"):
            return Resp(404)
        if url.endswith("/NOPE"):
            return Resp(404)
        return Resp(200, page(HEADING + "<div>محل نظر</div>"))

    monkeypatch.setattr(sharia, "_get", fake_get)
    out = sharia.fetch_yaqeen("AAPL")
    assert (out["available"], out["label"], out["code"], out["updated"]) == (True, "محل نظر", "questionable", "2026-09-20")
    assert out["url"] == "https://yaaqen.com/stocks/AAPL"
    missing = sharia.fetch_yaqeen("NOPE")
    assert missing["available"] is False and "غير موجود" in missing["reason"]


def yaqeen_result(label):
    return {"source": "يقين", "url": "https://yaaqen.com/stocks/X", "available": True,
            "label": label, "code": sharia.LABELS[label], "updated": "2026-09-20"}


def test_tool_compliant_verdict_needs_no_fallback(monkeypatch):
    monkeypatch.setattr(sharia, "fetch_yaqeen", lambda s: yaqeen_result("شرعي"))
    out = json.loads(asyncio.run(server.get_sharia_status("aaoi")))
    assert out["symbol"] == "AAOI" and out["verdict"] == "شرعي" and out["verdictSource"] == "يقين"
    assert len(out["sources"]) == 1 and "indicators" not in out


class FakeTicker:
    def __init__(self, symbol):
        pass

    @property
    def income_stmt(self):
        return pd.DataFrame({pd.Timestamp("2025-12-31"): [1e8, 2e6]}, index=["Total Revenue", "Interest Income"])


def test_tool_questionable_links_chart_idea_and_adds_indicators(monkeypatch):
    monkeypatch.setattr(sharia, "fetch_yaqeen", lambda s: yaqeen_result("محل نظر"))
    monkeypatch.setattr(server, "_fetch_info", lambda s: {
        "symbol": s, "quoteType": "EQUITY", "marketCap": 1e9, "totalDebt": 1e8, "industry": "Banks - Regional"})
    monkeypatch.setattr(server.yf, "Ticker", FakeTicker)
    out = json.loads(asyncio.run(server.get_sharia_status("AAPL")))
    assert out["verdict"] == "محل نظر"
    chart_idea = out["sources"][1]
    assert chart_idea["url"] == "https://chart-idea.com/filter/company_detail/AAPL/"
    assert chart_idea["available"] is False
    ind = out["indicators"]
    assert ind["debtToMarketCapPct"] == 10.0 and ind["interestIncomeToRevenuePct"] == 2.0
    assert "Banks" in ind["activityNote"] and ind["thresholds"]["debtToMarketCapPct"] == 30


def test_tool_reports_yaqeen_failure(monkeypatch):
    monkeypatch.setattr(sharia, "fetch_yaqeen", lambda s: (_ for _ in ()).throw(yd.RateLimited()))
    monkeypatch.setattr(server, "_fetch_info", lambda s: (_ for _ in ()).throw(yd.RateLimited()))
    monkeypatch.setattr(server.yf, "Ticker", FakeTicker)
    out = json.loads(asyncio.run(server.get_sharia_status("MSGY")))
    assert out["verdict"] == "غير متاح"
    assert "Too Many Requests" in out["sources"][0]["reason"]
    assert out["sources"][1]["source"] == "شارت آيديا"


def test_fetch_yaqeen_tries_dotted_class_share(monkeypatch):
    seen = []

    def fake_get(url, timeout=10.0):
        seen.append(url)
        if url.endswith("/robots.txt"):
            return Resp(404)
        if url.endswith("/BRK-B"):
            return Resp(404)
        return Resp(200, page(HEADING + "<div>شرعي</div>"))

    monkeypatch.setattr(sharia, "_get", fake_get)
    out = sharia.fetch_yaqeen("BRK-B")
    assert out["available"] is True and out["url"] == "https://yaaqen.com/stocks/BRK.B"
    assert seen[-2:] == ["https://yaaqen.com/stocks/BRK-B", "https://yaaqen.com/stocks/BRK.B"]


def test_startup_self_check_records_result(monkeypatch):
    async def no_sleep(_):
        return None

    async def fake_status(ticker):
        return json.dumps({"verdict": "محل نظر", "sources": [yaqeen_result("محل نظر")]}, ensure_ascii=False)

    monkeypatch.setattr(server.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(server, "get_sharia_status", fake_status)
    server.SHARIA_CHECK_STATE.clear()
    asyncio.run(server._sharia_self_check())
    state = server.SHARIA_CHECK_STATE
    assert state["verdict"] == "محل نظر" and state["yaqeenAvailable"] is True
    assert state["yaqeenUpdated"] == "2026-09-20"


def test_unreachable_site_is_a_temporary_failure_not_a_verdict(monkeypatch):
    def refused(url, timeout=10.0):
        raise yd.UpstreamError("ConnectionError: refused")

    monkeypatch.setattr(sharia, "_get", refused)
    with pytest.raises(yd.UpstreamError):
        sharia.fetch_yaqeen("AAPL")
    monkeypatch.setattr(server, "_fetch_info", lambda s: (_ for _ in ()).throw(yd.RateLimited()))
    monkeypatch.setattr(server.yf, "Ticker", FakeTicker)
    out = json.loads(asyncio.run(server.get_sharia_status("AAPL")))
    assert out["verdict"] == "غير متاح"
    assert "تعذّر الوصول" in out["sources"][0]["reason"]
    assert yd.CACHE.get("sharia|yaqeen|AAPL") is None  # nothing cached as a verdict


def test_questionable_result_also_links_stock_hunter_for_a_manual_check(monkeypatch):
    async def no_indicators(symbol):
        return {}

    monkeypatch.setattr(sharia, "fetch_yaqeen", lambda s: yaqeen_result("محل نظر"))
    monkeypatch.setattr(server, "_sharia_indicators", no_indicators)
    out = json.loads(asyncio.run(server.get_sharia_status("aapl")))
    hunter = out["sources"][2]
    assert hunter["url"] == sharia.STOCK_HUNTER_PAGE and hunter["available"] is False
    assert hunter["symbol"] == "AAPL" and "تسجيل الدخول" in hunter["reason"]
