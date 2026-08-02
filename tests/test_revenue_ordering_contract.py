"""月營收「新→舊排序」契約 + YoY 必須在裁切**之前**算完。

這是本 repo 踩過最痛的坑（2026-06-07，commit 90055f3）：
`get_monthly_revenue` 回傳的是**新→舊**排序，而舊版先裁切到呼叫端要的月數才算 YoY ——
於是最舊的 12 個月找不到「去年同月」，YoY 全部落回 0.0，營收監控從未觸發。

2026-08-02 code review 第六節把「缺這條回歸測試」列為測試缺口（實作目前正確，
但沒有任何測試釘住它）。
"""
import pandas as pd
import pytest

from dividend_revenue import RevenueTracker


@pytest.fixture()
def analyzer():
    return RevenueTracker()


def _months(start_year=2024, start_month=1, n=36, base=100.0, growth=1.0):
    """造 n 個月的營收，**升序**輸入（故意與輸出契約相反，測排序有沒有生效）。"""
    rows = []
    y, m = start_year, start_month
    for i in range(n):
        rows.append({'year_month': f'{y}-{m:02d}', 'revenue': base * (growth ** i)})
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
#  排序契約
# --------------------------------------------------------------------------- #

def test_metrics_output_is_newest_first(analyzer):
    out = analyzer._compute_revenue_metrics(_months(n=24))

    ym = list(out['year_month'])
    assert ym == sorted(ym, reverse=True), '契約是新→舊；下游用 iloc[0] 取最新一期'
    assert ym[0] == '2025-12'
    assert ym[-1] == '2024-01'


def test_latest_period_is_iloc_zero(analyzer):
    """`get_revenue_alert` 與多個 view 都用 df.iloc[0] 當「最新一期」。"""
    out = analyzer._compute_revenue_metrics(_months(n=18, base=100.0, growth=1.10))

    assert out['year_month'].iloc[0] == '2025-06'
    assert out['revenue'].iloc[0] == out['revenue'].max(), '成長序列的最新月應為最大值'


def test_descending_input_gives_the_same_result_as_ascending(analyzer):
    asc = _months(n=24)
    desc = asc.iloc[::-1].reset_index(drop=True)

    a = analyzer._compute_revenue_metrics(asc)
    b = analyzer._compute_revenue_metrics(desc)

    pd.testing.assert_frame_equal(a, b), '輸入順序不該影響輸出'


# --------------------------------------------------------------------------- #
#  YoY 必須算得出來（原始缺陷：先裁切 -> 全 0）
# --------------------------------------------------------------------------- #

def test_yoy_is_computed_when_prior_year_month_exists(analyzer):
    """固定 +10% 月成長 -> 12 個月後 YoY = 1.1^12 - 1 ≈ +213.8%。"""
    out = analyzer._compute_revenue_metrics(_months(n=24, growth=1.10))

    latest = out.iloc[0]
    assert latest['yoy_pct'] == pytest.approx((1.10 ** 12 - 1) * 100, abs=0.5)
    assert latest['mom_pct'] == pytest.approx(10.0, abs=0.1)


def test_oldest_twelve_months_have_no_yoy_by_construction(analyzer):
    """最舊 12 個月沒有去年同月可比，YoY 落回 0 是正確的 —— 但只該是最舊那 12 個。"""
    out = analyzer._compute_revenue_metrics(_months(n=24, growth=1.05))

    newest_12 = out.iloc[:12]
    oldest_12 = out.iloc[12:]
    assert (newest_12['yoy_pct'] != 0).all(), '最新 12 個月都要算得出 YoY'
    assert (oldest_12['yoy_pct'] == 0).all()


def test_truncating_before_computing_yoy_would_zero_everything(analyzer):
    """釘住原始缺陷：只餵 12 個月（等於先裁切）時 YoY 全 0。

    這是 2026-06-07 事故的形狀 —— 實作若退回「先 head(months) 再算指標」，
    營收監控就會像當時一樣永不觸發。
    """
    only_12 = _months(n=12, growth=1.10)

    out = analyzer._compute_revenue_metrics(only_12)

    assert (out['yoy_pct'] == 0).all(), \
        '窗內沒有去年同月 -> YoY 只能是 0，所以裁切必須在算完指標之後'


def test_fetch_unified_computes_metrics_before_truncating(analyzer, monkeypatch):
    """`_fetch_revenue_unified(months=12)` 必須先抓更長的窗、算完 YoY 才裁到 12。"""
    import cache_manager

    # 造 36 個月的 raw（新→舊由被測程式自己排），欄位比照 FinMind
    rows = []
    y, m = 2024, 1
    for i in range(36):
        rows.append({'date': f'{y}-{m:02d}-01', 'revenue': 100.0 * (1.10 ** i),
                     'revenue_year': y, 'revenue_month': m})
        m += 1
        if m > 12:
            y, m = y + 1, 1
    raw = pd.DataFrame(rows)
    monkeypatch.setattr(cache_manager, 'get_cached_fundamentals',
                        lambda *a, **k: raw.copy())
    monkeypatch.setattr(cache_manager, 'get_finmind_cached',
                        lambda *a, **k: raw.copy())

    out = analyzer._fetch_revenue_unified('2330', months=12)

    assert out is not None and len(out) == 12, '回傳應裁切到呼叫端要求的 12 個月'
    assert list(out['year_month']) == sorted(out['year_month'], reverse=True)
    assert (out['yoy_pct'] != 0).all(), \
        '12 個月全都要有 YoY —— 若為 0 表示又變成先裁切才算'
