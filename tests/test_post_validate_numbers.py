"""Tests for ai_report.post_validate_numbers (Phase 3 safety net).

Goal: 驗證 regex 抽數字邏輯處理千分位逗號、$ 單位、年份排除，
不誤判 EPS×PE 推導目標價（在 Section 8 但不在三欄內）。
"""
import pytest
from ai_report import post_validate_numbers


@pytest.fixture
def gt_action_plan():
    """2345 ground truth: entry 2212-2280 / SL 2030.71 / TP 3517.77."""
    return {
        'is_actionable': True,
        'rec_entry_low': 2212.0,
        'rec_entry_high': 2280.0,
        'rec_sl_price': 2030.71,
        'rec_tp_price': 3517.77,
    }


def _make_report(entry_zone, sl, tp, extra_section_8="", before="", after=""):
    """組一份最小可驗 markdown 報告 (Section 8 + 9)."""
    sec8 = (
        f"## 8. 投資建議與情境分析\n\n"
        f"| 項目 | 內容 |\n"
        f"|------|------|\n"
        f"| 綜合評級 | 買進 |\n"
        f"| **建議進場區間** | {entry_zone} |\n"
        f"| **停損價位** | {sl} |\n"
        f"| **停利價位** | {tp} |\n"
        f"| 建議倉位 | 0.7 |\n"
        f"{extra_section_8}\n"
        f"\n## 9. 資訊空白與不確定性\n\n暫無\n"
    )
    return f"{before}\n{sec8}\n{after}"


# ============================================================
# Verbatim PASS
# ============================================================

def test_verbatim_with_thousand_separator(gt_action_plan):
    """千分位逗號 2,212 / 2,030.71 應該 PASS."""
    md = _make_report(
        entry_zone="**2,212 元 至 2,280 元**",
        sl="**2,030.71 元**（方法：A. ATR）",
        tp="**3,517.77 元**",
    )
    r = post_validate_numbers(md, gt_action_plan)
    assert r['drift'] is False, f"unexpected drift: {r}"
    assert r['unexpected_numbers'] == []


def test_verbatim_no_thousand_separator(gt_action_plan):
    md = _make_report(
        entry_zone="2212 至 2280",
        sl="2030.71",
        tp="3517.77",
    )
    r = post_validate_numbers(md, gt_action_plan)
    assert r['drift'] is False, r


def test_verbatim_with_dollar_sign(gt_action_plan):
    """美股風格 $XXX.XX 也要過."""
    md = _make_report(
        entry_zone="$2,212 - $2,280",
        sl="$2,030.71",
        tp="$3,517.77",
    )
    r = post_validate_numbers(md, gt_action_plan)
    assert r['drift'] is False, r


# ============================================================
# Drift FAIL
# ============================================================

def test_drift_in_sl(gt_action_plan):
    """停損漂到 candidate sl_ma=1973.25."""
    md = _make_report(
        entry_zone="2,212 - 2,280",
        sl="**1,973.25 元** (B. MA20)",  # ← drift
        tp="3,517.77",
    )
    r = post_validate_numbers(md, gt_action_plan)
    assert r['drift'] is True, r
    assert 1973.25 in r['unexpected_numbers']


def test_drift_round_trip(gt_action_plan):
    """Claude 把 2030.71 round 成 2031 應該 PASS（在 0.5% tolerance 內）."""
    md = _make_report(
        entry_zone="2212 - 2280",
        sl="2031",  # 2030.71 ±0.5% = ±10.15, 2031 在範圍
        tp="3518",
    )
    r = post_validate_numbers(md, gt_action_plan)
    assert r['drift'] is False, r


def test_drift_paraphrase_range(gt_action_plan):
    """Claude 寫「2,150 附近進場」（漂掉超過 tolerance）."""
    md = _make_report(
        entry_zone="2,150 附近進場",  # 2150 距 2212 -2.8% > 0.5%
        sl="2,030.71",
        tp="3,517.77",
    )
    r = post_validate_numbers(md, gt_action_plan)
    assert r['drift'] is True, r
    assert 2150.0 in r['unexpected_numbers']


# ============================================================
# Edge cases
# ============================================================

def test_not_actionable_skip():
    """is_actionable=False 觀望路徑直接 skip."""
    ap = {'is_actionable': False, 'rec_entry_low': 0, 'rec_sl_price': 0, 'rec_tp_price': 0}
    md = "## 8. 投資建議\n| 建議進場區間 | 觀望，無進場價 |\n\n## 9.\n"
    r = post_validate_numbers(md, ap)
    assert r['drift'] is False
    assert 'skip' in r['note']


def test_section_8_not_found(gt_action_plan):
    """報告沒 Section 8（極端情況）→ skip 不 drift."""
    md = "## 1. 個股總覽\n\n... no section 8 ..."
    r = post_validate_numbers(md, gt_action_plan)
    assert r['drift'] is False
    assert 'not found' in r['note']


def test_eps_pe_in_other_section_not_checked(gt_action_plan):
    """情境目標價的 EPS=89.22 / PE=26 / 目標價 2,320 在 Section 8 內但不在三欄，
    不該被 flag 為 drift（屬合法 EPS×PE 推導）."""
    extra = (
        "\n### 情境目標價\n\n"
        "| 情境 | EPS 假設 | 本益比 | 目標價 |\n"
        "|------|---------|-------|--------|\n"
        "| 牛市 | EPS 100 | 30x | **3,000 元** |\n"
        "| 基本 | EPS 89.22 | 26x | **2,320 元** |\n"
        "| 熊市 | EPS 65 | 22x | **1,430 元** |\n"
    )
    md = _make_report(
        entry_zone="**2,212 元 至 2,280 元**",
        sl="**2,030.71 元**",
        tp="**3,517.77 元**",
        extra_section_8=extra,
    )
    r = post_validate_numbers(md, gt_action_plan)
    # 三欄是 verbatim → 不該 drift；情境目標價 1430/2320/3000 不在三欄 row 內，被 regex 排除
    assert r['drift'] is False, f"false positive on EPS×PE rows: {r}"


def test_year_2026_not_treated_as_price(gt_action_plan):
    """三欄文字含「2026」年份 不該被當價位."""
    md = _make_report(
        entry_zone="**2,212 元 至 2,280 元**（2026 Q1 進場）",  # 2026 是年份
        sl="**2,030.71 元**",
        tp="**3,517.77 元**",
    )
    r = post_validate_numbers(md, gt_action_plan)
    assert 2026.0 not in r['unexpected_numbers'], r


def test_empty_action_plan():
    r = post_validate_numbers("# Report", {})
    assert r['drift'] is False
    assert 'skip' in r['note']


def test_5ma_not_treated_as_price(gt_action_plan):
    """三欄 entry_desc 含「5MA-現價」「20MA」這類技術指標標記
    不該抓「5」「20」當價位（real 2345 case）."""
    md = _make_report(
        entry_zone="**2,212 元 至 2,280 元**（積極操作 5MA-現價）",
        sl="**2,030.71 元**（方法：A. ATR 波動停損）",
        tp="**3,517.77 元**",
    )
    r = post_validate_numbers(md, gt_action_plan)
    assert r['drift'] is False, f"5MA 中的 5 被誤抓: {r}"


def test_position_size_07_not_treated_as_price(gt_action_plan):
    """倉位 0.7 / RR 4.97 不該被抓（必須在三欄某 row 內才抓，
    倉位欄是另一個 row）."""
    md = _make_report(
        entry_zone="**2,212 元 至 2,280 元**",
        sl="**2,030.71 元**",
        tp="**3,517.77 元** RR 4.97",  # RR 4.97 在停利 row 末尾
    )
    r = post_validate_numbers(md, gt_action_plan)
    # 4.97 < threshold (gt_min=2030.71 × 0.3 = 609)，應被過濾
    assert 4.97 not in r['unexpected_numbers'], r
    assert r['drift'] is False, r
