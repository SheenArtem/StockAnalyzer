"""投組頭條數字不可誤導：短樣本不年化、空倉日不進報酬統計。

2026-08-02 code review 第六節：`portfolio_view.py:108` 的年化 CAGR / 日勝率把
「尚未建倉」的 0 報酬日一起算 —— 8 個交易日 1.95% 報酬被年化成 **+83.7%**，而那是
這個 tab 的頭條數字。`build_nav_series` 對 prev_mv=0（尚未建倉／全數出場）的日子
強制 ret=0，這些 0 併進分母會壓低日勝率、壓低波動度、進而虛增 Sharpe。
"""
import numpy as np
import pandas as pd
import pytest

import portfolio_view as pv


def _nav(rets, mvs=None):
    """組一段 nav_df；mvs 為當日收盤市值（None = 全程有部位）。"""
    idx = pd.bdate_range('2026-01-01', periods=len(rets))
    if mvs is None:
        mvs = [1_000_000.0] * len(rets)
    ret = pd.Series(rets, index=idx, dtype='float64')
    return pd.DataFrame({
        'mv': pd.Series(mvs, index=idx, dtype='float64'),
        'flow': 0.0,
        'ret': ret,
        'nav': (1 + ret).cumprod(),
    })


def test_eight_day_sample_is_not_annualised():
    """原始缺陷：8 日 +1.95% → 年化 +83.7%。"""
    daily = 1.0195 ** (1 / 8) - 1.0
    m = pv._portfolio_metrics(_nav([daily] * 8))

    assert m, '仍應回其他指標'
    assert m['total_return'] == pytest.approx(0.0195, abs=1e-4)
    assert m['cagr'] is None, '8 個交易日不可年化'
    assert m['sharpe'] is None
    assert m['can_annualize'] is False
    # 確認舊公式真的會噴出 +83.7%，即本測試釘住的就是那個錯誤
    assert (1.0195 ** (252 / 8) - 1.0) == pytest.approx(0.837, abs=0.01)


def test_long_enough_sample_still_annualises():
    m = pv._portfolio_metrics(_nav([0.0004] * pv._MIN_ANNUALIZE_DAYS))

    assert m['can_annualize'] is True
    assert m['cagr'] is not None
    assert m['sharpe'] is not None
    assert m['n_periods'] == pv._MIN_ANNUALIZE_DAYS


def test_threshold_is_inclusive_and_one_day_short_is_not():
    just_enough = pv._portfolio_metrics(_nav([0.0004] * pv._MIN_ANNUALIZE_DAYS))
    one_short = pv._portfolio_metrics(_nav([0.0004] * (pv._MIN_ANNUALIZE_DAYS - 1)))

    assert just_enough['cagr'] is not None
    assert one_short['cagr'] is None


def test_flat_no_position_days_excluded_from_win_rate():
    """前半段空倉（ret 被強制為 0）不可算成「輸的日子」。"""
    rets = [0.0] * 30 + [0.01, -0.005] * 15
    mvs = [0.0] * 30 + [1_000_000.0] * 30
    m = pv._portfolio_metrics(_nav(rets, mvs))

    # 有部位的 30 天裡，第一天的 prev_mv 仍是 0（空倉最後一天），故 29 天入統計；
    # 那 29 天由 rets[31:] 起算 = -0.005 開頭交替，正報酬 14 天。
    assert m['n_exposed'] == 29
    assert m['n_periods'] == 60, '年化分母仍是經過的交易日'
    assert m['win_rate'] == pytest.approx(14 / 29, abs=1e-6), \
        '空倉的 0 報酬日不得進日勝率分母'
    # 若把 30 個空倉 0 併進分母，勝率會被稀釋到 ~0.25
    assert m['win_rate'] > 0.4


def test_flat_days_do_not_deflate_volatility():
    """把空倉 0 併進來會壓低波動度、虛增 Sharpe。"""
    active = [0.01, -0.005] * 30
    with_flat = pv._portfolio_metrics(_nav([0.0] * 30 + active,
                                          [0.0] * 30 + [1e6] * 60))
    all_active = pv._portfolio_metrics(_nav(active, [1e6] * 60))

    assert with_flat['annual_vol'] == pytest.approx(all_active['annual_vol'], rel=0.05), \
        '波動度應只由有部位的日子決定'


def test_total_return_and_mdd_unaffected_by_the_filter():
    """乘 1.0 不改變累積；空倉也不會製造新的回撤。"""
    active = [0.02, -0.01, 0.015, -0.02] * 15
    m_flat = pv._portfolio_metrics(_nav([0.0] * 10 + active, [0.0] * 10 + [1e6] * 60))
    m_pure = pv._portfolio_metrics(_nav(active, [1e6] * 60))

    assert m_flat['total_return'] == pytest.approx(m_pure['total_return'], rel=1e-9)
    assert m_flat['mdd'] == pytest.approx(m_pure['mdd'], rel=1e-9)


def test_too_short_series_returns_empty_dict():
    assert pv._portfolio_metrics(_nav([0.01] * 5)) == {}


def test_missing_or_empty_frame_is_tolerated():
    assert pv._portfolio_metrics(None) == {}
    assert pv._portfolio_metrics(pd.DataFrame()) == {}
    assert pv._portfolio_metrics(pd.DataFrame({'nav': [1.0, 1.1]})) == {}


def test_frame_without_mv_column_falls_back_to_all_returns():
    """舊呼叫端若少 mv 欄不可整個爆掉。"""
    df = _nav([0.001] * pv._MIN_ANNUALIZE_DAYS).drop(columns=['mv'])

    m = pv._portfolio_metrics(df)

    assert m['n_exposed'] == pv._MIN_ANNUALIZE_DAYS
    assert m['cagr'] is not None


def test_all_flat_series_does_not_divide_by_zero():
    """全程空倉：不可丟例外，也不可回出 inf/nan 頭條數字。"""
    m = pv._portfolio_metrics(_nav([0.0] * 60, [0.0] * 60))

    assert m['total_return'] == pytest.approx(0.0)
    assert m['win_rate'] == pytest.approx(0.0)
    assert not np.isinf(m['mdd'])
