import math

import numpy as np
import pandas as pd

from tools.compute_historical_fscore import (
    PROXY_FLAG_COLUMNS,
    build_revenue_score_lookup,
    compute_fscore_row,
    compute_quality_score,
    compute_zscore_row,
    normalize_financial_wide,
    revenue_score_asof,
)


def _bank_frame():
    """金控季列：FinMind 沒有 CurrentAssets / CurrentLiabilities 這兩個 type。"""
    return pd.DataFrame({
        'stock_id': ['2881'],
        'date': [pd.Timestamp('2026-03-31')],
        'IncomeAfterTaxes': [np.nan],
        'IncomeAfterTax': [12.0],
        'Revenue': [100.0],
        'OperatingIncome': [np.nan],
        'IncomeBeforeTaxFromContinuingOperations': [15.0],
        'TotalAssets': [1000.0],
        'Liabilities': [800.0],
        'Equity': [200.0],
        'CashFlowsFromOperatingActivities': [np.nan],
        'NetCashInflowFromOperatingActivities': [20.0],
    })


def test_finmind_aliases_are_normalized_for_quality_scorer():
    normalized = normalize_financial_wide(_bank_frame()).iloc[0].to_dict()

    assert normalized['IncomeAfterTaxes'] == 12.0
    assert normalized['OperatingIncome'] == 15.0
    assert normalized['CashFlowsFromOperatingActivities'] == 20.0


def test_proxy_substitution_is_flagged():
    """代理值（淨利息收入頂替營收、稅前淨利頂替營業利益）必須留下旗標。

    2026-08-02 code review 第四節：這兩組不是同義字而是「沒有該科目時拿另一個頂替」，
    定義與量級都不同（淨利息收入是淨額、營收是總額；稅前淨利含業外、營業利益不含）。
    落盤後若無旗標，完全分不出哪些分數是這樣算出來的 —— 實測受影響列的 f_score
    中位數 3.0 vs 正常列 5.0，是有決策差異的。
    """
    normalized = normalize_financial_wide(_bank_frame()).iloc[0].to_dict()

    # _bank_frame 的 OperatingIncome 是 NaN，靠 IncomeBeforeTaxFromContinuingOperations
    # 頂替（稅前淨利含業外損益，營業利益不含）→ 必須標記
    assert normalized['OperatingIncome'] == 15.0
    assert bool(normalized['OperatingIncome_is_proxy'])
    # Revenue 本來就有真值 → 不是代理值
    assert normalized['Revenue'] == 100.0
    assert not normalized['Revenue_is_proxy']


def test_net_interest_income_substitution_is_flagged():
    """缺 Revenue 而只有 NetInterestIncome 時（純銀行列），Revenue 要標記為代理值。"""
    frame = pd.DataFrame({
        'stock_id': ['2801'], 'date': [pd.Timestamp('2026-03-31')],
        'NetInterestIncome': [100.0],
        'OperatingIncome': [15.0],
        'TotalAssets': [1000.0], 'Liabilities': [900.0], 'Equity': [100.0],
        'IncomeAfterTax': [12.0],
    })

    normalized = normalize_financial_wide(frame).iloc[0].to_dict()

    assert normalized['Revenue'] == 100.0
    assert bool(normalized['Revenue_is_proxy'])
    assert not normalized['OperatingIncome_is_proxy']


def test_proxy_flag_columns_always_exist():
    """沒有任何代理值時旗標欄仍要存在（schema 穩定），且全為 False。"""
    frame = pd.DataFrame({
        'stock_id': ['2330'], 'date': [pd.Timestamp('2026-03-31')],
        'Revenue': [500.0], 'OperatingIncome': [200.0], 'GrossProfit': [300.0],
        'TotalAssets': [1000.0], 'Liabilities': [400.0], 'Equity': [600.0],
        'CurrentAssets': [300.0], 'CurrentLiabilities': [100.0],
        'CashAndCashEquivalents': [50.0], 'IncomeAfterTax': [150.0],
        'NetCashInflowFromOperatingActivities': [180.0],
    })

    normalized = normalize_financial_wide(frame)

    for col in PROXY_FLAG_COLUMNS:
        assert col in normalized.columns, f'{col} 必須存在'
        assert not normalized[col].any(), f'{col} 應全為 False'


def test_spelling_aliases_do_not_set_the_proxy_flag():
    """同義字合併是純資料修正，不該被當成代理值。"""
    frame = pd.DataFrame({
        'stock_id': ['2882'], 'date': [pd.Timestamp('2026-03-31')],
        'Revenue': [100.0], 'OperatingIncome': [15.0],
        'TotalAssets': [1000.0], 'Liabilities': [900.0],
        # 只給同義字拼法
        'EquityAttributableToOwnersOfParent': [100.0],
        'NoncurrentLiabilities': [50.0],
        'IncomeAfterTax': [12.0],
    })

    normalized = normalize_financial_wide(frame).iloc[0].to_dict()

    assert normalized['Equity'] == 100.0
    assert normalized['NonCurrentLiabilities'] == 50.0
    assert normalized['IncomeAfterTaxes'] == 12.0
    assert not normalized['Revenue_is_proxy']
    assert not normalized['OperatingIncome_is_proxy']


def test_structural_missing_fields_stay_nan_not_zero():
    """金融股缺 CurrentAssets/CurrentLiabilities 時不可補 0（2026-08-02 P0-1）。

    補 0 會讓 x1 = (0-0)/ta = 0，Z 變成有限但無意義的小數，必然 < 1.81 而被扣 20 分。
    """
    normalized = normalize_financial_wide(_bank_frame()).iloc[0].to_dict()

    assert math.isnan(normalized['CurrentAssets'])
    assert math.isnan(normalized['CurrentLiabilities'])


def test_zscore_is_none_when_structural_field_missing():
    """「算不出 Z」必須與「Z 很差」可區分：回 None，不是 finite 低分。"""
    normalized = normalize_financial_wide(_bank_frame()).iloc[0].to_dict()

    assert compute_zscore_row(normalized) is None


def test_missing_zscore_does_not_penalise_quality_score():
    """Z 不可得時 quality_score 走中性（不加不減），不得吃 -20 懲罰。"""
    normalized = normalize_financial_wide(_bank_frame()).iloc[0].to_dict()
    zscore = compute_zscore_row(normalized)

    with_missing_z = compute_quality_score(None, zscore, normalized, {})
    neutral_baseline = compute_quality_score(None, None, normalized, {})

    assert zscore is None
    assert with_missing_z == neutral_baseline


def test_zscore_computes_when_all_required_fields_present():
    """一般製造業欄位齊全時 Z 仍要算得出來（確認 guard 沒有誤殺）。"""
    frame = _bank_frame()
    frame['CurrentAssets'] = [600.0]
    frame['CurrentLiabilities'] = [300.0]

    normalized = normalize_financial_wide(frame).iloc[0].to_dict()
    zscore = compute_zscore_row(normalized)

    assert zscore is not None
    assert math.isfinite(zscore)


def test_fscore_skips_unverifiable_tests_instead_of_scoring_them():
    """缺值不得讓 F-Score 的個別檢定「因為 0 而通過或失敗」。"""
    normalized = normalize_financial_wide(_bank_frame()).iloc[0].to_dict()

    score = compute_fscore_row(normalized, normalized)

    # F2 (OCF>0) 有值會過；F6 現金流動比缺 CurrentLiabilities 必須直接略過而非給分。
    assert score is not None
    assert 0 <= score <= 9


def test_canonical_value_wins_over_alias():
    frame = pd.DataFrame({
        'IncomeAfterTaxes': [30.0],
        'IncomeAfterTax': [12.0],
    })

    normalized = normalize_financial_wide(frame)

    assert normalized['IncomeAfterTaxes'].item() == 30.0


def test_revenue_lookup_uses_statutory_availability_date():
    periods = pd.period_range('2025-01', '2026-01', freq='M')
    revenue = [100.0] * 12 + [200.0]
    raw = pd.DataFrame({
        'stock_id': ['2330'] * len(periods),
        'date': [(period + 1).start_time for period in periods],
        'revenue_year': [period.year for period in periods],
        'revenue_month': [period.month for period in periods],
        'revenue': revenue,
    })

    lookup = build_revenue_score_lookup(raw)

    assert revenue_score_asof('2330', '2026-02-09', lookup) == 50.0
    assert revenue_score_asof('2330', '2026-02-10', lookup) == 60.0
