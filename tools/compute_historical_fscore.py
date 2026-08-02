"""
從 financials_*.parquet 計算每支股票每季的 F-Score + 營收分
輸出: data_cache/backtest/quality_scores.parquet
欄位: stock_id, date (季末), f_score, z_score, quality_score, revenue_score, combined_score
"""

import logging
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

DATA_DIR = _ROOT / "data_cache" / "backtest"
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("fscore")


def _pivot_by_type(df, types_to_keep):
    """Pivot long-format financials to wide: (stock_id, date, type1, type2, ...)."""
    sub = df[df['type'].isin(types_to_keep)].copy()
    sub['value'] = pd.to_numeric(sub['value'], errors='coerce')
    pivot = sub.pivot_table(index=['stock_id', 'date'], columns='type',
                             values='value', aggfunc='first').reset_index()
    pivot.columns.name = None
    return pivot


# 同義字：FinMind 對同一個會計科目的不同拼法／表達，合併是純粹的資料修正。
# 其中 NoncurrentLiabilities（小寫 c）是 FinMind 的主要拼法，佔 76,367 / 83,934 列；
# 只找 NonCurrentLiabilities 會讓 F5 長期負債比與 ROIC 兩個分支對 93% 的股票失效。
_SPELLING_ALIASES = {
    'IncomeAfterTaxes': ('IncomeAfterTax', 'IncomeFromContinuingOperations'),
    'Equity': ('EquityAttributableToOwnersOfParent',),
    'NonCurrentLiabilities': ('NoncurrentLiabilities',),
    'CashFlowsFromOperatingActivities': ('NetCashInflowFromOperatingActivities',),
}

# ⚠️ 代理值：不是同義字，是「沒有該科目時拿另一個科目頂替」。
#   NetInterestIncome 是淨額（已扣利息支出），Revenue 是總額；
#   稅前淨利含業外損益，營業利益不含。兩者定義與量級都不同。
# 目前影響 569 + 1,406 列（幾乎全是金控銀行），落盤後無法從輸出分辨哪些是代理值。
# 2026-08-02 code review 建議改為「不代入或加 *_is_proxy 旗標」，尚未處理。
_PROXY_ALIASES = {
    'Revenue': ('NetInterestIncome',),
    'OperatingIncome': (
        'IncomeBeforeTaxFromContinuingOperations',
        'PreTaxIncome',
    ),
}

_CANONICAL_ALIASES = {**_SPELLING_ALIASES, **_PROXY_ALIASES}

_SCORER_NUMERIC_FIELDS = {
    'Revenue', 'GrossProfit', 'OperatingIncome', 'IncomeAfterTaxes',
    'TotalAssets', 'Liabilities', 'Equity', 'CashAndCashEquivalents',
    'CurrentAssets', 'CurrentLiabilities', 'NonCurrentLiabilities',
    'CashFlowsFromOperatingActivities', 'CashProvidedByInvestingActivities',
    'Depreciation', 'AmortizationExpense', 'PropertyAndPlantAndEquipment',
}

# compute_zscore_row 的五個 X 項全部需要這些欄位；任一缺值代表「這檔算不出 Z」，
# 必須與「Z 很差」區分。金控/銀行/保險的資產負債表不分流動與非流動，FinMind
# 沒有 CurrentAssets / CurrentLiabilities 這兩個 type，屬結構性缺值。
_ZSCORE_REQUIRED_FIELDS = (
    'TotalAssets', 'CurrentAssets', 'CurrentLiabilities',
    'Equity', 'Liabilities', 'OperatingIncome', 'Revenue',
)


def normalize_financial_wide(frame):
    """Coalesce FinMind aliases; keep missing values as NaN (never 0).

    缺值一律保持 NaN。所有 scorer 的分支都用 `> 0` 之類的條件包住，NaN 比較恆為
    False，效果是「這一項無法評估就不給分也不扣分」—— 這才是 missing 的正確語意。

    ⚠️ 不要在這裡 fillna(0)：對金融股而言 CurrentAssets/CurrentLiabilities 是
    結構性缺值，補 0 會讓 Altman Z 從「算不出來」變成「有限但極低」，必然觸發
    compute_quality_score 的 `zscore < 1.81 → -20`，整個金融族群被系統性誤扣約
    22 分（2026-08-02 code review P0-1，實測 104 檔 / 1,820 列）。
    """
    out = frame.copy()
    for canonical, aliases in _CANONICAL_ALIASES.items():
        if canonical in out.columns:
            values = pd.to_numeric(out[canonical], errors='coerce')
        else:
            values = pd.Series(np.nan, index=out.index, dtype=float)
        for alias in aliases:
            if alias in out.columns:
                values = values.combine_first(
                    pd.to_numeric(out[alias], errors='coerce')
                )
        out[canonical] = values
    for column in _SCORER_NUMERIC_FIELDS:
        if column not in out.columns:
            # 建欄但留 NaN：讓 curr.get(col) 回 NaN 而不是 dict 預設值 0。
            out[column] = np.nan
        else:
            out[column] = pd.to_numeric(out[column], errors='coerce')
    return out


def build_financial_wide():
    """Load parquet and pivot to wide format per quarter."""
    logger.info("Loading financial parquets...")

    income_long = pd.read_parquet(DATA_DIR / "financials_income.parquet")
    balance_long = pd.read_parquet(DATA_DIR / "financials_balance.parquet")
    cashflow_long = pd.read_parquet(DATA_DIR / "financials_cashflow.parquet")
    revenue_raw = pd.read_parquet(DATA_DIR / "financials_revenue.parquet")

    income_long['date'] = pd.to_datetime(income_long['date'])
    balance_long['date'] = pd.to_datetime(balance_long['date'])
    cashflow_long['date'] = pd.to_datetime(cashflow_long['date'])
    revenue_raw['date'] = pd.to_datetime(revenue_raw['date'])

    income = _pivot_by_type(income_long, [
        'Revenue', 'GrossProfit', 'OperatingIncome',
        'IncomeAfterTaxes', 'IncomeAfterTax', 'IncomeFromContinuingOperations',
        'NetInterestIncome', 'IncomeBeforeTaxFromContinuingOperations',
        'PreTaxIncome', 'EPS',
    ])
    balance = _pivot_by_type(balance_long, [
        'TotalAssets', 'Liabilities', 'Equity', 'CashAndCashEquivalents',
        'CurrentAssets', 'CurrentLiabilities', 'NonCurrentLiabilities',
        'NoncurrentLiabilities', 'EquityAttributableToOwnersOfParent',
    ])
    cashflow = _pivot_by_type(cashflow_long, [
        'CashFlowsFromOperatingActivities', 'CashProvidedByInvestingActivities',
        'Depreciation', 'AmortizationExpense', 'PropertyAndPlantAndEquipment',
        'NetCashInflowFromOperatingActivities',
    ])

    logger.info(f"  income: {len(income)} quarter-stocks")
    logger.info(f"  balance: {len(balance)} quarter-stocks")
    logger.info(f"  cashflow: {len(cashflow)} quarter-stocks")

    # Merge on (stock_id, date)
    wide = income.merge(balance, on=['stock_id', 'date'], how='outer')
    wide = wide.merge(cashflow, on=['stock_id', 'date'], how='outer')
    wide = normalize_financial_wide(wide)
    wide = wide.sort_values(['stock_id', 'date'])

    return wide, revenue_raw


def compute_fscore_row(curr, prev, curr_12m_avg_assets=None):
    """
    Piotroski F-Score (0-9), 9 criteria:

    Profitability (4):
    F1 ROA > 0
    F2 Operating CF > 0
    F3 ROA increase
    F4 Operating CF > Net income (quality of earnings)

    Leverage/Liquidity (3):
    F5 Long-term debt ratio decrease
    F6 Current ratio increase
    F7 No new share issuance

    Efficiency (2):
    F8 Gross margin increase
    F9 Asset turnover increase
    """
    score = 0

    try:
        # ROA = net income / avg total assets
        ni_c = curr.get('IncomeAfterTaxes', 0)
        ta_c = curr.get('TotalAssets', 0)
        ta_p = prev.get('TotalAssets', 0)
        avg_ta_c = (ta_c + ta_p) / 2 if ta_p > 0 else ta_c

        ni_p = prev.get('IncomeAfterTaxes', 0)
        ta_pp = prev.get('TotalAssets', 0)  # Not ideal, but OK for proxy
        avg_ta_p = ta_p  # Simplified

        # F1: ROA > 0
        if avg_ta_c > 0 and ni_c > 0:
            score += 1

        # F2: Operating CF > 0
        ocf_c = curr.get('CashFlowsFromOperatingActivities', 0)
        if ocf_c > 0:
            score += 1

        # F3: ROA increase (current > previous)
        if avg_ta_c > 0 and avg_ta_p > 0:
            roa_c = ni_c / avg_ta_c
            roa_p = ni_p / avg_ta_p
            if roa_c > roa_p:
                score += 1

        # F4: Accruals - OCF > Net income
        if ocf_c > ni_c:
            score += 1

        # F5: Long-term debt ratio decrease
        ltd_c = curr.get('NonCurrentLiabilities', 0)
        ltd_p = prev.get('NonCurrentLiabilities', 0)
        if ta_c > 0 and ta_p > 0:
            ltd_ratio_c = ltd_c / ta_c
            ltd_ratio_p = ltd_p / ta_p
            if ltd_ratio_c < ltd_ratio_p:
                score += 1

        # F6: Current ratio increase
        ca_c = curr.get('CurrentAssets', 0)
        cl_c = curr.get('CurrentLiabilities', 0)
        ca_p = prev.get('CurrentAssets', 0)
        cl_p = prev.get('CurrentLiabilities', 0)
        if cl_c > 0 and cl_p > 0:
            cr_c = ca_c / cl_c
            cr_p = ca_p / cl_p
            if cr_c > cr_p:
                score += 1

        # F7: No new share issuance (using equity as proxy, very rough)
        # Skip (hard to verify without share count)
        # Give 0.5 default to avoid systematic bias

        # F8: Gross margin increase
        rev_c = curr.get('Revenue', 0)
        gp_c = curr.get('GrossProfit', 0)
        rev_p = prev.get('Revenue', 0)
        gp_p = prev.get('GrossProfit', 0)
        if rev_c > 0 and rev_p > 0:
            gm_c = gp_c / rev_c
            gm_p = gp_p / rev_p
            if gm_c > gm_p:
                score += 1

        # F9: Asset turnover increase
        if avg_ta_c > 0 and avg_ta_p > 0:
            at_c = rev_c / avg_ta_c
            at_p = rev_p / avg_ta_p
            if at_c > at_p:
                score += 1

    except Exception as e:
        logger.debug(f"F-Score computation error: {e}")
        return None

    return score


def compute_zscore_row(curr):
    """
    Altman Z-Score (simplified, without market cap):
    Z = 0.717*X1 + 0.847*X2 + 3.107*X3 + 0.420*X4 + 0.998*X5

    X1 = Working capital / total assets
    X2 = Retained earnings / total assets (proxy: Equity / TA)
    X3 = EBIT / total assets (proxy: Operating income / TA)
    X4 = Book value equity / total liabilities
    X5 = Sales / total assets
    """
    try:
        # 必要欄位缺值 → 這檔算不出 Z，回 None 讓 compute_quality_score 略過該分支。
        # 不可退化成 0（見 normalize_financial_wide 的警告）。
        for field in _ZSCORE_REQUIRED_FIELDS:
            value = curr.get(field)
            if value is None or pd.isna(value):
                return None

        ta = curr.get('TotalAssets', 0)
        if ta <= 0:
            return None

        ca = curr.get('CurrentAssets', 0)
        cl = curr.get('CurrentLiabilities', 0)
        equity = curr.get('Equity', 0)
        liabs = curr.get('Liabilities', 0)
        op_inc = curr.get('OperatingIncome', 0)
        rev = curr.get('Revenue', 0)

        x1 = (ca - cl) / ta
        x2 = equity / ta  # proxy
        x3 = op_inc / ta
        x4 = equity / liabs if liabs > 0 else 0
        x5 = rev / ta

        z = 0.717*x1 + 0.847*x2 + 3.107*x3 + 0.420*x4 + 0.998*x5
        return round(z, 2)
    except Exception:
        return None


def compute_quality_score(fscore, zscore, curr, prev):
    """
    Map F-Score + Z-Score + ROIC/FCF to 0-100 quality score
    (approximates value_screener._score_quality scoring rules)
    """
    score = 50  # baseline

    # F-Score
    if fscore is not None:
        if fscore >= 7:
            score += 25
        elif fscore >= 5:
            score += 10
        elif fscore <= 3:
            score -= 20

    # Z-Score
    if zscore is not None:
        if zscore > 2.99:
            score += 8
        elif zscore < 1.81:
            score -= 20

    try:
        # ROIC proxy
        op_inc = curr.get('OperatingIncome', 0)
        equity = curr.get('Equity', 0)
        ltd = curr.get('NonCurrentLiabilities', 0)
        invested = equity + ltd
        if invested > 0:
            roic = op_inc / invested * 100  # annualized would multiply by 4
            if roic > 15:
                score += 8
            elif roic < 0:
                score -= 5

        # FCF Yield proxy (FCF = OCF - CapEx)
        ocf = curr.get('CashFlowsFromOperatingActivities', 0)
        capex = curr.get('PropertyAndPlantAndEquipment', 0)  # approximate
        fcf = ocf - abs(capex)
        if equity > 0:
            fcf_yield = fcf / equity * 100
            if fcf_yield > 8:
                score += 8
            elif fcf_yield < -5:
                score -= 5

        # ROE
        ni = curr.get('IncomeAfterTaxes', 0)
        if equity > 0:
            roe = ni / equity * 100
            if roe > 15:
                score += 5
            elif roe < 0:
                score -= 10

        # Gross margin
        rev = curr.get('Revenue', 0)
        gp = curr.get('GrossProfit', 0)
        if rev > 0:
            gm = gp / rev * 100
            if gm > 40:
                score += 5
            elif gm < 10:
                score -= 5

        # Operating margin
        if rev > 0:
            om = op_inc / rev * 100
            if om > 20:
                score += 5
            elif om < 0:
                score -= 8

        # Debt/Equity
        if equity > 0:
            de = curr.get('Liabilities', 0) / equity * 100
            if de > 200:
                score -= 5

        # Current ratio
        cl = curr.get('CurrentLiabilities', 0)
        ca = curr.get('CurrentAssets', 0)
        if cl > 0:
            cr = ca / cl
            if cr > 2.0:
                score += 5
            elif cr < 1.0:
                score -= 8

    except Exception:
        pass

    return max(0, min(100, score))


def compute_revenue_score(stock_id, curr_date, revenue_raw):
    """
    營收分 0-100 (baseline 50, ±): 1m 單月 YoY 分數

    VF-VC 驗證（2026-04-20）:
      舊 3m rolling YoY + tail(15) bug: IR -1.075 反向（衰退分支死碼，只剩 50/60 二元）
      新 1m 單月 YoY:                   IR +0.335, quarterly walk-forward IR +0.615 (A)
                                        5/6 年度正向、17/24 季度正向

    注：舊版為何方向錯 — value pool 中「月營收 YoY 已轉正」的股票通常已反彈完，
    後續動能有限；真正的左側機會是營收衰退但正在收斂的股票。
    """
    score = 50
    try:
        sub = revenue_raw[
            (revenue_raw['stock_id'] == stock_id) &
            (revenue_raw['date'] <= curr_date)
        ].sort_values('date').tail(16).copy()

        if len(sub) < 13:
            return score

        sub['rev'] = pd.to_numeric(sub['revenue'], errors='coerce')
        sub = sub.dropna(subset=['rev']).reset_index(drop=True)
        if len(sub) < 13:
            return score

        # 1m YoY: 最新月 vs 12 個月前同月
        latest = sub.iloc[-1]['rev']
        yr_ago = sub.iloc[-13]['rev']
        if yr_ago <= 0:
            return score
        yoy_latest = (latest / yr_ago - 1) * 100

        # prev-month YoY (3 月前) for trend convergence/acceleration
        yoy_prev = None
        if len(sub) >= 16:
            prev_month = sub.iloc[-4]['rev']
            yr_ago_prev = sub.iloc[-16]['rev']
            if yr_ago_prev > 0:
                yoy_prev = (prev_month / yr_ago_prev - 1) * 100

        if yoy_latest > 0:
            score += 10
        elif yoy_prev is not None and abs(yoy_latest - yoy_prev) >= 0.5:
            if yoy_latest > yoy_prev:
                # 衰退收斂: 低點信號
                bonus = min(20, (yoy_latest - yoy_prev) * 2)
                score += bonus
            else:
                # 加速衰退
                penalty = min(20, abs(yoy_latest - yoy_prev) * 2)
                score -= penalty
    except Exception:
        pass

    return max(0, min(100, score))


def build_revenue_score_lookup(revenue_raw):
    """Build PIT-safe monthly score arrays once instead of rescanning per quarter."""
    from tools.compute_revenue_scores_monthly import (
        _prepare_revenue_input,
        _score_revenue_rows,
    )

    prepared = _prepare_revenue_input(revenue_raw)
    scored = _score_revenue_rows(prepared)
    lookup = {}
    for stock_id, group in scored.groupby('stock_id', sort=False):
        ordered = group.sort_values('date')
        lookup[str(stock_id)] = (
            pd.to_datetime(ordered['date']).to_numpy(dtype='datetime64[ns]'),
            pd.to_numeric(ordered['revenue_score'], errors='coerce').to_numpy(),
        )
    return lookup


def revenue_score_asof(stock_id, curr_date, lookup):
    """Return the last score legally available on ``curr_date`` (baseline 50)."""
    item = lookup.get(str(stock_id))
    if item is None:
        return 50.0
    dates, scores = item
    position = int(
        np.searchsorted(dates, np.datetime64(pd.Timestamp(curr_date)), side='right')
    ) - 1
    if position < 0 or not np.isfinite(scores[position]):
        return 50.0
    return float(scores[position])


def process():
    wide, revenue_raw = build_financial_wide()
    revenue_lookup = build_revenue_score_lookup(revenue_raw)

    logger.info(f"Computing quality scores for {wide['stock_id'].nunique()} stocks...")

    results = []
    stocks = wide['stock_id'].unique()
    for i, sid in enumerate(stocks):
        if (i + 1) % 50 == 0:
            logger.info(f"  [{i+1}/{len(stocks)}] scoring...")

        sdf = wide[wide['stock_id'] == sid].sort_values('date').reset_index(drop=True)
        if len(sdf) < 2:
            continue

        for idx in range(1, len(sdf)):
            curr = sdf.iloc[idx].to_dict()
            prev = sdf.iloc[idx - 1].to_dict()
            date = curr['date']

            fscore = compute_fscore_row(curr, prev)
            zscore = compute_zscore_row(curr)
            q_score = compute_quality_score(fscore, zscore, curr, prev)
            r_score = revenue_score_asof(sid, date, revenue_lookup)
            combined = round(q_score * 0.6 + r_score * 0.4)

            results.append({
                'stock_id': sid,
                'date': date,
                'f_score': fscore,
                'z_score': zscore,
                'quality_score': q_score,
                'revenue_score': r_score,
                'combined_score': combined,
            })

    out = pd.DataFrame(results)
    if out.empty:
        raise RuntimeError("quality score rebuild produced no rows")
    out_path = DATA_DIR / "quality_scores.parquet"
    temp = out_path.with_name(f".{out_path.name}.{os.getpid()}.tmp")
    try:
        out.to_parquet(temp, index=False)
        os.replace(temp, out_path)
    finally:
        temp.unlink(missing_ok=True)
    logger.info(f"Saved {out_path}: {len(out)} rows, {out['stock_id'].nunique()} stocks")
    logger.info(f"  F-Score range: {out['f_score'].min()} to {out['f_score'].max()}, mean={out['f_score'].mean():.1f}")
    logger.info(f"  Z-Score p25/50/75: {out['z_score'].quantile([.25,.5,.75]).values}")
    logger.info(f"  Combined p25/50/75: {out['combined_score'].quantile([.25,.5,.75]).values}")


if __name__ == '__main__':
    process()
