"""
Piotroski F-Score Calculator

9-point scoring system to assess financial strength of value stocks.
Designed to filter out "value traps" from low P/B stock selections.

Score 7-9: Strong fundamentals
Score 4-6: Average
Score 0-3: Weak (potential value trap)

Data source: FinMind (TaiwanStockFinancialStatements, TaiwanStockBalanceSheet,
TaiwanStockCashFlowsStatement)
"""

import logging
from datetime import datetime, timedelta

import pandas as pd

logger = logging.getLogger(__name__)


def calculate_fscore(stock_id, dl=None):
    """
    Calculate Piotroski F-Score (0-9) for a Taiwan stock.

    Args:
        stock_id: Taiwan stock ID (e.g., '2330')
        dl: FinMind DataLoader (optional, auto-creates if None)

    Returns:
        dict: {
            'fscore': int (0-9),
            'details': list of str (each criterion explanation),
            'components': {
                'profitability': int (0-4),
                'leverage': int (0-3),
                'efficiency': int (0-2),
            },
            'data': dict of raw values used for calculation
        }
        or None if insufficient data
    """
    if dl is None:
        from cache_manager import get_finmind_loader
        dl = get_finmind_loader()

    start_date = (datetime.now() - timedelta(days=900)).strftime('%Y-%m-%d')

    # Fetch financial data
    try:
        income = _fetch_income(dl, stock_id, start_date)
        balance = _fetch_balance(dl, stock_id, start_date)
        cashflow = _fetch_cashflow(dl, stock_id, start_date)
    except Exception as e:
        logger.debug("F-Score data fetch failed for %s: %s", stock_id, e)
        return None

    if income is None or balance is None:
        return None

    # Need at least 2 periods to compare
    periods = sorted(income.keys())
    if len(periods) < 2:
        return None

    curr = periods[-1]  # Most recent period
    prev = periods[-2]  # Previous period

    score = 0
    details = []
    raw = {}

    # ================================================================
    # PROFITABILITY (4 points)
    # ================================================================
    prof_score = 0

    # F1: ROA > 0
    roa = _safe_div(income[curr].get('net_income', 0),
                     balance[curr].get('total_assets', 1))
    raw['roa'] = roa
    if roa > 0:
        prof_score += 1
        details.append(f"F1 ROA={roa:.3f} > 0 (+1)")
    else:
        details.append(f"F1 ROA={roa:.3f} <= 0 (0)")

    # F2: Operating Cash Flow > 0
    ocf = 0
    if cashflow and curr in cashflow:
        ocf = cashflow[curr].get('operating_cf', 0)
    raw['operating_cf'] = ocf
    if ocf > 0:
        prof_score += 1
        details.append(f"F2 Operating CF={ocf:,.0f} > 0 (+1)")
    else:
        details.append(f"F2 Operating CF={ocf:,.0f} <= 0 (0)")

    # F3: ROA increasing (curr > prev)
    roa_prev = _safe_div(income[prev].get('net_income', 0),
                          balance[prev].get('total_assets', 1))
    raw['roa_prev'] = roa_prev
    if roa > roa_prev:
        prof_score += 1
        details.append(f"F3 ROA {roa_prev:.3f}->{roa:.3f} improving (+1)")
    else:
        details.append(f"F3 ROA {roa_prev:.3f}->{roa:.3f} declining (0)")

    # F4: Quality of Earnings (OCF/TA > ROA, cash > accruals)
    cfroa = _safe_div(ocf, balance[curr].get('total_assets', 1))
    raw['cfroa'] = cfroa
    if cfroa > roa:
        prof_score += 1
        details.append(f"F4 CFROA={cfroa:.3f} > ROA={roa:.3f} quality (+1)")
    else:
        details.append(f"F4 CFROA={cfroa:.3f} <= ROA={roa:.3f} accrual risk (0)")

    # ================================================================
    # LEVERAGE & LIQUIDITY (3 points)
    # ================================================================
    lev_score = 0

    # F5: Leverage decreasing (long-term debt / total assets)
    leverage_curr = _safe_div(balance[curr].get('long_term_debt', 0),
                               balance[curr].get('total_assets', 1))
    leverage_prev = _safe_div(balance[prev].get('long_term_debt', 0),
                               balance[prev].get('total_assets', 1))
    raw['leverage_curr'] = leverage_curr
    raw['leverage_prev'] = leverage_prev
    if leverage_curr < leverage_prev:
        lev_score += 1
        details.append(f"F5 Leverage {leverage_prev:.3f}->{leverage_curr:.3f} decreasing (+1)")
    else:
        details.append(f"F5 Leverage {leverage_prev:.3f}->{leverage_curr:.3f} not decreasing (0)")

    # F6: Current Ratio increasing
    cr_curr = _safe_div(balance[curr].get('current_assets', 0),
                         balance[curr].get('current_liabilities', 1))
    cr_prev = _safe_div(balance[prev].get('current_assets', 0),
                         balance[prev].get('current_liabilities', 1))
    raw['current_ratio'] = cr_curr
    raw['current_ratio_prev'] = cr_prev
    if cr_curr > cr_prev:
        lev_score += 1
        details.append(f"F6 Current Ratio {cr_prev:.2f}->{cr_curr:.2f} improving (+1)")
    else:
        details.append(f"F6 Current Ratio {cr_prev:.2f}->{cr_curr:.2f} declining (0)")

    # F7: No new shares issued (share count not increased)
    shares_curr = balance[curr].get('shares_outstanding', 0)
    shares_prev = balance[prev].get('shares_outstanding', 0)
    raw['shares_curr'] = shares_curr
    raw['shares_prev'] = shares_prev
    if shares_prev > 0 and shares_curr <= shares_prev:
        lev_score += 1
        details.append(f"F7 Shares {shares_prev:,.0f}->{shares_curr:,.0f} no dilution (+1)")
    elif shares_prev > 0:
        details.append(f"F7 Shares {shares_prev:,.0f}->{shares_curr:,.0f} diluted (0)")
    else:
        details.append("F7 Shares data unavailable (0)")

    # ================================================================
    # EFFICIENCY (2 points)
    # ================================================================
    eff_score = 0

    # F8: Gross Margin increasing
    gm_curr = _safe_div(income[curr].get('gross_profit', 0),
                          income[curr].get('revenue', 1))
    gm_prev = _safe_div(income[prev].get('gross_profit', 0),
                          income[prev].get('revenue', 1))
    raw['gross_margin'] = gm_curr
    raw['gross_margin_prev'] = gm_prev
    if gm_curr > gm_prev:
        eff_score += 1
        details.append(f"F8 Gross Margin {gm_prev:.1%}->{gm_curr:.1%} improving (+1)")
    else:
        details.append(f"F8 Gross Margin {gm_prev:.1%}->{gm_curr:.1%} declining (0)")

    # F9: Asset Turnover increasing (revenue / total assets)
    at_curr = _safe_div(income[curr].get('revenue', 0),
                         balance[curr].get('total_assets', 1))
    at_prev = _safe_div(income[prev].get('revenue', 0),
                         balance[prev].get('total_assets', 1))
    raw['asset_turnover'] = at_curr
    raw['asset_turnover_prev'] = at_prev
    if at_curr > at_prev:
        eff_score += 1
        details.append(f"F9 Asset Turnover {at_prev:.3f}->{at_curr:.3f} improving (+1)")
    else:
        details.append(f"F9 Asset Turnover {at_prev:.3f}->{at_curr:.3f} declining (0)")

    score = prof_score + lev_score + eff_score

    return {
        'fscore': score,
        'details': details,
        'components': {
            'profitability': prof_score,
            'leverage': lev_score,
            'efficiency': eff_score,
        },
        'data': raw,
    }


# ================================================================
# FinMind Data Parsers
# ================================================================

def _fetch_income(dl, stock_id, start_date):
    """
    Fetch income statement data, return dict keyed by period.
    {period_str: {revenue, gross_profit, operating_income, net_income}}
    """
    try:
        df = dl.taiwan_stock_financial_statement(
            stock_id=stock_id, start_date=start_date)
        if df.empty:
            return None
    except Exception:
        return None

    # FinMind returns rows like: date, stock_id, type, value
    # type = 'Revenue', 'GrossProfit', 'OperatingIncome', 'IncomeAfterTaxes', etc.
    result = {}
    type_map = {
        'Revenue': 'revenue',
        'GrossProfit': 'gross_profit',
        'OperatingIncome': 'operating_income',
        'IncomeAfterTaxes': 'net_income',
        'EPS': 'eps',
    }

    for _, row in df.iterrows():
        period = str(row.get('date', ''))[:7]  # 'YYYY-QQ' or 'YYYY-MM'
        ftype = row.get('type', '')
        value = _to_float(row.get('value', 0))

        if ftype in type_map:
            if period not in result:
                result[period] = {}
            result[period][type_map[ftype]] = value

    return result if result else None


def _fetch_balance(dl, stock_id, start_date):
    """
    Fetch balance sheet data, return dict keyed by period.
    {period_str: {total_assets, current_assets, current_liabilities,
                  long_term_debt, shares_outstanding, total_liabilities,
                  retained_earnings, equity}}
    """
    try:
        df = dl.taiwan_stock_balance_sheet(
            stock_id=stock_id, start_date=start_date)
        if df.empty:
            return None
    except Exception:
        return None

    result = {}
    type_map = {
        'TotalAssets': 'total_assets',
        'CurrentAssets': 'current_assets',
        'CurrentLiabilities': 'current_liabilities',
        'NonCurrentLiabilities': 'long_term_debt',
        'Liabilities': 'total_liabilities',
        'RetainedEarnings': 'retained_earnings',
        'Equity': 'equity',
        'CommonStockSharesOutstanding': 'shares_outstanding',
        'OrdinaryShare': 'shares_outstanding',
    }

    for _, row in df.iterrows():
        period = str(row.get('date', ''))[:7]
        ftype = row.get('type', '')
        value = _to_float(row.get('value', 0))

        if ftype in type_map:
            if period not in result:
                result[period] = {}
            key = type_map[ftype]
            # Don't overwrite if already set (first match wins)
            if key not in result[period]:
                result[period][key] = value

    return result if result else None


def _fetch_cashflow(dl, stock_id, start_date):
    """
    Fetch cash flow statement data, return dict keyed by period.
    {period_str: {operating_cf, investing_cf, financing_cf, capex}}
    """
    try:
        df = dl.taiwan_stock_cash_flows_statement(
            stock_id=stock_id, start_date=start_date)
        if df.empty:
            return None
    except Exception:
        return None

    result = {}
    type_map = {
        'CashFlowsFromOperatingActivities': 'operating_cf',
        'CashFlowsFromInvestingActivities': 'investing_cf',
        'CashFlowsFromFinancingActivities': 'financing_cf',
        'AcquisitionOfPropertyPlantAndEquipment': 'capex',
    }

    for _, row in df.iterrows():
        period = str(row.get('date', ''))[:7]
        ftype = row.get('type', '')
        value = _to_float(row.get('value', 0))

        if ftype in type_map:
            if period not in result:
                result[period] = {}
            result[period][type_map[ftype]] = value

    return result if result else None


# ================================================================
# Altman Z-Score
# ================================================================

def calculate_zscore(stock_id, market_cap, dl=None):
    """
    Calculate Altman Z-Score for bankruptcy risk assessment.

    Z = 1.2(WC/TA) + 1.4(RE/TA) + 3.3(EBIT/TA) + 0.6(MV/TL) + 1.0(Sales/TA)

    Args:
        stock_id: Taiwan stock ID
        market_cap: Current market capitalization (TWD)
        dl: FinMind DataLoader

    Returns:
        dict: {zscore, zone ('safe'/'grey'/'distress'), details}
        or None if insufficient data
    """
    if dl is None:
        from cache_manager import get_finmind_loader
        dl = get_finmind_loader()

    start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')

    income = _fetch_income(dl, stock_id, start_date)
    balance = _fetch_balance(dl, stock_id, start_date)

    if not income or not balance:
        return None

    # Use most recent period
    periods = sorted(balance.keys())
    if not periods:
        return None
    curr = periods[-1]

    b = balance[curr]
    ta = b.get('total_assets', 0)
    if ta <= 0:
        return None

    # Find matching income period
    i_periods = sorted(income.keys())
    if not i_periods:
        return None
    i_curr = i_periods[-1]
    inc = income[i_curr]

    wc = b.get('current_assets', 0) - b.get('current_liabilities', 0)
    re = b.get('retained_earnings', 0)
    ebit = inc.get('operating_income', 0)
    tl = b.get('total_liabilities', 0)
    sales = inc.get('revenue', 0)

    x1 = wc / ta
    x2 = re / ta
    x3 = ebit / ta
    x4 = market_cap / tl if tl > 0 else 5.0  # Cap if no debt
    x5 = sales / ta

    z = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 1.0 * x5

    if z > 3.0:
        zone = 'safe'
    elif z > 1.8:
        zone = 'grey'
    else:
        zone = 'distress'

    return {
        'zscore': round(z, 2),
        'zone': zone,
        'components': {
            'x1_wc_ta': round(x1, 3),
            'x2_re_ta': round(x2, 3),
            'x3_ebit_ta': round(x3, 3),
            'x4_mv_tl': round(x4, 3),
            'x5_sales_ta': round(x5, 3),
        },
    }


# ================================================================
# Additional Metrics (Current Ratio, FCF Yield, ROIC)
# ================================================================

def calculate_extra_metrics(stock_id, market_cap, dl=None):
    """
    Calculate Current Ratio, FCF Yield, ROIC.

    Returns:
        dict: {current_ratio, fcf, fcf_yield, roic}
        or None if insufficient data
    """
    if dl is None:
        from cache_manager import get_finmind_loader
        dl = get_finmind_loader()

    start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')

    balance = _fetch_balance(dl, stock_id, start_date)
    income = _fetch_income(dl, stock_id, start_date)
    cashflow = _fetch_cashflow(dl, stock_id, start_date)

    if not balance:
        return None

    periods = sorted(balance.keys())
    curr = periods[-1]
    b = balance[curr]

    result = {}

    # Current Ratio
    cl = b.get('current_liabilities', 0)
    if cl > 0:
        result['current_ratio'] = round(b.get('current_assets', 0) / cl, 2)

    # FCF & FCF Yield
    if cashflow:
        cf_periods = sorted(cashflow.keys())
        if cf_periods:
            cf = cashflow[cf_periods[-1]]
            ocf = cf.get('operating_cf', 0)
            capex = abs(cf.get('capex', 0))
            fcf = ocf - capex
            result['fcf'] = fcf
            if market_cap > 0:
                result['fcf_yield'] = round(fcf / market_cap * 100, 2)

    # ROIC = NOPAT / Invested Capital
    if income:
        i_periods = sorted(income.keys())
        if i_periods:
            inc = income[i_periods[-1]]
            op_income = inc.get('operating_income', 0)
            nopat = op_income * 0.8  # Assume ~20% tax rate
            ta = b.get('total_assets', 0)
            cl_val = b.get('current_liabilities', 0)
            invested_capital = ta - cl_val
            if invested_capital > 0:
                result['roic'] = round(nopat / invested_capital * 100, 2)

    return result if result else None


# ================================================================
# Helpers
# ================================================================

def _safe_div(numerator, denominator):
    if denominator and denominator != 0:
        return numerator / denominator
    return 0.0


def _to_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


# ================================================================
# US Stock Support (yfinance-based)
# ================================================================

def calculate_fscore_us(ticker):
    """
    Calculate Piotroski F-Score (0-9) for a US stock using yfinance.

    Returns same format as calculate_fscore() for compatibility.
    """
    try:
        import yfinance as yf
    except ImportError:
        return None

    try:
        stock = yf.Ticker(ticker)
        inc = stock.quarterly_income_stmt
        bal = stock.quarterly_balance_sheet
        cf = stock.quarterly_cashflow
    except Exception as e:
        logger.debug("yfinance data fetch failed for %s: %s", ticker, e)
        return None

    if inc is None or inc.empty or bal is None or bal.empty:
        return None
    if len(inc.columns) < 2 or len(bal.columns) < 2:
        return None

    # yfinance returns columns as dates (most recent first)
    curr_date, prev_date = inc.columns[0], inc.columns[1]

    def _get(df, field, col):
        try:
            return float(df.loc[field, col])
        except (KeyError, ValueError, TypeError):
            return 0.0

    # Income data
    revenue_curr = _get(inc, 'Total Revenue', curr_date)
    revenue_prev = _get(inc, 'Total Revenue', prev_date)
    gross_curr = _get(inc, 'Gross Profit', curr_date)
    gross_prev = _get(inc, 'Gross Profit', prev_date)
    net_income_curr = _get(inc, 'Net Income', curr_date)
    net_income_prev = _get(inc, 'Net Income', prev_date)

    # Balance data
    bc = bal.columns[0]
    bp = bal.columns[1] if len(bal.columns) >= 2 else bc
    ta_curr = _get(bal, 'Total Assets', bc) or 1
    ta_prev = _get(bal, 'Total Assets', bp) or 1
    ca_curr = _get(bal, 'Current Assets', bc)
    ca_prev = _get(bal, 'Current Assets', bp)
    cl_curr = _get(bal, 'Current Liabilities', bc) or 1
    cl_prev = _get(bal, 'Current Liabilities', bp) or 1
    ltd_curr = _get(bal, 'Long Term Debt', bc)
    ltd_prev = _get(bal, 'Long Term Debt', bp)
    shares_curr = _get(bal, 'Ordinary Shares Number', bc) or _get(bal, 'Share Issued', bc)
    shares_prev = _get(bal, 'Ordinary Shares Number', bp) or _get(bal, 'Share Issued', bp)

    # Cashflow data
    ocf = 0.0
    if cf is not None and not cf.empty:
        cf_date = cf.columns[0]
        ocf = _get(cf, 'Operating Cash Flow', cf_date)

    score = 0
    details = []
    raw = {}

    # PROFITABILITY (4 pts)
    prof_score = 0

    roa = net_income_curr / ta_curr
    raw['roa'] = roa
    if roa > 0:
        prof_score += 1
        details.append(f"F1 ROA={roa:.3f} > 0 (+1)")
    else:
        details.append(f"F1 ROA={roa:.3f} <= 0 (0)")

    raw['operating_cf'] = ocf
    if ocf > 0:
        prof_score += 1
        details.append(f"F2 Operating CF={ocf:,.0f} > 0 (+1)")
    else:
        details.append(f"F2 Operating CF={ocf:,.0f} <= 0 (0)")

    roa_prev = net_income_prev / ta_prev
    raw['roa_prev'] = roa_prev
    if roa > roa_prev:
        prof_score += 1
        details.append(f"F3 ROA {roa_prev:.3f}->{roa:.3f} improving (+1)")
    else:
        details.append(f"F3 ROA {roa_prev:.3f}->{roa:.3f} declining (0)")

    cfroa = ocf / ta_curr
    raw['cfroa'] = cfroa
    if cfroa > roa:
        prof_score += 1
        details.append(f"F4 CFROA={cfroa:.3f} > ROA={roa:.3f} quality (+1)")
    else:
        details.append(f"F4 CFROA={cfroa:.3f} <= ROA={roa:.3f} accrual risk (0)")

    # LEVERAGE (3 pts)
    lev_score = 0

    lev_curr = ltd_curr / ta_curr if ta_curr else 0
    lev_prev = ltd_prev / ta_prev if ta_prev else 0
    raw['leverage_curr'] = lev_curr
    raw['leverage_prev'] = lev_prev
    if lev_curr < lev_prev:
        lev_score += 1
        details.append(f"F5 Leverage {lev_prev:.3f}->{lev_curr:.3f} decreasing (+1)")
    else:
        details.append(f"F5 Leverage {lev_prev:.3f}->{lev_curr:.3f} not decreasing (0)")

    cr_curr = ca_curr / cl_curr
    cr_prev = ca_prev / cl_prev
    raw['current_ratio'] = cr_curr
    raw['current_ratio_prev'] = cr_prev
    if cr_curr > cr_prev:
        lev_score += 1
        details.append(f"F6 Current Ratio {cr_prev:.2f}->{cr_curr:.2f} improving (+1)")
    else:
        details.append(f"F6 Current Ratio {cr_prev:.2f}->{cr_curr:.2f} declining (0)")

    raw['shares_curr'] = shares_curr
    raw['shares_prev'] = shares_prev
    if shares_prev > 0 and shares_curr <= shares_prev:
        lev_score += 1
        details.append(f"F7 Shares {shares_prev:,.0f}->{shares_curr:,.0f} no dilution (+1)")
    elif shares_prev > 0:
        details.append(f"F7 Shares {shares_prev:,.0f}->{shares_curr:,.0f} diluted (0)")
    else:
        details.append("F7 Shares data unavailable (0)")

    # EFFICIENCY (2 pts)
    eff_score = 0

    gm_curr = _safe_div(gross_curr, revenue_curr) if revenue_curr else 0
    gm_prev = _safe_div(gross_prev, revenue_prev) if revenue_prev else 0
    raw['gross_margin'] = gm_curr
    raw['gross_margin_prev'] = gm_prev
    if gm_curr > gm_prev:
        eff_score += 1
        details.append(f"F8 Gross Margin {gm_prev:.1%}->{gm_curr:.1%} improving (+1)")
    else:
        details.append(f"F8 Gross Margin {gm_prev:.1%}->{gm_curr:.1%} declining (0)")

    at_curr = revenue_curr / ta_curr if ta_curr else 0
    at_prev = revenue_prev / ta_prev if ta_prev else 0
    raw['asset_turnover'] = at_curr
    raw['asset_turnover_prev'] = at_prev
    if at_curr > at_prev:
        eff_score += 1
        details.append(f"F9 Asset Turnover {at_prev:.3f}->{at_curr:.3f} improving (+1)")
    else:
        details.append(f"F9 Asset Turnover {at_prev:.3f}->{at_curr:.3f} declining (0)")

    score = prof_score + lev_score + eff_score

    return {
        'fscore': score,
        'details': details,
        'components': {
            'profitability': prof_score,
            'leverage': lev_score,
            'efficiency': eff_score,
        },
        'data': raw,
    }


def calculate_zscore_us(ticker, market_cap):
    """
    Calculate Altman Z-Score for a US stock using yfinance.
    Z = 1.2(WC/TA) + 1.4(RE/TA) + 3.3(EBIT/TA) + 0.6(MV/TL) + 1.0(Sales/TA)
    """
    try:
        import yfinance as yf
    except ImportError:
        return None

    try:
        stock = yf.Ticker(ticker)
        inc = stock.quarterly_income_stmt
        bal = stock.quarterly_balance_sheet
    except Exception:
        return None

    if inc is None or inc.empty or bal is None or bal.empty:
        return None

    def _get(df, field, col):
        try:
            return float(df.loc[field, col])
        except (KeyError, ValueError, TypeError):
            return 0.0

    bc = bal.columns[0]
    ta = _get(bal, 'Total Assets', bc)
    if ta <= 0:
        return None

    ca = _get(bal, 'Current Assets', bc)
    cl = _get(bal, 'Current Liabilities', bc)
    re = _get(bal, 'Retained Earnings', bc)
    tl = _get(bal, 'Total Liabilities Net Minority Interest', bc)
    if tl == 0:
        tl = ta - _get(bal, 'Stockholders Equity', bc)

    ic = inc.columns[0]
    ebit = _get(inc, 'EBIT', ic) or _get(inc, 'Operating Income', ic)
    sales = _get(inc, 'Total Revenue', ic)

    wc = ca - cl

    x1 = wc / ta
    x2 = re / ta
    x3 = ebit / ta
    x4 = market_cap / tl if tl > 0 else 5.0
    x5 = sales / ta

    z = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 1.0 * x5

    if z > 3.0:
        zone = 'safe'
    elif z > 1.8:
        zone = 'grey'
    else:
        zone = 'distress'

    return {
        'zscore': round(z, 2),
        'zone': zone,
        'components': {
            'x1_wc_ta': round(x1, 3),
            'x2_re_ta': round(x2, 3),
            'x3_ebit_ta': round(x3, 3),
            'x4_mv_tl': round(x4, 3),
            'x5_sales_ta': round(x5, 3),
        },
    }


def calculate_extra_metrics_us(ticker, market_cap):
    """
    Calculate Current Ratio, FCF Yield, ROIC for a US stock via yfinance.
    """
    try:
        import yfinance as yf
    except ImportError:
        return None

    try:
        stock = yf.Ticker(ticker)
        bal = stock.quarterly_balance_sheet
        inc = stock.quarterly_income_stmt
        cf = stock.quarterly_cashflow
    except Exception:
        return None

    if bal is None or bal.empty:
        return None

    def _get(df, field, col):
        try:
            return float(df.loc[field, col])
        except (KeyError, ValueError, TypeError):
            return 0.0

    bc = bal.columns[0]
    result = {}

    cl = _get(bal, 'Current Liabilities', bc)
    if cl > 0:
        result['current_ratio'] = round(_get(bal, 'Current Assets', bc) / cl, 2)

    if cf is not None and not cf.empty:
        cc = cf.columns[0]
        ocf = _get(cf, 'Operating Cash Flow', cc)
        capex = abs(_get(cf, 'Capital Expenditure', cc))
        fcf = ocf - capex
        result['fcf'] = fcf
        if market_cap > 0:
            result['fcf_yield'] = round(fcf / market_cap * 100, 2)

    if inc is not None and not inc.empty:
        ic = inc.columns[0]
        op_income = _get(inc, 'Operating Income', ic)
        nopat = op_income * 0.79  # ~21% US corp tax
        ta = _get(bal, 'Total Assets', bc)
        cl_val = _get(bal, 'Current Liabilities', bc)
        invested_capital = ta - cl_val
        if invested_capital > 0:
            result['roic'] = round(nopat / invested_capital * 100, 2)

    return result if result else None


# ================================================================
# CLI Test
# ================================================================

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(message)s')

    test_id = '2330'
    print(f"=== Piotroski F-Score for {test_id} ===")
    result = calculate_fscore(test_id)
    if result:
        print(f"F-Score: {result['fscore']}/9")
        print(f"  Profitability: {result['components']['profitability']}/4")
        print(f"  Leverage:      {result['components']['leverage']}/3")
        print(f"  Efficiency:    {result['components']['efficiency']}/2")
        for d in result['details']:
            print(f"  {d}")
    else:
        print("  Insufficient data")

    print(f"\n=== Altman Z-Score for {test_id} ===")
    # Rough market cap for 2330: ~20T TWD
    z = calculate_zscore(test_id, market_cap=20_000_000_000_000)
    if z:
        print(f"Z-Score: {z['zscore']} ({z['zone']})")
    else:
        print("  Insufficient data")
