"""Unit tests for piotroski.py pure-compute functions.

Targets _compute_fscore / _compute_zscore / _compute_extra (non-network) and
the _safe_div / _to_float helpers. The top-level calculate_* functions hit
FinMind / yfinance and are skipped here.

Coverage focus:
  - boundary cases (every criterion pass / fail)
  - edge inputs (zero division, missing periods, missing fields)
  - component split (profitability + leverage + efficiency = total)
"""
import pytest

from piotroski import (
    _compute_fscore,
    _compute_zscore,
    _compute_extra,
    _safe_div,
    _to_float,
)


# ---------- helpers ----------

def _make_fscore_inputs(scenario: str):
    """Build (income, balance, cashflow, periods) for synthetic F-Score scenarios.

    Periods: 'Q1' (prev), 'Q2' (curr).
    """
    if scenario == "perfect":
        # All 9 criteria pass -> score 9
        income = {
            "Q1": {"net_income": 100, "revenue": 1000, "gross_profit": 200},
            "Q2": {"net_income": 200, "revenue": 1100, "gross_profit": 300},
        }
        balance = {
            "Q1": {
                "total_assets": 5000,
                "long_term_debt": 1000,
                "current_assets": 1500,
                "current_liabilities": 1000,
                "shares_outstanding": 1_000_000,
            },
            "Q2": {
                "total_assets": 5000,           # same -> ROA up via NI up
                "long_term_debt": 800,           # decreasing -> F5 +
                "current_assets": 1800,
                "current_liabilities": 1000,    # current ratio improving -> F6 +
                "shares_outstanding": 1_000_000,  # no dilution -> F7 +
            },
        }
        # CFROA must exceed ROA: ROA_Q2 = 200/5000 = 0.04, so OCF/TA > 0.04 -> OCF > 200
        cashflow = {"Q2": {"operating_cf": 300}}
    elif scenario == "zero":
        # Every criterion fails -> score 0
        income = {
            "Q1": {"net_income": 200, "revenue": 1100, "gross_profit": 300},
            "Q2": {"net_income": -50, "revenue": 900, "gross_profit": 100},
        }
        balance = {
            "Q1": {
                "total_assets": 5000,
                "long_term_debt": 800,
                "current_assets": 1800,
                "current_liabilities": 900,
                "shares_outstanding": 1_000_000,
            },
            "Q2": {
                "total_assets": 5000,
                "long_term_debt": 1200,          # increasing -> F5 fail
                "current_assets": 1500,
                "current_liabilities": 1100,    # current ratio dropping -> F6 fail
                "shares_outstanding": 1_500_000,  # diluted -> F7 fail
            },
        }
        cashflow = {"Q2": {"operating_cf": -100}}
    else:
        raise ValueError(f"unknown scenario: {scenario}")

    periods = ["Q1", "Q2"]
    return income, balance, cashflow, periods


# ---------- _safe_div ----------

class TestSafeDiv:
    def test_normal_division(self):
        assert _safe_div(10, 2) == 5.0

    def test_zero_denominator_returns_zero(self):
        assert _safe_div(10, 0) == 0.0

    def test_none_denominator_returns_zero(self):
        # Falsy denominator (None) handled gracefully
        assert _safe_div(10, None) == 0.0

    def test_zero_numerator(self):
        assert _safe_div(0, 5) == 0.0

    def test_negative_numerator(self):
        assert _safe_div(-10, 2) == -5.0


# ---------- _to_float ----------

class TestToFloat:
    def test_int_to_float(self):
        assert _to_float(42) == 42.0

    def test_string_number(self):
        assert _to_float("3.14") == 3.14

    def test_invalid_string_returns_zero(self):
        assert _to_float("abc") == 0.0

    def test_none_returns_zero(self):
        assert _to_float(None) == 0.0


# ---------- _compute_fscore ----------

class TestComputeFScore:
    def test_perfect_score_is_9(self):
        income, balance, cashflow, periods = _make_fscore_inputs("perfect")
        result = _compute_fscore(income, balance, cashflow, periods)
        assert result["fscore"] == 9
        assert result["components"]["profitability"] == 4
        assert result["components"]["leverage"] == 3
        assert result["components"]["efficiency"] == 2

    def test_worst_score_is_0(self):
        income, balance, cashflow, periods = _make_fscore_inputs("zero")
        result = _compute_fscore(income, balance, cashflow, periods)
        assert result["fscore"] == 0
        assert result["components"]["profitability"] == 0
        assert result["components"]["leverage"] == 0
        assert result["components"]["efficiency"] == 0

    def test_components_sum_equals_total(self):
        """Score should always equal sum of three component scores."""
        for scenario in ("perfect", "zero"):
            income, balance, cashflow, periods = _make_fscore_inputs(scenario)
            r = _compute_fscore(income, balance, cashflow, periods)
            comp_sum = sum(r["components"].values())
            assert r["fscore"] == comp_sum, f"scenario={scenario}: {r['fscore']} != {comp_sum}"

    def test_details_length_is_9(self):
        """One detail line per criterion."""
        income, balance, cashflow, periods = _make_fscore_inputs("perfect")
        r = _compute_fscore(income, balance, cashflow, periods)
        assert len(r["details"]) == 9

    def test_missing_cashflow_zeros_f2_and_f4(self):
        """When cashflow data missing, OCF=0 -> F2 fails; CFROA=0 < ROA -> F4 fails."""
        income, balance, _, periods = _make_fscore_inputs("perfect")
        result = _compute_fscore(income, balance, cashflow=None, periods=periods)
        # Profitability max with no cashflow: F1 (ROA>0) + F3 (improving) = 2
        assert result["components"]["profitability"] == 2

    def test_no_dilution_edge_equal_shares(self):
        """F7 should pass when shares stay flat (<=, not <)."""
        income, balance, cashflow, periods = _make_fscore_inputs("perfect")
        # Already equal in 'perfect' scenario; sanity-check via raw data
        result = _compute_fscore(income, balance, cashflow, periods)
        assert "no dilution" in " ".join(result["details"])

    def test_raw_data_returned(self):
        income, balance, cashflow, periods = _make_fscore_inputs("perfect")
        r = _compute_fscore(income, balance, cashflow, periods)
        # Check expected raw fields
        for field in ("roa", "operating_cf", "leverage_curr", "current_ratio",
                      "gross_margin", "asset_turnover"):
            assert field in r["data"]


# ---------- _compute_zscore ----------

class TestComputeZScore:
    def test_healthy_balance_sheet(self):
        """Strong company: positive working capital, retained earnings, low leverage."""
        balance = {
            "Q1": {
                "total_assets": 10000,
                "current_assets": 5000,
                "current_liabilities": 2000,
                "retained_earnings": 3000,
                "total_liabilities": 3000,
            }
        }
        income = {
            "Q1": {"ebit": 1000, "revenue": 12000}
        }
        result = _compute_zscore(income, balance, market_cap=8000)
        # Healthy firm should be in safe zone (Z > 2.99) generally
        assert result is not None
        assert "zscore" in result
        # Z = 1.2*WC/TA + 1.4*RE/TA + 3.3*EBIT/TA + 0.6*MC/TL + 1.0*Sales/TA
        # = 1.2*0.3 + 1.4*0.3 + 3.3*0.1 + 0.6*(8000/3000) + 1.0*1.2
        # = 0.36 + 0.42 + 0.33 + 1.6 + 1.2 = 3.91 -> safe zone
        assert result["zscore"] > 2.99

    def test_distressed_balance_sheet(self):
        """Distressed firm: negative WC, high leverage."""
        balance = {
            "Q1": {
                "total_assets": 10000,
                "current_assets": 1000,
                "current_liabilities": 4000,    # negative WC
                "retained_earnings": -2000,     # accumulated losses
                "total_liabilities": 9000,      # high leverage
            }
        }
        income = {"Q1": {"ebit": -500, "revenue": 3000}}
        result = _compute_zscore(income, balance, market_cap=500)
        assert result["zscore"] < 1.81  # distress zone

    def test_empty_balance_returns_none(self):
        result = _compute_zscore({}, {}, market_cap=1000)
        assert result is None


# ---------- _compute_extra (ROIC / FCF / margins) ----------

class TestComputeExtra:
    def test_returns_expected_metrics(self):
        income = {
            "Q1": {
                "net_income": 200,
                "revenue": 1000,
                "gross_profit": 350,
                "operating_income": 200,
                "ebit": 200,
                "interest_expense": 10,
                "income_tax": 50,
            }
        }
        balance = {
            "Q1": {
                "total_assets": 5000,
                "total_equity": 3000,
                "long_term_debt": 800,
                "short_term_debt": 200,
                "cash": 500,
            }
        }
        cashflow = {"Q1": {"operating_cf": 300, "capex": -100}}
        result = _compute_extra(income, balance, cashflow, market_cap=4000)
        assert result is not None
        # FCF should be computed
        assert "fcf" in result or "fcf_yield" in result
