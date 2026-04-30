"""Unit tests for scenario_engine.py pure functions.

Targets safe_get + determine_scenario (the 4-scenario logic + ADX correction).
generate_action_plan / generate_monitoring_checklist are integration-level
(many strategy_params), tested separately if needed.
"""
import numpy as np
import pandas as pd
import pytest

from scenario_engine import safe_get, determine_scenario


# ---------- helpers ----------

def _make_df_day(adx=20.0, plus_di=25.0, minus_di=20.0, n=30):
    """Build a synthetic df_day with ADX/+DI/-DI on the last row."""
    dates = pd.date_range("2026-01-01", periods=n, freq="D")
    df = pd.DataFrame({
        "Open": np.linspace(100, 110, n),
        "High": np.linspace(101, 112, n),
        "Low": np.linspace(99, 109, n),
        "Close": np.linspace(100, 111, n),
        "Volume": np.full(n, 1_000_000),
        "ADX": np.full(n, adx),
        "+DI": np.full(n, plus_di),
        "-DI": np.full(n, minus_di),
    }, index=dates)
    return df


# ---------- safe_get ----------

class TestSafeGet:
    def test_normal_value(self):
        s = pd.Series({"x": 42.0})
        assert safe_get(s, "x") == 42.0

    def test_missing_key_returns_default(self):
        s = pd.Series({"x": 1.0})
        assert safe_get(s, "y", default=99) == 99

    def test_nan_value_returns_default(self):
        s = pd.Series({"x": np.nan})
        assert safe_get(s, "x", default=0) == 0

    def test_default_is_zero_when_unspecified(self):
        s = pd.Series({})
        assert safe_get(s, "missing") == 0


# ---------- determine_scenario: 4 scenarios by trend_score ----------

class TestDetermineScenarioBaseline:
    """ADX < 30 -> no correction, pure trend_score mapping."""

    def test_strong_bull_scenario_a(self):
        df = _make_df_day(adx=15)
        sc = determine_scenario(trend_score=4, df_day=df)
        assert sc["code"] == "A"

    def test_mild_bull_scenario_b(self):
        df = _make_df_day(adx=15)
        sc = determine_scenario(trend_score=2, df_day=df)
        assert sc["code"] == "B"

    def test_neutral_scenario_c(self):
        df = _make_df_day(adx=15)
        sc = determine_scenario(trend_score=-1, df_day=df)
        assert sc["code"] == "C"

    def test_bear_scenario_d(self):
        df = _make_df_day(adx=15)
        sc = determine_scenario(trend_score=-3, df_day=df)
        assert sc["code"] == "D"

    def test_threshold_boundary_3_is_a(self):
        """trend_score == 3 -> A (>=3)"""
        df = _make_df_day(adx=15)
        sc = determine_scenario(trend_score=3, df_day=df)
        assert sc["code"] == "A"

    def test_threshold_boundary_neg2_is_c(self):
        """trend_score == -2 is in [-2, 0] range -> C"""
        df = _make_df_day(adx=15)
        sc = determine_scenario(trend_score=-2, df_day=df)
        assert sc["code"] == "C"


# ---------- determine_scenario: ADX correction ----------

class TestDetermineScenarioADXCorrection:
    """ADX > 30 + daily/weekly direction conflict -> correct scenario."""

    def test_a_bull_weekly_adx_bear_daily_demotes_to_b(self):
        # weekly trend_score=4 (A), daily ADX 35 with -DI > +DI (bearish)
        df = _make_df_day(adx=35, plus_di=20, minus_di=30)
        sc = determine_scenario(trend_score=4, df_day=df)
        assert sc["code"] == "B"
        assert "ADX" in sc["title"]

    def test_b_bull_weekly_adx_bear_daily_demotes_to_c(self):
        # weekly trend_score=2 (B), daily ADX 35 + -DI > +DI
        df = _make_df_day(adx=35, plus_di=20, minus_di=30)
        sc = determine_scenario(trend_score=2, df_day=df)
        assert sc["code"] == "C"

    def test_c_bear_weekly_adx_bull_daily_promotes_to_b(self):
        # weekly trend_score=-1 (C), daily ADX 35 + +DI > -DI
        df = _make_df_day(adx=35, plus_di=30, minus_di=20)
        sc = determine_scenario(trend_score=-1, df_day=df)
        assert sc["code"] == "B"

    def test_no_correction_when_adx_below_30(self):
        # weekly A, daily bearish but ADX 25 -> stays A
        df = _make_df_day(adx=25, plus_di=20, minus_di=30)
        sc = determine_scenario(trend_score=4, df_day=df)
        assert sc["code"] == "A"

    def test_no_correction_when_directions_aligned(self):
        # weekly A, daily strong bullish ADX 40 -> stays A
        df = _make_df_day(adx=40, plus_di=35, minus_di=10)
        sc = determine_scenario(trend_score=4, df_day=df)
        assert sc["code"] == "A"

    def test_empty_df_skips_adx_correction(self):
        df = pd.DataFrame()
        sc = determine_scenario(trend_score=4, df_day=df)
        assert sc["code"] == "A"

    def test_short_df_skips_adx_correction(self):
        # < 20 rows skips ADX check
        df = _make_df_day(adx=35, plus_di=20, minus_di=30, n=10)
        sc = determine_scenario(trend_score=4, df_day=df)
        assert sc["code"] == "A"

    def test_scenario_dict_has_required_keys(self):
        df = _make_df_day(adx=15)
        sc = determine_scenario(trend_score=4, df_day=df)
        for key in ("code", "title", "color", "desc"):
            assert key in sc
