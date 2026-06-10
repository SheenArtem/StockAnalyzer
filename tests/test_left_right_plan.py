"""Unit tests for scenario_engine.generate_left_right_plan + ai_report._build_left_right_plan.

左側 Fib 承接階梯 / 右側突破確認 — deterministic 價位引擎 (2026-06-10)。
合成資料完全控制 swing_low / swing_high，fib 數字用公式對齊驗證。
"""
import numpy as np
import pandas as pd
import pytest

from scenario_engine import (
    generate_left_right_plan,
    LEFT_RIGHT_MIN_BARS,
    LEFT_RIGHT_MIN_SWING_PCT,
)


# ---------- helpers ----------

def _make_swing_df(n_flat=50, n_rally=100, n_pullback=50,
                   base=100.0, dip_low=95.0, peak=300.0, end_close=230.0,
                   with_ma20=True):
    """合成「打底 → 主升段 → 回檔」日線。

    swing_low 控制在打底段 bar 20 的 dip_low；swing_high = 主升段末根的 peak。
    """
    n = n_flat + n_rally + n_pullback
    dates = pd.date_range("2025-06-01", periods=n, freq="B")

    close = np.concatenate([
        np.full(n_flat, base),
        np.linspace(base, peak, n_rally),
        np.linspace(peak, end_close, n_pullback),
    ])
    high = close * 1.001
    low = close * 0.999

    # 控制極值：主升段最後一根 High = peak（全場最高）；打底段 bar 20 Low = dip_low（高點前最低）
    high[n_flat + n_rally - 1] = peak
    close[n_flat + n_rally - 1] = peak * 0.999
    low[20] = dip_low
    # 回檔段不創高、不破 dip_low
    high[n_flat + n_rally:] = np.minimum(high[n_flat + n_rally:], peak * 0.99)
    close[-1] = end_close

    df = pd.DataFrame({
        "Open": close,
        "High": high,
        "Low": low,
        "Close": close,
        "Volume": np.full(n, 1_000_000),
    }, index=dates)
    if with_ma20:
        df["MA20"] = df["Close"].rolling(20).mean()
    return df


# ---------- applicable=True 主路徑 ----------

class TestUpSwingLadder:
    def setup_method(self):
        self.df = _make_swing_df()
        self.lr = generate_left_right_plan(self.df)

    def test_applicable(self):
        assert self.lr["applicable"] is True

    def test_swing_detection(self):
        assert self.lr["swing_low"] == 95.0
        assert self.lr["swing_high"] == 300.0

    def test_swing_dates_ordered(self):
        assert self.lr["swing_low_date"] <= self.lr["swing_high_date"]

    def test_fib_ladder_math(self):
        amp = 300.0 - 95.0
        expected = {
            "23.6%": round(300.0 - amp * 0.236, 2),
            "38.2%": round(300.0 - amp * 0.382, 2),
            "50.0%": round(300.0 - amp * 0.500, 2),
            "61.8%": round(300.0 - amp * 0.618, 2),
        }
        got = {r["pct"]: r["price"] for r in self.lr["left_ladder"]}
        assert got == expected

    def test_ladder_actions_and_order(self):
        actions = [r["action"] for r in self.lr["left_ladder"]]
        assert actions == ["首批 1/4", "加碼 1/4", "加碼 1/4", "末批 1/4"]
        prices = [r["price"] for r in self.lr["left_ladder"]]
        assert prices == sorted(prices, reverse=True)  # 23.6% 最高 → 61.8% 最低

    def test_invalidation_786(self):
        amp = 300.0 - 95.0
        assert self.lr["invalidation_price"] == round(300.0 - amp * 0.786, 2)

    def test_right_side_levels(self):
        amp = 300.0 - 95.0
        assert self.lr["right_breakout_low"] == 300.0
        assert self.lr["right_breakout_high"] == round(300.0 * 1.025, 2)
        assert self.lr["right_ext_1272"] == round(300.0 + amp * 0.272, 2)
        assert self.lr["right_ext_1618"] == round(300.0 + amp * 0.618, 2)
        # 右側結構停損 = 38.2% 回測
        assert self.lr["right_stop"] == round(300.0 - amp * 0.382, 2)

    def test_posture_pullback(self):
        # close=230 在 23.6% (251.62) 與 50% (197.5) 之間 → pullback
        assert self.lr["posture"] == "pullback"


class TestPostures:
    def test_near_high(self):
        df = _make_swing_df(end_close=296.0)
        lr = generate_left_right_plan(df)
        assert lr["applicable"] and lr["posture"] == "near_high"

    def test_shallow_pullback(self):
        df = _make_swing_df(end_close=260.0)  # > fib23.6=251.62
        lr = generate_left_right_plan(df)
        assert lr["applicable"] and lr["posture"] == "shallow_pullback"

    def test_deep_pullback(self):
        df = _make_swing_df(end_close=160.0)  # < fib50=197.5, > fib78.6=138.87
        lr = generate_left_right_plan(df)
        assert lr["applicable"] and lr["posture"] == "deep_pullback"


# ---------- applicable=False 各防線 ----------

class TestNotApplicable:
    def test_too_few_bars(self):
        df = _make_swing_df(n_flat=10, n_rally=50, n_pullback=20)  # 80 < 120
        lr = generate_left_right_plan(df)
        assert lr["applicable"] is False
        assert str(LEFT_RIGHT_MIN_BARS) in lr["reason"]

    def test_none_df(self):
        assert generate_left_right_plan(None)["applicable"] is False

    def test_downtrend_no_swing(self):
        # 一路下跌：最高點在窗口最前端 → 高點前無波段 → amplitude ~0
        n = 200
        dates = pd.date_range("2025-06-01", periods=n, freq="B")
        close = np.linspace(300, 100, n)
        df = pd.DataFrame({
            "Open": close, "High": close * 1.001, "Low": close * 0.999,
            "Close": close, "Volume": np.full(n, 1e6),
        }, index=dates)
        lr = generate_left_right_plan(df)
        assert lr["applicable"] is False
        assert "無明確大波段" in lr["reason"]

    def test_flat_below_min_swing(self):
        n = 200
        dates = pd.date_range("2025-06-01", periods=n, freq="B")
        close = 100 + 5 * np.sin(np.linspace(0, 6, n))  # ±5% 盤整
        df = pd.DataFrame({
            "Open": close, "High": close * 1.001, "Low": close * 0.999,
            "Close": close, "Volume": np.full(n, 1e6),
        }, index=dates)
        lr = generate_left_right_plan(df)
        assert lr["applicable"] is False

    def test_structure_broken_below_786(self):
        # 漲後崩破 78.6% (138.87) → 結構失效
        df = _make_swing_df(end_close=120.0)
        lr = generate_left_right_plan(df)
        assert lr["applicable"] is False
        assert "78.6%" in lr["reason"]


# ---------- 邊界 ----------

class TestEdgeCases:
    def test_no_ma20_column(self):
        df = _make_swing_df(with_ma20=False)
        lr = generate_left_right_plan(df)
        assert lr["applicable"] is True
        assert lr["ma20"] == 0.0
        assert lr["ma20_slope_up"] is False

    def test_lookback_window(self):
        # 兩年資料：lookback=250 應只看近一年（舊高 500 在窗外不影響）
        n_old, n_new = 250, 200
        dates = pd.date_range("2024-06-01", periods=n_old + n_new, freq="B")
        old = np.linspace(500, 100, n_old)          # 舊跌段（窗外）
        new = np.concatenate([
            np.full(50, 100.0),
            np.linspace(100, 300, 100),
            np.linspace(300, 230, 50),
        ])
        close = np.concatenate([old, new])
        df = pd.DataFrame({
            "Open": close, "High": close * 1.001, "Low": close * 0.999,
            "Close": close, "Volume": np.full(n_old + n_new, 1e6),
        }, index=dates)
        lr = generate_left_right_plan(df, lookback=250)
        assert lr["applicable"] is True
        # swing_high 應是近段的 ~300，不是兩年前的 500
        assert lr["swing_high"] < 320


# ---------- prompt builder ----------

class TestPromptBuilder:
    def test_block_contains_verbatim_levels(self):
        from ai_report import _build_left_right_plan
        df = _make_swing_df()
        txt = _build_left_right_plan(df)
        assert "DETERMINISTIC" in txt
        amp = 300.0 - 95.0
        for price in (round(300.0 - amp * 0.236, 2), round(300.0 - amp * 0.618, 2)):
            assert str(price) in txt
        assert "invalidation_786" in txt
        assert "entry_A_breakout" in txt

    def test_not_applicable_one_liner(self):
        from ai_report import _build_left_right_plan
        txt = _build_left_right_plan(None)
        assert txt.startswith("不適用")
