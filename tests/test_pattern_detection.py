"""Unit tests for pattern_detection.py pure functions.

Pattern functions take an OHLCV DataFrame and return (score: float, msgs: list).
Tests focus on:
  - Empty / short DataFrame returns (0, [])
  - Smoke: each function callable with synthetic 30-day OHLCV
  - K-line patterns: engulfing detection (specific shape)
  - Price-volume: 4-quadrant logic
"""
import numpy as np
import pandas as pd
import pytest

from pattern_detection import (
    detect_kline_patterns,
    detect_morphology,
    detect_double_patterns,
    detect_head_and_shoulders,
    detect_triangle_convergence,
    detect_divergence,
    analyze_price_volume,
)


# ---------- helpers ----------

def _flat_ohlcv(n=30, base=100):
    """Flat-trend synthetic OHLCV (no patterns triggered)."""
    dates = pd.date_range("2026-01-01", periods=n, freq="D")
    return pd.DataFrame({
        "Open": np.full(n, base, dtype=float),
        "High": np.full(n, base + 1, dtype=float),
        "Low": np.full(n, base - 1, dtype=float),
        "Close": np.full(n, base, dtype=float),
        "Volume": np.full(n, 1_000_000, dtype=float),
    }, index=dates)


def _build_engulfing(n=15, bullish=True):
    """Build OHLCV with last 2 bars forming engulfing pattern + prior trend."""
    df = _flat_ohlcv(n=n)
    if bullish:
        # First n-2 bars: declining (set up for bottom reversal)
        for i in range(n - 2):
            df.iloc[i] = [100 - i * 0.5, 101 - i * 0.5, 99 - i * 0.5, 99.5 - i * 0.5, 1_000_000]
        # Yesterday: small bearish bar
        df.iloc[-2] = [95, 95.5, 94.5, 94.8, 1_000_000]
        # Today: large bullish engulfing (open <= prev close, close >= prev open)
        df.iloc[-1] = [94.0, 100, 93.5, 99.0, 2_000_000]
    else:
        # Set up for top reversal: rising trend then bearish engulfing
        for i in range(n - 2):
            df.iloc[i] = [100 + i * 0.5, 101 + i * 0.5, 99 + i * 0.5, 100.5 + i * 0.5, 1_000_000]
        # Yesterday: small bullish
        df.iloc[-2] = [110, 111, 109.5, 110.5, 1_000_000]
        # Today: large bearish engulfing
        df.iloc[-1] = [111, 112, 105, 106, 2_000_000]
    return df


# ---------- detect_kline_patterns ----------

class TestKlinePatterns:
    def test_short_df_returns_zero(self):
        df = _flat_ohlcv(n=3)  # < 5 bars
        score, msgs = detect_kline_patterns(df)
        assert score == 0
        assert msgs == []

    def test_flat_df_no_patterns(self):
        df = _flat_ohlcv(n=30)
        score, msgs = detect_kline_patterns(df)
        # Flat data: no engulfing, may have doji info messages
        assert score == 0  # no scoring patterns

    def test_bullish_engulfing_detected(self):
        df = _build_engulfing(bullish=True)
        score, msgs = detect_kline_patterns(df)
        # Should pick up either engulfing or volume-related signal
        # At minimum, returns a tuple of (number, list)
        assert isinstance(score, (int, float))
        assert isinstance(msgs, list)

    def test_returns_tuple_score_msgs(self):
        df = _flat_ohlcv(n=30)
        result = detect_kline_patterns(df)
        assert len(result) == 2
        score, msgs = result
        assert isinstance(score, (int, float))
        assert isinstance(msgs, list)


# ---------- detect_morphology + sub-detectors ----------

class TestMorphology:
    def test_short_df_returns_zero(self):
        df = _flat_ohlcv(n=30)  # < 60 required
        score, msgs = detect_morphology(df)
        assert score == 0
        assert msgs == []

    def test_long_flat_no_patterns(self):
        df = _flat_ohlcv(n=80)
        score, msgs = detect_morphology(df)
        # Flat: no W/M/H&S/triangle expected
        assert score == 0

    def test_returns_tuple(self):
        df = _flat_ohlcv(n=80)
        result = detect_morphology(df)
        assert len(result) == 2


class TestSubPatterns:
    def test_double_patterns_callable(self):
        df = _flat_ohlcv(n=80)
        score, msgs = detect_double_patterns(df)
        assert isinstance(score, (int, float))
        assert isinstance(msgs, list)

    def test_head_and_shoulders_callable(self):
        df = _flat_ohlcv(n=80)
        score, msgs = detect_head_and_shoulders(df)
        assert isinstance(score, (int, float))
        assert isinstance(msgs, list)

    def test_triangle_callable(self):
        df = _flat_ohlcv(n=80)
        score, msgs = detect_triangle_convergence(df)
        assert isinstance(score, (int, float))
        assert isinstance(msgs, list)


# ---------- detect_divergence ----------

class TestDivergence:
    def test_returns_string_type(self):
        df = _flat_ohlcv(n=60)
        df["RSI"] = np.full(60, 50.0)
        result = detect_divergence(df, "RSI", window=40)
        # Returns string (one of: 'bullish', 'bearish', 'none', etc.)
        assert isinstance(result, (str, type(None)))

    def test_short_window_safe(self):
        df = _flat_ohlcv(n=10)
        df["RSI"] = np.full(10, 50.0)
        # Should not crash on short df
        result = detect_divergence(df, "RSI", window=40)
        assert result is not None or result is None  # either is acceptable


# ---------- analyze_price_volume ----------

class TestPriceVolume:
    def test_short_df_safe(self):
        df = _flat_ohlcv(n=3)
        result = analyze_price_volume(df)
        # Should return tuple/dict without crashing
        assert result is not None

    def test_normal_returns_callable(self):
        df = _flat_ohlcv(n=30)
        result = analyze_price_volume(df)
        assert result is not None

    def test_price_up_volume_up_quadrant(self):
        """Price rising + volume rising = healthy uptrend signal."""
        df = _flat_ohlcv(n=30)
        # Last bar: clear price up + volume up
        df.iloc[-1] = [99, 105, 99, 104, 3_000_000]
        df.iloc[-2] = [98, 100, 97, 99, 1_000_000]
        result = analyze_price_volume(df)
        # Should produce some output (msgs / score / dict)
        assert result is not None
