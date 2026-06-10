"""_evaluate_trend 帶號回傳 (2026-06-10 修)。

原全正值害 addon_factors / analysis_engine 的 `consec <= -3` 衰退分支永遠
到不了 — 連續衰退股反拿「📈 營收連續 N 個月成長」+0.3 bonus（5009 實例）。
"""
import pandas as pd

from dividend_revenue import RevenueTracker


def _df(yoys):
    return pd.DataFrame({'yoy_pct': yoys})


class TestEvaluateTrendSigned:
    def test_decline_returns_negative(self):
        # 5009 實例: 新->舊 YoY -0.2, -4.8, -19.3, +8.9 -> 連續衰退 3 個月
        trend, consec = RevenueTracker._evaluate_trend(
            _df([-0.2, -4.8, -19.3, 8.9, -7.0, -8.9]))
        assert trend == '連續衰退'
        assert consec == -3  # 修正前回 +3

    def test_growth_returns_positive(self):
        trend, consec = RevenueTracker._evaluate_trend(_df([5.0, 3.0, 1.0, -2.0]))
        assert trend == '連續成長'
        assert consec == 3

    def test_single_month_is_volatile(self):
        trend, consec = RevenueTracker._evaluate_trend(_df([5.0, -3.0, 2.0]))
        assert trend == '波動'
        assert consec == 1

    def test_zero_first_month(self):
        trend, consec = RevenueTracker._evaluate_trend(_df([0.0, 5.0]))
        assert trend == '波動'
        assert consec == 0

    def test_consumer_branches_now_reachable(self):
        # addon_factors / analysis_engine 用 consec >= 3 vs <= -3 分支
        _, up = RevenueTracker._evaluate_trend(_df([1, 2, 3, 4]))
        _, down = RevenueTracker._evaluate_trend(_df([-1, -2, -3, -4]))
        assert up >= 3
        assert down <= -3
