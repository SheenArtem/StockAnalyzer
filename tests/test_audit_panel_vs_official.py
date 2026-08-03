"""panel vs 官方對帳：釘住「回填日不能當比對基準」這個陷阱。

2026-08-03 實際踩到：`tools/backfill_panel_gaps.py` 補的列是「官方價 × 還原係數」
算出來的，所以拿它當基準對帳等於「官方比官方」，失敗率會趨近 0。
當時 `2016-09-12` 因此回報 0.09% 看起來完美，而同一週未被污染的四天全是 24~27%；
第一個「對照組」`2017-02-20` 也中同一個坑（前一交易日 2017-02-18 是回填日）。

這種假綠燈比沒有工具更危險 —— 它會讓人宣告資料乾淨。所以 BACKFILLED 清單的完整性
要有測試背書。
"""
import pandas as pd
import pytest

from tools import audit_panel_vs_official as A
from tools import backfill_panel_gaps as B


def test_backfilled_set_covers_every_backfilled_date():
    """回填工具補過的每一天都必須在對帳工具的排除清單裡。

    漏一天的後果不是報錯而是**那天回報假乾淨**，靜默誤導。
    """
    filled = set(B.THIN_DATES) | set(B.GAP_DATES)
    missing = sorted(filled - A.BACKFILLED)
    assert not missing, f'對帳工具沒排除這些回填日：{missing}'


def test_backfilled_set_has_no_extra_dates():
    """反向也要對：排除了沒被回填的日期會白白少掉可對帳的樣本。"""
    extra = sorted(A.BACKFILLED - (set(B.THIN_DATES) | set(B.GAP_DATES)))
    assert not extra, f'排除了非回填日：{extra}'


def test_tolerance_sits_above_quantization_floor():
    """官方原始價只有 2 位小數。容忍度必須高於量化下限，否則低價股會被誤判。

    實測半 tick 相對大小中位 0.016%（價位 20~50 帶），而 2016 那個窗口的真實偏差
    中位是 0.34~0.38% —— 容忍度要落在兩者之間。
    """
    assert 0.0002 < A.RET_TOL < 0.003


def test_suspect_rate_separates_normal_from_anomalous():
    """正常日失敗率中位 0.75%、p90 1.41%（27 天抽樣）；2016 窗口是 22~27%。
    門檻要把兩群分開，且靠近正常那側留餘裕。"""
    assert 0.0141 < A.SUSPECT_RATE < 0.20


def test_audit_day_returns_failure_set_median_not_overall():
    """偏差中位數要取「不吻合者」的。取全體會幾乎恆為 0（多數股票吻合），
    看起來永遠漂亮，等於沒有嚴重程度資訊 —— 這是本工具第一版的缺陷。"""
    # panel 與官方在 A/B 完全一致，C 差 2%
    panel = {'d0': {'A': 10.0, 'B': 20.0, 'C': 30.0},
             'd1': {'A': 11.0, 'B': 22.0, 'C': 33.6}}

    class Src:
        def closes(self, day):
            return {'d0': {'A': 10.0, 'B': 20.0, 'C': 30.0},
                    'd1': {'A': 11.0, 'B': 22.0, 'C': 33.0}}[day]

    old = A.MIN_COMPARABLE
    A.MIN_COMPARABLE = 1
    try:
        n, bad, med, mx = A.audit_day('d1', 'd0', panel, Src())
    finally:
        A.MIN_COMPARABLE = old

    assert n == 3 and bad == 1
    # C: panel ret 0.12 vs official 0.10 -> 偏差 0.02
    assert med == pytest.approx(0.02, abs=1e-6), '取到全體中位數就會是 0'
    assert mx == pytest.approx(0.02, abs=1e-6)


def test_audit_day_skips_when_too_few_comparable():
    """可比檔數不足時要回 None，不可用 3 檔算出「失敗率 33%」這種假結論。"""
    panel = {'d0': {'A': 10.0}, 'd1': {'A': 11.0}}

    class Src:
        def closes(self, day):
            return {'d0': {'A': 10.0}, 'd1': {'A': 11.0}}[day]

    assert A.audit_day('d1', 'd0', panel, Src()) is None
