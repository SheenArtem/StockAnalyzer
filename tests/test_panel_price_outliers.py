"""價格離群掃描：門檻要抓得到真毀損，且不能誤殺真實崩跌。

2026-08-02 回填 panel 時意外撞到 `3666` 整段歷史被 ×10000（1,784 列在 10~88 萬元），
存在至少三年沒被發現 —— 因為每一欄都是正數的合理數字，既有的健康度檢查（缺值率、
覆蓋率、日期新鮮度）全都通過。這支掃描補的就是「這個數字在台股是否物理上可能」。

門檻的兩難在這裡：抓 3666（35,149 倍）很容易，難的是**不要誤殺 4943**
（從 808 跌到 8.84 = 91 倍，那是真實崩跌）。所以下面兩個方向都要有測試。
"""
import pandas as pd
import pytest

from tools import scan_panel_price_outliers as S


def _series(sid, closes, start='2020-01-01'):
    idx = pd.bdate_range(start, periods=len(closes))
    return pd.DataFrame({'date': idx, 'stock_id': sid,
                         'Close': closes, 'Volume': [1000.0] * len(closes)})


def _df(*frames):
    return pd.concat(frames, ignore_index=True)


# --- A. 絕對量級 ---------------------------------------------------------

def test_detects_bogus_split_magnitude():
    """3666 型：整段被 ×10000。"""
    res = S.scan(_df(_series('3666', [561552.19, 544891.81, 543911.81])))
    assert len(res['A']) == 3
    assert set(res['A']['stock_id']) == {'3666'}


def test_legit_high_priced_stock_not_flagged():
    """信驊 14,525 / 川湖 7,850 是 2026 年真實股價，不可誤報。"""
    res = S.scan(_df(_series('5274', [14525.0, 18950.0]),
                     _series('2059', [7850.0, 8655.0])))
    assert len(res['A']) == 0, '合法高價股被誤判為毀損'


# --- B. 相對現價的量級落差 ----------------------------------------------

def test_detects_level_shift_versus_recent_price():
    """3666：歷史 884,000 而最近 25.15（35,149 倍）。"""
    res = S.scan(_df(_series('3666', [884000.0, 200000.0, 25.15])))
    assert '3666' in res['B'].index


def test_real_crash_is_not_flagged():
    """4943 從 808.8 跌到 8.84（91 倍）是真實崩跌 —— 台股跌 99% 的公司不少，
    門檻設 1000 就是為了留這個餘裕。這條若壞掉，掃描會天天叫。"""
    res = S.scan(_df(_series('4943', [808.8, 400.0, 100.0, 8.84])))
    assert '4943' not in res['B'].index, '真實崩跌被誤判為毀損'


def test_ratio_threshold_sits_between_the_two_cases():
    """門檻必須落在「最大合法倍數」與「最小毀損倍數」之間，否則兩邊必犧牲一邊。"""
    assert 91.5 < S.MAX_LEVEL_RATIO < 35149.0


# --- C. 尖刺後反轉 -------------------------------------------------------

def test_spike_and_revert_is_reported():
    res = S.scan(_df(_series('9999', [10.0, 10.0, 100.0, 10.0, 10.0])))
    assert len(res['C']) == 1
    assert res['C'].iloc[0]['Close'] == pytest.approx(100.0)


def test_daily_limit_moves_are_not_reported():
    """台股漲跌幅 ±10%，連續漲停不是錯值。C 用 100% 當門檻正是為了只抓物理不可能。"""
    res = S.scan(_df(_series('9998', [10.0, 11.0, 12.1, 13.31, 12.0, 10.8])))
    assert len(res['C']) == 0


def test_spike_threshold_exceeds_daily_limit():
    assert S.SPIKE_RET >= 1.0, '低於 100% 會被連續漲跌停洗出一堆偽陽性'


# --- 迴歸：修好的 3666 不該再被抓到 --------------------------------------

def test_repaired_3666_passes():
    """除以 10000 後的真實值（含 2022-05-05 接縫）。"""
    res = S.scan(_df(_series('3666', [56.16, 54.49, 20.90, 21.65, 25.15])))
    assert len(res['A']) == 0 and '3666' not in res['B'].index
