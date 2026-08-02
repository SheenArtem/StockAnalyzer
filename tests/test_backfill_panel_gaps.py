"""回填工具的兩段高風險邏輯：還原係數判定 + 鄰日挑選。

這兩處出錯的後果不是「補不到」而是「補進錯的價格」，而錯價每一欄都是正數的合理
數字，下游健康度檢查抓不到 —— 跟 2026-08-02 那次 TPEX 蓋錯日期同一類failure mode。

背景（實測數據見 tools/backfill_panel_gaps.py 檔頭）：
- CSV 存還原價、官方給原始價；無公司行為的股票係數剛好 1，所以「多數看起來相同」
  不能當成「panel 是原始價」的證據。
- 量不能套同一招：yfinance 與官方的成交股數本來就只有 41~43% 完全相同。
"""
import pandas as pd
import pytest

from tools import backfill_panel_gaps as B


def _csv(rows):
    """rows: {date: (close, volume)}"""
    return pd.DataFrame(
        [{'Open': c, 'High': c, 'Low': c, 'Close': c, 'Volume': v}
         for c, v in rows.values()],
        index=pd.to_datetime(list(rows.keys())))


# 官方橫斷面：{sid: (open, high, low, close, volume)}
def _off(close, volume):
    return {'X': (close, close, close, close, volume)}


# --- 還原係數 -------------------------------------------------------------

def test_price_factor_applied_from_neighbours():
    """panel 548.97 / 官方 602 -> 係數 0.912（2330 實測值）。"""
    csv = _csv({'2021-04-01': (548.9657, 1000), '2021-04-07': (556.2609, 1000)})
    pf, vf, why = B.resolve_factors(csv, 'X', '2021-04-01', '2021-04-07',
                                    _off(602.0, 1000), _off(610.0, 1000))
    assert why is None
    assert pf == pytest.approx(0.9119, abs=1e-3)
    assert vf == 1.0


def test_inconsistent_price_factor_is_skipped():
    """前後係數不一致 = 期間有除權息，硬補會製造假跳空。"""
    csv = _csv({'2021-04-01': (100.0, 1000), '2021-04-07': (90.0, 1000)})
    pf, _, why = B.resolve_factors(csv, 'X', '2021-04-01', '2021-04-07',
                                   _off(100.0, 1000), _off(100.0, 1000))
    assert pf is None and '不一致' in why


def test_absurd_factor_is_rejected():
    """3666 當時在 panel 是 653000 而官方 65.3（假 1:10000 反向分割，1,784 列）。
    不設上界就會把毀損值當成合法還原係數，把它擴散到補進去的每一列。

    該檔已由 `tools/repair_3666_bogus_split.py` 修好，**但這條測試要留** —— 驗的是
    「離譜係數要拒絕」這條規則，不是那一檔的狀態。事前阻擋（這裡）與事後偵測
    （`tools/scan_panel_price_outliers.py`）是兩道獨立防線。"""
    csv = _csv({'2021-04-01': (653000.0, 1000), '2021-04-07': (653000.0, 1000)})
    pf, _, why = B.resolve_factors(csv, 'X', '2021-04-01', '2021-04-07',
                                   _off(65.3, 1000), _off(65.3, 1000))
    assert pf is None and '超出合理範圍' in why


# --- 成交量 ---------------------------------------------------------------

def test_volume_noise_does_not_block_the_row():
    """yfinance 與官方的量本來就有出入。用價格那套 0.1% 容忍度會砍掉 84% 的可補列
    （2025-08-01 實測 912/1087），所以量對不上時要照補、直接採用官方量。"""
    csv = _csv({'2021-04-01': (100.0, 980), '2021-04-07': (100.0, 1050)})
    pf, vf, why = B.resolve_factors(csv, 'X', '2021-04-01', '2021-04-07',
                                    _off(100.0, 1000), _off(100.0, 1000))
    assert why is None, f'量的雜訊不該擋下整列：{why}'
    assert vf == 1.0, '採用官方原始量'


def test_volume_split_is_detected_and_applied():
    """真分割時前後比值會一致且明顯偏離 1（例：2 倍），這時才縮放。"""
    csv = _csv({'2021-04-01': (100.0, 2000), '2021-04-07': (100.0, 2020)})
    _, vf, why = B.resolve_factors(csv, 'X', '2021-04-01', '2021-04-07',
                                   _off(100.0, 1000), _off(100.0, 1010))
    assert why is None
    assert vf == pytest.approx(2.0, abs=0.01)


# --- 鄰日挑選 -------------------------------------------------------------

def _dates(*ds):
    return [pd.Timestamp(d) for d in ds]


def test_neighbours_skip_other_gap_days():
    """2026-04-13~04-29 是連續 13 天缺口。若拿隔壁同樣殘缺的那天當基準，缺的股票
    在鄰日多半也缺，整批會被「CSV 鄰日無資料」擋掉 = 白做。"""
    panel = _dates('2026-04-10', '2026-04-14', '2026-04-15', '2026-04-16', '2026-04-30')
    gaps = {'2026-04-14', '2026-04-15', '2026-04-16'}

    prev, nxt = B.neighbours(panel, '2026-04-15', gaps)

    assert (prev, nxt) == ('2026-04-10', '2026-04-30')


def test_neighbours_returns_none_at_series_edge():
    """序列頭尾沒有雙側鄰日時要回 None，不可單邊外推。"""
    panel = _dates('2016-01-30', '2016-02-01')
    assert B.neighbours(panel, '2016-01-30', {'2016-01-30'}) == (None, None)


def test_known_gap_dates_are_disjoint():
    """兩份清單重疊的話，exclude 集合與報告的日期數會對不起來。"""
    assert not (set(B.THIN_DATES) & set(B.GAP_DATES))
    assert len(B.THIN_DATES) == 11 and len(B.GAP_DATES) == 13
