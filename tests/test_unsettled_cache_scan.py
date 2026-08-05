"""scan_unsettled_cache_bars 的分類護欄 — 2026-08-04 迴歸釘子。

這支工具最容易出錯的不是偵測，是**分類**：分割 / 分拆會讓 yfinance 回溯調整整段
歷史，價量同時變，觸發跟未完成 bar 一模一樣的判據。第一版修復腳本因此對 5 檔
分割股做了部分列覆寫，在覆寫邊界憑空造出假跳空（DD 04-02 收 136.44 → 04-06
變成 136.71，＝ +200% 的假缺口）。

判別特徵：**連續多日呈幾乎相同比例＝分割；單日隨機比例＝未完成 bar。**
分錯的代價不對稱 —— 把分割誤判成未完成 bar 會製造假資料，反過來只是多抓一次。
所以邊界情況一律偏向 split（保守）。
"""
import datetime

import pandas as pd

from tools import scan_unsettled_cache_bars as S


def _bad(ratios, settled=True):
    """造 find_bad_rows 形狀的發現列表；ratio = real / cache。"""
    base = datetime.date(2026, 4, 6)
    return [{
        'date': pd.Timestamp(base + datetime.timedelta(days=i)),
        'cache_close': 100.0, 'real_close': 100.0 * r,
        'ratio': r, 'vol_ratio': 0.3, 'settled': settled,
    } for i, r in enumerate(ratios)]


# ====================================================================
#  分割：連續多日同比例
# ====================================================================

def test_classify_split_1_to_3_reverse():
    """DD 的真實形狀：8 列全部 ×3（1:3 反向分割）。"""
    assert S.classify(_bad([3.0] * 8)) == 'split'


def test_classify_split_1_to_10():
    """KLAC：×0.1（1:10 分割）。"""
    assert S.classify(_bad([0.1] * 35)) == 'split'


def test_classify_split_with_tiny_jitter():
    """真實資料的比例不會完全相同（四捨五入），仍須判 split。"""
    assert S.classify(_bad([0.806, 0.805, 0.807, 0.806, 0.804, 0.806])) == 'split'


def test_classify_split_spinoff_ratio():
    """FDX 分拆的 -19.4%（不是常見分割比例，一樣要抓到）。"""
    assert S.classify(_bad([0.806] * 35)) == 'split'


# ====================================================================
#  未完成 bar：比例隨機散在 1 附近
# ====================================================================

def test_classify_unsettled_single_row():
    assert S.classify(_bad([1.017])) == 'unsettled'


def test_classify_unsettled_random_scatter():
    """MSFU/TSMX 的真實形狀：多列但比例方向不一、幅度不一。"""
    ratios = [1.017, 0.988, 1.005, 0.963, 1.031, 0.994, 1.009]
    assert S.classify(_bad(ratios)) == 'unsettled'


def test_classify_unsettled_leveraged_etf_extreme():
    """槓桿 ETF 盤中偏差可以到 7%（NVDL 07-10 實例），仍是未完成 bar。"""
    ratios = [1.0756, 0.982, 1.011, 0.995, 1.032]
    assert S.classify(_bad(ratios)) == 'unsettled'


# ====================================================================
#  邊界：分錯的代價不對稱，一律偏保守
# ====================================================================

def test_classify_few_rows_but_huge_offset_is_split():
    """只有 2 列但偏 200% —— 未完成 bar 不可能偏這麼多，判 split 才安全。"""
    assert S.classify(_bad([3.0, 3.0])) == 'split'


def test_classify_many_rows_scattered_but_one_huge_is_split():
    """比例散亂但其中一列偏離超過 MAX_SANE_OFFSET -> 不可部分覆寫。"""
    ratios = [1.01, 0.99, 1.02, 0.98, 2.0]
    assert S.classify(_bad(ratios)) == 'split'


def test_max_sane_offset_boundary():
    """護欄門檻本身：15% 以內算 unsettled，超過就是 split。"""
    assert S.classify(_bad([1.14])) == 'unsettled'
    assert S.classify(_bad([1.16])) == 'split'


# ====================================================================
#  偵測層
# ====================================================================

def _frames(cache_close, cache_vol, real_close, real_vol):
    idx = pd.DatetimeIndex([pd.Timestamp('2026-08-03')])
    cache = pd.DataFrame({'Close': [cache_close], 'Volume': [cache_vol],
                          'Open': [1.0], 'High': [1.0], 'Low': [1.0],
                          'Adj Close': [cache_close]}, index=idx)
    real = pd.DataFrame({'Close': [real_close], 'Volume': [real_vol],
                         'Open': [1.0], 'High': [1.0], 'Low': [1.0],
                         'Adj Close': [real_close]}, index=idx)
    return cache, real


def test_find_bad_rows_flags_low_volume_even_when_price_matches():
    """量比價靈敏 —— 收盤剛好相等但量只有 58%，仍是未完成 bar（NET/ORCL 實例）。"""
    cache, real = _frames(100.0, 5_800_000, 100.0, 10_000_000)
    bad = S.find_bad_rows(cache, real)
    assert len(bad) == 1
    assert bad[0]['vol_ratio'] < S.VOL_TOL


def test_find_bad_rows_flags_price_mismatch():
    cache, real = _frames(35.84, 11_840_600, 36.47, 11_840_600)
    assert len(S.find_bad_rows(cache, real)) == 1


def test_find_bad_rows_clean_row_not_flagged():
    cache, real = _frames(36.47, 11_840_600, 36.47, 11_840_600)
    assert S.find_bad_rows(cache, real) == []


def test_find_bad_rows_ignores_dates_absent_from_real():
    cache, real = _frames(100.0, 1000, 100.0, 1000)
    real = real.iloc[0:0]
    assert S.find_bad_rows(cache, real) == []


# ====================================================================
#  範圍：只掃美股
# ====================================================================

def test_cache_name_to_yf_restores_index_caret():
    assert S.cache_name_to_yf('GSPC') == '^GSPC'
    assert S.cache_name_to_yf('AAPL') == 'AAPL'


def test_list_us_caches_excludes_tw(tmp_path, monkeypatch):
    """台股一律數字開頭，含 `5483O`（.TWO 被 _get_path 吃成 O）與 `00981A`。"""
    for name in ['AAPL', 'BRK-B', 'GSPC', '2330', '5483O', '00981A']:
        p = tmp_path / f'{name}_price.csv'
        p.write_text('Date,Close,Volume\n2026-08-03,100,1000\n', encoding='utf-8')
    monkeypatch.setattr(S, 'CACHE_DIR', tmp_path)
    got = sorted(n for n, _p, _d in S.list_us_caches(include_stale=True))
    assert got == ['AAPL', 'BRK-B', 'GSPC']
