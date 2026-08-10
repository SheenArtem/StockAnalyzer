#!/usr/bin/env python
"""
build_corporate_actions.py -- 台股公司行為事件表（除權息／現金增資／減資／面額變更）

用途：把「還原係數」從**存起來的欄位**改成**算出來的值**。

背景（2026-08-10 實測）：`data_cache/*_price.csv` 的 `Adj Close` 大面積不可用 ——
1,965 檔裡可判定的 1,034 檔只有 **262 檔（25.3%）健康**，772 檔的還原係數中途往下掉
（舊列停在舊基準、新列是新基準）。成因是夜間增量只 append 新列，而資料源每次除權息
會**回溯調整**整段歷史，所以「把係數存進檔案」這個作法每過一次除權息季就再分裂一次。

本工具改存**事件表**（一檔股票一年約 1 筆），係數由下游在需要時即時累乘算出 ——
事件是不可變的歷史事實，不會過期。

四層資料源（同一張表，用 `source` 欄區分）
-------------------------------------------------------------------------
| 層                | 來源                                    | 呼叫數 | 涵蓋起點 |
|-------------------|-----------------------------------------|--------|----------|
| 除權息／現金增資  | FinMind `taiwan_stock_dividend_result`  | 每檔 1 | 全歷史   |
| 減資（上市）      | TWSE `rwd/zh/reducation/TWTAUU`         | **1**  | 2011-01-25 |
| 減資（上櫃）      | TPEX `www/zh-tw/bulletin/revivt`        | **1**  | 2013-01-16 |
| 面額變更          | FinMind `taiwan_stock_split_price`      | **1**  | 2019-09-09 |

後三層是**全市場一次抓完**，所以它們不需要（也沒有）逐檔續跑狀態；
每跑一次都重抓，靠 append-only 合併自然收斂。

驗證（全部實測，數字見 `.ai/HANDOFF.md`）
-------------------------------------------------------------------------
- 除權息表對官方 TWSE `TWT49U`：2025 全年 1,338 筆／972 檔，抽 40 檔
  **官方獨有 0 筆、FinMind 獨有 0 筆、前價與參考價最大偏差 0.0000**
- 除權息表對官方 TPEX `/tpex_exright_prepost`：61 筆已實現事件**調整值 54/54 正確**
- 減資表：官方 TWSE + TPEX 兩張表合起來 675 列 / 去重後 **670 筆事件 / 451 檔**，
  其中 **625 筆落在本專案的 1,965 檔宇宙內**（上市 365 / 上櫃 260）

⚠️ **減資刻意不用 FinMind `taiwan_stock_capital_reduction_reference_price`**
（2026-08-10 抽 24 檔 / 41 筆事件對官方逐筆比對的結論）：
**價格 38/38 完全吻合，但日期會壞**，而壞日期正好是最致命的一種錯誤 ——
係數會被套到錯的區間，且沒有任何跡象。實測到的三種壞法：

- `2327` 2022 年那次減資出現**兩列**（10/21 與 10/31，價格一字不差），
  官方只有 10/31。照單全收會把 1.2468 的係數乘兩次 ＝ 憑空 **+55%**。
- `2412` 官方 2011-01-25、FinMind 記成 **2011-10-25**（差 9 個月）。
- `3018` 官方 2023-11-27、FinMind 記成 **2023-11-11**（**星期六**，根本不是交易日）。

官方兩張表是全市場一次抓完（2 次呼叫 vs 1,965 次），又是權威來源，沒有理由用它。
FinMind 那個資料集在抽樣的 24 檔裡**沒有任何一筆早於官方視窗**，所以它也補不了
下面那個歷史深度缺口。

⚠️ **`reference_price` 有一部分是壞的，除權息層必須有 fallback**：FinMind 會把
`reference_price` 留成跟 `before_price` 一樣（係數變成 1.0 ＝ 完全沒調整），
但股利欄明明有值。實測壞掉比率 **上市 2.3% / 上櫃 7.5%**。
fallback 用 `stock_and_cache_dividend`（＝官方報表的「權值+息值」）反推：

    factor = (before_price - stock_and_cache_dividend) / before_price

這條在 `reference_price` 正常的事件上 **132/133（99.2%）精確重現**，而且正是官方
`TWT49U` 滿足的恆等式（`參考價 = 前收盤價 − (權值+息值)`）。
**現金增資也適用** —— FinMind 把增資的價值調整一併編碼在同一個欄位（實測 `8054`
2026-07-30：官方公式算出扣減 0.046180，FinMind 給 0.04618，吻合到小數第 6 位）。

已宣告的涵蓋邊界（**接 panel 前必讀**）
-------------------------------------------------------------------------
1. **減資只回溯到 2011-01-25（上市）／2013-01-16（上櫃）** —— 官方端點給不出更早的：
   TWSE 對 `startDate < 2011-01-01` 直接回「查詢開始日期小於100年1月1日」，
   TPEX 不論起始日給多早都只回到 2013-01-16。而 panel 的歷史從 **2006** 開始，
   所以 2006~2010（上市）／2006~2012（上櫃）的減資**沒有涵蓋**，那段期間的
   還原序列在減資日仍會留著假跳空。
2. **面額變更只回溯到 2019-09-09**（FinMind 該資料集的起點；全市場合計 32 筆）。
3. **上櫃除權息的歷史深度沒有跨源驗過** —— TPEX 官方只給得出約兩週的窗口
   （TWSE 那道是整年 1,338 筆），所以「上櫃的歷史事件都驗過了」這句話**不能講**。
4. **還原後除權息當天仍有 +0.46% 的殘餘正向漂移**（37 檔／500 個觀測）。
   無法用現有測試區分它是「過度校正」還是台股的「填權息」傾向，兩者都會產生
   除權息日的正報酬。要分辨得另設對照。

執行
-------------------------------------------------------------------------
    python tools/build_corporate_actions.py --dry-run --limit 50
    python tools/build_corporate_actions.py --limit 200
    python tools/build_corporate_actions.py            # 全市場（FinMind 600/hr，約 3.3h）

⚠️ `--limit N` **不會分頁**：它在續跑過濾**之前**就把宇宙切成固定的前 N 檔，
所以那 N 檔抓完之後再跑 `--limit N` 只會得到 0 筆新事件，不會往後推進。
回補要跑全市場（不帶 `--limit`）；`--limit` 只適合冒煙測試。

輸出：`data_cache/backtest/corporate_actions_tw.parquet`
      欄位 [stock_id, date, before_price, reference_price, value_cut, factor, source]

增量：既有事件**永不覆寫**（只新增），所以中途被額度打斷可以直接再跑一次接續。
      唯一的例外是既有列的 factor 是 NaN（算不出係數）—— 那種列不帶資訊卻會
      永久佔住 key，所以會讓位給日後抓到的可用係數。

Exit code（給 .bat / 排程判讀用；本工具目前是手動執行）
      0 = 跑完
      1 = 拒絕寫入（除權息來源疑似整個壞掉，或既有表讀不出來且搬不走）
      3 = **逐檔除權息層被 FinMind 額度打斷**，已抓到的有寫進去、但還沒抓完
          -> 再跑一次即續跑

⚠️ exit code 只描述**逐檔層**的完成度。三個市場層（TWSE 減資／TPEX 減資／
面額變更）任一失敗都只 log.error 後繼續，**不改 exit code** —— 它們每跑一次
都是全市場重抓，下一輪自然補回來，沒有「續跑進度」可言。所以看到 exit 0
不等於三個市場層都成功，要判斷得看 log。
"""
import argparse
import logging
import os
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")
REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from cache_manager import get_finmind_loader  # noqa: E402
from tools.refresh_universe_prices import _tw_cache_stems  # noqa: E402

OUT_PATH = REPO / 'data_cache' / 'backtest' / 'corporate_actions_tw.parquet'
CACHE_DIR = REPO / 'data_cache'
COLS = ['stock_id', 'date', 'before_price', 'reference_price',
        'value_cut', 'factor', 'source']

SRC_REFERENCE = 'reference'
SRC_FALLBACK = 'fallback'
SRC_UNUSABLE = 'unusable'
SRC_REDUCTION = 'capital_reduction'
SRC_PAR_VALUE = 'par_value_change'

# 只有這三種是「逐檔抓」的產物。續跑的完成判定**只能**看它們 ——
# 全市場層（減資／面額變更）會讓沒抓過除權息的股票也出現在表裡，
# 拿「股票在不在表中」當判據會讓那些股票的除權息永遠補不到。
DIVIDEND_SOURCES = frozenset({SRC_REFERENCE, SRC_FALLBACK, SRC_UNUSABLE})

# 「資料源整個壞掉」的 fail-loud 門檻：至少要問過這麼多檔、而且一筆事件都沒有，
# 才有資格判定不是巧合。低於這個數量的樣本不夠、不出聲（見 main() 那段長註解）。
FAIL_LOUD_MIN_STOCKS = 20

# 係數的合理範圍；超出的一律剔除，否則一筆爛事件會把整段歷史的累乘係數毀掉。
# 邊界取自實測極值再留一倍餘裕，**不是憑直覺挑的**：
#   最小 0.1  = 面額變更 10 元改 1 元（`6548` 2019-09-09，312 -> 31.2）
#   最大 40.03 = 彌補虧損減資（`5364` 2013-12-10，1.15 -> 46.04）
# 除權息本身的實測範圍是 0.690 ~ 1.000（3,403 筆）。
FACTOR_MIN, FACTOR_MAX = 0.05, 50.0


def in_factor_range(f):
    """係數是否落在可用範圍（含端點）。

    ⚠️ 寫入端（`merge_events`）與讀取端（`cumulative_factor`）**必須共用這一個**
    判準（2026-08-10 round-2 審查 F8）。原本寫入端用 `between()`（含端點）、
    讀取端用 `MIN < f < MAX`（不含），於是係數**正好**等於邊界的事件會被存進表裡、
    卻在算累積係數時被靜默略過 —— 存了但不生效是最難察覺的一種不一致。
    """
    return np.isfinite(f) and FACTOR_MIN <= f <= FACTOR_MAX

# --- 官方減資恢復買賣參考價（全市場、各 1 次呼叫）---
TWSE_REDUCTION_URL = 'https://www.twse.com.tw/rwd/zh/reducation/TWTAUU'
TPEX_REDUCTION_URL = 'https://www.tpex.org.tw/www/zh-tw/bulletin/revivt'
# TWSE 端點對更早的起始日直接回「查詢開始日期小於100年1月1日，請重新查詢!」
TWSE_REDUCTION_MIN_DATE = '2011-01-01'

# 欄位用**名字**定位不用位置：兩個市場欄名不同，而且改版時要吵不要靜默錯位。
_TWSE_RED_FIELDS = {'date': '恢復買賣日期', 'stock_id': '股票代號',
                    'before': '停止買賣前收盤價格', 'reference': '恢復買賣參考價',
                    'exright': '除權參考價'}
_TPEX_RED_FIELDS = {'date': '恢復買賣日期', 'stock_id': '股票代號',
                    'before': '最後交易日之收盤價格',
                    'reference': '減資恢復買賣開始日參考價格',
                    'exright': '除權參考價'}

log = logging.getLogger('corp_actions')


def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S')


def derive_factor(before_price, reference_price, value_cut):
    """單一事件的還原係數，回 (factor, source)；無法判定回 (nan, 'unusable')。

    primary  : reference_price / before_price
    fallback : (before_price - value_cut) / before_price
               —— 只在 primary **正好等於 1**（FinMind 沒把參考價填進來）
                  而且 value_cut **不等於 0**（確實發生了價值調整）時啟用。

    ⚠️ 判準是 `value_cut != 0` **不是** `> 0`（2026-08-10 round-2 審查 F1）。
    `value_cut` 是官方報表的「權值+息值」，它**可以是負的** —— 認購價**高於**市價的
    現金增資就是，此時官方參考價比前一日收盤**高**、係數 > 1。實測 3 筆：

        1312 2026-06-11  13.20 -> 13.40  (權值+息值 -0.209613)
        1735 2013-01-07  12.60 -> 12.73  (-0.135448)
        1464 2011-09-15   9.47 ->  9.63  (-0.164495)

    用 `vc > 0` 的話這三筆會掉進下面的 `return f, SRC_REFERENCE`，寫進表裡是
    **factor 1.0 而且 source 標成 `reference`** —— 既沒還原（除權日留著 +1.0~1.7% 的
    機械跳空），又假稱「用了真實參考價」，連事後用 source 欄稽核都找不到。
    同一條式子對負值天生就對：`(13.20 - (-0.209613)) / 13.20 = 1.015880`，
    對上官方 13.40/13.20 = 1.015152（差一個檔位以內）。

    `value_cut == 0` 仍然走 primary —— 那是「真的沒配發」的正常 factor=1.0 事件，
    不可以硬套 fallback（見 `test_no_fallback_when_nothing_was_distributed`）。

    減資與面額變更走同一條式子（參考價 / 前價），只是 `value_cut` 給 NaN；
    減資的參考價**高於**前價，所以 factor > 1 —— 那是合法的，不要當成壞資料。
    """
    bp = pd.to_numeric(before_price, errors='coerce')
    rp = pd.to_numeric(reference_price, errors='coerce')
    vc = pd.to_numeric(value_cut, errors='coerce')

    if not np.isfinite(bp) or bp <= 0:
        return np.nan, SRC_UNUSABLE

    if np.isfinite(rp) and rp > 0:
        f = rp / bp
        # 參考價沒填（等於前價）而且確實有價值調整（正負皆可）-> 走 fallback
        if abs(f - 1.0) < 1e-9 and np.isfinite(vc) and vc != 0:
            return (bp - vc) / bp, SRC_FALLBACK
        return f, SRC_REFERENCE

    if np.isfinite(vc) and vc != 0:
        return (bp - vc) / bp, SRC_FALLBACK
    return np.nan, SRC_UNUSABLE


def cumulative_factor(dates, events):
    """給一組日期，回傳每個日期的累積還原係數（`pd.Series`，index = dates）。

    定義：`f(t) = Π factor(e)`，對所有**除權息日嚴格晚於 t** 的事件 e。
    所以最後一個事件之後的日期係數 = 1.0。

    用法：`adjusted = raw_close * cumulative_factor(raw_close.index, ev)`。
    事件當天本身**不套用**該事件的係數（當天的價格已經是除權／減資後的），
    只有它之前的日期要套 —— 這正是讓事件當天報酬回到正常水準的關鍵。

    ⚠️ **只吃單一股票的事件**。整張事件表餵進來會把所有股票的係數乘在一起，
    而且不會有任何錯誤跡象，所以這裡直接擋掉（`events` 帶 `stock_id` 欄時）。

    ⚠️ 係數序列**不是**單調的：除權息（factor < 1）讓它往回遞減，
    但減資（factor > 1）會讓它往回遞增。只有「最後一天 = 1.0」是恆成立的。
    """
    idx = pd.DatetimeIndex(dates)
    fac = pd.Series(1.0, index=idx)
    if events is None or len(events) == 0:
        return fac
    if 'stock_id' in getattr(events, 'columns', []):
        ids = events['stock_id'].dropna().unique()
        if len(ids) > 1:
            raise ValueError(
                "cumulative_factor() takes ONE stock's events, got %d stock_ids "
                "(%s...). Passing the whole table compounds every stock's factors "
                "into one series -- filter first: events[events.stock_id == sid]"
                % (len(ids), ', '.join(map(str, ids[:5]))))
    ev = events.dropna(subset=['date', 'factor'])
    for d, f in zip(pd.to_datetime(ev['date']), ev['factor'].astype(float)):
        if not in_factor_range(f):
            continue
        fac[idx < d] *= f
    return fac


def _empty_events():
    return pd.DataFrame(columns=COLS)


def _drop_unusable(df, layer):
    """丟掉係數算不出來（NaN）的列（2026-08-10 round-2 審查 F4）。

    理由跟減資層丟掉「已公告未生效」列完全相同：表是 append-only 且
    `keep='first'`，NaN 係數列**不帶任何資訊卻會永久佔住那個 key**，
    把日後抓到的正確值擋在門外，而 `--refetch-all` 依設計無法覆寫既有列
    ＝ 沒有任何修復路徑。原本只有減資層有這道過濾，除權息層與面額變更層沒有。

    觸發情境是來源換欄名或給了壞價格（`before_price` 缺、0 或負）。
    ⚠️ 代價：某檔股票**所有**事件都算不出係數時，它不會在表裡留下痕跡，
    於是每次續跑都會再問一次（跟「本來就沒有除權息事件」那 4% 同一個已知限制）。
    """
    if df is None or len(df) == 0:
        return _empty_events()
    bad = ~df['factor'].map(lambda f: np.isfinite(f))
    if bad.any():
        log.warning("  %s: dropping %d rows with an underivable factor "
                    "(bad/missing before_price, or a source column rename)",
                    layer, int(bad.sum()))
        df = df[~bad]
    return df


def _normalize(df):
    """把任意來源的事件 frame 校正成本表的 schema / dtype。"""
    if df is None or len(df) == 0:
        return _empty_events()
    out = df.copy()
    for c in COLS:
        if c not in out.columns:
            out[c] = np.nan
    out = out[COLS]
    out['stock_id'] = out['stock_id'].astype(str)
    out['date'] = pd.to_datetime(out['date'], errors='coerce')
    for c in ('before_price', 'reference_price', 'value_cut', 'factor'):
        out[c] = pd.to_numeric(out[c], errors='coerce')
    return out.dropna(subset=['date'])


def merge_events(existing, fetched):
    """把新抓到的事件併進既有表，回 `(merged, stats)`。

    **純函式**：不讀檔、不寫檔、不打網路 —— 這是持久化不變量唯一的實作處，
    所以它必須能被離線測試釘住（`tests/test_corporate_actions.py`）。

    規則：
      1. **既有列永遠獲勝**（`keep='first'`）—— 只新增、不覆寫。
         **唯一例外**：既有列的 factor 是 NaN 時，讓給新抓到的可用係數（見下）。
      2. **範圍過濾只作用在新抓的列**；既有列一律原封不動（就算它超出範圍）。
      3. 同一個 `(stock_id, date)` 由不同 source、**或同 source 但數值不同**時
         **出聲**（`stats`），不靜默挑一個。實測既有 3,403 筆除權息與 670 筆減資
         **零碰撞**，所以這是護欄不是常態路徑。

    ⚠️ NaN factor 讓位（2026-08-10 round-2 審查 F4）：NaN 係數**不帶任何資訊**，
    但在 `keep='first'` 下它會把日後抓到的正確值**永久**擋在門外，而
    `--refetch-all` 依設計無法覆寫既有列 ＝ 沒有任何修復路徑。這跟
    `_reduction_rows_to_events` 丟掉「已公告未生效」列是同一個理由。
    上游（`fetch_events` / `fetch_par_value_changes`）已經不再產生 NaN 列，
    這條是給**過去版本可能已經寫進去的列**留的修復路徑，以及日後新增資料層時的護欄。
    """
    existing = _normalize(existing)
    fetched = _normalize(fetched)
    stats = {'dropped_out_of_range': 0, 'collisions': 0, 'collision_keys': [],
             'repaired_nan': 0, 'added': 0}

    if len(fetched):
        # 寫入端與讀取端共用 `in_factor_range`（F8）。NaN 不算「超出範圍」——
        # 它走下面的讓位規則，不在這裡被計進 dropped_out_of_range。
        bad = fetched['factor'].notna() & ~fetched['factor'].map(in_factor_range)
        stats['dropped_out_of_range'] = int(bad.sum())
        fetched = fetched[~bad]

    if len(existing) and len(fetched):
        keys = ['stock_id', 'date']
        e = existing[keys + ['source', 'factor']].rename(
            columns={'source': 'src_old', 'factor': 'fac_old'})
        f = fetched[keys + ['source', 'factor']].rename(
            columns={'source': 'src_new', 'factor': 'fac_new'})
        clash = e.merge(f, on=keys)
        # 既有列是 NaN、新抓的可用 -> 這是**修復**不是碰撞，另外計數不要混進警告。
        repair = clash['fac_old'].isna() & clash['fac_new'].notna()
        stats['repaired_nan'] = int(repair.sum())
        # 同 source 也可能不一致（官方表自己就有重複列，見 F3）——
        # 只有「來源相同**且**數值相同」才是真的無害，其餘一律報出來。
        both_nan = clash['fac_old'].isna() & clash['fac_new'].isna()
        same_value = ((clash['fac_old'] - clash['fac_new']).abs() <= 1e-9) | both_nan
        harmless = (clash['src_old'] == clash['src_new']) & same_value
        clash = clash[~harmless & ~repair]
        if len(clash):
            stats['collisions'] += len(clash)
            stats['collision_keys'] += [
                '%s@%s(%s/%s)' % (r.stock_id, r.date.date(), r.src_old, r.src_new)
                for r in clash.head(5).itertuples()]

    parts = [d for d in (existing, fetched) if len(d)]
    merged = pd.concat(parts, ignore_index=True) if parts else _empty_events()
    if len(merged):
        # 穩定排序：可用係數排到前面，其餘順序（既有 -> 新抓）原封不動保留，
        # 所以「既有列獲勝」只在既有列的係數是 NaN 時才被推翻。
        order = np.argsort(merged['factor'].isna().to_numpy(), kind='stable')
        merged = merged.iloc[order]
        merged = merged.drop_duplicates(subset=['stock_id', 'date'], keep='first')
        merged = merged.sort_values(['stock_id', 'date']).reset_index(drop=True)
    stats['added'] = len(merged) - len(existing)
    return merged, stats


def fetch_events(dl, stock_id, start_date):
    """單一股票的**除權息／現金增資**事件（已算好 factor / source）。"""
    raw = dl.taiwan_stock_dividend_result(stock_id=stock_id, start_date=start_date)
    if raw is None or len(raw) == 0:
        return _empty_events()
    df = raw.copy()
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    if df.empty:
        return _empty_events()

    vc_col = 'stock_and_cache_dividend'
    out = []
    for _, r in df.iterrows():
        f, src = derive_factor(r.get('before_price'), r.get('reference_price'),
                               r.get(vc_col))
        out.append({'stock_id': str(stock_id),
                    'date': r['date'],
                    'before_price': pd.to_numeric(r.get('before_price'), errors='coerce'),
                    'reference_price': pd.to_numeric(r.get('reference_price'), errors='coerce'),
                    'value_cut': pd.to_numeric(r.get(vc_col), errors='coerce'),
                    'factor': f, 'source': src})
    return _drop_unusable(pd.DataFrame(out, columns=COLS), 'dividends %s' % stock_id)


# --------------------------------------------------------------------------- #
#  減資（官方，全市場一次抓完）
# --------------------------------------------------------------------------- #
def _to_float(x):
    try:
        return float(str(x).replace(',', '').strip())
    except (TypeError, ValueError):
        return np.nan


def _roc_to_iso(value):
    """民國日期轉西元。吃 `111/10/31`（TWSE）與 `1150630`（TPEX）兩種寫法。"""
    s = str(value).strip()
    if '/' in s:
        y, m, d = s.split('/')
    else:
        y, m, d = s[:-4], s[-4:-2], s[-2:]
    return pd.Timestamp('%04d-%02d-%02d' % (int(y) + 1911, int(m), int(d)))


def official_session():
    s = requests.Session()
    s.verify = False  # 沿用 twse_api 的慣例（部分環境 TWSE SSL 憑證驗證失敗）
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/html, */*',
        'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
    })
    return s


def _reduction_rows_to_events(fields, rows, fieldmap):
    """官方減資列 -> 事件 frame。欄位用名字定位，找不到就 raise（不要靜默錯位）。"""
    missing = [n for n in fieldmap.values() if n not in (fields or [])]
    if missing:
        raise RuntimeError('官方減資表欄位不見了: %s (實際 fields=%s)' % (missing, fields))
    idx = {k: fields.index(n) for k, n in fieldmap.items()}
    out = []
    for r in rows or []:
        before = _to_float(r[idx['before']])
        reference = _to_float(r[idx['reference']])
        exright = _to_float(r[idx['exright']])
        # 「彌補虧損之減資並現金增資」：當天恢復買賣後緊接著除權，
        # 真正的起跳價是除權參考價而不是恢復買賣參考價。
        has_exright = bool(np.isfinite(exright) and exright > 0)
        if has_exright:
            reference = exright
        factor, _src = derive_factor(before, reference, np.nan)
        out.append({'stock_id': str(r[idx['stock_id']]).strip(),
                    'date': _roc_to_iso(r[idx['date']]),
                    'before_price': before,
                    'reference_price': reference,
                    'value_cut': np.nan,
                    'factor': factor,
                    'source': SRC_REDUCTION,
                    '_has_exright': has_exright})
    df = pd.DataFrame(out, columns=COLS + ['_has_exright'])
    # 已公告但還沒生效的列（價格欄是 "-"）不可入表：它會用 keep='first' 把
    # 未來真的抓到的那筆正確資料永遠擋在門外。
    announced = df['factor'].isna()
    if announced.any():
        log.info("  dropping %d announced-but-not-yet-effective rows (no prices yet)",
                 int(announced.sum()))
        df = df[~announced]
    return _dedupe_reduction_rows(df).drop(columns='_has_exright')


def _dedupe_reduction_rows(df):
    """官方減資表**自己就有重複列**，這裡用明確的偏好去重（2026-08-10 審查 F3）。

    實測（TWTAUU 2011-01-01~2026-08-10，392 列）：4 個 `(代號, 恢復買賣日期)`
    重複、共 5 個多餘列。其中 3 組（`2408`/`2438`/`5906`）只差在公司名稱與
    詳細資料欄，本工具讀的 5 個欄位完全一致 ＝ 挑哪列都一樣。

    但 **`3536` 104/03/20 出現三次，而且不一致**：第一列的「除權參考價」是 `--`，
    另外兩列是 `13.06`。交給 `drop_duplicates(keep='first')` 就是**用 payload 順序
    決定答案**，而 payload 順序跟本工具「有除權參考價就用它」的規則無關 ——
    會得到 13.33/6.58 = 2.0259 而不是正確的 13.06/6.58 = 1.9848（+2.07% 誤差）。

    ⚠️ 這必須在**全市場回補之前**修好：表是 append-only 的，錯的係數一旦寫進去
    就沒有任何修復路徑（`--refetch-all` 依設計無法覆寫既有列）。
    `3536` 目前不在本專案的 1,965 檔宇宙內（沒有 `data_cache/3536_price.csv`），
    所以現在還沒踩到 —— 但宇宙是照快取目錄長出來的，會變。
    """
    if len(df) < 2:
        return df
    dup = df.duplicated(subset=['stock_id', 'date'], keep=False)
    if not dup.any():
        return df
    # 真的不一致的（本工具讀的欄位有差）要出聲，不要靜默挑一個
    read_cols = ['before_price', 'reference_price', 'factor']
    for (sid, dt), g in df[dup].groupby(['stock_id', 'date']):
        if len(g[read_cols].drop_duplicates()) > 1:
            log.warning("  official reduction table has %d DISAGREEING rows for "
                        "%s@%s (factor %s) -- keeping the one with an ex-right "
                        "reference price", len(g), sid, dt.date(),
                        ", ".join('%.5f' % f for f in g['factor']))
    # 偏好帶除權參考價的那列；同級則維持官方順序（stable）
    ordered = df.sort_values('_has_exright', ascending=False, kind='stable')
    kept = ordered.drop_duplicates(subset=['stock_id', 'date'], keep='first')
    log.info("  de-duplicated %d official reduction rows (%d -> %d)",
             len(df) - len(kept), len(df), len(kept))
    return kept.sort_index()


def fetch_reductions_twse(session, start_date, end_date):
    """上市減資恢復買賣參考價（`TWT AUU`）。一次呼叫拿全市場全區間。"""
    start = max(str(start_date), TWSE_REDUCTION_MIN_DATE)
    params = {'startDate': start.replace('-', ''),
              'endDate': str(end_date).replace('-', ''),
              'response': 'json'}
    j = session.get(TWSE_REDUCTION_URL, params=params, timeout=60).json()
    if str(j.get('stat', '')).upper() != 'OK':
        raise RuntimeError('TWSE 減資表拒絕查詢: %s' % j.get('stat'))
    # data-sources.md 的硬規則：日期一律以 payload 自報值為準，
    # 端點沒吃我們給的區間就當它壞了（TPEX 舊端點無視 `d` 參數的教訓）。
    if (str(j.get('strDate')) != params['startDate']
            or str(j.get('endDate')) != params['endDate']):
        raise RuntimeError('TWSE 減資表沒有採用請求的區間: 要 %s~%s 回 %s~%s'
                           % (params['startDate'], params['endDate'],
                              j.get('strDate'), j.get('endDate')))
    return _reduction_rows_to_events(j.get('fields'), j.get('data'), _TWSE_RED_FIELDS)


def fetch_reductions_tpex(session, start_date, end_date):
    """上櫃減資恢復買賣參考價。一次呼叫拿全市場全區間。

    ⚠️ 這個端點**只認斜線格式**的 `startDate` / `endDate`（`2013/01/01`）；
    緊湊格式（`20130101`）會回「參數錯誤」，而裸 `date=` 參數則是**完全被忽略**
    （不論給什麼都回今天）。
    """
    params = {'startDate': str(start_date).replace('-', '/'),
              'endDate': str(end_date).replace('-', '/')}
    j = session.get(TPEX_REDUCTION_URL, params=params, timeout=60).json()
    if str(j.get('stat', '')).lower() != 'ok':
        raise RuntimeError('TPEX 減資表拒絕查詢: %s' % j.get('stat'))
    want = '%s~%s' % (params['startDate'].replace('/', ''),
                      params['endDate'].replace('/', ''))
    if str(j.get('date')) != want:
        raise RuntimeError('TPEX 減資表沒有採用請求的區間: 要 %s 回 %s'
                           % (want, j.get('date')))
    table = (j.get('tables') or [{}])[0]
    return _reduction_rows_to_events(table.get('fields'), table.get('data'),
                                     _TPEX_RED_FIELDS)


def fetch_par_value_changes(dl, start_date):
    """面額變更（FinMind `taiwan_stock_split_price`）。全市場一次呼叫。

    面額由 10 元改成 1 元 ＝ 股數 ×10、股價 ÷10，係數 0.1 —— 是本表裡最小的係數，
    也是 `FACTOR_MIN` 要壓到 0.05 的原因。
    """
    raw = dl.taiwan_stock_split_price(start_date=str(start_date))
    if raw is None or len(raw) == 0:
        return _empty_events()
    out = []
    for r in raw.itertuples():
        before = _to_float(getattr(r, 'before_price', np.nan))
        after = _to_float(getattr(r, 'after_price', np.nan))
        factor, _src = derive_factor(before, after, np.nan)
        out.append({'stock_id': str(r.stock_id).strip(),
                    'date': pd.to_datetime(r.date, errors='coerce'),
                    'before_price': before, 'reference_price': after,
                    'value_cut': np.nan, 'factor': factor,
                    'source': SRC_PAR_VALUE})
    df = pd.DataFrame(out, columns=COLS).dropna(subset=['date'])
    return _drop_unusable(df, 'par-value changes')


def _replace_with_retry(tmp_path, path, attempts=3, delay=0.3):
    """沿用 refresh_universe_prices 的作法：Windows 上常駐的 Streamlit 會開著同一批
    檔案，os.replace 可能短暫拿到 PermissionError，重試即可。"""
    for i in range(attempts):
        try:
            os.replace(tmp_path, path)
            return
        except PermissionError:
            if i == attempts - 1:
                raise
            time.sleep(delay)


def load_existing():
    """讀既有表，回 `(DataFrame, ok)`。

    讀不出來時**先把壞檔改名保留**（`.corrupt-<timestamp>`）再重新開始 ——
    直接「starting fresh」會讓稍後那次成功的寫入把可能還救得回來的位元組蓋掉。
    搬不走就整個放棄（回 `(None, False)`），寧可什麼都不寫。

    ⚠️ 「讀不到」與「內容壞掉」要先分辨（2026-08-10 round-2 審查 F10）：
    檔案鎖、防毒掃描、另一個同時在跑的實例都會讓 `read_parquet` 拋例外，
    但那些**都不是毀損**。原本任何例外都改名，於是一次暫時性的檔案鎖就會
    丟掉整份續跑狀態 —— 下一輪要重燒約 3.3 小時的 FinMind 額度，而且要人工改回檔名。
    判準：能不能用二進位開檔讀到位元組。能讀 ＝ 沒有鎖，才有資格判定毀損。
    """
    if not OUT_PATH.exists():
        return _empty_events(), True
    try:
        existing = pd.read_parquet(OUT_PATH)
        existing['date'] = pd.to_datetime(existing['date'])
        existing['stock_id'] = existing['stock_id'].astype(str)
        log.info("existing table: %d events / %d stocks",
                 len(existing), existing['stock_id'].nunique())
        return existing, True
    except Exception as e:
        try:
            with open(OUT_PATH, 'rb') as fh:
                fh.read(1)
        except OSError as open_err:
            log.error("existing table could not be OPENED (%s: %s; parse error was "
                      "%s) -- refusing to write. This looks like a file lock or a "
                      "concurrent run, NOT corruption, so the table is left "
                      "untouched and the resume state survives.",
                      type(open_err).__name__, open_err, e)
            return None, False
        aside = OUT_PATH.with_name('%s.corrupt-%s'
                                   % (OUT_PATH.name, time.strftime('%Y%m%d-%H%M%S')))
        try:
            os.replace(OUT_PATH, aside)
        except OSError as move_err:
            log.error("existing table unreadable (%s) and could not be moved aside "
                      "(%s) -- refusing to overwrite it", e, move_err)
            return None, False
        log.warning("existing table unreadable (%s) -- moved to %s, starting fresh",
                    e, aside.name)
        return _empty_events(), True


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--limit', type=int, default=0,
                    help='only process the first N stocks of the universe (0 = all). '
                         'NOTE: the slice is taken BEFORE the resume filter, so it is '
                         'always the SAME first N stems -- re-running with --limit does '
                         'NOT page through the market and will not advance a backfill '
                         'once those N are done. The market-wide layers are filtered by '
                         'the limited universe too, so a limited run also keeps only '
                         'that slice of the capital-reduction events. Use it for '
                         'smoke tests; drop it for the real backfill.')
    ap.add_argument('--start-date', default='2000-01-01',
                    help='earliest event date to request')
    ap.add_argument('--dry-run', action='store_true',
                    help='do everything except write the parquet')
    ap.add_argument('--sleep', type=float, default=0.0,
                    help='seconds to sleep between stocks')
    ap.add_argument('--refetch-all', action='store_true',
                    help='also re-request stocks already present in the table. '
                         'NOTE: existing rows ALWAYS win (the merge is append-only), '
                         'so this flag can only fill in MISSING events -- it cannot '
                         'repair or refresh a row that is already there. '
                         'Default: skip them, so a quota-interrupted run resumes '
                         'instead of burning quota on work already done.')
    ap.add_argument('--skip-market-wide', action='store_true',
                    help='skip the market-wide layers (capital reductions, par-value '
                         'changes) and only fetch per-stock dividends')
    args = ap.parse_args()
    _setup_logging()

    files = sorted(CACHE_DIR.glob('*_price.csv'))
    universe, skipped_dup = _tw_cache_stems(files)
    if skipped_dup:
        # 不靜默：略過了什麼要講清楚（沿用 refresh_universe_prices 的慣例）
        log.info("skipping %d .TWO-mangled cache keys (e.g. %s)",
                 len(skipped_dup), ", ".join(skipped_dup[:3]))
    if args.limit:
        universe = universe[:args.limit]
    uni_set = set(universe)
    end_date = pd.Timestamp.today().strftime('%Y-%m-%d')
    log.info("building corporate actions for %d TW stocks (%s..%s)%s",
             len(universe), args.start_date, end_date,
             "  [DRY RUN]" if args.dry_run else "")

    existing, ok = load_existing()
    if not ok:
        return 1

    fetched_parts = []
    quota_hit = False

    # ------------------------------------------------------------------ #
    #  全市場層：減資 + 面額變更（3 次呼叫，每次都重抓）
    #
    #  ⚠️ 這裡**必須用完整的 universe 過濾，不能用續跑後的待辦清單** ——
    #  否則已經抓過除權息的股票（例如首輪那 195 檔）就永遠補不到減資事件。
    # ------------------------------------------------------------------ #
    if args.skip_market_wide:
        log.info("skipping market-wide layers (--skip-market-wide)")
    else:
        sess = official_session()
        for label, fn in (('TWSE', fetch_reductions_twse),
                          ('TPEX', fetch_reductions_tpex)):
            try:
                ev = fn(sess, args.start_date, end_date)
                mine = ev[ev['stock_id'].isin(uni_set)]
                log.info("%s capital reductions: %d events (%d in our universe / %d stocks)",
                         label, len(ev), len(mine), mine['stock_id'].nunique())
                fetched_parts.append(mine)
            except Exception as e:
                # 官方端點掛掉不該讓整輪除權息白跑，但要吵。
                log.error("%s capital reductions FAILED (%s: %s) -- continuing without them",
                          label, type(e).__name__, str(e)[:160])

    dl = get_finmind_loader()

    # ⚠️ 面額變更（也是 FinMind、也是全市場一次抓完）**刻意排在逐檔層之後**，
    #    見那之後的註解（2026-08-10 round-2 審查 F5）。

    # ------------------------------------------------------------------ #
    #  逐檔層：除權息 / 現金增資
    #
    #  續跑：完成判定只看**除權息來源**的列。FinMind 額度是這支工具唯一的瓶頸
    #  （2026-08-10 實測伺服器端在第 200 檔就擋下），重抓已完成的部分等於
    #  讓「被額度打斷 -> 再跑一次」永遠跑不完。
    #  ⚠️ 已知限制：**沒有任何除權息事件的股票不會留下痕跡**，所以每次續跑都會
    #  再問一次（實測約佔 4%）。要完全避免得另存「抓過哪些股票」的清單，目前不值得。
    # ------------------------------------------------------------------ #
    todo = list(universe)
    if not args.refetch_all and len(existing):
        done = set(existing.loc[existing['source'].isin(DIVIDEND_SOURCES),
                                'stock_id'].astype(str))
        before = len(todo)
        todo = [s for s in todo if s not in done]
        if before != len(todo):
            log.info("resuming: skipping %d stocks whose dividends are already in the "
                     "table (%d left; use --refetch-all to override)",
                     before - len(todo), len(todo))

    stats = {'ok': 0, 'no_events': 0, 'error': 0}
    t0 = time.time()
    div_frames = []

    for i, sid in enumerate(todo, 1):
        try:
            ev = fetch_events(dl, sid, args.start_date)
        except Exception as e:
            name = type(e).__name__
            # 額度類錯誤：停下來保存已完成的部分，而不是硬跑完把成果丟掉。
            # 事件表是「只新增」的，所以下次直接再跑一次就會接續。
            if 'Quota' in name or 'RateLimit' in name or '429' in str(e):
                log.warning("quota/rate limit at %s (%d/%d) -- stopping early "
                            "and keeping what we have", sid, i, len(todo))
                quota_hit = True
                break
            log.warning("  %s: %s: %s", sid, name, str(e)[:80])
            stats['error'] += 1
            continue
        if ev.empty:
            stats['no_events'] += 1
        else:
            div_frames.append(ev)
            stats['ok'] += 1
        if i % 100 == 0:
            log.info("  [%d/%d] ok=%d no_events=%d error=%d elapsed=%.0fs",
                     i, len(todo), stats['ok'], stats['no_events'],
                     stats['error'], time.time() - t0)
        if args.sleep:
            time.sleep(args.sleep)

    # ------------------------------------------------------------------ #
    #  面額變更（FinMind，全市場 1 次呼叫）
    #
    #  ⚠️ **必須排在逐檔除權息之後**（2026-08-10 round-2 審查 F5）。
    #  原本排在前面，而且它的例外處理會在「例外名稱含 Quota」時設一個
    #  **全域**旗標，那個旗標又是整個逐檔迴圈的閘門 —— 於是這個市場層一失敗，
    #  整輪除權息就一筆都不跑，卻回報「被額度打斷，再跑一次即可續跑」。
    #
    #  這不是假想風險：FinMind 免費層**不支援不帶 `data_id` 的全市場查詢**，
    #  回 HTTP 400 `Your level is register`，而 SDK 會把它變成 `KeyError('data')`，
    #  `cache_manager` 再把 `KeyError('data')` 一律當成伺服器端額度爆掉
    #  （`cache_manager.py:553-585`）。`taiwan_stock_dividend_result` 與
    #  `capital_reduction` 今天就是這樣，只有 `split_price` 還能全市場抓 ——
    #  哪天它跟著改，每一輪都會是「零進度 + exit 3」，而且等多久再跑都不會好。
    #
    #  ⚠️ 只把旗標改成區域變數**不夠**：`cache_manager` 的 negative cache
    #  （`_quota_blocked_until`）是**行程層級**的，被誤判一次之後接下來 300 秒
    #  內每一個 FinMind 呼叫都會直接 fail-fast，逐檔迴圈照樣死在第 1 檔。
    #  真正的解法是順序 —— 昂貴、可續跑的那層先跑完，再跑便宜、每次都重抓的這層。
    #
    #  失敗處理與 TWSE / TPEX 兩個減資層**完全一致**：log.error 後繼續，
    #  不改 exit code。三個市場層都是每跑一次就全量重抓，下一輪自然補回來；
    #  exit code 的語義只留給「逐檔層還沒跑完」（見 docstring）。
    # ------------------------------------------------------------------ #
    if not args.skip_market_wide:
        try:
            ev = fetch_par_value_changes(dl, args.start_date)
            mine = ev[ev['stock_id'].isin(uni_set)]
            log.info("par-value changes: %d events (%d in our universe / %d stocks)",
                     len(ev), len(mine), mine['stock_id'].nunique())
            fetched_parts.append(mine)
        except Exception as e:
            log.error("par-value changes FAILED (%s: %s) -- continuing without them",
                      type(e).__name__, str(e)[:160])

    dividends = (pd.concat(div_frames, ignore_index=True) if div_frames
                 else _empty_events())
    log.info("fetched %d dividend events from %d stocks in %.0fs",
             len(dividends), stats['ok'], time.time() - t0)
    if len(dividends):
        n_fallback = int((dividends['source'] == SRC_FALLBACK).sum())
        log.info("  source: reference=%d fallback=%d unusable=%d (fallback share %.1f%%)",
                 int((dividends['source'] == SRC_REFERENCE).sum()), n_fallback,
                 int((dividends['source'] == SRC_UNUSABLE).sum()),
                 100.0 * n_fallback / max(len(dividends), 1))

    # 減資／面額變更先進 fetched，除權息後進 —— 萬一同日撞號（實測 0 筆），
    # 涵蓋較完整的那筆（減資參考價已含當期股利）會留下來。
    fetched_parts.append(dividends)
    fetched = pd.concat([p for p in fetched_parts if len(p)], ignore_index=True) \
        if any(len(p) for p in fetched_parts) else _empty_events()

    merged, mstats = merge_events(existing, fetched)
    if mstats['dropped_out_of_range']:
        log.warning("  dropped %d fetched events with out-of-range factor "
                    "(outside %.2f..%.2f)", mstats['dropped_out_of_range'],
                    FACTOR_MIN, FACTOR_MAX)
    if mstats['collisions']:
        log.warning("  %d (stock_id,date) collisions -- existing row kept: %s",
                    mstats['collisions'], ", ".join(mstats['collision_keys']))
    if mstats['repaired_nan']:
        log.info("  repaired %d existing rows whose factor was NaN (a fetched row with "
                 "a usable factor replaced them)", mstats['repaired_nan'])

    log.info("table: %d events / %d stocks (%+d events vs existing)",
             len(merged), merged['stock_id'].nunique() if len(merged) else 0,
             mstats['added'])
    if len(merged):
        log.info("  by source: %s", merged['source'].value_counts().to_dict())

    # ------------------------------------------------------------------ #
    #  fail loud：問了一大批「還沒抓過」的股票卻一筆事件都沒有 ＝ 資料源壞了
    #
    #  ⚠️ 原本的條件 `stats['ok'] > 20 and len(dividends) == 0` **永遠不可能成立**
    #     （2026-08-10 round-2 審查 F2）：`stats['ok']` 只在「收到非空 frame」
    #     那個 else 分支裡 +1，所以 `len(dividends) == 0` 邏輯上等價於
    #     `stats['ok'] == 0` —— 一個計數器不可能同時 > 20 又 == 0，
    #     `refusing to write` 這行從來沒有機會執行。
    #     它本來要擋的失敗（token 失效／資料集改名／上游回 200 但零列）
    #     全部落在 `stats['no_events']`，而那個計數器沒有任何人檢查
    #     ＝ 整個除權息來源死掉也照樣回 exit 0，排程只看 exit code 會當成成功。
    #
    #  ⚠️ 但不能直接換成「`no_events` 很多就報錯」：全市場抓完之後，續跑剩下的
    #     待辦**正好就是**「真的沒有除權息事件」那批（docstring 記為約 4%，
    #     實測 `--limit 200` 剩 4 檔且全部合法零事件），那樣每次例行重跑都誤報。
    #     所以再加一個判準：這一輪問的是不是「一大批沒抓過的股票」——
    #     待辦清單要佔宇宙一半以上（首建或回補進行中）。全市場抓完之後
    #     todo 只剩約 4%，這道閘門就自動退場。
    # ------------------------------------------------------------------ #
    processed = stats['ok'] + stats['no_events'] + stats['error']
    bulk_run = len(todo) >= max(FAIL_LOUD_MIN_STOCKS, 0.5 * len(universe))
    if bulk_run and stats['ok'] == 0 and stats['no_events'] >= FAIL_LOUD_MIN_STOCKS:
        log.error("processed %d of %d to-do stocks (universe %d) and EVERY one of them "
                  "returned zero dividend events -- the source is almost certainly "
                  "broken (expired/downgraded token, dataset rename, or an upstream "
                  "200-with-no-rows), not %d genuinely dividend-free stocks. "
                  "Refusing to write.", processed, len(todo), len(universe),
                  stats['no_events'])
        return 1

    if args.dry_run:
        log.info("DRY RUN -- not writing %s", OUT_PATH)
    elif len(merged):
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = OUT_PATH.with_name(f'.{OUT_PATH.name}.{os.getpid()}.tmp')
        try:
            merged.to_parquet(tmp, index=False)
            _replace_with_retry(tmp, OUT_PATH)
            log.info("wrote %s", OUT_PATH)
        finally:
            Path(tmp).unlink(missing_ok=True)

    if quota_hit:
        log.warning("stopped early on quota -- re-run to continue "
                    "(events already saved are not refetched); exit code 3")
        return 3
    return 0


if __name__ == '__main__':
    sys.exit(main())
