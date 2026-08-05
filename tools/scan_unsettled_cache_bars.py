"""scan_unsettled_cache_bars.py -- 美股 price cache 未完成 bar 偵測（資料毀損）

## 為什麼需要

2026-08-04：使用者問「投資組合報酬率為什麼暴升」（答案是真的 —— MSFT 財報單日
+15.51%，持股 MSFU 是 2x 槓桿 ETF），查證時翻出 `data_cache/*_price.csv` 存著
**盤中未完成 bar**：Close 只是抓取當下的報價、Volume 只累積到當下。

它一旦落地就永久有效 —— 增量更新只從 cache 最後一列往後續抓，那根壞 bar 不會
再被重抓修正。實測 `MSFU` 08-03 cache 收 35.84 / 量 6.85M，真實收盤 36.47 /
量 11.84M；`TSMX` 07-28 差 -3.4% **躺了六天**。全庫掃出 **475 檔 / 5,951 列**，
源頭是 04-15 與 05-22 兩次在美股盤中跑的批次抓取。

寫入端已於 `e95745a` 治本（`technical_analysis.drop_unsettled_bars` 擋落地 +
增量起算日含 `last_date` 讓壞列被覆寫）。本工具是**防回歸監控**：治本後這裡
應該長期回 0，一旦又冒出來就是有新的寫入路徑繞過了防護。

## 判別

**成交量比收盤價靈敏** —— 未完成 bar 的量只累積到抓取當下，比價差更早、更明顯
（實例：量只有真實的 4%，而價差「只有」-7%）。所以主判據是
`cache_volume / real_volume < VOL_TOL`，價差是輔助。

## ⚠️ 「不符」不等於「未完成 bar」——分割是最大的假陽性來源

分割 / 分拆會讓 yfinance **回溯調整整段歷史**，價與量同時變，一樣觸發上面的判據。
2026-08-04 第一版修復腳本因此對 5 檔分割股（`DD` +200% / `KLAC` -90% /
`CVNA` -80% / `CRWD` -75% / `FDX` -19.4%）做了**部分列覆寫**，在覆寫邊界憑空造出
假跳空（DD 04-02 收 136.44 → 04-06 變成 136.71）。**分割股只能整檔重抓。**

本工具因此把發現分成兩類，`--apply` 的處理方式完全不同：

| 類別 | 特徵 | 修法 |
|---|---|---|
| `unsettled` | 單日或少數列、比例隨機散在 1 附近 | 已定案 -> 用真值覆寫；未定案 -> 移除該列 |
| `split` | **連續多日呈幾乎相同比例**（變異係數 < 2%）且偏離 1 超過 5% | **整檔 `force_update=True` 重抓**，絕不部分覆寫 |

第二道護欄：單列偏離超過 `MAX_SANE_OFFSET`（15%）一律不做部分覆寫 —— 未完成 bar
不可能偏那麼多（實測最極端是槓桿 ETF 的 -7%）。

## 範圍

只掃**美股**（cache 檔名字母開頭；台股一律數字開頭）。台股不掃的理由：
主力路徑已有防護（掃描排程 00:00 在台股收盤後跑 + `_try_intraday_quote_as_today_bar`
的 mis.twse partial bar 不落地），而拿 `ohlcv_tw.parquet` 來比對會因**還原價**
差異產生大量假陽性，需要另設方法。

用法：
    python tools/scan_unsettled_cache_bars.py                    # 只報告（夜間鏈用）
    python tools/scan_unsettled_cache_bars.py --fail-on unsettled,split
    python tools/scan_unsettled_cache_bars.py --apply            # 實際修正（含備份）
    python tools/scan_unsettled_cache_bars.py --all              # 連 stale cache 一起掃
"""
from __future__ import annotations

import argparse
import datetime
import glob
import io
import os
import shutil
import sys
from pathlib import Path

# 手動執行時 stdout 可能是 cp950，中文報告會 UnicodeEncodeError 中斷（排程有設
# PYTHONIOENCODING=utf-8，手動跑沒有）。比照 technical_analysis.py 檔頭做法。
if (sys.stdout and getattr(sys.stdout, 'encoding', None)
        and sys.stdout.encoding.lower() in ('cp950', 'cp936', 'cp932')):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pandas as pd  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import technical_analysis as ta  # noqa: E402  (is_bar_settled / load_and_resample)

CACHE_DIR = REPO_ROOT / 'data_cache'
BACKUP_DIR = REPO_ROOT / 'data_cache' / '_unsettled_fix_backup'

VOL_TOL = 0.95          # cache 量 < 真實量 * 這個比例 -> 可疑
CLOSE_TOL = 0.001       # 收盤相對誤差 > 0.1% -> 可疑
RECENT_DAYS = 60        # 預設只掃「最後一列在這區間內」的 cache
BATCH = 50              # yfinance 批次檔數（單一請求）
SPLIT_MIN_ROWS = 3      # 判定分割所需的最少壞列數
SPLIT_CV_MAX = 0.02     # 比例變異係數上限（越小越像固定倍數）
SPLIT_MIN_OFFSET = 0.05 # 平均比例偏離 1 的下限
MAX_SANE_OFFSET = 0.15  # 單列偏離超過此值就不做部分覆寫


def cache_name_to_yf(name: str) -> str:
    """cache 檔名還原 yfinance ticker（指數的 `^` 被 _get_path 的白名單濾掉）。"""
    return '^GSPC' if name == 'GSPC' else name


def list_us_caches(include_stale: bool) -> list:
    """回 [(name, path, last_date)]，只取字母開頭（＝美股）。"""
    cut = datetime.date.today() - datetime.timedelta(days=RECENT_DAYS)
    out = []
    for path in sorted(glob.glob(str(CACHE_DIR / '*_price.csv'))):
        name = os.path.basename(path).replace('_price.csv', '')
        if not name[:1].isalpha():
            continue
        try:
            df = pd.read_csv(path, index_col=0)
            idx = pd.to_datetime(df.index, errors='coerce')
            idx = idx[idx.notna()]
            if len(idx) == 0:
                continue
            last = idx.max().date()
        except Exception as e:
            print(f'  [read-err] {name}: {e}')
            continue
        if include_stale or last >= cut:
            out.append((name, path, last))
    return out


def fetch_real(names: list, start: str) -> dict:
    """批次抓真實 OHLCV。回 {name: DataFrame}；抓不到者不在 dict 裡。"""
    import yfinance as yf
    real = {}
    for i in range(0, len(names), BATCH):
        chunk = names[i:i + BATCH]
        syms = [cache_name_to_yf(n) for n in chunk]
        try:
            raw = yf.download(syms, start=start, interval='1d', progress=False,
                              auto_adjust=False, timeout=90, group_by='ticker')
        except Exception as e:
            print(f'  [fetch-err] batch {i // BATCH + 1}: {e}')
            continue
        for n, sym in zip(chunk, syms):
            try:
                d = raw[sym] if isinstance(raw.columns, pd.MultiIndex) else raw
                d = d.dropna(subset=['Close'])
                if len(d):
                    d.index = pd.DatetimeIndex(d.index).tz_localize(None)
                    real[n] = d
            except Exception:
                pass
    return real


def find_bad_rows(cache_df: pd.DataFrame, real_df: pd.DataFrame) -> list:
    """回 [dict(date, cache_close, real_close, ratio, vol_ratio, settled)]。"""
    bad = []
    cidx = pd.to_datetime(cache_df.index, errors='coerce')
    for pos, d in enumerate(cidx):
        if pd.isna(d) or d not in real_df.index:
            continue
        try:
            cc = float(pd.to_numeric(cache_df.iloc[pos]['Close'], errors='coerce'))
            cv = float(pd.to_numeric(cache_df.iloc[pos]['Volume'], errors='coerce'))
            rc = float(real_df.loc[d, 'Close'])
            rv = float(real_df.loc[d, 'Volume'])
        except Exception:
            continue
        if pd.isna(cc) or pd.isna(cv) or pd.isna(rc) or not rv or not cc:
            continue
        if cv / rv < VOL_TOL or abs(cc - rc) / rc > CLOSE_TOL:
            bad.append({
                'date': d, 'cache_close': cc, 'real_close': rc,
                'ratio': rc / cc, 'vol_ratio': cv / rv,
                'settled': ta.is_bar_settled(d.date(), 'us'),
            })
    return bad


def classify(bad: list) -> str:
    """'split'（整檔重抓）或 'unsettled'（可部分覆寫）。

    分割特徵是「連續多日呈幾乎相同比例」；未完成 bar 的比例隨機散在 1 附近。
    """
    if len(bad) < SPLIT_MIN_ROWS:
        ratios = [b['ratio'] for b in bad]
        # 列數不足以看出比例一致性時，改用「偏離幅度」保守判斷
        if any(abs(r - 1.0) > MAX_SANE_OFFSET for r in ratios):
            return 'split'
        return 'unsettled'
    s = pd.Series([b['ratio'] for b in bad])
    mean = float(s.mean())
    if not mean:
        return 'unsettled'
    cv = float(s.std()) / abs(mean)
    if cv < SPLIT_CV_MAX and abs(mean - 1.0) > SPLIT_MIN_OFFSET:
        return 'split'
    if float(s.sub(1.0).abs().max()) > MAX_SANE_OFFSET:
        return 'split'
    return 'unsettled'


def apply_unsettled(path: str, cache_df: pd.DataFrame, real_df: pd.DataFrame,
                    bad: list) -> tuple:
    """已定案的列用真值覆寫、未定案的列移除。回 (overwritten, dropped)。"""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, BACKUP_DIR / os.path.basename(path))

    cidx = pd.to_datetime(cache_df.index, errors='coerce')
    drop_pos, over_pos = [], []
    for b in bad:
        pos_list = [i for i, x in enumerate(cidx) if x == b['date']]
        (over_pos if b['settled'] else drop_pos).extend(pos_list)

    for pos in over_pos:
        src = real_df.loc[cidx[pos]]
        for col in cache_df.columns:
            if col in src.index and pd.notna(src[col]):
                cache_df.iloc[pos, cache_df.columns.get_loc(col)] = float(src[col])
    if drop_pos:
        keep = [i for i in range(len(cache_df)) if i not in set(drop_pos)]
        cache_df = cache_df.iloc[keep]

    cache_df.index.name = 'Date'
    cache_df.to_csv(path)
    return len(over_pos), len(drop_pos)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true',
                    help='實際修正（unsettled 部分覆寫 / split 整檔重抓；先備份）')
    ap.add_argument('--all', action='store_true', help='連 stale cache 一起掃')
    ap.add_argument('--fail-on', default='',
                    help='逗號分隔：unsettled,split — 有發現就 exit 1')
    args = ap.parse_args()
    fail_on = {s.strip() for s in args.fail_on.split(',') if s.strip()}

    caches = list_us_caches(include_stale=args.all)
    scope = 'all' if args.all else f'last {RECENT_DAYS}d'
    print(f'[scan_unsettled_cache_bars] US price caches: {len(caches)} ({scope})')
    if not caches:
        print('[OK] 沒有符合條件的 cache')
        return 0

    oldest = min(d for _n, _p, d in caches)
    start = (oldest - datetime.timedelta(days=5)).strftime('%Y-%m-%d')
    real = fetch_real([n for n, _p, _d in caches], start)
    unverified = [n for n, _p, _d in caches if n not in real]

    found = {'unsettled': [], 'split': []}
    n_over = n_drop = n_refetch = 0

    for name, path, _last in caches:
        if name not in real:
            continue
        cache_df = pd.read_csv(path, index_col=0)
        bad = find_bad_rows(cache_df, real[name])
        if not bad:
            continue
        kind = classify(bad)
        worst = max(bad, key=lambda b: abs(b['ratio'] - 1.0))
        found[kind].append((name, len(bad), worst))
        print(f'  [{kind:9s}] {name:8s} {len(bad):3d} row(s); worst '
              f'{worst["date"].date()} cache {worst["cache_close"]:.4f} vs real '
              f'{worst["real_close"]:.4f} '
              f'({(1 / worst["ratio"] - 1) * 100:+.2f}%, vol {worst["vol_ratio"]:.2f}x)')

        if not args.apply:
            continue
        if kind == 'split':
            # 分割股：部分覆寫會製造假跳空，只能整檔重抓
            print(f'      -> 整檔重抓（分割，禁止部分覆寫）')
            ta.load_and_resample(name, force_update=True)
            n_refetch += 1
        else:
            o, d = apply_unsettled(path, cache_df, real[name], bad)
            n_over += o
            n_drop += d

    print()
    print(f'unsettled : {len(found["unsettled"])} 檔 / '
          f'{sum(n for _t, n, _w in found["unsettled"])} 列')
    print(f'split     : {len(found["split"])} 檔 / '
          f'{sum(n for _t, n, _w in found["split"])} 列（需整檔重抓）')
    if unverified:
        # 不靜默：抓不到就是無法驗證，可能是改代號 / 下市（例：CTRA 2026-05 下市）
        print(f'[WARN] 無法驗證 {len(unverified)} 檔（yfinance 無資料，可能已下市 / 改代號）：'
              f'{", ".join(unverified[:12])}{" ..." if len(unverified) > 12 else ""}')
    if args.apply:
        print(f'已覆寫 {n_over} 列 / 已移除 {n_drop} 列 / 整檔重抓 {n_refetch} 檔')
        print(f'備份：{BACKUP_DIR}')

    hits = [k for k in ('unsettled', 'split') if found[k] and k in fail_on]
    if hits:
        print(f'\n[FAIL] 檢測 {",".join(hits)} 有發現')
        return 1
    print('\n[OK] 未觸發指定的失敗條件')
    return 0


if __name__ == '__main__':
    sys.exit(main())
