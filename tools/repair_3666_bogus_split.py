"""repair_3666_bogus_split.py -- 修 3666(光耀) 的假 1:10000 反向分割

## 症狀

`data_cache/3666_price.csv` 在 **2015-01-05 ~ 2022-05-04** 共 1,784 列的價格是
10~88 萬元（台股史上最高價股約 6,000 元），2022-05-05 單日從 209,000 掉到 21.65，
之後回到正常的 16~30 元。中位數 168,073 —— **超過一半的序列是壞的**。

## 根因

Yahoo 的資料裡有一筆不存在的 1:10000 反向分割，於是 yfinance 的還原把該段
**價格 ×10000、成交量 ÷10000**。實打官方 TPEX 對照（3666 是上櫃股）：

| 日期 | CSV Close | 官方原始 | 倍率 | CSV Vol | 官方 Vol |
|---|---|---|---|---|---|
| 2015-01-05 | 561552.19 | 57.30 | 9800.21 | 93 | 916,751 |
| 2019-09-06 | 190124.12 | 19.40 | 9800.21 | 100 | 986,326 |
| 2021-04-01 | 653000.00 | 65.30 | 10000.00 | 88 | 893,779 |
| 2022-05-05 | 21.65 | 21.65 | 1.00 | 111,000 | 113,041 |

9800.21 與 10000 的差是**合法的配息還原**（10000 / 9800.21 = 1.0204），必須保留。

## 修法（兩邊不對稱，別想用同一招）

- **價格：直接除以 10000，精確還原，不需要 API。**
  驗證：653000 / 10000 = 65.30 = 官方原始價；
  561552.19 / 10000 = 56.155 = 57.30 × 0.980021（合法配息係數原封保留）。
- **成交量：必須重抓，除不回來。** 916,751 被除以 9800 再取整成 93 —— 精度已經丟了，
  乘回 10000 得 930,000（差 1.4%），低量日更慘（5 → 50,000 vs 實際 57,000，差 12%）。
  改抓 TPEX 逐檔月歷史 `www/zh-tw/afterTrading/tradingStock`（一次一個月，
  約 89 次請求就涵蓋整段），欄位「成交仟股」×1000。

⚠️ 為什麼不整段改抓官方原始價就好：CSV 存的是**還原價**，直接寫原始價會抹掉
2015-2019 那段合法的 0.98 配息還原。除以 10000 才是既去掉假分割、又保留真還原。

用法：
    python tools/repair_3666_bogus_split.py --dry-run     # 預設，只報告
    python tools/repair_3666_bogus_split.py --apply --rebuild-panel
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
import urllib3

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

STOCK_ID = "3666"
CSV_PATH = REPO / "data_cache" / f"{STOCK_ID}_price.csv"
# 假分割倍率。實測 9800.21 / 10000.00 兩段，差值是合法配息還原；除以 10000 才對。
BOGUS_SPLIT = 10000.0
# 毀損區間（含）。2022-05-05 起 CSV 已正常。
CORRUPT_FROM = pd.Timestamp("2015-01-05")
CORRUPT_TO = pd.Timestamp("2022-05-04")
# 低於這個價位就不可能是「被 ×10000 過」的列，用來確認區間抓對了
SANITY_MIN_CORRUPT_CLOSE = 1000.0

TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
THROTTLE_SEC = 1.5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("repair_3666")


def _roc_to_date(s: str):
    """'110/04/01' -> Timestamp。"""
    try:
        y, m, d = str(s).strip().split("/")
        return pd.Timestamp(year=int(y) + 1911, month=int(m), day=int(d))
    except (ValueError, AttributeError):
        return None


def fetch_official_volume(months) -> dict:
    """TPEX 逐檔月歷史 -> {date: volume_shares}。「成交仟股」×1000。"""
    s = requests.Session()
    s.verify = False
    s.headers.update({"User-Agent": UA, "Accept": "application/json",
                      "Referer": "https://www.tpex.org.tw/"})
    out = {}
    for i, m in enumerate(months, 1):
        time.sleep(THROTTLE_SEC)
        try:
            r = s.get(TPEX_URL, params={"code": STOCK_ID, "date": m,
                                        "id": "", "response": "json"}, timeout=25)
            r.raise_for_status()
            j = r.json()
        except Exception as exc:
            log.warning("[%d/%d] %s 抓取失敗：%s", i, len(months), m, repr(exc)[:90])
            continue
        tb = (j.get("tables") or [{}])[0]
        fields = [str(f).strip().replace(" ", "") for f in (tb.get("fields") or [])]
        try:
            di, vi = fields.index("日期"), fields.index("成交仟股")
        except ValueError:
            log.warning("[%d/%d] %s 欄位非預期：%s", i, len(months), m, fields)
            continue
        n = 0
        for row in (tb.get("data") or []):
            d = _roc_to_date(row[di])
            if d is None:
                continue
            try:
                out[d] = float(str(row[vi]).replace(",", "")) * 1000.0
            except (ValueError, IndexError):
                continue
            n += 1
        if i % 12 == 0 or i == len(months):
            log.info("[%d/%d] %s 累計取得 %d 個交易日", i, len(months), m, len(out))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True)
    g.add_argument("--apply", action="store_true")
    ap.add_argument("--rebuild-panel", action="store_true")
    args = ap.parse_args()
    apply_changes = bool(args.apply)

    if not CSV_PATH.exists():
        log.error("找不到 %s", CSV_PATH)
        return 2
    df = pd.read_csv(CSV_PATH, index_col=0)
    df.index = pd.to_datetime(df.index)
    df = df[df.index.notna()].sort_index()

    mask = (df.index >= CORRUPT_FROM) & (df.index <= CORRUPT_TO)
    corrupt = df[mask]
    if corrupt.empty:
        log.info("毀損區間內沒有列，無事可做（可能已修過）")
        return 0

    # 區間抓對了嗎：毀損段的價格都該是被放大過的
    below = (corrupt["Close"] < SANITY_MIN_CORRUPT_CLOSE).sum()
    after = df[df.index > CORRUPT_TO]
    if below:
        log.error("毀損區間內有 %d 列 Close < %.0f，區間可能抓錯，中止",
                  below, SANITY_MIN_CORRUPT_CLOSE)
        return 3
    if not after.empty and after["Close"].max() >= SANITY_MIN_CORRUPT_CLOSE:
        log.error("區間之後仍有 Close >= %.0f 的列，中止", SANITY_MIN_CORRUPT_CLOSE)
        return 3

    months = sorted({f"{d.year}/{d.month:02d}/01" for d in corrupt.index})
    log.info("毀損 %d 列（%s ~ %s），需抓 %d 個月的官方成交量",
             len(corrupt), corrupt.index.min().date(), corrupt.index.max().date(),
             len(months))

    vol = fetch_official_volume(months)
    hit = sum(1 for d in corrupt.index if d in vol)
    log.info("官方成交量涵蓋 %d / %d 個毀損日", hit, len(corrupt))

    new = df.copy()
    for col in ("Open", "High", "Low", "Close", "Adj Close"):
        if col in new.columns:
            new.loc[mask, col] = pd.to_numeric(new.loc[mask, col],
                                               errors="coerce") / BOGUS_SPLIT
    missing_vol = []
    for d in corrupt.index:
        if d in vol:
            new.loc[d, "Volume"] = vol[d]
        else:
            missing_vol.append(d)
            new.loc[d, "Volume"] = pd.to_numeric(df.loc[d, "Volume"]) * BOGUS_SPLIT

    print()
    print("=" * 74)
    print("3666 假分割修復（%s）" % ("已寫入" if apply_changes else "DRY-RUN，未寫入"))
    print("=" * 74)
    print("價格：%d 列 ÷ %.0f" % (len(corrupt), BOGUS_SPLIT))
    frozen = [d for d in missing_vol if float(pd.to_numeric(df.loc[d, "Volume"])) == 0]
    print("成交量：%d 列改用官方值；%d 列官方無資料，退回 ×%.0f"
          % (hit, len(missing_vol), BOGUS_SPLIT))
    if frozen:
        print("  其中 %d 列是 V=0 的停市填充列（OHLC 四價相同，颱風停市日 yfinance 用"
              "參考價填的）—— 官方沒有資料是正確的，不是抓取失敗；0 × %.0f 仍是 0，"
              "不會產生垃圾量。詳 Claude memory project_v0_frozen_rows。"
              % (len(frozen), BOGUS_SPLIT))
    print("\n抽樣（日期 / 舊 Close -> 新 Close / 舊 Vol -> 新 Vol）：")
    for d in list(corrupt.index[:3]) + list(corrupt.index[-3:]):
        print("  %s  %12.2f -> %8.2f   %10.0f -> %10.0f"
              % (d.date(), df.loc[d, "Close"], new.loc[d, "Close"],
                 df.loc[d, "Volume"], new.loc[d, "Volume"]))
    print("\n修復後全序列 Close：min=%.2f  中位=%.2f  max=%.2f"
          % (new["Close"].min(), new["Close"].median(), new["Close"].max()))
    print("接縫檢查（2022-05-04 -> 05-05）：%.2f -> %.2f"
          % (new.loc[CORRUPT_TO, "Close"],
             new[new.index > CORRUPT_TO]["Close"].iloc[0]))
    if missing_vol:
        print("\n抓不到官方量的日期（%d 個）：%s%s"
              % (len(missing_vol), [str(d.date()) for d in missing_vol[:10]],
                 " …" if len(missing_vol) > 10 else ""))

    if apply_changes:
        tmp = CSV_PATH.with_suffix(".csv.tmp")
        new.index.name = None
        new.to_csv(tmp, date_format="%Y-%m-%d")
        os.replace(tmp, CSV_PATH)
        log.info("已寫回 %s", CSV_PATH.name)
        if args.rebuild_panel:
            from tools.refresh_backtest_panels import aggregate_csv_to_parquet
            log.info("重建 ohlcv_tw.parquet …")
            aggregate_csv_to_parquet()
            log.info("重建完成")
    else:
        print("\n這是 dry-run。確認無誤後加 --apply（可再加 --rebuild-panel）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
