"""scan_panel_price_outliers.py -- ohlcv_tw 價格離群掃描（資料毀損偵測）

## 為什麼需要

2026-08-02 回填 panel 時**意外**撞到 `3666`（光耀）整段歷史被 ×10000
（2015-01-05~2022-05-04 共 1,784 列在 10~88 萬元，台股史上最高價股才約 6,000 元）。
那是 Yahoo 資料裡一筆不存在的 1:10000 反向分割，存在至少三年沒人發現 ——
因為**每一欄都是正數的合理數字**，既有的健康度檢查（缺值率、覆蓋率、日期新鮮度）
全都通過。要抓這種毀損，得問「這個數字在台股是否物理上可能」。

本工具是那次的產物：不打任何 API，純 panel 內部一致性，可以排進夜間鏈。

## 三道檢測

**A. 絕對量級** —— 台股史上最高價股約 6,000 元（大立光）。還原價可能被減資推高，
   所以門檻放在 20,000（`ABS_MAX_CLOSE`），寧可漏也不要一堆假警報。
   2026-08-02 修完 3666 後此項為 0。

**B. 相對現價的量級落差** —— 某檔歷史最高價 / 最近價 > `MAX_LEVEL_RATIO`（1000）。
   抓「整段序列量級可疑」這型（3666 是 35,149 倍）。
   注意 **91 倍是合法的**：4943 從 808 跌到 8.84 是真的，台股跌 99% 的公司不少。
   門檻設 1000 就是為了不誤殺這類。

**C. 尖刺後立刻反轉** —— 單日 |報酬| > `SPIKE_RET` 且隔日反向抵銷，是單列錯值的特徵。
   ⚠️ 這項**會有真陽性以外的東西**：2026-08-02 全歷史掃出 4 列（有成精密 2017-02-09
   −73% 再 +165%、榮科、必應、宇瞻），逐筆看成交量都正常，是**真實的暴跌暴漲**不是錯值。
   所以本項只回報供人工複核，不當成毀損斷言。

用法：
    python tools/scan_panel_price_outliers.py              # 報告
    python tools/scan_panel_price_outliers.py --fail-on A,B   # A/B 有發現就 exit 1
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
PANEL = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"

# 台股史上最高價股約 6,000 元；還原價可能更高，門檻放寬到 20,000 避免假警報
ABS_MAX_CLOSE = 20_000.0
# 歷史最高 / 最近價。合法上限實測約 91 倍（真實崩跌），設 1000 留足餘裕
MAX_LEVEL_RATIO = 1000.0
# 尖刺門檻（僅供人工複核，不當毀損斷言）
SPIKE_RET = 1.0
SPIKE_REVERT = 0.4


def scan(df: pd.DataFrame) -> dict:
    df = df[df["Close"] > 0].sort_values(["stock_id", "date"])

    a = df[df["Close"] > ABS_MAX_CLOSE]

    last = df.groupby("stock_id")["Close"].last()
    mx = df.groupby("stock_id")["Close"].max()
    ratio = (mx / last.replace(0, np.nan)).dropna()
    b = ratio[ratio > MAX_LEVEL_RATIO].sort_values(ascending=False)

    g = df.groupby("stock_id")["Close"]
    prev, nxt = g.shift(1), g.shift(-1)
    ok = df.assign(prev=prev, nxt=nxt).dropna(subset=["prev", "nxt"])
    r1 = ok["Close"] / ok["prev"] - 1
    r2 = ok["nxt"] / ok["Close"] - 1
    c = ok[(r1.abs() > SPIKE_RET) & (r2.abs() > SPIKE_REVERT) & (r1 * r2 < 0)]

    return {"A": a, "B": b, "C": c, "ratio": ratio}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fail-on", default="",
                    help="逗號分隔的檢測代號（A/B/C），有發現就 exit 1。"
                         "建議 'A,B' —— C 會有真實暴漲暴跌的偽陽性")
    args = ap.parse_args()

    if not PANEL.exists():
        print(f"[FAIL] 找不到 {PANEL}")
        return 2
    df = pd.read_parquet(PANEL, columns=["date", "stock_id", "Close", "Volume"])
    df["date"] = pd.to_datetime(df["date"])
    df["stock_id"] = df["stock_id"].astype(str)
    res = scan(df)

    print("panel %d 列 / %d 檔 / %d 日" % (len(df), df.stock_id.nunique(), df.date.nunique()))
    a, b, c = res["A"], res["B"], res["C"]

    print(f"\n[A] Close > {ABS_MAX_CLOSE:,.0f}：{len(a)} 列 / {a.stock_id.nunique()} 檔")
    for sid, gg in a.groupby("stock_id"):
        print("    %-6s %5d 列  max=%12.1f  %s ~ %s"
              % (sid, len(gg), gg.Close.max(), gg.date.min().date(), gg.date.max().date()))

    print(f"\n[B] 歷史最高 / 最近價 > {MAX_LEVEL_RATIO:,.0f}：{len(b)} 檔")
    for sid, r in b.items():
        print("    %-6s 倍數=%.1f" % (sid, r))
    top = res["ratio"].sort_values(ascending=False).head(3)
    print("    （目前最大三檔：%s —— 數十倍是真實崩跌，不是毀損）"
          % ", ".join("%s=%.0fx" % (s, v) for s, v in top.items()))

    print(f"\n[C] 尖刺後反轉（人工複核，非毀損斷言）：{len(c)} 列")
    for r in c.itertuples():
        print("    %-6s %s  %.2f -> %.2f -> %.2f  vol=%.0f"
              % (r.stock_id, r.date.date(), r.prev, r.Close, r.nxt, r.Volume))

    fail = {x.strip().upper() for x in args.fail_on.split(",") if x.strip()}
    hits = [k for k in ("A", "B", "C") if k in fail and len(res[k])]
    if hits:
        print(f"\n[FAIL] 檢測 {','.join(hits)} 有發現")
        return 1
    print("\n[OK] 未觸發指定的失敗條件")
    return 0


if __name__ == "__main__":
    sys.exit(main())
