"""tdcc_ic_validation.py -- TDCC 集保股權分散因子 IC 驗證（BL-2 的未完成部分）

## 為什麼現在跑

TDCC 1-5 於 2026-04-19 上線，當時說「IC 驗證等累積 13 週」。2026-08-03 實查發現
**時間鎖早就解了**（`data_cache/tdcc/1-5/` 已有 60 個週快照，2025-05-23~2026-07-31 —— **但其中 45 週只有 top-500，見下方■**），
但驗證從沒跑過，而這份資料已經有 4 個活的消費端 —— 其中 `ai_report.py:328` 的
`format_shareholding_for_prompt()` 把持股分布**餵進 Opus 的推理**。
未驗證的因子正在參與投資決策論述，這是 Robustness First 要處理的事。

## 先驗（寫在前面，避免事後合理化）

本專案的籌碼因子歷史一路是 D：`chip_ic` sync 因子全市場負 IC、dual-inst 事件型輸
0050、System 2/3 的 chip features 全反向 fail，`project_ic_research` 結論是
「籌碼不加分，維持純技術」。**所以本驗證的期望值不是找到 alpha，是結案** ——
若為 D，那就是把它從 AI 報告 prompt 移除的依據。

## 方法（照 project_validation_bias_warning 的強制項）

- **截面 Spearman IC + IR**，block bootstrap 95% CI（保留自相關），複用
  `tools/chip_ic_analysis.py` 的 `compute_daily_ic` / `summarize_ic` / `block_bootstrap_ci`。
- **decile spread Q10-Q1 符號必須與 IC 同號**（2026-04-27 SOP）—— 不同號即「IC 假象」，
  不論 t 值多顯著都降 D。起因是 ROIC factor IC=+0.042/t=+4.23 但 decile spread −1.73%，
  報酬呈倒 U 形被 Spearman 誤判成正相關。
- **decile 單調性**（Q1..Q10 報酬的 Spearman）**≥ +0.5** 才算單調有效。
- **leave-one-quarter-out**：看 edge 是否由單一季度主導。
- **多重比較**：因子數 × horizon 數即檢定次數，報告裡明列，不只看單一 t 值。

## 🚨 樣本的致命問題：TDCC 宇宙在期間內從 476 檔長到 4,019 檔

2026-08-03 實查各週快照檔數：

| 期間 | 檔數 | 來源 |
|---|---|---|
| 2025-05-23 ~ 2026-04-10（**45 週**）| **475~496** | `31e40cd` 的 portal scraper **top-500 回補** |
| 2026-04-17 ~ 2026-07-31（**15 週**）| **3,956~4,019** | 全市場排程（週六 08:00）|

**所以「已累積 60 週」不等於「60 個可比較的截面」。** 把兩段混在一起算截面 IC，測到的
是「成分從大型股 500 檔變成全市場含大量小型股」這個構造性變化，不是持股結構的資訊。
第一次全量跑就踩到：`retail_pct` h=60 IC=+0.13 / p<0.0001 看起來很漂亮，但那是假的。

**本工具因此預設 `--constant-universe`**：只用「每一期都在」的 **470 檔**（top-500 的
交集）。犧牲是結論只涵蓋大型股、且大型股的持股結構離散度較小；換到的是截面成分固定、
IC 可解讀。要看全市場只有 15 週，太短，用 `--full-universe` 自負後果。

## ⚠️ PIT 對齊（最容易做錯的地方）

`data_date` 是**週五**的持股狀態，`download_ts` 顯示**週六 08:00** 才公布。所以：
- 訊號最早只能在**下一個交易日（通常週一）**使用。
- **forward return 必須從那個交易日的收盤起算**，不能從 data_date（週五）收盤起算 ——
  週五收盤發生在公布之前，那是 look-ahead。
本工具把每個 snapshot 對齊到 panel 裡「data_date 之後的第一個交易日」再算 forward return。

## 因子（刻意只取 10 個，控制多重比較）

級距定義（`tdcc_reader.LEVEL_LABELS`）：retail = level 1-5（≤20 張）、
large = 11-15（>200 張）、whale = 15（>1000 張）；level 16 是差異數調整、17 是合計，均排除。

- 水準：`whale_pct` / `large_pct` / `retail_pct`
- 變動（1 週、4 週）：上述三者的 `_chg_1w` / `_chg_4w`
- 集中度：`holders_chg_4w`（總持有人數變動；人數減少＝集中）

用法：
    python tools/tdcc_ic_validation.py                 # 全量
    python tools/tdcc_ic_validation.py --sample-stocks 300   # 快速測試
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from tools.chip_ic_analysis import (  # noqa: E402  複用既有統計，不重寫
    block_bootstrap_ci, compute_daily_ic, summarize_ic,
)

TDCC_DIR = REPO / "data_cache" / "tdcc" / "1-5"
PANEL = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"
OUT_CSV = REPO / "reports" / "tdcc_ic_matrix.csv"
OUT_MD = REPO / "reports" / "tdcc_ic_validation.md"

# TDCC 是週資料，日級 horizon 沒意義。結構性持股變化應該慢，20d/60d 最有可能。
HORIZONS = [5, 20, 60]
RETAIL_LEVELS = range(1, 6)      # 1-5：≤20 張
LARGE_LEVELS = range(11, 16)     # 11-15：>200 張
WHALE_LEVELS = [15]              # >1000 張
EXCLUDE_LEVELS = [16, 17]        # 差異數調整 / 合計
N_DECILES = 10
# decile 單調性門檻（2026-04-27 SOP）
MONO_MIN = 0.5
# 每個截面至少要這麼多檔才算
MIN_CROSS = 30
# 非重疊子樣本的步長：TDCC 是週資料，h 交易日 ≈ h/5 週。
# h=20 -> 每 4 週取一、h=60 -> 每 12 週取一。
OVERLAP_STEP = {5: 1, 20: 4, 60: 12}
# 非重疊子樣本至少要這麼多個觀測才能談顯著性
MIN_INDEP_N = 12

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("tdcc_ic")


# --------------------------------------------------------------------------- #
#  載入
# --------------------------------------------------------------------------- #

def load_tdcc() -> pd.DataFrame:
    """把 60 個週快照壓成 per (data_date, stock_id) 的聚合欄。"""
    files = sorted(TDCC_DIR.glob("*.parquet"))
    if not files:
        raise SystemExit(f"找不到 TDCC 快照：{TDCC_DIR}")
    log.info("讀 %d 個 TDCC 週快照", len(files))
    frames = []
    for fp in files:
        d = pd.read_parquet(fp, columns=["data_date", "stock_id", "level",
                                         "people_count", "shares", "pct"])
        d = d[~d["level"].isin(EXCLUDE_LEVELS)]
        d["stock_id"] = d["stock_id"].astype(str)
        g = d.groupby("stock_id")
        agg = pd.DataFrame({
            "data_date": str(d["data_date"].iloc[0]),
            "retail_pct": g.apply(lambda x: x.loc[x.level.isin(RETAIL_LEVELS), "pct"].sum(),
                                  include_groups=False),
            "large_pct": g.apply(lambda x: x.loc[x.level.isin(LARGE_LEVELS), "pct"].sum(),
                                 include_groups=False),
            "whale_pct": g.apply(lambda x: x.loc[x.level.isin(WHALE_LEVELS), "pct"].sum(),
                                 include_groups=False),
            "holders": g["people_count"].sum(),
        })
        frames.append(agg.reset_index())
    out = pd.concat(frames, ignore_index=True)
    out["data_date"] = pd.to_datetime(out["data_date"], format="%Y%m%d")
    # 全 0 列（TDCC 對無資料個股會回全 0）沒有資訊，剔除
    out = out[(out[["retail_pct", "large_pct"]].sum(axis=1) > 0) & (out["holders"] > 0)]
    log.info("聚合後 %d 列 / %d 檔 / %d 個週期",
             len(out), out.stock_id.nunique(), out.data_date.nunique())
    return out.sort_values(["stock_id", "data_date"]).reset_index(drop=True)


def build_factors(t: pd.DataFrame) -> pd.DataFrame:
    """加變動類因子。TDCC 是週頻，所以 1w = shift(1)、4w = shift(4)。"""
    g = t.groupby("stock_id")
    for col in ("retail_pct", "large_pct", "whale_pct"):
        t[f"{col}_chg_1w"] = g[col].diff(1)
        t[f"{col}_chg_4w"] = g[col].diff(4)
    t["holders_chg_4w"] = g["holders"].pct_change(4)
    return t


FACTORS = [
    "whale_pct", "large_pct", "retail_pct",
    "whale_pct_chg_1w", "large_pct_chg_1w", "retail_pct_chg_1w",
    "whale_pct_chg_4w", "large_pct_chg_4w", "retail_pct_chg_4w",
    "holders_chg_4w",
]


def align_pit(t: pd.DataFrame, sample_stocks=None) -> pd.DataFrame:
    """把 snapshot 對齊到「data_date 之後的第一個交易日」，再算 forward return。

    這是本工具最關鍵的一步 —— data_date 是週五狀態、週六才公布，從週五收盤起算
    forward return 就是 look-ahead。
    """
    px = pd.read_parquet(PANEL, columns=["date", "stock_id", "Close"])
    px["date"] = pd.to_datetime(px["date"])
    px["stock_id"] = px["stock_id"].astype(str)
    if sample_stocks:
        keep = sorted(px.stock_id.unique())[:sample_stocks]
        px = px[px.stock_id.isin(keep)]
        t = t[t.stock_id.isin(keep)]

    px = px.sort_values(["stock_id", "date"])
    for h in HORIZONS:
        px[f"fwd_{h}d"] = px.groupby("stock_id")["Close"].pct_change(h).shift(-h)

    # merge_asof 每檔各自做：找 data_date 之後（strictly after）的第一個交易日
    t = t.sort_values("data_date")
    out = []
    px_by = {sid: g.sort_values("date") for sid, g in px.groupby("stock_id")}
    for sid, g in t.groupby("stock_id"):
        p = px_by.get(sid)
        if p is None:
            continue
        # +1 天讓 merge_asof(direction='forward') 排除 data_date 當天本身
        left = g.assign(_key=g["data_date"] + pd.Timedelta(days=1)).sort_values("_key")
        m = pd.merge_asof(left, p[["date", "Close"] + [f"fwd_{h}d" for h in HORIZONS]],
                          left_on="_key", right_on="date", direction="forward",
                          tolerance=pd.Timedelta(days=7))
        out.append(m)
    res = pd.concat(out, ignore_index=True)
    res = res.dropna(subset=["date"])
    log.info("PIT 對齊後 %d 列；訊號日中位落後 data_date %.1f 天",
             len(res), (res["date"] - res["data_date"]).dt.days.median())
    return res


# --------------------------------------------------------------------------- #
#  decile 檢查（既有 harness 沒有，2026-04-27 SOP 要求）
# --------------------------------------------------------------------------- #

def decile_stats(df, factor, ret_col):
    """回 (spread Q10-Q1, 單調性 Spearman, 各 decile 平均報酬)。"""
    x = df.dropna(subset=[factor, ret_col])
    rows = []
    for _, g in x.groupby("date"):
        if len(g) < N_DECILES * 3:
            continue
        try:
            q = pd.qcut(g[factor].rank(method="first"), N_DECILES, labels=False)
        except ValueError:
            continue
        rows.append(g.assign(_d=q).groupby("_d")[ret_col].mean())
    if not rows:
        return np.nan, np.nan, None
    per_decile = pd.concat(rows, axis=1).mean(axis=1)
    if len(per_decile) < N_DECILES:
        return np.nan, np.nan, None
    spread = float(per_decile.iloc[-1] - per_decile.iloc[0])
    mono, _ = stats.spearmanr(range(len(per_decile)), per_decile.values)
    return spread, float(mono), per_decile


def indep_stats(ic_series, horizon):
    """非重疊子樣本的 mean IC / t / p / n。

    這是既有 harness 沒做、但對週頸因子**決定性**的檢查。
    TDCC 是週資料，h=60 交易日 ≈ 12 週，相鄰兩個週觀測的 forward-return
    視窗重疊 11/12。用 n=47 算 t 檢定會把顯著性吹到 p<0.0001，
    而真正的獨立觀測只有 4 個。實測 IC 序列 lag-1 自相關 +0.71~+0.84。
    """
    step = OVERLAP_STEP.get(horizon, 1)
    sub = ic_series.iloc[::step].dropna()
    n = len(sub)
    if n < 3 or sub.std(ddof=1) == 0:
        return dict(n=n, mean=np.nan, t=np.nan, p=np.nan)
    tt = sub.mean() * np.sqrt(n) / sub.std(ddof=1)
    return dict(n=n, mean=float(sub.mean()), t=float(tt),
                p=float(2 * (1 - stats.t.cdf(abs(tt), df=n - 1))))


def leave_one_quarter_out(ic_series):
    """逐季剔除後的 mean IC 範圍 —— 看 edge 是否被單一季度主導。"""
    if len(ic_series) < 8:
        return np.nan, np.nan
    q = pd.Series(ic_series.index).dt.to_period("Q").values
    means = []
    for qq in pd.unique(q):
        keep = ic_series.values[q != qq]
        if len(keep) >= 4:
            means.append(keep.mean())
    return (float(np.min(means)), float(np.max(means))) if means else (np.nan, np.nan)


def verdict(s, spread, mono, ind=None, bonf=None):
    """依 SOP 給等級。IC 與 decile spread 不同號 -> D 假象，不看 t 值。

    2026-08-03 加兩道關（既有 harness 沒有，但不加就會給假綿燈）：
    - **非重疊子樣本**：週頸資料 + h=60 的全樣本 p 值是重疊吹出來的。
    - **多重比較**：30 次檢定的 Bonferroni 門檻是 0.05/30 = 0.0017。
    """
    if s["n"] < 20 or np.isnan(s["mean"]):
        return "D (樣本不足)"
    if np.isnan(spread):
        return "D (decile 算不出)"
    if s["mean"] * spread < 0:
        return "D 假象 (IC 與 decile spread 反向)"
    if s["p"] > 0.05:
        return "D (IC 不顯著 p>0.05)"
    if abs(mono) < MONO_MIN:
        return "D (decile 非單調)"
    if s["ci_low"] * s["ci_high"] < 0:
        return "D (bootstrap CI 跨 0)"
    if ind is not None:
        if ind["n"] < MIN_INDEP_N:
            return "D (非重疊觀測只有 %d 個)" % ind["n"]
        if np.isnan(ind["p"]) or ind["p"] > 0.05:
            return "D (非重疊子樣本 p=%.3f 不顯著)" % ind["p"]
        if bonf is not None and ind["p"] > bonf:
            return "D (過不了多重比較 p=%.3f > %.4f)" % (ind["p"], bonf)
    return "C+ (通過全部關卡，需 portfolio 驗證才可上線)"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample-stocks", type=int, default=0, help="只取前 N 檔（測試用）")
    ap.add_argument("--full-universe", action="store_true",
                    help="不限固定宇寙。⚠️ 宇寙從 476 長到 4019 檔，"
                         "IC 測到的會是成分變化而非訊號（見檔頭）")
    args = ap.parse_args()

    t = load_tdcc()
    if not args.full_universe:
        per = t.groupby("data_date")["stock_id"].apply(set)
        const = set.intersection(*per.tolist())
        log.info("固定宇寙：%d 檔（每一期都在），"
                 "原宇寙 %d 檔", len(const), t.stock_id.nunique())
        t = t[t.stock_id.isin(const)]
    else:
        log.warning("全宇寙模式：宇寙大小在期間內由 %d 變到 %d，"
                    "IC 不可解讀",
                    t.groupby("data_date")["stock_id"].nunique().min(),
                    t.groupby("data_date")["stock_id"].nunique().max())
    t = build_factors(t)
    df = align_pit(t, args.sample_stocks or None)

    n_tests = len(FACTORS) * len(HORIZONS)
    log.info("檢定次數 %d（%d 因子 × %d horizon）—— 多重比較下單一 p<0.05 不足採信",
             n_tests, len(FACTORS), len(HORIZONS))

    rows = []
    for f in FACTORS:
        for h in HORIZONS:
            rc = f"fwd_{h}d"
            ic = compute_daily_ic(df.rename(columns={"date": "date"}), f, rc)
            s = summarize_ic(ic)
            spread, mono, per_dec = decile_stats(df, f, rc)
            lo, hi = leave_one_quarter_out(ic)
            ind = indep_stats(ic, h)
            bonf = 0.05 / n_tests
            rows.append(dict(
                factor=f, horizon=h, n_periods=s["n"], ic_mean=s["mean"], ic_ir=s["ir"],
                ic_p=s["p"], ci_low=s["ci_low"], ci_high=s["ci_high"],
                win_rate=s["win_rate"], decile_spread=spread, decile_mono=mono,
                loqo_min=lo, loqo_max=hi,
                indep_n=ind["n"], indep_ic=ind["mean"], indep_p=ind["p"],
                bonferroni=bonf,
                verdict=verdict(s, spread, mono, ind, bonf),
            ))
            log.info("%-22s h=%-3d IC=%+.4f p=%.3f spread=%+.4f mono=%+.2f -> %s",
                     f, h, s["mean"], s["p"], spread if spread is not None else np.nan,
                     mono if mono is not None else np.nan, rows[-1]["verdict"])

    res = pd.DataFrame(rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    res.to_csv(OUT_CSV, index=False, encoding="utf-8")
    log.info("已寫 %s", OUT_CSV)

    print()
    print("=" * 100)
    print("TDCC 集保股權分散因子 IC 驗證")
    print("=" * 100)
    print("樣本：%d 個週期 / %d 檔（%s）；檢定 %d 次"
          % (df.data_date.nunique(), df.stock_id.nunique(),
             "全宇寙，成分有變不可解讀" if args.full_universe
             else "固定宇寙", n_tests))
    print("%-22s %4s %8s %7s %7s %9s %7s  %s"
          % ("factor", "h", "IC", "IR", "p", "Q10-Q1", "mono", "verdict"))
    for r in rows:
        print("%-22s %4d %+8.4f %+7.2f %7.3f %+9.4f %+7.2f  %s"
              % (r["factor"], r["horizon"], r["ic_mean"], r["ic_ir"] or np.nan,
                 r["ic_p"], r["decile_spread"], r["decile_mono"], r["verdict"]))

    passed = [r for r in rows if r["verdict"].startswith("C")]
    print("\n" + "-" * 100)
    print("通過基本關卡：%d / %d" % (len(passed), len(rows)))
    for r in passed:
        print("   %s h=%d  IC=%+.4f p=%.4f  LOQO 區間 [%+.4f, %+.4f]"
              % (r["factor"], r["horizon"], r["ic_mean"], r["ic_p"],
                 r["loqo_min"], r["loqo_max"]))
    if not passed:
        print("   無 —— 與本專案籌碼因子的歷史一致（見檔頭「先驗」）")
    print("\n⚠️ 樣本限制：60 週 = 約 14 個月，且 2025-05~2026-07 幾乎全是 AI 多頭，"
          "\n   沒有空頭段。即使有因子過關也只能標「多頭條件下」，不足以升 A/B。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
