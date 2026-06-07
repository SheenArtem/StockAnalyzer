"""
RVOL / ATR% 因子正式驗證 (2026-06-07)

驗證兩條技術延伸是否有可上線 alpha：
  1. sig_rvol_log = log(clip(Volume/Volume.rolling(20).mean(), 0.01))
  2. sig_atr_pct  = ATR(14)/Close  (低波動異象，取低)
  + combo_rvol_lowatr = (rvol_log_rn - atr_pct_rn)/2

**訊號定義完全沿用 tools/indicator_ic_analysis.py / indicator_combo_analysis.py**
(import _compute_one_ticker / rank_normalize_signals / build_combos, 不重新發明)。

與既有腳本差異 (為何需要本驅動)：
  - 當前 data_cache/backtest/ohlcv_tw.parquet 已被 refresh_universe_prices 改 schema
    (stock_id + raw OHLCV, **無 AdjClose / 無 yf_ticker**)；既有 load_ohlcv 直接 crash。
    本檔提供 schema-compatible loader (alias stock_id->yf_ticker, Close->AdjClose)。
  - 加入既有腳本沒有的驗證項目：流動性過濾敏感度、年度 walk-forward IC/spread、
    成本後淨 spread、h=10/20 decile+topN (既有只有 h=60)。

輸出 (reports/, rvol_atr_* 前綴，不覆寫既有檔)：
  - rvol_atr_decile_returns.csv        (各 liquidity 過濾 × horizon × decile)
  - rvol_atr_topn_portfolio.csv        (top/bot N portfolio)
  - rvol_atr_walkforward_annual.csv    (年度 20d IC + D10-D1 spread)
  - rvol_atr_atr_forensic.csv          (atr_pct D1 斷崖 forensic：D1 成分股流動性/報酬結構)
  - rvol_atr_net_spread.csv            (成本前後淨 spread)

用法:
    python tools/rvol_atr_validate.py --sample 300   # 迭代測試
    python tools/rvol_atr_validate.py                # 全量 (最終數字)
"""
import argparse
import logging
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore")

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from tools.indicator_ic_analysis import (
    _compute_one_ticker, SIGNAL_COLS, SIGNAL_LABELS,
)
from tools.indicator_combo_analysis import rank_normalize_signals, build_combos

PARQUET_PATH = _ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MIN_HISTORY = 200
MIN_CROSS_SECTION = 50          # 每日至少 N 檔才算 decile (同 quantile 腳本)
MIN_CROSS_SECTION_IC = 30       # IC 每日 cross-section 門檻 (同 ic 腳本)
N_DECILES = 10
TOP_N_VARIANTS = [10, 20, 50]
HORIZONS = [10, 20]             # IC 最強 horizon (既有只跑過 60)

# 焦點因子 (rank-normalized 個別訊號 + combo)
FOCUS_SCORES = {
    "sig_rvol_log_rn": "log(RVOL)",          # 正 IC 預期 → 做多高分 (top)
    "sig_atr_pct_rn": "ATR% (raw rank)",     # 負 IC 預期 → 做多低分 (bot)
    "combo_rvol_lowatr": "RVOL - ATR% (低波動放量)",
}

# 流動性過濾門檻 (20 日均成交額 NTD)。None = 全 universe。
# 紅旗驗證需求：至少兩檔位 + 排除最低 20% 成交額。
LIQUIDITY_TIERS = [
    ("all", None),                    # 無過濾 (含殭屍/處置股)
    ("liq_50m", 5e7),                 # >= 5000 萬 NTD (mandate 主門檻)
    ("liq_100m", 1e8),                # >= 1 億 NTD (更嚴)
    ("ex_bottom20pct", "pct20"),      # 排除當日成交額最低 20%
]

# 成本假設 (台股單邊摩擦)，h=20 月度再平衡 ≈ 年換手 12 次
# 單邊 = 手續費(折扣後 ~0.05-0.0855%) + 賣出證交稅 0.15%/2 攤 (買賣各算一次, 稅只賣出收)
# 採 round-trip ~0.25-0.35%；保守取兩檔
COST_ROUNDTRIP_TIERS = [0.0025, 0.0035]   # 0.25% / 0.35% per rebalance round-trip

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("rvol_atr")


# ============================================================
# Loader — schema-compatible with current ohlcv_tw (stock_id, no AdjClose)
# ============================================================
def load_panel(sample=None, since=None):
    logger.info(f"Loading {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    df["date"] = pd.to_datetime(df["date"])
    if since:
        df = df[df["date"] >= since].copy()

    # alias to schema expected by tools.indicator_ic_analysis._compute_one_ticker
    if "yf_ticker" not in df.columns:
        df = df.rename(columns={"stock_id": "yf_ticker"})
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # 無 AdjClose → 用 raw Close (披露於報告：股息缺口會壓低高動能、可能虛抬低波動)
    if "AdjClose" not in df.columns:
        df["AdjClose"] = df["Close"]

    # 資料清洗 (披露於報告)：current panel 有 12,589 rows Close<=0 (penny/停牌/還原失敗)
    # → pct_change 產生 inf 或 -100%/+62萬% 等 artifact 報酬。剔除非正價格列。
    n_bad = (df["Close"] <= 0).sum()
    if n_bad:
        logger.info(f"Dropping {n_bad:,} rows with Close<=0 (invalid price)")
        df = df[df["Close"] > 0].copy()

    counts = df.groupby("yf_ticker").size()
    keep = counts[counts >= MIN_HISTORY].index
    df = df[df["yf_ticker"].isin(keep)].copy()

    if sample:
        rng = np.random.default_rng(42)
        tickers = df["yf_ticker"].unique()
        chosen = rng.choice(tickers, min(sample, len(tickers)), replace=False)
        df = df[df["yf_ticker"].isin(chosen)].copy()

    df = df.sort_values(["yf_ticker", "date"]).reset_index(drop=True)
    logger.info(
        f"Loaded {len(df):,} rows, {df['yf_ticker'].nunique()} tickers, "
        f"{df['date'].min().date()} ~ {df['date'].max().date()}"
    )
    return df


def compute_indicators(df):
    logger.info("Computing indicators (reusing _compute_one_ticker)...")
    t0 = time.time()
    df = df.groupby("yf_ticker", group_keys=False).apply(_compute_one_ticker)
    logger.info(f"Indicators done in {time.time()-t0:.1f}s")
    return df


def add_fwd_returns(df):
    logger.info("Forward returns (raw Close, no AdjClose available)...")
    df = df.sort_values(["yf_ticker", "date"]).reset_index(drop=True)
    for h in HORIZONS:
        # fill_method=None → 不跨缺口 ffill (避免停牌缺口製造假報酬)
        r = df.groupby("yf_ticker")["AdjClose"].pct_change(h, fill_method=None).shift(-h)
        # 殘餘 inf (前一價=0 漏網) → NaN；正常台股 20d 報酬不會 >+500%/<-95%，視為 artifact 剔除
        r = r.replace([np.inf, -np.inf], np.nan)
        df[f"fwd_{h}d"] = r.where((r > -0.95) & (r < 5.0))
    return df


def add_liquidity(df):
    """20 日均成交額 (NTD) + 當日成交額 20th percentile 旗標。"""
    df["amount"] = df["Close"] * df["Volume"]
    df["avg_amount_20d"] = df.groupby("yf_ticker")["amount"].transform(
        lambda s: s.rolling(20).mean()
    )
    # 當日成交額 20th percentile (cross-section)
    df["amt_pct_rank"] = df.groupby("date")["avg_amount_20d"].transform(
        lambda s: s.rank(pct=True)
    )
    return df


def liquidity_mask(df, tier_val):
    """回傳布林 mask。tier_val: None=全部 / float=絕對門檻 / 'pct20'=排除最低 20%。"""
    if tier_val is None:
        return pd.Series(True, index=df.index)
    if tier_val == "pct20":
        return df["amt_pct_rank"] >= 0.20
    return df["avg_amount_20d"] >= tier_val


# ============================================================
# Decile returns
# ============================================================
def compute_decile_returns(df, score_col, return_col, mask):
    x = df[mask][[score_col, return_col, "date"]].dropna()

    def _bucket(g):
        if len(g) < MIN_CROSS_SECTION:
            return pd.Series([np.nan] * len(g), index=g.index)
        return pd.qcut(
            g[score_col].rank(method="first"), N_DECILES, labels=False, duplicates="drop"
        )

    x["decile"] = x.groupby("date", group_keys=False).apply(_bucket)
    x = x.dropna(subset=["decile"])
    x["decile"] = x["decile"].astype(int) + 1

    daily = x.groupby(["date", "decile"])[return_col].mean().reset_index()
    summary = daily.groupby("decile")[return_col].agg(
        mean_ret="mean", median_ret="median", std_ret="std",
        win_rate=lambda s: (s > 0).mean(), n_days="count",
    ).reset_index()
    return summary


# ============================================================
# Top-N portfolio
# ============================================================
def simulate_topn(df, score_col, return_col, n, mask, direction):
    x = df[mask][[score_col, return_col, "date"]].dropna()
    daily_rets = []
    for _, g in x.groupby("date"):
        if len(g) < n * 2:
            continue
        picks = g.nlargest(n, score_col) if direction == "top" else g.nsmallest(n, score_col)
        daily_rets.append(picks[return_col].mean())
    if not daily_rets:
        return None
    arr = np.array(daily_rets)
    m, s = arr.mean(), arr.std(ddof=1)
    return {
        "mean_ret": float(m), "std_ret": float(s),
        "sharpe_proxy": float((m / s) * np.sqrt(252)) if s > 0 else 0.0,
        "win_rate": float((arr > 0).mean()), "n_days": len(arr),
    }


# ============================================================
# Daily cross-sectional IC
# ============================================================
def daily_ic(df, score_col, return_col, mask):
    x = df[mask].dropna(subset=[score_col, return_col])
    ics, dates = [], []
    for date, g in x.groupby("date"):
        if len(g) < MIN_CROSS_SECTION_IC:
            continue
        ic, _ = stats.spearmanr(g[score_col], g[return_col])
        if not np.isnan(ic):
            ics.append(ic)
            dates.append(date)
    return pd.Series(ics, index=pd.to_datetime(dates), name="ic")


def ic_summary(ic_series):
    arr = ic_series.dropna().values
    n = len(arr)
    if n < 20:
        return dict(mean=np.nan, ir=np.nan, win=np.nan, t=np.nan, n=n)
    m, s = arr.mean(), arr.std(ddof=1)
    ir = m / s if s > 0 else np.nan
    t = m * np.sqrt(n) / s if s > 0 else 0.0
    return dict(mean=float(m), ir=float(ir),
                win=float((arr > 0).mean() * 100), t=float(t), n=int(n))


# ============================================================
# D10-D1 monotonic-aware spread (long-short)
# ============================================================
def decile_spread(summary, direction):
    """direction='top' (高分賺) → D10-D1; 'bot' (低分賺) → D1-D10.
    回傳 long-short spread (long leg - short leg)。"""
    s = summary.set_index("decile")["mean_ret"]
    if 1 not in s.index or N_DECILES not in s.index:
        return np.nan
    if direction == "top":
        return float(s[N_DECILES] - s[1])
    return float(s[1] - s[N_DECILES])


def monotonic_score(summary):
    """Spearman rank corr between decile index and mean_ret. +1=完美遞增, -1=完美遞減。"""
    s = summary.sort_values("decile")
    if len(s) < 3:
        return np.nan
    rho, _ = stats.spearmanr(s["decile"], s["mean_ret"])
    return float(rho)


# ============================================================
# Main
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=None)
    ap.add_argument("--since", type=str, default=None)
    args = ap.parse_args()

    t0 = time.time()
    df = load_panel(sample=args.sample, since=args.since)
    df = compute_indicators(df)
    df = add_fwd_returns(df)
    df = add_liquidity(df)
    df = rank_normalize_signals(df)
    build_combos(df)   # adds combo_rvol_lowatr etc. into df in-place

    # ---------- 1. Decile + Top-N (per liquidity tier × horizon) ----------
    logger.info("=== Decile + Top-N across liquidity tiers ===")
    decile_rows, topn_rows = [], []
    # 方向：rvol/combo 做多高分(top)；atr_pct 做多低分(bot)
    score_dir = {
        "sig_rvol_log_rn": "top",
        "sig_atr_pct_rn": "bot",
        "combo_rvol_lowatr": "top",
    }
    for tier_name, tier_val in LIQUIDITY_TIERS:
        mask = liquidity_mask(df, tier_val)
        for score_col, label in FOCUS_SCORES.items():
            for h in HORIZONS:
                ret = f"fwd_{h}d"
                summ = compute_decile_returns(df, score_col, ret, mask)
                for _, r in summ.iterrows():
                    decile_rows.append({
                        "score": score_col, "label": label, "liquidity": tier_name,
                        "horizon": h, "decile": int(r["decile"]),
                        "mean_ret": r["mean_ret"], "median_ret": r["median_ret"],
                        "win_rate": r["win_rate"], "n_days": int(r["n_days"]),
                    })
                # top-N both directions
                for n in TOP_N_VARIANTS:
                    for d in ["top", "bot"]:
                        res = simulate_topn(df, score_col, ret, n, mask, d)
                        if res:
                            topn_rows.append({
                                "score": score_col, "label": label,
                                "liquidity": tier_name, "horizon": h,
                                "top_n": n, "direction": d, **res,
                            })

    decile_df = pd.DataFrame(decile_rows)
    topn_df = pd.DataFrame(topn_rows)
    decile_df.to_csv(OUT_DIR / "rvol_atr_decile_returns.csv", index=False, encoding="utf-8-sig")
    topn_df.to_csv(OUT_DIR / "rvol_atr_topn_portfolio.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved decile ({len(decile_df)}) + topN ({len(topn_df)})")

    # ---------- 2. Net spread (cost-adjusted), h=20, long-short decile ----------
    logger.info("=== Net spread (cost-adjusted) ===")
    net_rows = []
    for tier_name, tier_val in LIQUIDITY_TIERS:
        mask = liquidity_mask(df, tier_val)
        for score_col, label in FOCUS_SCORES.items():
            d = score_dir[score_col]
            for h in HORIZONS:
                summ = compute_decile_returns(df, score_col, f"fwd_{h}d", mask)
                gross = decile_spread(summ, d)   # long-short gross spread per holding period
                mono = monotonic_score(summ)
                # 成本：long-short = 2 條腿，每腿每 rebalance 一次 round-trip
                for cost in COST_ROUNDTRIP_TIERS:
                    # 每 h 日換倉一次；long-short 兩腿 → 2*cost per period
                    net = gross - 2 * cost
                    net_rows.append({
                        "score": score_col, "label": label, "liquidity": tier_name,
                        "horizon": h, "direction": d,
                        "gross_spread": gross, "cost_roundtrip": cost,
                        "net_spread": net, "monotonic_rho": mono,
                    })
    net_df = pd.DataFrame(net_rows)
    net_df.to_csv(OUT_DIR / "rvol_atr_net_spread.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved net_spread ({len(net_df)})")

    # ---------- 3. Walk-forward annual IC + D10-D1 spread (20d) ----------
    logger.info("=== Walk-forward annual (20d) ===")
    df["year"] = df["date"].dt.year
    wf_rows = []
    years = sorted(df["year"].unique())
    for tier_name, tier_val in [("all", None), ("liq_50m", 5e7)]:
        base_mask = liquidity_mask(df, tier_val)
        for score_col, label in FOCUS_SCORES.items():
            d = score_dir[score_col]
            for yr in years:
                yr_mask = base_mask & (df["year"] == yr)
                if yr_mask.sum() < 1000:
                    continue
                ic = daily_ic(df, score_col, "fwd_20d", yr_mask)
                summ = compute_decile_returns(df, score_col, "fwd_20d", yr_mask)
                spread = decile_spread(summ, d)
                ics = ic_summary(ic)
                wf_rows.append({
                    "score": score_col, "label": label, "liquidity": tier_name,
                    "year": int(yr), "ic_mean": ics["mean"], "ic_ir": ics["ir"],
                    "ic_win": ics["win"], "ic_t": ics["t"], "ic_n_days": ics["n"],
                    "ls_spread_20d": spread,
                })
    wf_df = pd.DataFrame(wf_rows)
    wf_df.to_csv(OUT_DIR / "rvol_atr_walkforward_annual.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved walkforward ({len(wf_df)})")

    # ---------- 4. ATR% D1 forensic (斷崖) ----------
    # 比對 atr_pct 最低 decile (D1, 即最低波動) 成分股的流動性 / 報酬結構
    logger.info("=== ATR% D1 forensic ===")
    forensic_rows = []
    for tier_name, tier_val in LIQUIDITY_TIERS:
        mask = liquidity_mask(df, tier_val)
        x = df[mask][["sig_atr_pct_rn", "fwd_20d", "fwd_10d", "date",
                      "avg_amount_20d", "amt_pct_rank"]].dropna(subset=["sig_atr_pct_rn"])

        def _bucket(g):
            if len(g) < MIN_CROSS_SECTION:
                return pd.Series([np.nan] * len(g), index=g.index)
            return pd.qcut(g["sig_atr_pct_rn"].rank(method="first"),
                           N_DECILES, labels=False, duplicates="drop")
        x["decile"] = x.groupby("date", group_keys=False).apply(_bucket)
        x = x.dropna(subset=["decile"])
        x["decile"] = x["decile"].astype(int) + 1
        # atr_pct rank-normalized: 低值=低 atr_pct=低波動 → D1=最低波動
        for dec in [1, 2, 3, 9, 10]:
            sub = x[x["decile"] == dec]
            if len(sub) == 0:
                continue
            forensic_rows.append({
                "liquidity": tier_name, "decile": dec,
                "n_obs": len(sub),
                "fwd_20d_mean": float(sub["fwd_20d"].mean()),
                "fwd_20d_median": float(sub["fwd_20d"].median()),
                "fwd_20d_p95": float(sub["fwd_20d"].quantile(0.95)),
                "fwd_20d_p99": float(sub["fwd_20d"].quantile(0.99)),
                "fwd_20d_win": float((sub["fwd_20d"] > 0).mean()),
                "fwd_10d_mean": float(sub["fwd_10d"].mean()),
                "avg_amount_med_ntd": float(sub["avg_amount_20d"].median()),
                "amt_pct_rank_med": float(sub["amt_pct_rank"].median()),
                # 極端報酬佔比：fwd_20d > 30% 的比例 (reprice artifact 指標)
                "frac_ret_gt30pct": float((sub["fwd_20d"] > 0.30).mean()),
            })
    forensic_df = pd.DataFrame(forensic_rows)
    forensic_df.to_csv(OUT_DIR / "rvol_atr_atr_forensic.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved forensic ({len(forensic_df)})")

    # ---------- Console summary ----------
    print("\n" + "=" * 100)
    print("  DECILE (h=20) — D1 / D10 / monotonic by liquidity tier")
    print("=" * 100)
    for score_col, label in FOCUS_SCORES.items():
        print(f"\n  [{label}]  ({score_col}, dir={score_dir[score_col]})")
        print(f"  {'liquidity':<16}{'D1%':>9}{'D10%':>9}{'D10-D1%':>10}{'mono_rho':>10}{'D1_win':>9}")
        for tier_name, _ in LIQUIDITY_TIERS:
            sub = decile_df[(decile_df["score"] == score_col)
                            & (decile_df["liquidity"] == tier_name)
                            & (decile_df["horizon"] == 20)].sort_values("decile")
            if sub.empty:
                continue
            d1 = sub[sub["decile"] == 1]["mean_ret"].iloc[0] * 100
            d10 = sub[sub["decile"] == 10]["mean_ret"].iloc[0] * 100
            d1w = sub[sub["decile"] == 1]["win_rate"].iloc[0] * 100
            mono = monotonic_score(sub.rename(columns={"mean_ret": "mean_ret"}))
            print(f"  {tier_name:<16}{d1:>+9.3f}{d10:>+9.3f}{d10-d1:>+10.3f}{mono:>+10.3f}{d1w:>8.1f}%")

    print("\n" + "=" * 100)
    print("  ATR% D1 (lowest-vol) FORENSIC — does the cliff survive liquidity filter?")
    print("=" * 100)
    print(f"  {'liquidity':<16}{'dec':>4}{'n':>9}{'mean%':>9}{'med%':>9}"
          f"{'p99%':>8}{'>30%frac':>10}{'amt_med(億)':>13}{'amtRk':>7}")
    for _, r in forensic_df.iterrows():
        print(f"  {r['liquidity']:<16}{int(r['decile']):>4}{int(r['n_obs']):>9}"
              f"{r['fwd_20d_mean']*100:>+9.3f}{r['fwd_20d_median']*100:>+9.3f}"
              f"{r['fwd_20d_p99']*100:>+8.1f}{r['frac_ret_gt30pct']*100:>9.2f}%"
              f"{r['avg_amount_med_ntd']/1e8:>13.3f}{r['amt_pct_rank_med']:>7.2f}")

    print("\n" + "=" * 100)
    print("  WALK-FORWARD ANNUAL (20d IC + LS spread)")
    print("=" * 100)
    for score_col, label in FOCUS_SCORES.items():
        for tier_name in ["all", "liq_50m"]:
            sub = wf_df[(wf_df["score"] == score_col) & (wf_df["liquidity"] == tier_name)]
            if sub.empty:
                continue
            pos_ic = (sub["ic_mean"] * (1 if score_dir[score_col] == "top" else -1) > 0).sum()
            pos_spread = (sub["ls_spread_20d"] > 0).sum()
            n = len(sub)
            print(f"\n  [{label}] liquidity={tier_name}: "
                  f"signed-IC positive {pos_ic}/{n} yrs, LS-spread positive {pos_spread}/{n} yrs")
            print(f"  {'year':>6}{'ic_mean':>10}{'ic_ir':>8}{'ic_win%':>9}{'LS_spread%':>12}")
            for _, r in sub.sort_values("year").iterrows():
                print(f"  {int(r['year']):>6}{r['ic_mean']:>+10.4f}{r['ic_ir']:>+8.2f}"
                      f"{r['ic_win']:>8.1f}%{r['ls_spread_20d']*100:>+12.3f}")

    print("\n" + "=" * 100)
    print("  NET SPREAD (cost-adjusted, h=20, long-short)")
    print("=" * 100)
    print(f"  {'score':<22}{'liquidity':<16}{'gross%':>9}{'cost':>7}{'net%':>9}{'mono':>8}")
    for _, r in net_df[(net_df["horizon"] == 20)].iterrows():
        print(f"  {r['score']:<22}{r['liquidity']:<16}{r['gross_spread']*100:>+9.3f}"
              f"{r['cost_roundtrip']*100:>6.2f}%{r['net_spread']*100:>+9.3f}{r['monotonic_rho']:>+8.3f}")

    print(f"\nTotal time: {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
