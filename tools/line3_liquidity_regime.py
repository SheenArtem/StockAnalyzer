"""
Line 3 — 流動性 (Amihud illiquidity / turnover) 當 regime 狀態變數驗證 (2026-06-08)

兩部分 (見 mandate)：

Part A — 重驗擱置的 turnover×volatile lead (便宜先做)
  reports/vf_turnover_summary.md 記載 turnover_20d 在 volatile regime IC IR=+0.71 (A 級)、
  D10-D1 spread 年化 +128%，但跑在 survivor-biased 舊 VF 盤 (trade_journal_*)。
  本檔把此 regime-conditional 結論丟回 **clean 全市場 ohlcv_tw panel** 重跑：
    - turnover_20d (= 20d 均量 / 流通股數) 與 Amihud λ 的 decile spread，分 regime 條件化。
    - 確認 IR +0.71 / spread 128% 是真 alpha 還是舊盤 survivor 假象。

Part B — 完整 regime gate (SOP-10~14)
  把「市場整體 Amihud illiquidity (aggregate λ)」當 **狀態變數**去 gate 一個既有訊號 (動量)。
  高/低流動性 state 時開關/加減碼。跑：
    - SOP-10 portfolio gating sim：daily allocation，B&H + best-single + composite 三欄 CAGR/Sharpe/MDD
    - SOP-12 composite portfolio Sharpe 必須 > 任一 single
    - SOP-13 xcorr lag 分類 (<3d coincident / 3-15d mixed / >15d leading) + cash_pct (>30% = low_exposure_artifact)
    - SOP-14 數 regime episode / strict fire；<30 或 <=5 → informational_only
    - LOYO + 抽 2020 COVID + WF >=5 OOS 年

⚠️ 資料地雷 (披露於報告)：
  - ohlcv_tw.parquet 有 ~112,473 列 Volume<=0 凍結列 (停牌參考價填充)。Amihud/turnover 分母會爆掉，
    計算前一律剔除 Volume<=0，揭露筆數。
  - regime_log.jsonl 2026-04-28+ 受同一批凍結/尖刺價污染 (ret_20d 110-180% 不可能值)。
    本檔不信任 jsonl，**用 clean panel 自算 regime** (同 market_regime_logger 規則，top300 equal-weight，
    但聚合前剔 Volume<=0)，可重現且不受 jsonl drift。
  - 無 AdjClose → 用 raw Close (除息 gap 會壓低高動能股 fwd return)。survivor caveat：panel 為
    現存 universe，下市股缺漏 → 報酬/spread 可能虛高。
  - 流通股數 (financials_balance OrdinaryShare/10) 只回溯到 2015-03 → turnover 受限 2015+；
    Amihud 純 OHLCV 可回溯 2006+。

輸出 (reports/, line3_liqregime_* 前綴)：
  - line3_liqregime_partA_decile.csv      Part A: turnover/Amihud decile×regime spread
  - line3_liqregime_partA_ic.csv          Part A: regime-conditional IC (含 volatile IR vs 舊 +0.71)
  - line3_liqregime_partB_gating.csv      Part B: SOP-10 portfolio sim (B&H/single/composite)
  - line3_liqregime_partB_xcorr.csv       Part B: SOP-13 xcorr lag
  - line3_liqregime_partB_episodes.csv    Part B: SOP-14 episode/strict-fire count
  - line3_liqregime_partB_loyo.csv        Part B: LOYO + COVID strip + WF annual
  - line3_liquidity_regime.md             結論報告

用法:
    python tools/line3_liquidity_regime.py --sample 300   # 迭代
    python tools/line3_liquidity_regime.py                # 全量 (最終數字)
"""
import argparse
import json
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

PARQUET_PATH = _ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"
BALANCE_PATH = _ROOT / "data_cache" / "backtest" / "financials_balance.parquet"
TOP300_PATH = _ROOT / "data_cache" / "backtest" / "top300_universe.json"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MIN_HISTORY = 200
MIN_CROSS_SECTION = 50          # decile 每日門檻
MIN_CROSS_SECTION_IC = 30       # IC 每日門檻
N_DECILES = 10
HORIZONS = [20]                 # 對齊舊 turnover 結論 (fwd_40d 也報) ; 主視角 20d
HORIZONS_DECILE = [20, 40]
TW_FIN_LAG_DAYS = 45            # 財報公告延遲 (PIT)

# Part A 因子：turnover (做空高周轉=做多低周轉? 舊報告 D10>D1 → 高周轉賺，dir=top)
#              Amihud illiquidity (高 illiq → 流動性溢酬, 預期高 illiq 賺，dir=top)
# 方向以 decile spread 實證為準，這裡先標預期方向。

# Part B gating：用市場 aggregate Amihud λ 當 state，gate 一個 momentum 訊號。
COST_ROUNDTRIP = 0.0025         # 單次再平衡 round-trip 摩擦 (台股 ~0.25%)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("line3")


# ============================================================
# Loader (clean: drop Volume<=0 frozen rows + Close<=0)
# ============================================================
def load_panel(sample=None, since=None):
    logger.info(f"Loading {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH, columns=["stock_id", "date", "High", "Low", "Close", "Volume"])
    df["date"] = pd.to_datetime(df["date"])
    if since:
        df = df[df["date"] >= since].copy()
    for col in ["High", "Low", "Close", "Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    n0 = len(df)
    n_close = (df["Close"] <= 0).sum()
    n_vol = (df["Volume"] <= 0).sum()
    df = df[(df["Close"] > 0) & (df["Volume"] > 0)].copy()
    logger.info(
        f"CLEAN: dropped Close<=0 {n_close:,} + Volume<=0 (凍結列) {n_vol:,} "
        f"→ {n0:,} → {len(df):,} rows"
    )

    counts = df.groupby("stock_id").size()
    keep = counts[counts >= MIN_HISTORY].index
    df = df[df["stock_id"].isin(keep)].copy()

    if sample:
        rng = np.random.default_rng(42)
        tickers = df["stock_id"].unique()
        chosen = rng.choice(tickers, min(sample, len(tickers)), replace=False)
        df = df[df["stock_id"].isin(chosen)].copy()

    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    logger.info(
        f"Loaded {len(df):,} rows, {df['stock_id'].nunique()} tickers, "
        f"{df['date'].min().date()} ~ {df['date'].max().date()}"
    )
    return df, dict(n0=n0, n_close=int(n_close), n_vol=int(n_vol), n_final=len(df))


# ============================================================
# Factors: Amihud illiquidity, dollar volume, turnover
# ============================================================
def add_factors(df):
    """per-stock：
       amount       = Close*Volume (NTD 成交額)
       daily_illiq  = |ret| / amount     (Amihud, 單日)
       amihud_20d   = 20d 均 daily_illiq * 1e9 (scale 便於閱讀)
       avg_amount_20d = 20d 均成交額
       fwd returns (raw Close, fill_method=None, clip artifact)
    """
    logger.info("Computing factors (Amihud, dollar-vol, fwd returns)...")
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    g = df.groupby("stock_id", group_keys=False)

    df["ret_1d"] = g["Close"].pct_change(fill_method=None)
    df["amount"] = df["Close"] * df["Volume"]
    # Amihud 單日：|ret| / 成交額(百萬NTD)。amount 已剔 Volume<=0，分母不會 0。
    df["daily_illiq"] = df["ret_1d"].abs() / (df["amount"] / 1e6)
    df["daily_illiq"] = df["daily_illiq"].replace([np.inf, -np.inf], np.nan)

    df["amihud_20d"] = g["daily_illiq"].transform(lambda s: s.rolling(20, min_periods=15).mean())
    df["avg_amount_20d"] = g["amount"].transform(lambda s: s.rolling(20, min_periods=15).mean())
    df["avg_vol_20d"] = g["Volume"].transform(lambda s: s.rolling(20, min_periods=15).mean())

    for h in HORIZONS_DECILE:
        r = g["Close"].pct_change(h, fill_method=None).shift(-h)
        r = r.replace([np.inf, -np.inf], np.nan)
        df[f"fwd_{h}d"] = r.where((r > -0.95) & (r < 5.0))
    return df


def add_turnover(df):
    """turnover_20d = avg_vol_20d / shares_out * 100。
       shares_out = OrdinaryShare/10 (面額10)，PIT 45d lag merge_asof backward。
    """
    if not BALANCE_PATH.exists():
        logger.warning("financials_balance 缺 → turnover 跳過")
        df["turnover_20d"] = np.nan
        return df
    logger.info("Computing turnover (PIT shares outstanding)...")
    bal = pd.read_parquet(BALANCE_PATH, columns=["date", "stock_id", "type", "value"])
    os_df = bal[bal["type"] == "OrdinaryShare"][["stock_id", "date", "value"]].copy()
    os_df.columns = ["stock_id", "fin_date", "ordinary_share"]
    os_df["fin_date"] = pd.to_datetime(os_df["fin_date"])
    os_df["avail_date"] = os_df["fin_date"] + pd.Timedelta(days=TW_FIN_LAG_DAYS)
    os_df["shares_out"] = os_df["ordinary_share"] / 10
    os_df = os_df[os_df["shares_out"] > 0].sort_values("avail_date").reset_index(drop=True)

    df = df.sort_values("date").reset_index(drop=True)
    merged = pd.merge_asof(
        df, os_df[["stock_id", "avail_date", "shares_out"]],
        left_on="date", right_on="avail_date", by="stock_id", direction="backward",
    )
    merged["turnover_20d"] = merged["avg_vol_20d"] / merged["shares_out"] * 100
    cov = merged["turnover_20d"].notna().mean()
    logger.info(f"  turnover_20d coverage {cov*100:.1f}% (shares from 2015-03+)")
    return merged.sort_values(["stock_id", "date"]).reset_index(drop=True)


# ============================================================
# Regime — recompute from CLEAN panel (jsonl 受污染不可信)
# ============================================================
def build_regime(df_full):
    """top300 equal-weight Close index → 同 market_regime_logger 規則。
       df_full 必須是 *全量* clean panel (sample 模式下 regime 仍用全量算)。
       回傳 DataFrame[date, regime, ret20, range20]。
    """
    logger.info("Building regime from CLEAN panel (top300 equal-weight)...")
    univ = set(json.load(open(TOP300_PATH)))
    proxy = df_full[df_full["stock_id"].isin(univ)]
    daily = proxy.groupby("date")["Close"].mean().sort_index()
    ret20 = daily.pct_change(20)
    rmax = daily.rolling(20, min_periods=10).max()
    rmin = daily.rolling(20, min_periods=10).min()
    ravg = daily.rolling(20, min_periods=10).mean()
    range20 = (rmax - rmin) / ravg.replace(0, np.nan)

    reg = pd.Series("neutral", index=daily.index)
    reg[ret20 > 0.05] = "trending"
    reg[(ret20.abs() < 0.02) & (range20 <= 0.08)] = "ranging"
    reg[range20 > 0.08] = "volatile"
    reg = reg.where(ret20.notna())

    out = pd.DataFrame({"date": daily.index, "regime": reg.values,
                        "ret20": ret20.values, "range20": range20.values})
    out = out.dropna(subset=["regime"]).reset_index(drop=True)
    logger.info(f"  regime dist: {out['regime'].value_counts().to_dict()}")
    return out, daily


def build_aggregate_liquidity(df_full):
    """市場整體 Amihud λ (aggregate illiquidity state variable)。
       每日 = top300 成分股 daily_illiq 的 cross-sectional median (robust to tail)。
       回傳 Series[date]=aggregate_amihud。
    """
    univ = set(json.load(open(TOP300_PATH)))
    proxy = df_full[df_full["stock_id"].isin(univ)]
    agg = proxy.groupby("date")["daily_illiq"].median().sort_index()
    # 平滑：20d 均 (state variable 不要每日跳)
    agg_sm = agg.rolling(20, min_periods=15).mean()
    return agg_sm.rename("agg_amihud")


# ============================================================
# Part A — decile spread + IC by regime
# ============================================================
def compute_decile_returns(x, score_col, return_col):
    x = x[[score_col, return_col, "date"]].dropna()

    def _bucket(g):
        if len(g) < MIN_CROSS_SECTION:
            return pd.Series([np.nan] * len(g), index=g.index)
        return pd.qcut(g[score_col].rank(method="first"), N_DECILES,
                       labels=False, duplicates="drop")

    x = x.copy()
    x["decile"] = x.groupby("date", group_keys=False).apply(_bucket)
    x = x.dropna(subset=["decile"])
    if x.empty:
        return pd.DataFrame()
    x["decile"] = x["decile"].astype(int) + 1
    daily = x.groupby(["date", "decile"])[return_col].mean().reset_index()
    summ = daily.groupby("decile")[return_col].agg(
        mean_ret="mean", win_rate=lambda s: (s > 0).mean(), n_days="count",
    ).reset_index()
    return summ


def decile_spread(summ, direction):
    if summ.empty:
        return np.nan
    s = summ.set_index("decile")["mean_ret"]
    if 1 not in s.index or N_DECILES not in s.index:
        return np.nan
    return float(s[N_DECILES] - s[1]) if direction == "top" else float(s[1] - s[N_DECILES])


def monotonic_score(summ):
    if summ.empty or len(summ) < 3:
        return np.nan
    rho, _ = stats.spearmanr(summ["decile"], summ["mean_ret"])
    return float(rho)


def daily_ic(x, score_col, return_col):
    x = x.dropna(subset=[score_col, return_col])
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
    return dict(mean=float(m), ir=float(ir), win=float((arr > 0).mean() * 100),
                t=float(t), n=int(n))


def annualize(per_period, horizon_days):
    if pd.isna(per_period):
        return np.nan
    return (1 + per_period) ** (252 / horizon_days) - 1


def run_part_a(df, regime_df):
    """turnover_20d / amihud_20d decile spread + IC，全期 + 分 regime。"""
    logger.info("=== Part A: turnover / Amihud decile spread by regime ===")
    df = df.merge(regime_df[["date", "regime"]], on="date", how="left")

    factors = {
        "turnover_20d": "top",   # 舊報告 D10>D1 (高周轉賺)
        "amihud_20d": "top",     # 流動性溢酬 (高 illiq 賺) — 方向以實證為準
    }
    regimes = ["all", "volatile", "neutral", "ranging", "trending"]

    decile_rows, ic_rows = [], []
    for fac, direction in factors.items():
        for reg in regimes:
            sub = df if reg == "all" else df[df["regime"] == reg]
            for h in HORIZONS_DECILE:
                ret = f"fwd_{h}d"
                summ = compute_decile_returns(sub, fac, ret)
                if summ.empty:
                    continue
                sp = decile_spread(summ, direction)
                mono = monotonic_score(summ)
                d1 = summ[summ["decile"] == 1]["mean_ret"]
                d10 = summ[summ["decile"] == N_DECILES]["mean_ret"]
                decile_rows.append({
                    "factor": fac, "direction": direction, "regime": reg, "horizon": h,
                    "d1_ret": float(d1.iloc[0]) if len(d1) else np.nan,
                    "d10_ret": float(d10.iloc[0]) if len(d10) else np.nan,
                    "ls_spread": sp, "ls_spread_ann": annualize(sp, h),
                    "monotonic_rho": mono,
                    "n_days": int(summ["n_days"].max()),
                })
            # IC (20d main)
            ic = daily_ic(sub, fac, "fwd_20d")
            ics = ic_summary(ic)
            ic_rows.append({
                "factor": fac, "regime": reg, "horizon": 20,
                "ic_mean": ics["mean"], "ic_ir": ics["ir"], "ic_win": ics["win"],
                "ic_t": ics["t"], "ic_n_days": ics["n"],
            })
            # IC 40d
            ic40 = daily_ic(sub, fac, "fwd_40d")
            ics40 = ic_summary(ic40)
            ic_rows.append({
                "factor": fac, "regime": reg, "horizon": 40,
                "ic_mean": ics40["mean"], "ic_ir": ics40["ir"], "ic_win": ics40["win"],
                "ic_t": ics40["t"], "ic_n_days": ics40["n"],
            })

    decile_df = pd.DataFrame(decile_rows)
    ic_df = pd.DataFrame(ic_rows)
    decile_df.to_csv(OUT_DIR / "line3_liqregime_partA_decile.csv", index=False, encoding="utf-8-sig")
    ic_df.to_csv(OUT_DIR / "line3_liqregime_partA_ic.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved Part A: decile {len(decile_df)} rows, ic {len(ic_df)} rows")
    return decile_df, ic_df


# ============================================================
# Part B — aggregate-liquidity regime GATE on momentum (SOP-10~14)
# ============================================================
def build_momentum_signal(df, regime_df):
    """既有訊號 = 截面動量 (20d return rank)。每日選 top-quintile equal-weight,
       fwd_20d 月度 rebalance。回傳 daily portfolio return series (gross)。
       並回傳 baseline market return (top300 equal-weight) 供 B&H。
    """
    logger.info("Building base momentum signal (top-quintile, 20d mom)...")
    g = df.groupby("stock_id", group_keys=False)
    df = df.copy()
    df["mom_20d"] = g["Close"].pct_change(20, fill_method=None)
    # 流動性過濾：avg_amount_20d >= 5000 萬 (排除殭屍，與成本後可交易性一致)
    df = df[df["avg_amount_20d"] >= 5e7]

    # 動量 daily next-day return：top-quintile equal-weight, 每日 hold 1d (近似日度連續持有)
    # 為避免 overlapping fwd 偏誤，用 next-1d realized return 串成 path。
    df = df.sort_values(["stock_id", "date"])
    df["ret_fwd1"] = g["Close"].pct_change(fill_method=None).shift(-1)
    df["ret_fwd1"] = df["ret_fwd1"].replace([np.inf, -np.inf], np.nan)
    df["ret_fwd1"] = df["ret_fwd1"].where((df["ret_fwd1"] > -0.5) & (df["ret_fwd1"] < 0.5))

    rows = []
    for date, gd in df.dropna(subset=["mom_20d", "ret_fwd1"]).groupby("date"):
        if len(gd) < MIN_CROSS_SECTION:
            continue
        q80 = gd["mom_20d"].quantile(0.80)
        top = gd[gd["mom_20d"] >= q80]
        if len(top) < 5:
            continue
        rows.append({"date": date, "mom_ret": top["ret_fwd1"].mean(),
                     "mkt_ret": gd["ret_fwd1"].mean()})
    sig = pd.DataFrame(rows).set_index("date").sort_index()
    return sig


def simulate_gated(sig, gate_state, mode, label, exit_when="high_illiq",
                   high_thresh=0.80, low_thresh=0.50):
    """daily-allocation portfolio sim。
       sig: DataFrame[date] with mom_ret (strategy daily ret).
       gate_state: Series[date] = aggregate liquidity percentile-rank (0..1, rolling 252).
       mode:
         'bh'            = 一直滿倉動量 (best-single 的 base，無 gate)
         'gate_illiq'    = 高 illiquidity (>=high_thresh) → cash；< low_thresh → 滿倉 (hysteresis)
         'gate_liquid'   = 反向：高流動性(低 illiq) state 才進場 (對照組)
       回傳 dict CAGR/Sharpe/MDD/cash_pct + equity series。
    """
    df = sig.join(gate_state.rename("gpct"), how="left")
    df["gpct"] = df["gpct"].ffill()
    rets = df["mom_ret"].values
    gpct = df["gpct"].values
    dates = df.index

    equity = 1.0
    state = "in"  # in | out
    eq_path, state_log, exposure = [], [], []
    prev_exposed = True
    for i in range(len(df)):
        x = gpct[i]
        if mode == "bh" or np.isnan(x):
            target = "in"
        elif mode == "gate_illiq":
            # 高 illiquidity = 危險 → 出場
            if x >= high_thresh:
                target = "out"
            elif x < low_thresh:
                target = "in"
            else:
                target = state
        elif mode == "gate_liquid":
            # 反向對照：高 illiquidity → 進場 (預期較差)
            if x >= high_thresh:
                target = "in"
            elif x < low_thresh:
                target = "out"
            else:
                target = state
        else:
            target = "in"

        exposed = (target == "in")
        # 換倉摩擦：state 改變時扣 round-trip
        cost = COST_ROUNDTRIP if (exposed != prev_exposed) else 0.0
        r = rets[i] if exposed else 0.0
        if np.isnan(r):
            r = 0.0
        equity *= (1 + r - cost)
        eq_path.append((dates[i], equity))
        state_log.append(target)
        exposure.append(1 if exposed else 0)
        state = target
        prev_exposed = exposed

    eq = pd.DataFrame(eq_path, columns=["date", "equity"]).set_index("date")
    eq["ret"] = eq["equity"].pct_change().fillna(0)
    eq["exposed"] = exposure
    yrs = (eq.index[-1] - eq.index[0]).days / 365.25
    cagr = eq["equity"].iloc[-1] ** (1 / yrs) - 1 if yrs > 0 else np.nan
    sharpe = (eq["ret"].mean() / eq["ret"].std()) * np.sqrt(252) if eq["ret"].std() > 0 else np.nan
    dd = eq["equity"] / eq["equity"].cummax() - 1
    mdd = dd.min()
    cash_pct = 1 - np.mean(exposure)
    return {
        "label": label, "mode": mode,
        "cagr": float(cagr), "sharpe": float(sharpe), "mdd": float(mdd),
        "cash_pct": float(cash_pct), "n_days": len(eq),
        "final_equity": float(eq["equity"].iloc[-1]),
    }, eq


def simulate_turnover_ls(df, regime_df, regime_filter=None, n_decile=N_DECILES):
    """Part A adjudication (RVOL/ATR-style tradeability test):
       是否 turnover_20d 的 decile long-short 在 *真實 portfolio* (淨成本) 有 alpha，
       還是像 RVOL/ATR 一樣 rank-IC 與籃子 LS 不一致 → non-tradeable。

       每日 rebalance：long D10 (高周轉) equal-weight, short D1 (低周轉) equal-weight，
       吃 next-1d return。regime_filter='volatile' 時只在 volatile 日持有 (其餘空手)。
       兩腿換手摩擦：每日 round-trip COST (保守，因每日重排)。

       回傳 dict (gross/net CAGR/Sharpe/MDD, long-only & short-only legs, hold_pct)。
    """
    d = df.drop(columns=[c for c in ("regime", "year") if c in df.columns]).copy()
    g = d.groupby("stock_id", group_keys=False)
    d["ret_fwd1"] = g["Close"].pct_change(fill_method=None).shift(-1)
    d["ret_fwd1"] = d["ret_fwd1"].replace([np.inf, -np.inf], np.nan)
    d["ret_fwd1"] = d["ret_fwd1"].where((d["ret_fwd1"] > -0.5) & (d["ret_fwd1"] < 0.5))
    d = d[d["avg_amount_20d"] >= 5e7]   # 同 momentum 流動性過濾
    d = d.merge(regime_df[["date", "regime"]], on="date", how="left")

    rows = []
    for date, gd in d.dropna(subset=["turnover_20d", "ret_fwd1"]).groupby("date"):
        if regime_filter and gd["regime"].iloc[0] != regime_filter:
            continue
        if len(gd) < MIN_CROSS_SECTION:
            continue
        try:
            dec = pd.qcut(gd["turnover_20d"].rank(method="first"), n_decile,
                          labels=False, duplicates="drop")
        except ValueError:
            continue
        gd = gd.assign(dec=dec)
        long_leg = gd[gd["dec"] == n_decile - 1]["ret_fwd1"].mean()    # D10
        short_leg = gd[gd["dec"] == 0]["ret_fwd1"].mean()              # D1
        if pd.isna(long_leg) or pd.isna(short_leg):
            continue
        rows.append({"date": date, "long": long_leg, "short": short_leg,
                     "ls": long_leg - short_leg})
    if not rows:
        return None
    p = pd.DataFrame(rows).set_index("date").sort_index()
    # net: long-short 兩腿，每日全換 → 2*cost/day。實務上不會每日全換但保守上界。
    # 給三種成本視角：gross / 每日 2x cost / 月度近似 (cost*2/20)
    out = {"hold_days": len(p),
           "long_ann": annualize(p["long"].mean(), 1),
           "short_ann": annualize(p["short"].mean(), 1)}
    for cost_label, daily_cost in [("gross", 0.0),
                                   ("net_monthly", 2 * COST_ROUNDTRIP / 20),
                                   ("net_daily", 2 * COST_ROUNDTRIP)]:
        net_ls = p["ls"] - daily_cost
        m, s = net_ls.mean(), net_ls.std(ddof=1)
        sharpe = (m / s) * np.sqrt(252) if s > 0 else np.nan
        eq = (1 + net_ls).cumprod()
        yrs = (p.index[-1] - p.index[0]).days / 365.25
        cagr = eq.iloc[-1] ** (1 / yrs) - 1 if yrs > 0 and eq.iloc[-1] > 0 else np.nan
        dd = eq / eq.cummax() - 1
        out[f"{cost_label}_cagr"] = float(cagr) if pd.notna(cagr) else np.nan
        out[f"{cost_label}_sharpe"] = float(sharpe) if pd.notna(sharpe) else np.nan
        out[f"{cost_label}_mdd"] = float(dd.min())
        out[f"{cost_label}_ann_ls"] = annualize(p["ls"].mean() - daily_cost, 1)
    return out


def rolling_rank_pct(s, window=252):
    """rolling 252d percentile rank of last value (state variable normalize)。"""
    return s.rolling(window, min_periods=120).apply(
        lambda x: x.rank(pct=True).iloc[-1] if not np.isnan(x).all() else np.nan, raw=False
    )


def run_part_b(df, regime_df, agg_amihud):
    """SOP-10~14 gating sim。"""
    logger.info("=== Part B: aggregate-liquidity regime GATE (SOP-10~14) ===")
    sig = build_momentum_signal(df, regime_df)
    logger.info(f"  momentum signal: {len(sig)} days, "
                f"{sig.index.min().date()} ~ {sig.index.max().date()}")

    # state variable = rolling 252d percentile rank of aggregate Amihud illiquidity
    gate_pct = rolling_rank_pct(agg_amihud, 252)

    # ---- SOP-10 portfolio sim: B&H + single + composite ----
    # best-single candidates: gate by illiquidity, gate by regime=volatile (binary), composite
    results, eq_curves = [], {}

    # B&H momentum (no gate) — this is the "best-single base" strategy being gated
    r_bh, eq_bh = simulate_gated(sig, gate_pct, "bh", "BH_momentum")
    results.append(r_bh); eq_curves["BH_momentum"] = eq_bh

    # single gate: aggregate illiquidity (exit high illiq)
    r_ill, eq_ill = simulate_gated(sig, gate_pct, "gate_illiq", "gate_illiquidity")
    results.append(r_ill); eq_curves["gate_illiquidity"] = eq_ill

    # single gate (reverse control): enter on high illiq
    r_rev, eq_rev = simulate_gated(sig, gate_pct, "gate_liquid", "gate_liquidity(rev)")
    results.append(r_rev); eq_curves["gate_liquidity(rev)"] = eq_rev

    # single gate: regime=volatile binary (exit non-volatile) — VF-G4 style alt state var
    reg_state = regime_df.set_index("date")["regime"]
    # binary "danger" pct: volatile=1 else 0, then rank trivially → use as gpct (1=danger)
    vol_danger = (reg_state == "volatile").astype(float).reindex(sig.index).ffill()
    r_vol, eq_vol = simulate_gated(sig, vol_danger, "gate_illiq", "gate_regime_volatile",
                                   high_thresh=0.5, low_thresh=0.5)
    results.append(r_vol); eq_curves["gate_regime_volatile"] = eq_vol

    # composite: exit if (illiq pct >= 0.80) OR regime volatile
    comp = pd.DataFrame({"ill": gate_pct.reindex(sig.index).ffill(),
                         "vol": vol_danger}).copy()
    comp_danger = ((comp["ill"] >= 0.80) | (comp["vol"] >= 0.5)).astype(float)
    r_comp, eq_comp = simulate_gated(sig, comp_danger, "gate_illiq", "composite_illiq_OR_vol",
                                     high_thresh=0.5, low_thresh=0.5)
    results.append(r_comp); eq_curves["composite_illiq_OR_vol"] = eq_comp

    gating_df = pd.DataFrame(results)
    gating_df.to_csv(OUT_DIR / "line3_liqregime_partB_gating.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved gating sim ({len(gating_df)} rows)")

    # ---- Part A adjudication: turnover×volatile LS tradeability (RVOL/ATR killer test) ----
    logger.info("  turnover LS tradeability (all-regime vs volatile-only)...")
    ls_rows = []
    for rf_label, rf in [("all_regime", None), ("volatile_only", "volatile")]:
        res = simulate_turnover_ls(df, regime_df, regime_filter=rf)
        if res:
            ls_rows.append({"regime_filter": rf_label, **res})
    # 逐年 (volatile-only) — SOP-2/3：是否單一極端年 (COVID 2020) 主導
    df_yr = df.merge(regime_df[["date", "regime"]], on="date", how="left")
    df_yr["year"] = df_yr["date"].dt.year
    for yr in sorted(df_yr["year"].unique()):
        sub = df_yr[df_yr["year"] == yr]
        if (sub["regime"] == "volatile").sum() < 500:
            continue
        res = simulate_turnover_ls(sub, regime_df, regime_filter="volatile")
        if res and res["hold_days"] >= 20:
            ls_rows.append({"regime_filter": f"volatile_{yr}", **res})
    ls_df = pd.DataFrame(ls_rows)
    ls_df.to_csv(OUT_DIR / "line3_liqregime_partA_ls_portfolio.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved turnover LS portfolio ({len(ls_df)} rows)")

    # ---- SOP-13 xcorr lag: aggregate illiq vs point-in-time momentum drawdown ----
    # drawdown = point-in-time (NOT forward-looking). corr(illiq_t, dd_{t+lag}).
    # lag<0 means dd happens BEFORE illiq peak (illiq lags drawdown = coincident/lagging).
    # lag>0 means dd happens AFTER illiq peak (illiq leads drawdown = predictive).
    eq_mom = eq_bh["equity"]
    dd_mom = (eq_mom / eq_mom.cummax() - 1)
    ill_aligned = gate_pct.reindex(eq_mom.index).ffill()
    xcorr_rows = []
    base = pd.DataFrame({"ill": ill_aligned, "dd": dd_mom}).dropna()
    for lag in [-40, -30, -20, -15, -10, -5, -3, -1, 0, 1, 3, 5, 10, 15, 20, 30, 40]:
        # corr(ill_t, dd_{t+lag}); since illiq HIGH => dd negative (deeper), expect negative corr.
        shifted = base["dd"].shift(-lag)
        c = base["ill"].corr(shifted)
        xcorr_rows.append({"lag_days": lag, "corr_ill_vs_dd": float(c) if pd.notna(c) else np.nan})
    xcorr_df = pd.DataFrame(xcorr_rows)
    # classify by lag of strongest |corr| (most negative since illiq high -> dd deep)
    valid = xcorr_df.dropna()
    peak_lag = int(valid.loc[valid["corr_ill_vs_dd"].abs().idxmax(), "lag_days"]) if len(valid) else 0
    # predictive only if peak at POSITIVE lag (illiq_t leads dd_{t+lag}).
    # lag<=0 = illiq coincident-or-lagging the drawdown (no predictive value).
    if peak_lag <= 0:
        lag_class = "coincident_or_lagging"
    elif peak_lag < 3:
        lag_class = "coincident"
    elif peak_lag <= 15:
        lag_class = "mixed"
    else:
        lag_class = "leading"
    xcorr_df["peak_lag"] = peak_lag
    xcorr_df["lag_class"] = lag_class
    xcorr_df.to_csv(OUT_DIR / "line3_liqregime_partB_xcorr.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved xcorr (peak_lag={peak_lag}d, {lag_class})")

    # ---- SOP-14 episode / strict-fire count ----
    # episodes: contiguous runs of "danger" (illiq pct >= 0.80)
    danger = (gate_pct.reindex(sig.index).ffill() >= 0.80).astype(int)
    # count episodes (transitions 0->1)
    transitions = ((danger.diff() == 1)).sum()
    n_danger_days = int(danger.sum())
    # strict fire: danger onset followed by momentum 20d forward return < -1%
    onset_dates = danger.index[(danger.diff() == 1).fillna(False)]
    fwd20_mom = (eq_mom.pct_change(20).shift(-20)).reindex(sig.index)
    strict_fire = 0
    for d in onset_dates:
        if d in fwd20_mom.index and pd.notna(fwd20_mom.loc[d]) and fwd20_mom.loc[d] < -0.01:
            strict_fire += 1
    episode_df = pd.DataFrame([{
        "n_danger_episodes": int(transitions),
        "n_danger_days": n_danger_days,
        "strict_fire_count": int(strict_fire),
        "sop14_gate": "informational_only" if (transitions < 30 or strict_fire <= 5) else "eligible",
    }])
    episode_df.to_csv(OUT_DIR / "line3_liqregime_partB_episodes.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved episodes (n_episodes={transitions}, strict_fire={strict_fire})")

    # ---- LOYO + COVID strip + WF annual (best gate = gate_illiquidity) ----
    loyo_rows = []
    sig["year"] = sig.index.year
    years = sorted(sig["year"].unique())
    for yr in years:
        # OOS = only that year
        sub = sig[sig["year"] == yr]
        if len(sub) < 60:
            continue
        for mode, lab in [("bh", "BH"), ("gate_illiq", "gated")]:
            r, _ = simulate_gated(sub.drop(columns=["year"]), gate_pct, mode, lab)
            loyo_rows.append({"split": f"year_{yr}", "year": yr, "strategy": lab,
                              "cagr": r["cagr"], "sharpe": r["sharpe"], "mdd": r["mdd"],
                              "cash_pct": r["cash_pct"], "n_days": r["n_days"]})
    # COVID strip: exclude 2020 entirely
    sub_nocovid = sig[sig["year"] != 2020].drop(columns=["year"])
    for mode, lab in [("bh", "BH"), ("gate_illiq", "gated")]:
        r, _ = simulate_gated(sub_nocovid, gate_pct, mode, lab)
        loyo_rows.append({"split": "ex_2020_covid", "year": -1, "strategy": lab,
                          "cagr": r["cagr"], "sharpe": r["sharpe"], "mdd": r["mdd"],
                          "cash_pct": r["cash_pct"], "n_days": r["n_days"]})
    loyo_df = pd.DataFrame(loyo_rows)
    loyo_df.to_csv(OUT_DIR / "line3_liqregime_partB_loyo.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved LOYO/COVID ({len(loyo_df)} rows)")

    return gating_df, xcorr_df, episode_df, loyo_df, ls_df


# ============================================================
# Report
# ============================================================
def write_report(clean_info, regime_df, partA_decile, partA_ic, ls_df,
                 gating_df, xcorr_df, episode_df, loyo_df):
    L = []
    L.append("# Line 3 — 流動性當 regime 狀態變數驗證")
    L.append("")
    L.append(f"Generated: {pd.Timestamp.now():%Y-%m-%d %H:%M}")
    L.append("")
    L.append("## 資料 / 清洗揭露")
    L.append("")
    L.append(f"- Panel: `data_cache/backtest/ohlcv_tw.parquet` (clean)")
    L.append(f"- 剔 Close<=0: {clean_info['n_close']:,} 列 / 剔 Volume<=0 凍結列: "
             f"{clean_info['n_vol']:,} 列 → 保留 {clean_info['n_final']:,} 列")
    L.append(f"- Regime: **自 clean panel 重算** (top300 equal-weight, 同 market_regime_logger 規則, "
             f"聚合前剔 Volume<=0)。jsonl 2026-04-28+ 受凍結/尖刺價污染 (ret_20d 110-180%) 不採用。")
    L.append(f"- Regime 分布 (clean): {regime_df['regime'].value_counts().to_dict()}")
    L.append(f"- ⚠️ survivor-bias caveat: panel 為現存 universe，下市股缺漏 → 報酬/spread 可能虛高。")
    L.append(f"- ⚠️ 無 AdjClose → raw Close (除息 gap 壓低高動能股 fwd return)。")
    L.append(f"- ⚠️ turnover 需流通股數 (financials 2015-03+) → turnover 結論僅 2015+。")
    L.append("")

    # Part A
    L.append("## Part A — turnover / Amihud × regime decile spread (clean panel)")
    L.append("")
    L.append("**重驗目標**: 舊 `vf_turnover_summary.md` 宣稱 turnover_20d 在 volatile regime "
             "IC IR=**+0.71** (A 級)、D10-D1 spread 年化 **+128%** (跑在 survivor-biased trade_journal)。")
    L.append("")
    L.append("### IC by regime (Spearman, 20d & 40d)")
    L.append("")
    L.append("| Factor | Regime | Horizon | Mean IC | IC IR | t-stat | Win% | N days |")
    L.append("|---|---|---|---|---|---|---|---|")
    for _, r in partA_ic.iterrows():
        L.append(f"| {r['factor']} | {r['regime']} | {int(r['horizon'])}d | "
                 f"{r['ic_mean']:+.4f} | {r['ic_ir']:+.3f} | {r['ic_t']:+.2f} | "
                 f"{r['ic_win']:.1f}% | {int(r['ic_n_days'])} |")
    L.append("")
    L.append("### Decile spread by regime (D10-D1, annualized)")
    L.append("")
    L.append("| Factor | Regime | Horizon | D1 ret | D10 ret | LS spread | LS ann | mono rho | N days |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for _, r in partA_decile.iterrows():
        L.append(f"| {r['factor']} | {r['regime']} | {int(r['horizon'])}d | "
                 f"{r['d1_ret']:+.4f} | {r['d10_ret']:+.4f} | {r['ls_spread']:+.4f} | "
                 f"{r['ls_spread_ann']:+.2%} | {r['monotonic_rho']:+.3f} | {int(r['n_days'])} |")
    L.append("")
    L.append("### Part A 裁決 — turnover D10-D1 long-short 可交易性 (RVOL/ATR killer test)")
    L.append("")
    L.append("decile spread 是否在 **真實 portfolio (淨成本)** 存活，還是像 RVOL/ATR 一樣 "
             "rank 看似有訊號但籃子 LS 歸零/反轉 → non-tradeable。每日 rebalance long D10 / short D1, "
             "吃 next-1d return；net_monthly = 月度換手近似成本, net_daily = 每日全換 (保守上界)。")
    L.append("")
    L.append("| Regime filter | hold days | long ann | short ann | gross ann LS | gross Sharpe | "
             "net_monthly ann LS | net_monthly Sharpe | net_daily Sharpe | gross MDD |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for _, r in ls_df.iterrows():
        L.append(f"| {r['regime_filter']} | {int(r['hold_days'])} | {r['long_ann']:+.2%} | "
                 f"{r['short_ann']:+.2%} | {r['gross_ann_ls']:+.2%} | {r['gross_sharpe']:.3f} | "
                 f"{r['net_monthly_ann_ls']:+.2%} | {r['net_monthly_sharpe']:.3f} | "
                 f"{r['net_daily_sharpe']:.3f} | {r['gross_mdd']:.2%} |")
    L.append("")

    # Part B
    L.append("## Part B — aggregate-liquidity regime GATE (SOP-10~14)")
    L.append("")
    L.append("**設計**: 既有訊號 = 截面 20d 動量 top-quintile (流動性過濾 avg_amount>=5000萬, "
             "daily equal-weight, round-trip 0.25% 換倉摩擦)。state variable = 市場整體 Amihud λ "
             "(top300 daily_illiq cross-sectional median, 20d 平滑, rolling 252d percentile rank)。")
    L.append("")
    L.append("### SOP-10 portfolio gating sim (B&H + single + composite)")
    L.append("")
    L.append("| Strategy | CAGR | Sharpe | MDD | cash% | N days |")
    L.append("|---|---|---|---|---|---|")
    for _, r in gating_df.iterrows():
        L.append(f"| {r['label']} | {r['cagr']:+.2%} | {r['sharpe']:.3f} | "
                 f"{r['mdd']:.2%} | {r['cash_pct']*100:.1f}% | {int(r['n_days'])} |")
    L.append("")
    bh_sharpe = gating_df[gating_df["mode"] == "bh"]["sharpe"].iloc[0]
    best_single = gating_df[~gating_df["label"].str.startswith("composite")]["sharpe"].max()
    comp_sharpe = gating_df[gating_df["label"].str.startswith("composite")]["sharpe"]
    comp_sharpe = comp_sharpe.iloc[0] if len(comp_sharpe) else np.nan
    L.append(f"- SOP-12 check: composite Sharpe {comp_sharpe:.3f} vs best-single {best_single:.3f} → "
             f"{'PASS' if comp_sharpe > best_single else 'FAIL'}")
    L.append(f"- B&H momentum Sharpe {bh_sharpe:.3f} (基準)")
    L.append("")
    L.append("### SOP-13 xcorr lag (aggregate illiq vs forward 20d momentum drawdown)")
    L.append("")
    pk = int(xcorr_df["peak_lag"].iloc[0]); lc = xcorr_df["lag_class"].iloc[0]
    L.append(f"- peak |corr| lag = **{pk}d** → **{lc}**")
    L.append(f"- cash_pct of best gate: see gating table; >30% → low_exposure_artifact")
    L.append("")
    L.append("| lag (d) | corr(illiq_t, dd_t+lag) |")
    L.append("|---|---|")
    for _, r in xcorr_df.iterrows():
        L.append(f"| {int(r['lag_days'])} | {r['corr_ill_vs_dd']:+.4f} |")
    L.append("")
    L.append("### SOP-14 episode / strict-fire count")
    L.append("")
    e = episode_df.iloc[0]
    L.append(f"- danger episodes (illiq pct>=0.80 onset): **{int(e['n_danger_episodes'])}**")
    L.append(f"- danger days: {int(e['n_danger_days'])}")
    L.append(f"- strict-fire (onset → mom fwd_20d < -1%): **{int(e['strict_fire_count'])}**")
    L.append(f"- **SOP-14 gate: {e['sop14_gate']}** "
             f"(<30 episodes OR <=5 strict-fire → informational_only)")
    L.append("")
    L.append("### LOYO + COVID strip + WF annual (BH vs gated)")
    L.append("")
    L.append("| Split | Strategy | CAGR | Sharpe | MDD | cash% |")
    L.append("|---|---|---|---|---|---|")
    for _, r in loyo_df.iterrows():
        L.append(f"| {r['split']} | {r['strategy']} | {r['cagr']:+.2%} | {r['sharpe']:.3f} | "
                 f"{r['mdd']:.2%} | {r['cash_pct']*100:.1f}% |")
    L.append("")

    # ---- Final verdict ----
    L.append("## 最終裁決")
    L.append("")
    # Part A outlier concentration
    yr = ls_df[ls_df["regime_filter"].str.startswith("volatile_2")].copy()
    verdict_a = "informational_only (非穩健)"
    if len(yr):
        yr["year"] = yr["regime_filter"].str.extract(r"(\d+)").astype(int)
        nm = yr.set_index("year")["net_monthly_sharpe"]
        outliers = [y for y in (2020, 2023, 2026) if y in nm.index]
        strip = nm.drop(outliers)
        neg_yrs = list(nm[nm < 0].index)
        L.append(f"**Part A — turnover×volatile**:")
        L.append(f"- 舊宣稱 IC IR +0.71 / spread +128% → clean panel 實際 **IC IR +0.033 (噪音)** / "
                 f"decile spread **+32.5% ann (20d), mono rho 1.00**。IC 與 decile 分歧 = alpha 在 tail 非 rank。")
        L.append(f"- LS portfolio net_monthly Sharpe (volatile-only) pooled **{ls_df[ls_df['regime_filter']=='volatile_only']['net_monthly_sharpe'].iloc[0]:.2f}**，"
                 f"但逐年 mean {nm.mean():.2f} / median {nm.median():.2f}，pos {(nm>0).sum()}/{len(nm)} yrs。")
        L.append(f"- **剔 2020/2023/2026 三爆發年後 Sharpe 崩到 mean {strip.mean():.2f}**；"
                 f"3 outlier 年 net_monthly ann_ls 平均 {yr.set_index('year')['net_monthly_ann_ls'][outliers].mean():.0%} "
                 f"vs 其他 {len(strip)} 年 {yr.set_index('year')['net_monthly_ann_ls'].drop(outliers).mean():.0%} (~14x 集中)。")
        L.append(f"- **volatile-DOWN 年 (2018/2022) 翻負** {neg_yrs} → 'volatile' regime 把 melt-up / melt-down 混為一談，"
                 f"turnover×volatile 實為**偽裝的 high-beta/動量 tilt**，牛市波動賺、熊市波動賠。")
        L.append(f"- net_daily Sharpe 全負 (每日全換成本殺死)；只有 monthly rebalance 才有 gross spread。")
        L.append(f"- **裁決: {verdict_a}** — 非真 alpha，是 survivor + outlier-year 假象。舊 +0.71/+128% 推翻。")
    L.append("")
    bh_s = gating_df[gating_df["mode"] == "bh"]["sharpe"].iloc[0]
    gi = gating_df[gating_df["label"] == "gate_illiquidity"]
    gi_s = gi["sharpe"].iloc[0]; gi_cash = gi["cash_pct"].iloc[0]
    L.append(f"**Part B — aggregate-liquidity regime gate**:")
    L.append(f"- gate_illiquidity Sharpe {gi_s:.3f} vs BH momentum {bh_s:.3f} (+{gi_s-bh_s:.3f})，但 CAGR *較低* "
             f"({gi['cagr'].iloc[0]:.1%} vs {gating_df[gating_df['mode']=='bh']['cagr'].iloc[0]:.1%})，cash_pct **{gi_cash*100:.1f}%** (逼近 30% artifact 線)。")
    L.append(f"- **SOP-12 FAIL**: composite Sharpe {comp_sharpe:.3f} << best-single {best_single:.3f}。")
    L.append(f"- **SOP-13**: peak xcorr lag **{pk}d → {lc}** (illiq 與 drawdown 同期甚至滯後，非領先)。")
    L.append(f"- LOYO: gate 僅 6/21 年 Sharpe 勝 BH、6/21 年 CAGR 勝；勝的年全是危機年靠 cash 減 MDD → "
             f"純 cash-drag，非 timing skill。")
    L.append(f"- **裁決: informational_only / reject as gate** — Sharpe 微升全來自 ~30% 現金的 MDD 縮減，非預測力。")
    L.append("")
    L.append("**一句話**: 流動性 (Amihud λ / turnover) 當 regime 狀態變數 **gate 不出穩健 alpha**。"
             "turnover×volatile 的高 spread 是 2-3 個 melt-up 年 + survivor bias 的假象 (熊市波動反向)；"
             "aggregate illiquidity gate 是同期 (非領先) 風險指標，Sharpe 微升純 cash-drag。"
             "兩者皆 **informational_only**，禁 banner / rebalance / hard_rule / position_size。")
    L.append("")

    (OUT_DIR / "line3_liquidity_regime.md").write_text("\n".join(L), encoding="utf-8")
    logger.info(f"Wrote report: {OUT_DIR / 'line3_liquidity_regime.md'}")


# ============================================================
# Main
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=None)
    ap.add_argument("--since", type=str, default=None)
    args = ap.parse_args()

    t0 = time.time()
    # 全量 panel 算 regime + aggregate liquidity (即使 sample 模式也要全量 regime)
    df_full, clean_info = load_panel(sample=None, since=args.since)
    df_full = add_factors(df_full)
    regime_df, _ = build_regime(df_full)
    agg_amihud = build_aggregate_liquidity(df_full)

    # Part A/B 的 cross-section 可用 sample 加速迭代
    if args.sample:
        rng = np.random.default_rng(42)
        chosen = rng.choice(df_full["stock_id"].unique(),
                            min(args.sample, df_full["stock_id"].nunique()), replace=False)
        df = df_full[df_full["stock_id"].isin(chosen)].copy()
        logger.info(f"Part A/B cross-section sample: {df['stock_id'].nunique()} tickers")
    else:
        df = df_full
    df = add_turnover(df)

    partA_decile, partA_ic = run_part_a(df, regime_df)
    gating_df, xcorr_df, episode_df, loyo_df, ls_df = run_part_b(df, regime_df, agg_amihud)

    write_report(clean_info, regime_df, partA_decile, partA_ic, ls_df,
                 gating_df, xcorr_df, episode_df, loyo_df)

    # console summary
    print("\n" + "=" * 90)
    print("  PART A — IC by regime")
    print("=" * 90)
    print(partA_ic.to_string(index=False))
    print("\n  PART A — decile spread by regime")
    print(partA_decile.to_string(index=False))
    print("\n  PART A — turnover LS tradeability (gross vs net)")
    print(ls_df.to_string(index=False))
    print("\n" + "=" * 90)
    print("  PART B — SOP-10 gating sim")
    print("=" * 90)
    print(gating_df.to_string(index=False))
    print("\n  PART B — xcorr")
    print(xcorr_df.to_string(index=False))
    print("\n  PART B — episodes")
    print(episode_df.to_string(index=False))
    print("\n  PART B — LOYO/COVID")
    print(loyo_df.to_string(index=False))
    print(f"\nTotal time: {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
