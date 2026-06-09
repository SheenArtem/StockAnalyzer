"""
格2 量條件化動量 (Volume-Conditioned Momentum) 正式驗證 (2026-06-08)
====================================================================

學術根據: Lee & Swaminathan (2000) "Price Momentum and Trading Volume" (JF).
核心假設: 同樣幅度的前段價格移動, 在爆量 vs 量縮下後向報酬意義相反 ——
  帶量的移動是「資訊」(續勢), 量縮的移動是「流動性」(反轉)。

驗證對象: prior_ret (前段報酬, 5d/20d) × RVOL (量狀態) 的交互作用是否在台股
有可上線 alpha, 且其成本後淨 spread 是否顯著贏過純 RVOL baseline。

**沿用 harness**: import tools/rvol_atr_validate.py 的 load_panel / add_liquidity /
liquidity_mask / compute_decile_returns / daily_ic / ic_summary / decile_spread /
monotonic_score + 常數 LIQUIDITY_TIERS / COST_ROUNDTRIP_TIERS。RVOL 計算沿用
tools/indicator_ic_analysis._compute_one_ticker 的 sig_rvol_log。

**必須超越的 baseline**: 純 RVOL 因子在 reports/rvol_atr_* 已驗為 MARGINAL
(liq_50m 成本後淨 spread ~0)。交互版淨 spread 必須顯著贏純 RVOL 才上線。

輸出 (reports/, line2_volcond_* 前綴):
  - line2_volcond_double_sort.csv      5x5 (prior_ret × RVOL) fwd_20d 均值表 (各 W × 流動性)
  - line2_volcond_decile.csv           交互分數 decile 1-10 (各 horizon × 流動性)
  - line2_volcond_gauntlet.csv         IC/IR/gross/net spread/mono + vs RVOL baseline
  - line2_volcond_walkforward.csv      年度 IC + LS spread + sign-stability
  - line2_volcond_robust.csv           LOYO / ex-2020 / cross-regime / deflated sharpe
  - line2_vol_conditioned.md           報告 (含 5x5 雙重排序表)

用法:
    python tools/line2_vol_conditioned_validate.py --sample 300   # 迭代
    python tools/line2_vol_conditioned_validate.py                # 全量 (最終數字)
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

# 沿用 RVOL 驗證 harness (loader / liquidity / decile / IC / spread / 常數)
from tools.rvol_atr_validate import (
    load_panel, add_liquidity, liquidity_mask,
    compute_decile_returns, daily_ic, ic_summary,
    decile_spread, monotonic_score,
    LIQUIDITY_TIERS, COST_ROUNDTRIP_TIERS,
    MIN_CROSS_SECTION, MIN_CROSS_SECTION_IC, N_DECILES,
)

PARQUET_PATH = _ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"
REGIME_LOG = _ROOT / "data" / "tracking" / "regime_log.jsonl"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# 本驗證固定 horizon (前段報酬訊號的資訊半衰期短-中, 對齊 RVOL baseline 的 h=10/20 + 加 60 看延伸)
HORIZONS = [10, 20, 60]
PRIOR_WINDOWS = [5, 20]          # 前段報酬窗口 (短/中)
N_SORT = 5                       # 5x5 雙重排序
RVOL_WINDOW = 20                 # RVOL 滾動均量窗口 (沿用 _compute_one_ticker)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("line2")


# ============================================================
# 1. 因子建構 (volume-conditioned momentum)
# ============================================================
def _drop_frozen_volume(df):
    """剔除 Volume<=0 凍結列 (停牌參考價填充)。
    這些列的 RVOL = 0/rolling_mean → log clip 到 -4.6 (假極低量),
    且恢復交易日 rolling_mean 被 0 拉低 → RVOL 爆高。計算量特徵前必剔。
    披露於報告。"""
    n_before = len(df)
    n_frozen = (df["Volume"] <= 0).sum()
    df = df[df["Volume"] > 0].copy()
    logger.info(f"Dropped {n_frozen:,} frozen rows (Volume<=0) / {n_before:,} total "
                f"({n_frozen/n_before*100:.2f}%)")
    return df


def compute_factors(df):
    """逐 ticker 計算 prior_ret (5d/20d) + RVOL (log clip) + turnover 代理。
    全部 PIT: prior_ret 用截至 t 的 Close (含 t), 不含未來; RVOL 用截至 t 的量。"""
    logger.info("Computing volume-conditioned momentum factors...")
    t0 = time.time()

    def _one(sub):
        sub = sub.sort_values("date")
        close = sub["Close"]
        vol = sub["Volume"]
        amount = close * vol

        # --- prior_ret (前段報酬, PIT, 截至 t) ---
        for w in PRIOR_WINDOWS:
            # close[t] / close[t-w] - 1 ; 不 shift, 截至當日收盤已知
            sub[f"prior_ret_{w}d"] = close.pct_change(w, fill_method=None)

        # --- RVOL = Volume / 20日均量 (沿用 _compute_one_ticker 定義), log clip ---
        rvol = vol / vol.rolling(RVOL_WINDOW).mean()
        sub["rvol_log"] = np.log(rvol.clip(lower=0.01))

        # --- turnover 代理: 當日成交額相對自身 60日中位數水位 (panel 無流通股數) ---
        # log(amount / amount.rolling(60).median()) — 個股自身放量倍數, 跨股可比 (已去 size)
        amt_med60 = amount.rolling(60).median()
        sub["turnover_proxy"] = np.log((amount / amt_med60).clip(lower=0.01))

        return sub

    df = df.groupby("yf_ticker", group_keys=False).apply(_one)
    logger.info(f"Factors done in {time.time()-t0:.1f}s")
    return df


def add_fwd_returns(df):
    """前向報酬 (clip, fill_method=None) — 沿用 rvol_atr_validate 的 clip 邏輯。"""
    logger.info("Forward returns (raw Close)...")
    df = df.sort_values(["yf_ticker", "date"]).reset_index(drop=True)
    for h in HORIZONS:
        r = df.groupby("yf_ticker")["AdjClose"].pct_change(h, fill_method=None).shift(-h)
        r = r.replace([np.inf, -np.inf], np.nan)
        df[f"fwd_{h}d"] = r.where((r > -0.95) & (r < 5.0))
    return df


def rank_normalize(df, cols):
    """每日橫截面 rank 正規化到 [-1, +1] (沿用 indicator_combo_analysis 作法)。"""
    for col in cols:
        df[f"{col}_rn"] = df.groupby("date")[col].transform(
            lambda s: (s.rank(pct=True) - 0.5) * 2
        )
    return df


def build_interaction_scores(df):
    """建構量條件化交互分數 (tradeable 形式)。

    雙重排序假說: 高 prior_ret + 高量 = 續勢(做多); 高 prior_ret + 低量 = 反轉(做空)。
    對稱地: 低 prior_ret + 高量 = 續跌(做空); 低 prior_ret + 低量 = 反彈(做多)。

    → 可交易交互分數 = prior_ret_rn × rvol_rn (兩個 [-1,1] rank-norm 的乘積)
      高分(>0): (高 prior + 高量) 或 (低 prior + 低量) → 預期續勢方向 (做多訊號)
      低分(<0): (高 prior + 低量) 或 (低 prior + 高量) → 預期反轉方向 (放空訊號)
      若假說成立, 此交互分數對 fwd_ret 應有正 IC (高分→漲, 低分→跌)。

    另建「方向化動量 × 量確認」變體:
      vol_confirmed_mom = sign(prior_ret) × rvol_rn
      = 上漲且帶量(+) / 上漲但量縮(-) / 下跌且帶量(-) / 下跌但量縮(+)
      捕捉「帶量move=資訊續勢 / 量縮move=流動性反轉」更直接, 但用 sign 丟失幅度。
    """
    logger.info("Building interaction scores...")
    scores = {}
    for w in PRIOR_WINDOWS:
        pr = f"prior_ret_{w}d_rn"
        # 主交互分數: rank-norm 乘積
        col = f"interact_{w}d"
        df[col] = df[pr] * df["rvol_log_rn"]
        scores[col] = f"prior_ret({w}d)×RVOL 交互 (乘積)"

        # 方向化動量 × 量確認 (sign × rvol)
        col2 = f"volconf_mom_{w}d"
        df[col2] = np.sign(df[f"prior_ret_{w}d"]) * df["rvol_log_rn"]
        scores[col2] = f"sign(prior_ret {w}d)×RVOL 量確認"

        # turnover 版交互 (代理量)
        col3 = f"interact_turn_{w}d"
        df[col3] = df[pr] * df["turnover_proxy_rn"]
        scores[col3] = f"prior_ret({w}d)×turnover 交互"

    # baseline 對照: 純 RVOL (rank-norm) + 純動量 (rank-norm)
    df["rvol_only"] = df["rvol_log_rn"]
    scores["rvol_only"] = "純 RVOL baseline (對照)"
    for w in PRIOR_WINDOWS:
        df[f"mom_only_{w}d"] = df[f"prior_ret_{w}d_rn"]
        scores[f"mom_only_{w}d"] = f"純動量 {w}d baseline (對照)"

    return scores


# ============================================================
# 2. 5x5 雙重排序 (double sort)
# ============================================================
def double_sort(df, prior_col, vol_col, ret_col, mask, n=N_SORT):
    """每日 5x5 雙重排序 (prior_ret 列 × vol 行), 各格填 ret_col 均值。
    回傳: (table DataFrame[n×n], n_table[n×n] 每格樣本日數)。
    獨立排序 (independent sort): 各維度分別 qcut, 不條件化。"""
    x = df[mask][[prior_col, vol_col, ret_col, "date"]].dropna()

    def _bucket(g, col):
        if len(g) < MIN_CROSS_SECTION:
            return pd.Series([np.nan] * len(g), index=g.index)
        return pd.qcut(g[col].rank(method="first"), n, labels=False, duplicates="drop")

    x["p_bin"] = x.groupby("date", group_keys=False).apply(lambda g: _bucket(g, prior_col))
    x["v_bin"] = x.groupby("date", group_keys=False).apply(lambda g: _bucket(g, vol_col))
    x = x.dropna(subset=["p_bin", "v_bin"])
    x["p_bin"] = x["p_bin"].astype(int)
    x["v_bin"] = x["v_bin"].astype(int)

    # 每日 × 每格均值 → 跨日平均 (equal-weight by day, 避免大樣本日 dominate)
    daily = x.groupby(["date", "p_bin", "v_bin"])[ret_col].mean().reset_index()
    cell = daily.groupby(["p_bin", "v_bin"])[ret_col].agg(["mean", "count"]).reset_index()

    table = cell.pivot(index="p_bin", columns="v_bin", values="mean")
    n_table = cell.pivot(index="p_bin", columns="v_bin", values="count")
    return table, n_table


def analyze_double_sort(table):
    """從 5x5 表萃取關鍵診斷:
    - 最高 prior_ret 列 (top row) 的 fwd 報酬, 隨 vol bucket (低→高) 的變化
    - 是否符號翻轉 (低量列右端 vs 左端)
    - 「高prior×高量」(右上角) - 「高prior×低量」(左上角) 的續勢-反轉 spread (LS 對角)
    """
    n = table.shape[0]
    top_row = table.iloc[n - 1]          # 最高 prior_ret 列
    bot_row = table.iloc[0]              # 最低 prior_ret 列
    diag = {
        "hi_prior_hi_vol": float(table.iloc[n - 1, n - 1]),   # 右上: 高動能高量 (續勢做多)
        "hi_prior_lo_vol": float(table.iloc[n - 1, 0]),       # 左上: 高動能低量 (反轉做空)
        "lo_prior_hi_vol": float(table.iloc[0, n - 1]),       # 右下: 低動能高量 (續跌做空)
        "lo_prior_lo_vol": float(table.iloc[0, 0]),           # 左下: 低動能低量 (反彈做多)
    }
    # 假說核心: 高 prior_ret 列, 高量端 - 低量端 應 > 0 (帶量續勢 > 量縮)
    diag["top_row_hivol_minus_lovol"] = float(top_row.iloc[n - 1] - top_row.iloc[0])
    diag["bot_row_hivol_minus_lovol"] = float(bot_row.iloc[n - 1] - bot_row.iloc[0])
    # 高量列是否單調 prior (續勢): 高量行內 prior 由低到高的 spearman
    hi_vol_col = table.iloc[:, n - 1]
    lo_vol_col = table.iloc[:, 0]
    diag["hivol_col_mono"] = float(stats.spearmanr(range(n), hi_vol_col.values)[0])
    diag["lovol_col_mono"] = float(stats.spearmanr(range(n), lo_vol_col.values)[0])
    # 可交易交互 LS: (右上 + 左下) - (左上 + 右下) = 續勢腿 - 反轉腿
    diag["interact_LS_diag"] = float(
        (diag["hi_prior_hi_vol"] + diag["lo_prior_lo_vol"])
        - (diag["hi_prior_lo_vol"] + diag["lo_prior_hi_vol"])
    )
    return diag


# ============================================================
# 3. Top-N portfolio (long-only & long-short)
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
# 4. Deflated Sharpe (Bailey & Lopez de Prado 2014)
# ============================================================
def deflated_sharpe(sr_observed, n_obs, n_trials, skew=0.0, kurt=3.0):
    """Deflated Sharpe Ratio.
    sr_observed: 觀測到的 (非年化) per-period Sharpe (mean/std of period returns)。
    n_obs: period return 樣本數。
    n_trials: 試了幾個策略變體 (multiple-testing haircut)。
    回傳 (DSR, sr0_threshold)。
    sr0 = 期望最大 Sharpe under null (多重檢定下純運氣的最大 SR)。"""
    if n_obs < 30 or n_trials < 1:
        return np.nan, np.nan
    # 期望最大 SR (Bailey-LdP): sr0 = E[max] ≈ sqrt(Var)*((1-γ)*Z^-1(1-1/N) + γ*Z^-1(1-1/(N*e)))
    # Var of SR estimates across trials 用 1/n_obs 近似 (假設各 trial SR 變異 ~ 1/sqrt(T))
    euler = 0.5772156649
    emax = (
        (1 - euler) * stats.norm.ppf(1 - 1.0 / n_trials)
        + euler * stats.norm.ppf(1 - 1.0 / (n_trials * np.e))
    )
    sr0 = emax / np.sqrt(n_obs)   # variance of SR under null ≈ 1/n_obs (SR per-period)
    # DSR: P(true SR > sr0 | observed)
    denom = np.sqrt(1 - skew * sr_observed + (kurt - 1) / 4.0 * sr_observed ** 2)
    z = (sr_observed - sr0) * np.sqrt(n_obs - 1) / denom
    dsr = stats.norm.cdf(z)
    return float(dsr), float(sr0)


def ls_period_returns(df, score_col, return_col, mask, direction, n_decile=N_DECILES):
    """回傳 long-short 每日(每期) decile spread 報酬時序 (用於 deflated sharpe)。
    direction='top': long D10 short D1; 'bot': long D1 short D10。"""
    x = df[mask][[score_col, return_col, "date"]].dropna()

    def _bucket(g):
        if len(g) < MIN_CROSS_SECTION:
            return pd.Series([np.nan] * len(g), index=g.index)
        return pd.qcut(g[score_col].rank(method="first"), n_decile,
                       labels=False, duplicates="drop")

    x["dec"] = x.groupby("date", group_keys=False).apply(_bucket)
    x = x.dropna(subset=["dec"])
    x["dec"] = x["dec"].astype(int) + 1
    daily = x.groupby(["date", "dec"])[return_col].mean().reset_index()
    piv = daily.pivot(index="date", columns="dec", values=return_col)
    if 1 not in piv.columns or n_decile not in piv.columns:
        return pd.Series(dtype=float)
    if direction == "top":
        ls = piv[n_decile] - piv[1]
    else:
        ls = piv[1] - piv[n_decile]
    return ls.dropna()


# ============================================================
# 5. Main
# ============================================================
def _signed(score_col):
    """交互/動量/RVOL 預期方向: 全部 'top' (高分做多)。
    交互分數設計成高分=續勢方向, 故 top。"""
    return "top"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=None)
    ap.add_argument("--since", type=str, default=None)
    args = ap.parse_args()

    t0 = time.time()
    df = load_panel(sample=args.sample, since=args.since)
    df = _drop_frozen_volume(df)
    df = compute_factors(df)
    df = add_fwd_returns(df)
    df = add_liquidity(df)
    df = rank_normalize(df, [f"prior_ret_{w}d" for w in PRIOR_WINDOWS]
                        + ["rvol_log", "turnover_proxy"])
    scores = build_interaction_scores(df)

    # 焦點 tradeable 分數 (跑完整 gauntlet); baseline 對照另列
    focus = [c for c in scores if c.startswith(("interact_", "volconf_mom_"))
             and not c.startswith("interact_turn_")]
    baselines = ["rvol_only"] + [f"mom_only_{w}d" for w in PRIOR_WINDOWS]
    turn_variants = [c for c in scores if c.startswith("interact_turn_")]
    all_eval = focus + turn_variants + baselines

    # ---------- A. 5x5 雙重排序 (核心: 符號是否翻轉) ----------
    logger.info("=== A. 5x5 double sort (prior_ret × RVOL) ===")
    ds_rows = []
    ds_tables = {}   # for MD 報告
    for w in PRIOR_WINDOWS:
        for tier_name, tier_val in [("all", None), ("liq_50m", 5e7)]:
            mask = liquidity_mask(df, tier_val)
            table, ntab = double_sort(
                df, f"prior_ret_{w}d", "rvol_log", "fwd_20d", mask, n=N_SORT
            )
            ds_tables[(w, tier_name)] = (table, ntab)
            diag = analyze_double_sort(table)
            # 攤平 5x5 表存 CSV
            for p in range(N_SORT):
                for v in range(N_SORT):
                    ds_rows.append({
                        "prior_window": w, "liquidity": tier_name,
                        "prior_bin": p + 1, "vol_bin": v + 1,
                        "fwd_20d_mean": float(table.iloc[p, v]),
                        "n_days": int(ntab.iloc[p, v]) if not pd.isna(ntab.iloc[p, v]) else 0,
                    })
            # diag 另存
            for k, val in diag.items():
                ds_rows.append({
                    "prior_window": w, "liquidity": tier_name,
                    "prior_bin": -1, "vol_bin": -1,
                    "fwd_20d_mean": val, "n_days": 0, "diag_key": k,
                })
    ds_df = pd.DataFrame(ds_rows)
    ds_df.to_csv(OUT_DIR / "line2_volcond_double_sort.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved double_sort ({len(ds_df)})")

    # ---------- B. Decile + IC + gross/net spread gauntlet ----------
    logger.info("=== B. Gauntlet: IC / decile / net spread ===")
    decile_rows, gaunt_rows = [], []
    for tier_name, tier_val in LIQUIDITY_TIERS:
        mask = liquidity_mask(df, tier_val)
        for score_col in all_eval:
            d = _signed(score_col)
            for h in HORIZONS:
                ret = f"fwd_{h}d"
                # decile
                summ = compute_decile_returns(df, score_col, ret, mask)
                for _, r in summ.iterrows():
                    decile_rows.append({
                        "score": score_col, "label": scores[score_col],
                        "liquidity": tier_name, "horizon": h,
                        "decile": int(r["decile"]), "mean_ret": r["mean_ret"],
                        "win_rate": r["win_rate"], "n_days": int(r["n_days"]),
                    })
                # IC
                ic = daily_ic(df, score_col, ret, mask)
                ics = ic_summary(ic)
                gross = decile_spread(summ, d)
                mono = monotonic_score(summ)
                for cost in COST_ROUNDTRIP_TIERS:
                    net = gross - 2 * cost
                    gaunt_rows.append({
                        "score": score_col, "label": scores[score_col],
                        "liquidity": tier_name, "horizon": h, "direction": d,
                        "ic_mean": ics["mean"], "ic_ir": ics["ir"],
                        "ic_win": ics["win"], "ic_t": ics["t"], "ic_n": ics["n"],
                        "gross_spread": gross, "cost_roundtrip": cost,
                        "net_spread": net, "monotonic_rho": mono,
                    })
    decile_df = pd.DataFrame(decile_rows)
    gaunt_df = pd.DataFrame(gaunt_rows)
    decile_df.to_csv(OUT_DIR / "line2_volcond_decile.csv", index=False, encoding="utf-8-sig")
    gaunt_df.to_csv(OUT_DIR / "line2_volcond_gauntlet.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved decile ({len(decile_df)}) + gauntlet ({len(gaunt_df)})")

    # vs RVOL baseline net-spread delta (核心判準: 交互版淨 spread 是否贏純 RVOL)
    rvol_net = gaunt_df[gaunt_df["score"] == "rvol_only"].set_index(
        ["liquidity", "horizon", "cost_roundtrip"])["net_spread"]
    gaunt_df["net_vs_rvol"] = gaunt_df.apply(
        lambda r: r["net_spread"] - rvol_net.get(
            (r["liquidity"], r["horizon"], r["cost_roundtrip"]), np.nan), axis=1)
    gaunt_df.to_csv(OUT_DIR / "line2_volcond_gauntlet.csv", index=False, encoding="utf-8-sig")

    # ---------- C. Walk-forward annual (IC + LS spread + sign-stability) ----------
    logger.info("=== C. Walk-forward annual ===")
    df["year"] = df["date"].dt.year
    wf_rows = []
    for tier_name, tier_val in [("all", None), ("liq_50m", 5e7)]:
        base_mask = liquidity_mask(df, tier_val)
        for score_col in focus + ["rvol_only"]:
            d = _signed(score_col)
            for yr in sorted(df["year"].unique()):
                yr_mask = base_mask & (df["year"] == yr)
                if yr_mask.sum() < 1000:
                    continue
                ic = daily_ic(df, score_col, "fwd_20d", yr_mask)
                summ = compute_decile_returns(df, score_col, "fwd_20d", yr_mask)
                spread = decile_spread(summ, d)
                ics = ic_summary(ic)
                wf_rows.append({
                    "score": score_col, "label": scores[score_col],
                    "liquidity": tier_name, "year": int(yr),
                    "ic_mean": ics["mean"], "ic_ir": ics["ir"],
                    "ls_spread_20d": spread, "ic_n": ics["n"],
                })
    wf_df = pd.DataFrame(wf_rows)
    wf_df.to_csv(OUT_DIR / "line2_volcond_walkforward.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved walkforward ({len(wf_df)})")

    # ---------- D. Robustness: LOYO / ex-2020 / cross-regime / deflated sharpe ----------
    logger.info("=== D. Robustness ===")
    robust_rows = []
    # 載入 regime log
    try:
        regime = pd.read_json(REGIME_LOG, lines=True)[["date", "regime"]]
        regime["date"] = pd.to_datetime(regime["date"])
        df = df.merge(regime, on="date", how="left")
        has_regime = True
    except Exception as e:
        logger.warning(f"Regime log load failed: {e}")
        has_regime = False

    for tier_name, tier_val in [("liq_50m", 5e7)]:   # robustness 聚焦可交易池
        base_mask = liquidity_mask(df, tier_val)
        for score_col in focus + ["rvol_only"]:
            d = _signed(score_col)
            ret = "fwd_20d"

            # (1) Full-sample LS spread + deflated sharpe
            ls = ls_period_returns(df, score_col, ret, base_mask, d)
            if len(ls) >= 30:
                sr_per = ls.mean() / ls.std(ddof=1) if ls.std(ddof=1) > 0 else 0
                # n_trials: 焦點分數變體數 (2 prior window × 2 form) + turnover(2) + 純對照(3) = 9
                n_trials = len(all_eval)
                dsr, sr0 = deflated_sharpe(sr_per, len(ls), n_trials,
                                           skew=float(stats.skew(ls)),
                                           kurt=float(stats.kurtosis(ls, fisher=False)))
                robust_rows.append({
                    "test": "full_deflated_sharpe", "score": score_col,
                    "liquidity": tier_name, "metric": "dsr",
                    "value": dsr, "extra": f"sr_per={sr_per:.4f} sr0={sr0:.4f} "
                    f"n_obs={len(ls)} n_trials={n_trials} ann_sr={sr_per*np.sqrt(252/20):.3f}",
                })

            # (2) ex-2020 COVID
            ex2020 = base_mask & (df["year"] != 2020)
            ls_ex = ls_period_returns(df, score_col, ret, ex2020, d)
            spread_ex = float(ls_ex.mean()) if len(ls_ex) else np.nan
            ls_full_mean = float(ls.mean()) if len(ls) else np.nan
            robust_rows.append({
                "test": "ex_2020", "score": score_col, "liquidity": tier_name,
                "metric": "ls_mean_spread", "value": spread_ex,
                "extra": f"full={ls_full_mean:.5f} delta={spread_ex-ls_full_mean:+.5f}",
            })

            # (3) LOYO: 逐年剔除, 看 LS spread 最差/最好 (edge 是否靠單一年)
            loyo_spreads = []
            for yr in sorted(df["year"].dropna().unique()):
                m = base_mask & (df["year"] != yr)
                lsy = ls_period_returns(df, score_col, ret, m, d)
                if len(lsy) >= 30:
                    loyo_spreads.append((int(yr), float(lsy.mean())))
            if loyo_spreads:
                vals = [v for _, v in loyo_spreads]
                worst = min(loyo_spreads, key=lambda x: x[1])
                best = max(loyo_spreads, key=lambda x: x[1])
                robust_rows.append({
                    "test": "loyo", "score": score_col, "liquidity": tier_name,
                    "metric": "ls_mean_spread_range", "value": float(np.mean(vals)),
                    "extra": f"worst(drop{worst[0]})={worst[1]:.5f} "
                    f"best(drop{best[0]})={best[1]:.5f} all_positive={all(v>0 for v in vals)}",
                })

            # (4) Cross-regime
            if has_regime:
                for reg in ["trending", "ranging", "volatile", "neutral"]:
                    m = base_mask & (df["regime"] == reg)
                    if m.sum() < 1000:
                        continue
                    ic_r = daily_ic(df, score_col, ret, m)
                    summ_r = compute_decile_returns(df, score_col, ret, m)
                    sp_r = decile_spread(summ_r, d)
                    icr = ic_summary(ic_r)
                    robust_rows.append({
                        "test": f"regime_{reg}", "score": score_col, "liquidity": tier_name,
                        "metric": "ic_and_spread", "value": icr["mean"],
                        "extra": f"ls_spread={sp_r:.5f} ic_ir={icr['ir']:.3f} n_days={icr['n']}",
                    })
    robust_df = pd.DataFrame(robust_rows)
    robust_df.to_csv(OUT_DIR / "line2_volcond_robust.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved robust ({len(robust_df)})")

    # ---------- Console summary ----------
    _print_summary(df, scores, focus, baselines, turn_variants,
                   ds_tables, gaunt_df, wf_df, robust_df)
    logger.info(f"Total time: {(time.time()-t0)/60:.1f} min")


def _print_summary(df, scores, focus, baselines, turn_variants,
                   ds_tables, gaunt_df, wf_df, robust_df):
    print("\n" + "=" * 100)
    print("  A. 5x5 DOUBLE SORT — fwd_20d mean (%) by prior_ret(row) × RVOL(col)")
    print("     列=prior_ret 由低(1)到高(5); 行=RVOL 由低(1)到高(5)")
    print("=" * 100)
    for (w, tier), (table, _) in ds_tables.items():
        print(f"\n  [prior_window={w}d, liquidity={tier}]")
        print("            " + "".join([f"  RVOL{v+1:>6}" for v in range(N_SORT)]))
        for p in range(N_SORT):
            row = "".join([f"{table.iloc[p, v]*100:>+9.3f}" for v in range(N_SORT)])
            print(f"  prior{p+1:>2}  {row}")
        diag = analyze_double_sort(table)
        print(f"    -> 高prior列 高量-低量 spread: {diag['top_row_hivol_minus_lovol']*100:+.3f}%"
              f"  (>0=帶量續勢假說成立)")
        print(f"    -> 高量行單調(prior↑→ret↑): rho={diag['hivol_col_mono']:+.2f}"
              f"  低量行單調: rho={diag['lovol_col_mono']:+.2f}")
        print(f"    -> 交互對角 LS (續勢腿-反轉腿): {diag['interact_LS_diag']*100:+.3f}%")

    print("\n" + "=" * 100)
    print("  B. GAUNTLET (h=20) — IC / gross / net spread vs RVOL baseline")
    print("=" * 100)
    print(f"  {'score':<20}{'liq':<14}{'IC':>9}{'IR':>7}{'gross%':>9}"
          f"{'net@.25%':>10}{'vsRVOL':>9}{'mono':>7}")
    sub = gaunt_df[(gaunt_df["horizon"] == 20) & (gaunt_df["cost_roundtrip"] == 0.0025)]
    for _, r in sub.sort_values(["liquidity", "score"]).iterrows():
        print(f"  {r['score']:<20}{r['liquidity']:<14}{r['ic_mean']:>+9.4f}"
              f"{r['ic_ir']:>+7.2f}{r['gross_spread']*100:>+9.3f}"
              f"{r['net_spread']*100:>+10.3f}{r['net_vs_rvol']*100:>+9.3f}"
              f"{r['monotonic_rho']:>+7.2f}")

    print("\n" + "=" * 100)
    print("  C. WALK-FORWARD (liq_50m, 20d) — sign-stability")
    print("=" * 100)
    for score_col in focus + ["rvol_only"]:
        s = wf_df[(wf_df["score"] == score_col) & (wf_df["liquidity"] == "liq_50m")]
        if s.empty:
            continue
        # 排除部分年 2026
        s2 = s[s["year"] <= 2025]
        pos_ic = (s2["ic_mean"] > 0).sum()
        pos_sp = (s2["ls_spread_20d"] > 0).sum()
        n = len(s2)
        print(f"  {score_col:<20} signed-IC+ {pos_ic}/{n}  LS-spread+ {pos_sp}/{n}  "
              f"(年均 IC {s2['ic_mean'].mean():+.4f}, 年均 LS {s2['ls_spread_20d'].mean()*100:+.3f}%)")

    print("\n" + "=" * 100)
    print("  D. ROBUSTNESS (liq_50m)")
    print("=" * 100)
    for score_col in focus + ["rvol_only"]:
        print(f"\n  [{score_col}]")
        for _, r in robust_df[robust_df["score"] == score_col].iterrows():
            print(f"    {r['test']:<22} {r['metric']:<22} {r['value']:>+10.5f}  {r['extra']}")


if __name__ == "__main__":
    main()
