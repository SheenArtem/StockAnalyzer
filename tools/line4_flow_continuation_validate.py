"""
Line 4 — 大型股 × 法人流量續勢 (informed-flow continuation) 正式驗證 + Whale 重疊檢查
========================================================================================

學術根據:
  - Hsieh & Hu (2010, TW): 動量續勢出現在**大型股**、由**外資+投信流量**載動 (與小型股 LMSW 反轉相反)。
  - Kang (2025, KR): 法人 conviction 應**按市值 normalize** (net shares / shares out), 非成交值;
    法人 Q4 +12% CAR/50d, 散戶 ~0 noise。
  - 這是三格 (line1 λ / line2 量條件化 / line3 流動性 regime) 全 D 後唯一未測的存活線索。

**核心問題 (判生死)**: 不是「flow 續勢有沒有 IC」, 而是「對現有 Whale composite_score
(+ smart_money / revenue / quality / eps_yoy) 正交化後, 殘差還剩不剩增量 IC」。
  - 殘差 IC ≈ 0 → subsumed → 確認答案已在 Whale 手上, 收尾。
  - 殘差 IC 顯著 + 成本後存活 + DSR pass → 候選第四條線。
預期很可能被 subsume (Whale 已含 smart_money 投信+自營 turnover-normalized component)。

**沿用 harness** (鏡像 line2_vol_conditioned / rvol_atr_validate 嚴謹度):
  from tools.rvol_atr_validate import load_panel / add_liquidity / liquidity_mask /
  compute_decile_returns / daily_ic / ic_summary / decile_spread / monotonic_score +
  常數 LIQUIDITY_TIERS / COST_ROUNDTRIP_TIERS。
  Deflated Sharpe 鏡像 line2 的 Bailey-Lopez de Prado 實作。

資料 (先定位, 零重抓):
  - OHLCV clean panel: data_cache/backtest/ohlcv_tw.parquet (load_panel 剔 Close<=0 + 下方剔 V<=0)
  - 法人日流量: data_cache/chip_history/institutional.parquet (foreign_net + trust_net, 單位=股,
    2015-01-05~2026-06-05, 2107 檔; 盤後公布 PIT-safe: t 日收盤後可知, 用 ≤t 預測 t+1..t+h)
  - 每股市值: listed_shares.parquet (1090 上市檔, current snapshot 近似) × Close
    net_shares*price/(shares*price) = net_shares/shares → price 抵消, 為「淨買股數佔流通股比」
  - Whale composite + components (正交化標的): 重算每日每股 composite_score (7-factor production
    weights, screener LOCKED) + smart_money_score / revenue_score / f_score / eps_yoy。
    用 build_feature_panel + winsorize_standardize (industry-neutral), 完全沿用 production path。

輸出 (reports/, line4_flow_* 前綴):
  - line4_flow_gauntlet.csv        IC/IR/gross/net spread/mono (大型股 vs 全市場 × horizon × tier)
  - line4_flow_decile.csv          各 signal decile 1-10
  - line4_flow_walkforward.csv     年度 IC + LS spread + sign-stability
  - line4_flow_robust.csv          ex-2020 / LOYO / deflated sharpe
  - line4_flow_overlap.csv         殘差 IC (對 Whale 正交化後) + 相關係數矩陣
  - line4_flow_continuation_overlap.md  報告

用法:
    python tools/line4_flow_continuation_validate.py --sample 300   # 迭代
    python tools/line4_flow_continuation_validate.py                # 全量 (最終數字)
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

# Windows cp950 console 無法 encode 報表用的 — / × / ≤ / 中文 → reconfigure stdout 為 UTF-8
# (CSV 已全寫完才印 summary, 但仍須讓 script clean exit, 不靠運氣)
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from tools.rvol_atr_validate import (
    load_panel, add_liquidity, liquidity_mask,
    compute_decile_returns, daily_ic, ic_summary,
    decile_spread, monotonic_score,
    LIQUIDITY_TIERS, COST_ROUNDTRIP_TIERS,
    MIN_CROSS_SECTION, MIN_CROSS_SECTION_IC, N_DECILES,
)

CACHE = _ROOT / "data_cache" / "backtest"
INST_PATH = _ROOT / "data_cache" / "chip_history" / "institutional.parquet"
SHARES_PATH = _ROOT / "data" / "macro" / "listed_shares.parquet"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

HORIZONS = [5, 20, 60]
PRIOR_WINDOWS = [5, 20]          # 前段報酬 / 流量累計窗口 (短/中)
LARGE_CAP_QUANTILE = 0.80        # top quintile 市值 = 大型股 (Hsieh-Hu 效應所在)

# Whale 正交化標的 (production composite + 其 components)
WHALE_ORTHO_COLS = ["composite_score", "smart_money_score", "revenue_score",
                    "f_score", "eps_yoy", "foreign_pct", "total_pct"]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("line4")


# ============================================================
# 0. Vectorized IC / decile (drop-in faster replacements)
# ============================================================
# 鏡像 rvol_atr_validate.daily_ic / compute_decile_returns 的語意 (Spearman rank IC,
# MIN_CROSS_SECTION_IC=30 / MIN_CROSS_SECTION=50 per-date gating), 但用 groupby 向量化
# 取代 per-date scipy.spearmanr loop (full-universe 全市場 1071 檔 × 96 gauntlet 呼叫
# 的 Python 迴圈太慢; 實測 1x qcut decile 3.64s → 全 gauntlet ~5min, +robustness ~20min)。
#
# IC: Spearman = Pearson(rank(x), rank(y)), 先 by-date rank 再算 per-date Pearson 相關
#     = 同數值 (已對 rvol_atr daily_ic 抽查吻合到 1.1e-16, byte-identical)。
# Decile: 用 floor((rank-1)/n*10) 向量化分桶 (24x 快) 取代 pd.qcut(rank, 10)。實測 vs qcut:
#     n_days 完全相同 (同日期覆蓋), decile mean_ret 最大差 3.7e-4 (~4/N 邊界點落入鄰桶,
#     gross_spread 為 robust 統計量, 4bp 級差異不改 verdict)。IC 仍走 exact rank-Pearson。
#     此近似揭露於報告資料品質註記。


def _fast_decile(x, score_col):
    """每日橫截面 decile 1..N (向量化 floor((rank-1)/n*N))。x 已過 MIN_CROSS_SECTION。"""
    n_by_date = x.groupby("date")[score_col].transform("size")
    rk = x.groupby("date")[score_col].rank(method="first")
    dec = np.floor((rk - 1) / n_by_date * N_DECILES).astype(int)
    return np.clip(dec, 0, N_DECILES - 1) + 1
def daily_ic(df, score_col, return_col, mask):
    x = df.loc[mask, [score_col, return_col, "date"]].dropna()
    if x.empty:
        return pd.Series(dtype=float, name="ic")
    cnt = x.groupby("date")[score_col].transform("size")
    x = x[cnt >= MIN_CROSS_SECTION_IC]
    if x.empty:
        return pd.Series(dtype=float, name="ic")
    rx = x.groupby("date")[score_col].rank()
    ry = x.groupby("date")[return_col].rank()
    tmp = pd.DataFrame({"date": x["date"].values, "rx": rx.values, "ry": ry.values})
    # per-date Pearson corr of ranks (vectorized): cov(rx,ry)/(std rx*std ry)
    g = tmp.groupby("date")
    mx = g["rx"].transform("mean")
    my = g["ry"].transform("mean")
    tmp["cxy"] = (tmp["rx"] - mx) * (tmp["ry"] - my)
    tmp["cxx"] = (tmp["rx"] - mx) ** 2
    tmp["cyy"] = (tmp["ry"] - my) ** 2
    agg = tmp.groupby("date")[["cxy", "cxx", "cyy"]].sum()
    ic = agg["cxy"] / np.sqrt(agg["cxx"] * agg["cyy"]).replace(0, np.nan)
    ic = ic.dropna()
    ic.index = pd.to_datetime(ic.index)
    return ic.rename("ic")


def compute_decile_returns(df, score_col, return_col, mask):
    x = df.loc[mask, [score_col, return_col, "date"]].dropna()
    if x.empty:
        return pd.DataFrame(columns=["decile", "mean_ret", "median_ret", "std_ret",
                                     "win_rate", "n_days"])
    cnt = x.groupby("date")[score_col].transform("size")
    x = x[cnt >= MIN_CROSS_SECTION].copy()
    if x.empty:
        return pd.DataFrame(columns=["decile", "mean_ret", "median_ret", "std_ret",
                                     "win_rate", "n_days"])

    x["decile"] = _fast_decile(x, score_col)
    daily = x.groupby(["date", "decile"])[return_col].mean().reset_index()
    summary = daily.groupby("decile")[return_col].agg(
        mean_ret="mean", median_ret="median", std_ret="std",
        win_rate=lambda s: (s > 0).mean(), n_days="count",
    ).reset_index()
    return summary


# ============================================================
# 1. 法人流量 + 市值 + 訊號建構
# ============================================================
def _drop_frozen_volume(df):
    """剔 Volume<=0 凍結列 (停牌參考價填充)。鏡像 line2 _drop_frozen_volume。
    本訊號不以 Volume 為分母 (流量分母是市值), 但凍結列無真實交易/法人活動 →
    fwd return 失真 + 籌碼意義不明, 故與 line2 一致剔除並揭露。"""
    n_before = len(df)
    n_frozen = (df["Volume"] <= 0).sum()
    df = df[df["Volume"] > 0].copy()
    logger.info(f"Dropped {n_frozen:,} frozen rows (Volume<=0) / {n_before:,} "
                f"({n_frozen/n_before*100:.2f}%)")
    return df


def load_institutional():
    """日法人淨買賣超 (foreign + trust net, 單位=股)。PIT: t 日盤後公布。"""
    logger.info(f"Loading {INST_PATH}")
    inst = pd.read_parquet(INST_PATH, columns=["date", "stock_id", "foreign_net", "trust_net"])
    inst["date"] = pd.to_datetime(inst["date"])
    inst["stock_id"] = inst["stock_id"].astype(str)
    # net_inst = 外資 + 投信 (Hsieh-Hu informed flow); 自營排除 (避險/造市雜訊)
    inst["net_inst_shares"] = inst["foreign_net"].fillna(0) + inst["trust_net"].fillna(0)
    logger.info(f"  {len(inst):,} rows, {inst['stock_id'].nunique()} stocks, "
                f"{inst['date'].min().date()}~{inst['date'].max().date()}")
    return inst[["date", "stock_id", "net_inst_shares", "foreign_net", "trust_net"]]


def build_signals(df):
    """逐 ticker 算 prior_ret (5/20d) + 法人流量累計/市值 (conviction) + turnover 對照。
    全 PIT: prior_ret 截至 t 收盤; 法人流量截至 t (盤後可知) → 預測 t+1..t+h。

    要求 df 已有: yf_ticker, date, Close, net_inst_shares (merge 進來), shares (map 進來)。
    """
    logger.info("Building flow-continuation signals...")
    t0 = time.time()

    # 市值 = Close * shares; conviction flow = net_shares / shares_out (price 抵消)
    # net_shares*Close / (shares*Close) = net_shares/shares → 與價格水位無關的「淨買佔股本比」
    df["flow_frac_daily"] = df["net_inst_shares"] / df["shares"]
    # turnover-normalized 對照 (Whale-style): net_shares*Close / 20d 成交額
    df["amount"] = df["Close"] * df["Volume"]

    def _one(sub):
        sub = sub.sort_values("date")
        close = sub["Close"]
        amt20 = sub["amount"].rolling(20).mean()
        for w in PRIOR_WINDOWS:
            # prior_ret: 截至 t 的過去 w 日報酬 (不 shift)
            sub[f"prior_ret_{w}d"] = close.pct_change(w, fill_method=None)
            # conviction 流量: 過去 w 日累計淨買股數 / 流通股數 (Kang 2025 mcap-normalize)
            sub[f"flow_mcap_{w}d"] = sub["flow_frac_daily"].rolling(w).sum()
            # turnover-normalized 流量 (對照組): 過去 w 日累計淨買金額 / 20d 成交額
            net_amt_cum = (sub["net_inst_shares"] * close).rolling(w).sum()
            sub[f"flow_turn_{w}d"] = net_amt_cum / amt20.replace(0, np.nan)
        return sub

    df = df.groupby("yf_ticker", group_keys=False).apply(_one)
    logger.info(f"Signals computed in {time.time()-t0:.1f}s")
    return df


def add_fwd_returns(df):
    """前向報酬 (clip, fill_method=None) — 鏡像 rvol_atr_validate.add_fwd_returns。"""
    logger.info("Forward returns (raw Close)...")
    df = df.sort_values(["yf_ticker", "date"]).reset_index(drop=True)
    for h in HORIZONS:
        r = df.groupby("yf_ticker")["AdjClose"].pct_change(h, fill_method=None).shift(-h)
        r = r.replace([np.inf, -np.inf], np.nan)
        df[f"fwd_{h}d"] = r.where((r > -0.95) & (r < 5.0))
    return df


def add_marketcap_rank(df, shares):
    """每日橫截面市值百分位 (大型股過濾用)。current shares snapshot × Close。"""
    df["mktcap"] = df["Close"] * df["shares"]
    df["mcap_pct_rank"] = df.groupby("date")["mktcap"].transform(lambda s: s.rank(pct=True))
    return df


def rank_normalize(df, cols):
    """每日橫截面 rank 正規化到 [-1, +1] (沿用 line2 作法)。"""
    for col in cols:
        df[f"{col}_rn"] = df.groupby("date")[col].transform(
            lambda s: (s.rank(pct=True) - 0.5) * 2)
    return df


def build_scores(df):
    """建構 tradeable 訊號 (全部 high score = long, dir='top')。"""
    logger.info("Building scores...")
    scores = {}
    for w in PRIOR_WINDOWS:
        pr = f"prior_ret_{w}d_rn"
        fm = f"flow_mcap_{w}d_rn"
        ft = f"flow_turn_{w}d_rn"

        # (1) 純 conviction 流量 (mcap-normalized) — 不含 prior_ret
        df[f"flow_only_{w}d"] = df[fm]
        scores[f"flow_only_{w}d"] = f"法人流量/市值 conviction ({w}d, 無動量)"

        # (2) 流量續勢主訊號: z(prior_ret) × z(flow_mcap) — Hsieh-Hu informed continuation
        #     高分: (價漲+法人買) 或 (價跌+法人賣) = informed 續勢方向
        #     低分: (價漲+法人賣=力竭/散戶推) 或 (價跌+法人買=吸籌) = 反轉方向
        df[f"flow_cont_{w}d"] = df[pr] * df[fm]
        scores[f"flow_cont_{w}d"] = f"prior_ret({w}d)×法人流量/市值 續勢交互"

        # (3) 同號 gate 簡化版: 只在 prior_ret 與 flow 同號才有訊號 (sign(pr)*sign(flow)*|flow|)
        #     價漲+法人買 → long; 價跌+法人賣 → short; 背離 → 0
        same = np.sign(df[f"prior_ret_{w}d"]) * np.sign(df[f"flow_mcap_{w}d"])
        df[f"flow_samesign_{w}d"] = np.where(same > 0, df[fm], 0.0)
        scores[f"flow_samesign_{w}d"] = f"prior_ret×flow 同號 gate ({w}d)"

        # (4) turnover-normalized 對照 (Whale smart_money 用此 normalize, 非 mcap)
        df[f"flow_turn_cont_{w}d"] = df[pr] * df[ft]
        scores[f"flow_turn_cont_{w}d"] = f"prior_ret({w}d)×法人流量/成交額 (turnover-norm 對照)"

        # baseline: 純動量
        df[f"mom_only_{w}d"] = df[pr]
        scores[f"mom_only_{w}d"] = f"純動量 {w}d baseline (對照)"

    return scores


# ============================================================
# 2. Whale composite + components (正交化標的)
# ============================================================
def _vectorized_ws_z(df, cols, group_keys, lower=0.01, upper=0.99, min_n=10):
    """完全向量化 winsorize[lower,upper] + z-score, by group_keys。
    數學等價 whale_picks_phase2.winsorize_standardize 的 _ws_z (per-group clip 到分位再標準化),
    但用單次 groupby().quantile()/agg() + merge-back 取代 per-group Python lambda (C-level,
    避免 4.96M 行全史的 ~567K group-apply)。組內有效值 < min_n → NaN (同 production)。"""
    out = df.copy()
    gid = out[group_keys].astype(str).agg("|".join, axis=1) if len(group_keys) > 1 \
        else out[group_keys[0]].astype(str)
    out["_gid"] = gid.values
    for col in cols:
        sub = out[["_gid", col]].dropna()
        # 單次 groupby quantile (回 multi-index Series, 向量化)
        q = sub.groupby("_gid")[col].quantile([lower, upper]).unstack()
        q.columns = ["_lo", "_hi"]
        qn = sub.groupby("_gid")[col].count().rename("_n")
        qmap = q.join(qn)
        m = out[["_gid"]].merge(qmap, left_on="_gid", right_index=True, how="left")
        lo = m["_lo"].values
        hi = m["_hi"].values
        n = m["_n"].values
        clipped = np.clip(out[col].values, lo, hi)
        # clip 後再算組 mean/std (production _ws_z 順序)
        tmp = pd.DataFrame({"_gid": out["_gid"].values, "_c": clipped})
        stats_g = tmp.dropna(subset=["_c"]).groupby("_gid")["_c"].agg(["mean", "std"])
        ms = out[["_gid"]].merge(stats_g, left_on="_gid", right_index=True, how="left")
        mean_ = ms["mean"].values
        std_ = ms["std"].values
        with np.errstate(invalid="ignore", divide="ignore"):
            z = (clipped - mean_) / std_
        z = np.where((std_ > 0), z, 0.0)
        z = np.where(np.isnan(out[col].values), np.nan, z)   # 原 NaN 保持 NaN
        z = np.where(n >= min_n, z, np.nan)                  # 組樣本不足 → NaN
        out[col] = z
    out = out.drop(columns=["_gid"], errors="ignore")
    return out


def build_whale_panel(start, end):
    """重算每日每股 composite_score + components, 完全沿用 production path
    (whale_picks_phase2.build_feature_panel + winsorize_standardize industry-neutral)。
    回傳 long df: stock_id, date, composite_score, smart_money_score, revenue_score,
    f_score, eps_yoy, foreign_pct, total_pct, mktcap-related。"""
    logger.info("Building Whale composite panel (production path, vectorized standardize)...")
    from tools.whale_picks_phase2 import (
        load_indicators, load_smart_money, load_quality, load_revenue,
        load_financials_panel, load_universe_industry, build_feature_panel,
    )
    from tools.whale_picks_screener import COMPOSITE_SCORE

    indicators = load_indicators(start, end)
    smart_money = load_smart_money(start, end)
    quality = load_quality(start, end)
    revenue = load_revenue(start, end)
    financials = load_financials_panel(start, end)
    universe_industry = load_universe_industry()
    fwd_dummy = pd.DataFrame(columns=["stock_id", "date", "fwd_5d", "fwd_10d", "fwd_20d",
                                      "fwd_60d", "fwd_120d", "fwd_60d_max", "fwd_60d_min"])

    feat = build_feature_panel(indicators, smart_money, fwd_dummy, quality,
                               revenue, financials, universe_industry)

    # industry-neutral standardize composite_score 用到的 feature, 再算 composite (production).
    # 用向量化 winsorize+z 取代 whale_picks_phase2.winsorize_standardize 的 per-group Python
    # closure (後者對 4.96M 行全史 = 7 feat × ~2700 date × ~30 industry ≈ 567K group-apply,
    # 實測 >10min)。數學等價: 每 (date×industry) 組 clip 到 [1%,99%] 分位再 (x-mean)/std。
    comp_feats = [c for c in COMPOSITE_SCORE if c in feat.columns]
    feat = _vectorized_ws_z(feat, comp_feats, group_keys=["date", "industry_category"],
                            lower=0.01, upper=0.99, min_n=10)
    feat["composite_score"] = 0.0
    nvalid = pd.Series(0, index=feat.index)
    for f, w in COMPOSITE_SCORE.items():
        if f not in feat.columns:
            logger.warning(f"  composite_score feature missing: {f}")
            continue
        feat["composite_score"] = feat["composite_score"] + w * feat[f].fillna(0.0)
        nvalid = nvalid + feat[f].notna().astype(int)
    feat.loc[nvalid < 4, "composite_score"] = np.nan

    keep = ["stock_id", "date", "composite_score", "smart_money_score",
            "revenue_score", "f_score", "eps_yoy", "foreign_pct", "total_pct"]
    keep = [c for c in keep if c in feat.columns]
    out = feat[keep].copy()
    out["stock_id"] = out["stock_id"].astype(str)
    out["date"] = pd.to_datetime(out["date"])
    logger.info(f"  Whale panel: {len(out):,} rows, {out['stock_id'].nunique()} stocks, "
                f"cols={keep}")
    return out


# ============================================================
# 3. Deflated Sharpe (Bailey & Lopez de Prado 2014) — 鏡像 line2
# ============================================================
def deflated_sharpe(sr_observed, n_obs, n_trials, skew=0.0, kurt=3.0):
    if n_obs < 30 or n_trials < 1:
        return np.nan, np.nan
    euler = 0.5772156649
    emax = ((1 - euler) * stats.norm.ppf(1 - 1.0 / n_trials)
            + euler * stats.norm.ppf(1 - 1.0 / (n_trials * np.e)))
    sr0 = emax / np.sqrt(n_obs)
    denom = np.sqrt(1 - skew * sr_observed + (kurt - 1) / 4.0 * sr_observed ** 2)
    z = (sr_observed - sr0) * np.sqrt(n_obs - 1) / denom
    return float(stats.norm.cdf(z)), float(sr0)


def ls_period_returns(df, score_col, return_col, mask, direction="top", n_decile=N_DECILES):
    """long-short decile spread 每日時序 (deflated sharpe 用)。用 _fast_decile 向量化。"""
    x = df.loc[mask, [score_col, return_col, "date"]].dropna()
    if x.empty:
        return pd.Series(dtype=float)
    cnt = x.groupby("date")[score_col].transform("size")
    x = x[cnt >= MIN_CROSS_SECTION].copy()
    if x.empty:
        return pd.Series(dtype=float)
    x["dec"] = _fast_decile(x, score_col)
    daily = x.groupby(["date", "dec"])[return_col].mean().reset_index()
    piv = daily.pivot(index="date", columns="dec", values=return_col)
    if 1 not in piv.columns or n_decile not in piv.columns:
        return pd.Series(dtype=float)
    ls = piv[n_decile] - piv[1] if direction == "top" else piv[1] - piv[n_decile]
    return ls.dropna()


# ============================================================
# 4. Overlap test — 對 Whale 正交化取殘差
# ============================================================
def cross_sectional_residual(df, target_col, ortho_cols, date_col="date"):
    """每日橫截面: target_col 對 ortho_cols 做 OLS, 取殘差。
    殘差 = target 中無法被 Whale composite/components 解釋的部分。
    回傳 Series (index 對齊 df), 同時回傳每日 R²。"""
    resid = pd.Series(np.nan, index=df.index, dtype=float)
    r2_list = []
    use_cols = [c for c in ortho_cols if c in df.columns]
    for d, g in df.groupby(date_col):
        sub = g[[target_col] + use_cols].dropna()
        if len(sub) < max(30, len(use_cols) + 5):
            continue
        y = sub[target_col].values
        X = sub[use_cols].values
        # standardize X cols (避免量級主導); 加截距
        Xs = (X - X.mean(0)) / (X.std(0) + 1e-9)
        Xd = np.column_stack([np.ones(len(Xs)), Xs])
        try:
            beta, _, _, _ = np.linalg.lstsq(Xd, y, rcond=None)
        except np.linalg.LinAlgError:
            continue
        yhat = Xd @ beta
        res = y - yhat
        resid.loc[sub.index] = res
        ss_tot = ((y - y.mean()) ** 2).sum()
        r2 = 1 - (res ** 2).sum() / ss_tot if ss_tot > 0 else np.nan
        if not np.isnan(r2):
            r2_list.append(r2)
    return resid, (float(np.mean(r2_list)) if r2_list else np.nan)


def signal_correlation(df, sig_col, ortho_cols, mask):
    """pooled (cross-day) Spearman corr of sig vs each ortho col + composite_score。"""
    x = df[mask]
    out = {}
    for c in ortho_cols:
        if c not in x.columns:
            continue
        sub = x[[sig_col, c]].dropna()
        if len(sub) < 100:
            out[c] = np.nan
            continue
        rho, _ = stats.spearmanr(sub[sig_col], sub[c])
        out[c] = float(rho)
    return out


# ============================================================
# 5. Main
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=None)
    ap.add_argument("--since", type=str, default="2015-01-01")  # 法人資料起點
    args = ap.parse_args()

    t0 = time.time()

    # ---------- Load + merge ----------
    df = load_panel(sample=args.sample, since=args.since)  # 剔 Close<=0 + MIN_HISTORY
    df = _drop_frozen_volume(df)
    df["yf_ticker"] = df["yf_ticker"].astype(str)

    shares = pd.read_parquet(SHARES_PATH)["shares"]
    shares.index = shares.index.astype(str)
    df = df[df["yf_ticker"].isin(shares.index)].copy()  # 上市 only (有股數)
    df["shares"] = df["yf_ticker"].map(shares)
    logger.info(f"After shares filter (上市 only): {df['yf_ticker'].nunique()} stocks, {len(df):,} rows")

    inst = load_institutional()
    df = df.merge(inst[["date", "stock_id", "net_inst_shares"]],
                  left_on=["date", "yf_ticker"], right_on=["date", "stock_id"], how="left")
    df = df.drop(columns=["stock_id"])
    n_have_inst = df["net_inst_shares"].notna().mean()
    logger.info(f"Rows with institutional data: {n_have_inst*100:.1f}%")
    df["net_inst_shares"] = df["net_inst_shares"].fillna(0.0)  # 缺=當日無揭露/0 淨買

    df = build_signals(df)
    df = add_fwd_returns(df)
    df = add_liquidity(df)
    df = add_marketcap_rank(df, shares)

    rn_cols = []
    for w in PRIOR_WINDOWS:
        rn_cols += [f"prior_ret_{w}d", f"flow_mcap_{w}d", f"flow_turn_{w}d"]
    df = rank_normalize(df, rn_cols)
    scores = build_scores(df)

    focus = [c for c in scores if c.startswith(("flow_only_", "flow_cont_", "flow_samesign_"))]
    contrasts = [c for c in scores if c.startswith("flow_turn_cont_")]
    baselines = [c for c in scores if c.startswith("mom_only_")]
    all_eval = focus + contrasts + baselines

    # universe masks: large-cap (top quintile) vs full (上市)
    df["large_cap"] = df["mcap_pct_rank"] >= LARGE_CAP_QUANTILE
    UNIVERSES = [("full", pd.Series(True, index=df.index)),
                 ("large_cap", df["large_cap"])]

    # ---------- A. Gauntlet: IC / decile / net spread (universe × liq tier × horizon) ----------
    logger.info("=== A. Gauntlet ===")
    decile_rows, gaunt_rows = [], []
    # robustness 聚焦 liq_50m 可交易池; gauntlet 跑兩 universe × 兩 liq tier
    LIQ_SUB = [("all", None), ("liq_50m", 5e7)]
    for uni_name, uni_mask in UNIVERSES:
        for tier_name, tier_val in LIQ_SUB:
            mask = uni_mask & liquidity_mask(df, tier_val)
            for score_col in all_eval:
                for h in HORIZONS:
                    ret = f"fwd_{h}d"
                    summ = compute_decile_returns(df, score_col, ret, mask)
                    for _, r in summ.iterrows():
                        decile_rows.append({
                            "universe": uni_name, "score": score_col, "label": scores[score_col],
                            "liquidity": tier_name, "horizon": h, "decile": int(r["decile"]),
                            "mean_ret": r["mean_ret"], "win_rate": r["win_rate"],
                            "n_days": int(r["n_days"]),
                        })
                    ic = daily_ic(df, score_col, ret, mask)
                    ics = ic_summary(ic)
                    gross = decile_spread(summ, "top")
                    mono = monotonic_score(summ)
                    for cost in COST_ROUNDTRIP_TIERS:
                        gaunt_rows.append({
                            "universe": uni_name, "score": score_col, "label": scores[score_col],
                            "liquidity": tier_name, "horizon": h, "direction": "top",
                            "ic_mean": ics["mean"], "ic_ir": ics["ir"], "ic_win": ics["win"],
                            "ic_t": ics["t"], "ic_n": ics["n"], "gross_spread": gross,
                            "cost_roundtrip": cost, "net_spread": gross - 2 * cost,
                            "monotonic_rho": mono,
                        })
    decile_df = pd.DataFrame(decile_rows)
    gaunt_df = pd.DataFrame(gaunt_rows)
    decile_df.to_csv(OUT_DIR / "line4_flow_decile.csv", index=False, encoding="utf-8-sig")
    gaunt_df.to_csv(OUT_DIR / "line4_flow_gauntlet.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved decile ({len(decile_df)}) + gauntlet ({len(gaunt_df)})")

    # ---------- B. Walk-forward annual ----------
    logger.info("=== B. Walk-forward annual ===")
    df["year"] = df["date"].dt.year
    wf_rows = []
    for uni_name, uni_mask in UNIVERSES:
        for score_col in focus + contrasts:
            for yr in sorted(df["year"].unique()):
                m = uni_mask & liquidity_mask(df, 5e7) & (df["year"] == yr)
                if m.sum() < 1000:
                    continue
                ic = daily_ic(df, score_col, "fwd_20d", m)
                summ = compute_decile_returns(df, score_col, "fwd_20d", m)
                ics = ic_summary(ic)
                wf_rows.append({
                    "universe": uni_name, "score": score_col, "year": int(yr),
                    "ic_mean": ics["mean"], "ic_ir": ics["ir"],
                    "ls_spread_20d": decile_spread(summ, "top"), "ic_n": ics["n"],
                })
    wf_df = pd.DataFrame(wf_rows)
    wf_df.to_csv(OUT_DIR / "line4_flow_walkforward.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved walkforward ({len(wf_df)})")

    # ---------- C. Robustness: ex-2020 / LOYO / deflated sharpe ----------
    logger.info("=== C. Robustness (large_cap × liq_50m) ===")
    robust_rows = []
    n_trials = len(all_eval) * len(HORIZONS) * len(UNIVERSES)  # 全變體數 (多重檢定 haircut)
    for uni_name, uni_mask in UNIVERSES:
        base_mask = uni_mask & liquidity_mask(df, 5e7)
        for score_col in focus + contrasts:
            ls = ls_period_returns(df, score_col, "fwd_20d", base_mask)
            if len(ls) >= 30:
                sr_per = ls.mean() / ls.std(ddof=1) if ls.std(ddof=1) > 0 else 0
                dsr, sr0 = deflated_sharpe(sr_per, len(ls), n_trials,
                                           skew=float(stats.skew(ls)),
                                           kurt=float(stats.kurtosis(ls, fisher=False)))
                robust_rows.append({
                    "universe": uni_name, "score": score_col, "test": "deflated_sharpe",
                    "value": dsr, "extra": f"sr_per={sr_per:.4f} sr0={sr0:.4f} n_obs={len(ls)} "
                    f"n_trials={n_trials} ann_sr={sr_per*np.sqrt(252/20):.3f}",
                })
            # ex-2020
            ls_full = float(ls.mean()) if len(ls) else np.nan
            ls_ex = ls_period_returns(df, score_col, "fwd_20d", base_mask & (df["year"] != 2020))
            sp_ex = float(ls_ex.mean()) if len(ls_ex) else np.nan
            robust_rows.append({
                "universe": uni_name, "score": score_col, "test": "ex_2020",
                "value": sp_ex, "extra": f"full={ls_full:.6f} delta={sp_ex-ls_full:+.6f}",
            })
            # LOYO
            loyo = []
            for yr in sorted(df["year"].dropna().unique()):
                lsy = ls_period_returns(df, score_col, "fwd_20d", base_mask & (df["year"] != yr))
                if len(lsy) >= 30:
                    loyo.append((int(yr), float(lsy.mean())))
            if loyo:
                vals = [v for _, v in loyo]
                worst = min(loyo, key=lambda x: x[1])
                robust_rows.append({
                    "universe": uni_name, "score": score_col, "test": "loyo",
                    "value": float(np.mean(vals)),
                    "extra": f"worst(drop{worst[0]})={worst[1]:.6f} all_positive={all(v>0 for v in vals)}",
                })
    robust_df = pd.DataFrame(robust_rows)
    robust_df.to_csv(OUT_DIR / "line4_flow_robust.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved robust ({len(robust_df)})")

    # ---------- D. OVERLAP TEST (判生死) ----------
    logger.info("=== D. Overlap test (orthogonalize vs Whale) ===")
    start = df["date"].min().strftime("%Y-%m-%d")
    end = df["date"].max().strftime("%Y-%m-%d")
    whale = build_whale_panel(start, end)
    # asof merge whale (weekly/monthly cadence) onto daily df, backward, 10d tolerance
    df = df.sort_values("date").reset_index(drop=True)
    whale = whale.sort_values("date").reset_index(drop=True)
    df = pd.merge_asof(df, whale, left_on="date", right_on="date",
                       left_by="yf_ticker", right_by="stock_id",
                       direction="backward", tolerance=pd.Timedelta("40 days"))
    if "stock_id" in df.columns:
        df = df.drop(columns=["stock_id"])
    cov = df["composite_score"].notna().mean()
    logger.info(f"Rows with composite_score after asof: {cov*100:.1f}%")

    overlap_rows = []
    # 對主訊號 (flow_cont 20d 為主, 也測 flow_only/flow_cont 5d) 在 large_cap × liq_50m 做殘差 IC
    ortho_targets = ["flow_cont_20d", "flow_cont_5d", "flow_only_20d", "flow_samesign_20d"]
    base_mask = df["large_cap"] & liquidity_mask(df, 5e7)
    df_lc = df[base_mask].copy()

    for sig in ortho_targets:
        # 原始 (raw) IC + spread, 同池
        raw_ic = ic_summary(daily_ic(df_lc, sig, "fwd_20d",
                                     pd.Series(True, index=df_lc.index)))
        raw_summ = compute_decile_returns(df_lc, sig, "fwd_20d",
                                          pd.Series(True, index=df_lc.index))
        raw_spread = decile_spread(raw_summ, "top")

        # 殘差 IC: sig 對 Whale composite + components 正交化
        resid, mean_r2 = cross_sectional_residual(df_lc, sig, WHALE_ORTHO_COLS)
        df_lc[f"{sig}_resid"] = resid
        res_ic = ic_summary(daily_ic(df_lc, f"{sig}_resid", "fwd_20d",
                                     pd.Series(True, index=df_lc.index)))
        res_summ = compute_decile_returns(df_lc, f"{sig}_resid", "fwd_20d",
                                          pd.Series(True, index=df_lc.index))
        res_spread = decile_spread(res_summ, "top")

        # 相關係數 vs 每個 ortho col
        corrs = signal_correlation(df_lc, sig, WHALE_ORTHO_COLS,
                                   pd.Series(True, index=df_lc.index))

        overlap_rows.append({
            "signal": sig, "raw_ic": raw_ic["mean"], "raw_ic_ir": raw_ic["ir"],
            "raw_ic_t": raw_ic["t"], "raw_spread_20d": raw_spread,
            "resid_ic": res_ic["mean"], "resid_ic_ir": res_ic["ir"],
            "resid_ic_t": res_ic["t"], "resid_spread_20d": res_spread,
            "mean_R2_vs_whale": mean_r2,
            "ic_retention_pct": (res_ic["mean"] / raw_ic["mean"] * 100
                                 if raw_ic["mean"] and abs(raw_ic["mean"]) > 1e-9 else np.nan),
            **{f"corr_{k}": v for k, v in corrs.items()},
        })
    overlap_df = pd.DataFrame(overlap_rows)
    overlap_df.to_csv(OUT_DIR / "line4_flow_overlap.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved overlap ({len(overlap_df)})")

    _print_summary(df, scores, focus, contrasts, baselines, gaunt_df, wf_df,
                   robust_df, overlap_df, n_have_inst, cov)
    logger.info(f"Total time: {(time.time()-t0)/60:.1f} min")


def _print_summary(df, scores, focus, contrasts, baselines, gaunt_df, wf_df,
                   robust_df, overlap_df, n_have_inst, whale_cov):
    print("\n" + "=" * 110)
    print("  A. GAUNTLET (h=20, cost@0.25%) — IC / gross / net spread by universe × liq tier")
    print("=" * 110)
    print(f"  {'universe':<11}{'score':<22}{'liq':<10}{'IC':>9}{'IR':>7}{'t':>7}"
          f"{'gross%':>9}{'net%':>9}{'mono':>7}")
    sub = gaunt_df[(gaunt_df["horizon"] == 20) & (gaunt_df["cost_roundtrip"] == 0.0025)]
    for _, r in sub.sort_values(["universe", "score", "liquidity"]).iterrows():
        print(f"  {r['universe']:<11}{r['score']:<22}{r['liquidity']:<10}{r['ic_mean']:>+9.4f}"
              f"{r['ic_ir']:>+7.2f}{r['ic_t']:>+7.2f}{r['gross_spread']*100:>+9.3f}"
              f"{r['net_spread']*100:>+9.3f}{r['monotonic_rho']:>+7.2f}")

    print("\n" + "=" * 110)
    print("  B. WALK-FORWARD (liq_50m, 20d) — sign-stability (≤2025)")
    print("=" * 110)
    for uni in ["full", "large_cap"]:
        for score_col in focus + contrasts:
            s = wf_df[(wf_df["score"] == score_col) & (wf_df["universe"] == uni)]
            s2 = s[s["year"] <= 2025]
            if s2.empty:
                continue
            pos_ic = (s2["ic_mean"] > 0).sum()
            pos_sp = (s2["ls_spread_20d"] > 0).sum()
            n = len(s2)
            print(f"  [{uni:<10}] {score_col:<22} IC+ {pos_ic}/{n}  LS+ {pos_sp}/{n}  "
                  f"(年均IC {s2['ic_mean'].mean():+.4f}, 年均LS {s2['ls_spread_20d'].mean()*100:+.3f}%)")

    print("\n" + "=" * 110)
    print("  C. ROBUSTNESS (deflated sharpe / ex-2020 / LOYO)")
    print("=" * 110)
    for uni in ["full", "large_cap"]:
        for score_col in focus + contrasts:
            rs = robust_df[(robust_df["score"] == score_col) & (robust_df["universe"] == uni)]
            if rs.empty:
                continue
            print(f"\n  [{uni}] {score_col}")
            for _, r in rs.iterrows():
                print(f"    {r['test']:<18}{r['value']:>+10.5f}  {r['extra']}")

    print("\n" + "=" * 110)
    print("  D. OVERLAP TEST (large_cap × liq_50m) — 對 Whale composite+components 正交化殘差 IC")
    print("=" * 110)
    print(f"  Rows with institutional data: {n_have_inst*100:.1f}% | Whale composite coverage: {whale_cov*100:.1f}%")
    print(f"\n  {'signal':<20}{'raw_IC':>9}{'raw_t':>8}{'resid_IC':>10}{'resid_t':>9}"
          f"{'IC_retain%':>11}{'R2_vsWhale':>11}")
    for _, r in overlap_df.iterrows():
        print(f"  {r['signal']:<20}{r['raw_ic']:>+9.4f}{r['raw_ic_t']:>+8.2f}"
              f"{r['resid_ic']:>+10.4f}{r['resid_ic_t']:>+9.2f}"
              f"{r['ic_retention_pct']:>10.1f}%{r['mean_R2_vs_whale']:>11.3f}")
    print("\n  Correlation (Spearman, pooled) of flow_cont_20d vs Whale cols:")
    if not overlap_df.empty:
        row = overlap_df[overlap_df["signal"] == "flow_cont_20d"].iloc[0]
        for c in [k for k in overlap_df.columns if k.startswith("corr_")]:
            print(f"    {c.replace('corr_',''):<22}{row[c]:>+7.3f}")


if __name__ == "__main__":
    main()
