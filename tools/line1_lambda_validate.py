"""
格1：量價彈性 (price impact / Kyle lambda) 背離 — 正式 SOP-14 gauntlet 驗證
(2026-06-08)

驗證一條新技術訊號是否有可上線 alpha：以 Amihud 式價格衝擊 (lambda) 為核心，
測 level baseline + 「價漲 / lambda 走低 = 吸籌」背離訊號。

訊號定義
--------
  lambda_raw    = |日報酬| / (Close * Volume)             # Amihud：每元成交推動的價格 %
  lambda_smooth = EWMA(lambda_raw, span=20)
  訊號 A (level): sig_lambda_level   = -rank(lambda_smooth)  (低衝擊=高分；兩方向都測)
  訊號 B (背離) : sig_lambda_diverge = z(price_slope_20d) - z(lambda_slope_20d)
                  語意：價漲+lambda 走低=吸籌(+)；價漲+lambda 走高=力竭(-)
  訊號 C (帶符號背離，僅 B 有脈搏才做):
                  CLV          = ((C-L)-(H-C))/(H-L)         # 買賣方向 proxy
                  lambda_signed_raw = sign-weighted lambda (買盤壓力 → 帶號)
                  sig_lambda_diverge_signed = z(price_slope) - z(signed_lambda_slope)

harness 沿用 tools/rvol_atr_validate.py 結構：load_panel / add_fwd_returns /
add_liquidity / liquidity_mask / compute_decile_returns / daily_ic / decile_spread /
monotonic_score / ic_summary 全部鏡像 (HORIZONS=[10,20,60], 同 LIQUIDITY_TIERS /
COST_ROUNDTRIP_TIERS)。

⚠️ 資料地雷 (本檔額外處理，rvol_atr 沒處理因其指標不以 V 為分母)
  - panel 內 ~112,473 列 Volume<=0 (110,749 為停牌參考價填充凍結列)。lambda 以
    成交額為分母 → V=0 除零爆 inf。**計算 lambda 前必先剔除 Volume<=0 列** (報告揭露)。
  - lambda_raw 跨股極右偏 → cross-section winsorize 1%/99% 後再 rank。

⚠️ Survivor bias caveat: 此 panel PIT 不完整 (~46% 缺價 backlog)、含部分倖存者偏差，
   下市股被排除會虛抬回測；所有結論標此 caveat，不可宣稱 PIT-clean。

gauntlet (每 horizon × 每 liquidity tier)
  1. Spearman IC + IR
  2. Decile Q10-Q1 spread (sign 必與 IC 同號，否則 reverse-artifact → D)
  3. 單調性 rho >= +0.5 (否則 U/倒U)
  4. 成本後淨 spread (round-trip 0.25%/0.35% × 2 腿)
  5. Walk-forward 年度 (>=5 OOS 年) sign-stability
  6. Leave-one-year-out + 抽掉 2020 COVID (edge 不可塌到 ~0)
  7. Cross-regime (vol percentile)：不可只有單一 cell 正
  8. Deflated Sharpe (Bailey/Lopez de Prado)：N=測過變體數，haircut 後是否顯著

輸出 (reports/line1_lambda_*):
  - line1_lambda_decile_returns.csv
  - line1_lambda_topn_portfolio.csv
  - line1_lambda_ic_matrix.csv
  - line1_lambda_net_spread.csv
  - line1_lambda_walkforward_annual.csv
  - line1_lambda_loyo.csv               (leave-one-year-out + ex-2020)
  - line1_lambda_regime.csv             (cross-regime breakdown)
  - line1_lambda_deflated_sharpe.csv
  - line1_lambda_divergence.md          (人讀結論)

用法:
    python tools/line1_lambda_validate.py --sample 300   # 迭代
    python tools/line1_lambda_validate.py                # 全量 (最終數字)
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

PARQUET_PATH = _ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MIN_HISTORY = 200
MIN_CROSS_SECTION = 50          # 每日至少 N 檔才算 decile
MIN_CROSS_SECTION_IC = 30       # IC 每日 cross-section 門檻
N_DECILES = 10
TOP_N_VARIANTS = [10, 20, 50]
HORIZONS = [10, 20, 60]         # mandate 指定

SLOPE_WINDOW = 20               # 背離斜率滾動窗
EWMA_SPAN = 20                  # lambda 平滑
WINSOR_LO, WINSOR_HI = 0.01, 0.99   # cross-section lambda winsorize

# 焦點訊號 — 方向在 gauntlet 內兩邊都測，這裡只列預設語意方向
FOCUS_SCORES = {
    "sig_lambda_level": "lambda level (低衝擊=高分)",
    "sig_lambda_diverge": "lambda 背離 (價漲+lambda 降=吸籌)",
}
# 帶符號版在 B 有脈搏後動態加入
SIGNED_SCORE = "sig_lambda_diverge_signed"
SIGNED_LABEL = "lambda 背離(帶符號 CLV)"

LIQUIDITY_TIERS = [
    ("all", None),
    ("liq_50m", 5e7),
    ("liq_100m", 1e8),
    ("ex_bottom20pct", "pct20"),
]
COST_ROUNDTRIP_TIERS = [0.0025, 0.0035]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("line1_lambda")


# ============================================================
# Loader — schema-compatible + V<=0 drop (lambda denominator landmine)
# ============================================================
def load_panel(sample=None, since=None):
    logger.info(f"Loading {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    df["date"] = pd.to_datetime(df["date"])
    if since:
        df = df[df["date"] >= since].copy()

    if "yf_ticker" not in df.columns:
        df = df.rename(columns={"stock_id": "yf_ticker"})
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "AdjClose" not in df.columns:
        df["AdjClose"] = df["Close"]

    # 毒列：Close<=0 (與 rvol_atr 同)
    n_badprice = (df["Close"] <= 0).sum()
    if n_badprice:
        logger.info(f"Dropping {n_badprice:,} rows with Close<=0")
        df = df[df["Close"] > 0].copy()

    # ⚠️ lambda 地雷：Volume<=0 (停牌凍結列) → Amihud 分母除零。剔除並記錄。
    n_zerovol = (df["Volume"] <= 0).sum()
    logger.info(
        f"Dropping {n_zerovol:,} rows with Volume<=0 "
        f"(frozen/halt rows; lambda denominator landmine)"
    )
    df = df[df["Volume"] > 0].copy()

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
    # 記錄剔除筆數供報告引用
    load_panel.n_zerovol = int(n_zerovol)
    load_panel.n_badprice = int(n_badprice)
    return df


# ============================================================
# Lambda + signal construction
# ============================================================
def _compute_lambda_one(sub):
    """單 ticker：lambda_raw / lambda_smooth / 背離斜率原料。輸入需按 date 排序。"""
    close = sub["AdjClose"]
    high = sub["High"]
    low = sub["Low"]
    vol = sub["Volume"]

    amount = close * vol                                   # 成交額 (V>0 已保證)
    daily_ret = close.pct_change(fill_method=None)
    # Amihud: |ret| / amount。amount 單位大 → 乘 1e8 提升數值可讀 (rank/winsor 不受影響)
    lam_raw = (daily_ret.abs() / amount) * 1e8
    sub["lambda_raw"] = lam_raw
    sub["lambda_smooth"] = lam_raw.ewm(span=EWMA_SPAN, min_periods=EWMA_SPAN // 2).mean()

    # 帶符號 lambda 原料：CLV 買賣方向 proxy
    rng = (high - low).replace(0, np.nan)
    clv = ((close - low) - (high - close)) / rng           # +1 收最高 / -1 收最低
    sub["clv"] = clv.fillna(0.0)
    # 帶符號 lambda：lambda 大小 × 方向 (買壓 → 正衝擊)
    # 用 CLV 當「該日買賣壓方向」，lambda_raw 當「衝擊強度」
    sub["lambda_signed_raw"] = lam_raw * clv.fillna(0.0)
    sub["lambda_signed_smooth"] = (
        sub["lambda_signed_raw"].ewm(span=EWMA_SPAN, min_periods=EWMA_SPAN // 2).mean()
    )

    # 價格 log 供斜率
    sub["logp"] = np.log(close)
    return sub


def _rolling_slope(series, window):
    """滾動 OLS 斜率 (對 0..window-1 時間軸)。回傳每個視窗末端的 beta。"""
    x = np.arange(window, dtype=float)
    x_mean = x.mean()
    x_dev = x - x_mean
    denom = (x_dev ** 2).sum()

    def _beta(y):
        if np.isnan(y).any():
            return np.nan
        return float((x_dev * (y - y.mean())).sum() / denom)

    return series.rolling(window).apply(_beta, raw=True)


def compute_signals(df):
    """全 universe：lambda + 三個訊號的 raw 值 (rank/z 在 cross-section 階段做)。"""
    logger.info("Computing lambda + slopes per ticker...")
    t0 = time.time()
    df = df.groupby("yf_ticker", group_keys=False).apply(_compute_lambda_one)

    # 滾動斜率 (price / lambda / signed-lambda)
    logger.info(f"Rolling slopes (window={SLOPE_WINDOW})...")
    df["price_slope"] = df.groupby("yf_ticker")["logp"].transform(
        lambda s: _rolling_slope(s, SLOPE_WINDOW)
    )
    # lambda 趨勢斜率：用 log(lambda) 抑制右偏 (lambda>0)
    df["loglam"] = np.log(df["lambda_smooth"].clip(lower=1e-12))
    df["lambda_slope"] = df.groupby("yf_ticker")["loglam"].transform(
        lambda s: _rolling_slope(s, SLOPE_WINDOW)
    )
    # signed lambda 斜率 (可正可負 → 不取 log，直接用值)
    df["lambda_signed_slope"] = df.groupby("yf_ticker")["lambda_signed_smooth"].transform(
        lambda s: _rolling_slope(s, SLOPE_WINDOW)
    )
    logger.info(f"Signals done in {time.time()-t0:.1f}s")
    return df


def _cs_winsor_rank(g, col):
    """cross-section：winsorize 後 rank(pct)。回傳 0..1。"""
    s = g[col]
    lo, hi = s.quantile(WINSOR_LO), s.quantile(WINSOR_HI)
    s = s.clip(lo, hi)
    return s.rank(pct=True)


def _cs_zscore(g, col):
    s = g[col]
    lo, hi = s.quantile(WINSOR_LO), s.quantile(WINSOR_HI)
    s = s.clip(lo, hi)
    mu, sd = s.mean(), s.std()
    if sd == 0 or np.isnan(sd):
        return pd.Series(0.0, index=s.index)
    return (s - mu) / sd


def build_cross_section_scores(df):
    """組 cross-section 訊號分數 (每日 winsorize + rank/z)。"""
    logger.info("Building cross-section scores (winsorize + rank/z per date)...")
    t0 = time.time()

    # 訊號 A: level = -rank(lambda_smooth)  (低衝擊=高分)
    lam_rank = df.groupby("date", group_keys=False).apply(
        lambda g: _cs_winsor_rank(g, "lambda_smooth")
    )
    df["sig_lambda_level"] = -lam_rank

    # 訊號 B: z(price_slope) - z(lambda_slope)
    zps = df.groupby("date", group_keys=False).apply(lambda g: _cs_zscore(g, "price_slope"))
    zls = df.groupby("date", group_keys=False).apply(lambda g: _cs_zscore(g, "lambda_slope"))
    df["sig_lambda_diverge"] = zps - zls

    # 訊號 C: z(price_slope) - z(signed_lambda_slope)
    zss = df.groupby("date", group_keys=False).apply(
        lambda g: _cs_zscore(g, "lambda_signed_slope")
    )
    df["sig_lambda_diverge_signed"] = zps - zss

    logger.info(f"Cross-section scores done in {time.time()-t0:.1f}s")
    return df


# ============================================================
# Forward returns / liquidity (mirror rvol_atr)
# ============================================================
def add_fwd_returns(df):
    logger.info("Forward returns (raw Close, no AdjClose; fill_method=None)...")
    df = df.sort_values(["yf_ticker", "date"]).reset_index(drop=True)
    for h in HORIZONS:
        r = df.groupby("yf_ticker")["AdjClose"].pct_change(h, fill_method=None).shift(-h)
        r = r.replace([np.inf, -np.inf], np.nan)
        df[f"fwd_{h}d"] = r.where((r > -0.95) & (r < 5.0))
    return df


def add_liquidity(df):
    df["amount"] = df["Close"] * df["Volume"]
    df["avg_amount_20d"] = df.groupby("yf_ticker")["amount"].transform(
        lambda s: s.rolling(20).mean()
    )
    df["amt_pct_rank"] = df.groupby("date")["avg_amount_20d"].transform(
        lambda s: s.rank(pct=True)
    )
    return df


def liquidity_mask(df, tier_val):
    if tier_val is None:
        return pd.Series(True, index=df.index)
    if tier_val == "pct20":
        return df["amt_pct_rank"] >= 0.20
    return df["avg_amount_20d"] >= tier_val


# ============================================================
# Regime (vol percentile, self-computed — robust, no external join)
# ============================================================
def add_regime(df):
    """大盤波動三分類：用全 universe 日均報酬 proxy TAIEX，rolling 20d vol percentile。"""
    logger.info("Tagging regime (vol percentile)...")
    daily = df.groupby("date")["AdjClose"].apply(
        lambda s: s.pct_change(fill_method=None).mean()
    ).rename("mkt_ret").reset_index()
    daily["vol_20d"] = daily["mkt_ret"].rolling(20).std()
    daily["trend_20d"] = daily["mkt_ret"].rolling(20).mean()
    vol_hi = daily["vol_20d"].quantile(0.70)
    vol_lo = daily["vol_20d"].quantile(0.30)

    def classify(v):
        if pd.isna(v):
            return "unknown"
        if v > vol_hi:
            return "high_vol"
        if v < vol_lo:
            return "low_vol"
        return "mid_vol"

    daily["regime"] = daily["vol_20d"].apply(classify)
    df = df.merge(daily[["date", "regime"]], on="date", how="left")
    return df


# ============================================================
# Decile / Top-N / IC (mirror rvol_atr)
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
    if x.empty:
        return pd.DataFrame(columns=["decile", "mean_ret", "median_ret",
                                     "std_ret", "win_rate", "n_days"])
    x["decile"] = x["decile"].astype(int) + 1
    daily = x.groupby(["date", "decile"])[return_col].mean().reset_index()
    summary = daily.groupby("decile")[return_col].agg(
        mean_ret="mean", median_ret="median", std_ret="std",
        win_rate=lambda s: (s > 0).mean(), n_days="count",
    ).reset_index()
    return summary


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


def decile_spread(summary, direction):
    """direction='top' → D10-D1; 'bot' → D1-D10 (long-short)。"""
    if summary.empty:
        return np.nan
    s = summary.set_index("decile")["mean_ret"]
    if 1 not in s.index or N_DECILES not in s.index:
        return np.nan
    if direction == "top":
        return float(s[N_DECILES] - s[1])
    return float(s[1] - s[N_DECILES])


def monotonic_score(summary):
    s = summary.sort_values("decile")
    if len(s) < 3:
        return np.nan
    rho, _ = stats.spearmanr(s["decile"], s["mean_ret"])
    return float(rho)


def best_direction(summary):
    """回傳 IC-agnostic 的「哪一邊賺」：D10>D1 → top，否則 bot。"""
    if summary.empty:
        return "top"
    s = summary.set_index("decile")["mean_ret"]
    if 1 not in s.index or N_DECILES not in s.index:
        return "top"
    return "top" if s[N_DECILES] >= s[1] else "bot"


# ============================================================
# Deflated Sharpe (Bailey & Lopez de Prado 2014)
# ============================================================
def deflated_sharpe(sr_obs, n_trials, n_obs, skew=0.0, kurt=3.0):
    """
    sr_obs   : 觀測到的 (非年化) per-period Sharpe (這裡用 daily-IR 當 SR proxy 也可，
               但本檔餵 long-short daily 報酬序列的 per-period Sharpe)
    n_trials : 測過的獨立變體數 (多重比較)
    n_obs    : 報酬序列長度
    回傳 (psr, dsr_threshold_sr, deflated_passes)
    """
    if n_obs < 30 or np.isnan(sr_obs):
        return dict(sr0=np.nan, psr=np.nan, dsr_pass=False)
    # expected max SR under N null trials (Bailey 2014 eq.)
    e = 0.5772156649  # Euler-Mascheroni
    z1 = stats.norm.ppf(1 - 1.0 / n_trials)
    z2 = stats.norm.ppf(1 - 1.0 / (n_trials * np.e))
    sr0 = (z1 * (1 - e) + z2 * e)  # expected max Sharpe (per-period, unit variance)
    # PSR(sr0): prob observed SR exceeds benchmark sr0 given higher moments
    num = (sr_obs - sr0) * np.sqrt(n_obs - 1)
    den = np.sqrt(1 - skew * sr_obs + ((kurt - 1) / 4.0) * sr_obs ** 2)
    if den <= 0:
        return dict(sr0=float(sr0), psr=np.nan, dsr_pass=False)
    psr = float(stats.norm.cdf(num / den))
    return dict(sr0=float(sr0), psr=psr, dsr_pass=bool(psr > 0.95))


def ls_daily_series(df, score_col, return_col, mask, direction, n_each=None):
    """long-short 每日報酬序列 (用 decile D10/D1 或 top/bot N)。回傳 np.array。"""
    x = df[mask][[score_col, return_col, "date"]].dropna()
    rets = []
    for _, g in x.groupby("date"):
        if len(g) < MIN_CROSS_SECTION:
            continue
        ranked = g[score_col].rank(method="first")
        q = pd.qcut(ranked, N_DECILES, labels=False, duplicates="drop")
        if q is None or q.isna().all():
            continue
        g = g.assign(_dec=q)
        top = g[g["_dec"] == g["_dec"].max()][return_col].mean()
        bot = g[g["_dec"] == 0][return_col].mean()
        if np.isnan(top) or np.isnan(bot):
            continue
        rets.append(top - bot if direction == "top" else bot - top)
    return np.array(rets)


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
    df = compute_signals(df)
    df = build_cross_section_scores(df)
    df = add_fwd_returns(df)
    df = add_liquidity(df)
    df = add_regime(df)

    # 起手只測 level + 無符號背離 (便宜)；signed 之後視 B 脈搏動態加
    active_scores = dict(FOCUS_SCORES)

    # ---------- 0. 先快速看 h=20 liq_50m 的 IC + decile，判 B 是否有脈搏 ----------
    logger.info("=== Pulse check: B (diverge) @ h=20 liq_50m ===")
    pulse_mask = liquidity_mask(df, 5e7)
    b_ic = ic_summary(daily_ic(df, "sig_lambda_diverge", "fwd_20d", pulse_mask))
    b_summ = compute_decile_returns(df, "sig_lambda_diverge", "fwd_20d", pulse_mask)
    b_dir = best_direction(b_summ)
    b_spread = decile_spread(b_summ, b_dir)
    b_mono = monotonic_score(b_summ)
    logger.info(
        f"  B pulse: IC={b_ic['mean']:+.4f} (t={b_ic['t']:+.2f}) "
        f"spread={b_spread*100 if not np.isnan(b_spread) else float('nan'):+.3f}% "
        f"mono={b_mono:+.3f} dir={b_dir}"
    )
    # 脈搏門檻：|IC|>0.01 或 |spread|>0.3% → 值得做帶符號版
    b_has_pulse = (abs(b_ic["mean"]) > 0.01) or (
        not np.isnan(b_spread) and abs(b_spread) > 0.003
    )
    if b_has_pulse:
        logger.info("  -> B has pulse, adding SIGNED variant (C)")
        active_scores[SIGNED_SCORE] = SIGNED_LABEL
    else:
        logger.info("  -> B no pulse, SKIP signed variant (saves compute)")

    # 各訊號方向 (用全量 liq_50m h=20 decile 決定)
    score_dir = {}
    for sc in active_scores:
        summ = compute_decile_returns(df, sc, "fwd_20d", pulse_mask)
        score_dir[sc] = best_direction(summ)
    logger.info(f"Score directions (liq_50m h=20): {score_dir}")

    # ---------- 1. IC matrix (per tier × horizon) ----------
    logger.info("=== IC matrix ===")
    ic_rows = []
    for tier_name, tier_val in LIQUIDITY_TIERS:
        mask = liquidity_mask(df, tier_val)
        for sc, label in active_scores.items():
            for h in HORIZONS:
                ic = daily_ic(df, sc, f"fwd_{h}d", mask)
                s = ic_summary(ic)
                ic_rows.append({
                    "score": sc, "label": label, "liquidity": tier_name,
                    "horizon": h, "ic_mean": s["mean"], "ic_ir": s["ir"],
                    "ic_win": s["win"], "ic_t": s["t"], "n_days": s["n"],
                })
    ic_df = pd.DataFrame(ic_rows)
    ic_df.to_csv(OUT_DIR / "line1_lambda_ic_matrix.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved IC matrix ({len(ic_df)})")

    # ---------- 2. Decile + Top-N ----------
    logger.info("=== Decile + Top-N ===")
    decile_rows, topn_rows = [], []
    for tier_name, tier_val in LIQUIDITY_TIERS:
        mask = liquidity_mask(df, tier_val)
        for sc, label in active_scores.items():
            for h in HORIZONS:
                ret = f"fwd_{h}d"
                summ = compute_decile_returns(df, sc, ret, mask)
                for _, r in summ.iterrows():
                    decile_rows.append({
                        "score": sc, "label": label, "liquidity": tier_name,
                        "horizon": h, "decile": int(r["decile"]),
                        "mean_ret": r["mean_ret"], "median_ret": r["median_ret"],
                        "win_rate": r["win_rate"], "n_days": int(r["n_days"]),
                    })
                for n in TOP_N_VARIANTS:
                    for d in ["top", "bot"]:
                        res = simulate_topn(df, sc, ret, n, mask, d)
                        if res:
                            topn_rows.append({
                                "score": sc, "label": label, "liquidity": tier_name,
                                "horizon": h, "top_n": n, "direction": d, **res,
                            })
    decile_df = pd.DataFrame(decile_rows)
    topn_df = pd.DataFrame(topn_rows)
    decile_df.to_csv(OUT_DIR / "line1_lambda_decile_returns.csv", index=False, encoding="utf-8-sig")
    topn_df.to_csv(OUT_DIR / "line1_lambda_topn_portfolio.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved decile ({len(decile_df)}) + topN ({len(topn_df)})")

    # ---------- 3. Net spread (cost-adjusted) ----------
    logger.info("=== Net spread ===")
    net_rows = []
    for tier_name, tier_val in LIQUIDITY_TIERS:
        mask = liquidity_mask(df, tier_val)
        for sc, label in active_scores.items():
            d = score_dir[sc]
            for h in HORIZONS:
                summ = compute_decile_returns(df, sc, f"fwd_{h}d", mask)
                gross = decile_spread(summ, d)
                mono = monotonic_score(summ)
                # IC sign vs spread sign 一致性檢查
                ic_m = ic_df[(ic_df.score == sc) & (ic_df.liquidity == tier_name)
                             & (ic_df.horizon == h)]["ic_mean"]
                ic_m = float(ic_m.iloc[0]) if len(ic_m) else np.nan
                signed_ic = ic_m * (1 if d == "top" else -1)
                sign_ok = (not np.isnan(gross)) and (not np.isnan(signed_ic)) \
                    and (np.sign(gross) == np.sign(signed_ic))
                for cost in COST_ROUNDTRIP_TIERS:
                    net = gross - 2 * cost
                    net_rows.append({
                        "score": sc, "label": label, "liquidity": tier_name,
                        "horizon": h, "direction": d,
                        "gross_spread": gross, "cost_roundtrip": cost,
                        "net_spread": net, "monotonic_rho": mono,
                        "ic_mean": ic_m, "ic_spread_sign_ok": sign_ok,
                    })
    net_df = pd.DataFrame(net_rows)
    net_df.to_csv(OUT_DIR / "line1_lambda_net_spread.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved net_spread ({len(net_df)})")

    # ---------- 4. Walk-forward annual ----------
    logger.info("=== Walk-forward annual (20d) ===")
    df["year"] = df["date"].dt.year
    wf_rows = []
    years = sorted(df["year"].unique())
    for tier_name, tier_val in [("all", None), ("liq_50m", 5e7)]:
        base_mask = liquidity_mask(df, tier_val)
        for sc, label in active_scores.items():
            d = score_dir[sc]
            for yr in years:
                yr_mask = base_mask & (df["year"] == yr)
                if yr_mask.sum() < 1000:
                    continue
                ic = daily_ic(df, sc, "fwd_20d", yr_mask)
                summ = compute_decile_returns(df, sc, "fwd_20d", yr_mask)
                spread = decile_spread(summ, d)
                s = ic_summary(ic)
                wf_rows.append({
                    "score": sc, "label": label, "liquidity": tier_name,
                    "year": int(yr), "ic_mean": s["mean"], "ic_ir": s["ir"],
                    "ic_win": s["win"], "ic_t": s["t"], "ic_n_days": s["n"],
                    "ls_spread_20d": spread, "direction": d,
                })
    wf_df = pd.DataFrame(wf_rows)
    wf_df.to_csv(OUT_DIR / "line1_lambda_walkforward_annual.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved walkforward ({len(wf_df)})")

    # ---------- 5. Leave-one-year-out + ex-2020 ----------
    logger.info("=== Leave-one-year-out + ex-2020 ===")
    loyo_rows = []
    for tier_name, tier_val in [("liq_50m", 5e7)]:
        base_mask = liquidity_mask(df, tier_val)
        for sc, label in active_scores.items():
            d = score_dir[sc]
            # full
            full_summ = compute_decile_returns(df, sc, "fwd_20d", base_mask)
            full_spread = decile_spread(full_summ, d)
            full_ic = ic_summary(daily_ic(df, sc, "fwd_20d", base_mask))["mean"]
            loyo_rows.append({
                "score": sc, "label": label, "drop_year": "none(full)",
                "ls_spread_20d": full_spread, "ic_mean": full_ic, "direction": d,
            })
            # drop each year
            for yr in years:
                m = base_mask & (df["year"] != yr)
                if m.sum() < 5000:
                    continue
                summ = compute_decile_returns(df, sc, "fwd_20d", m)
                sp = decile_spread(summ, d)
                ic_m = ic_summary(daily_ic(df, sc, "fwd_20d", m))["mean"]
                loyo_rows.append({
                    "score": sc, "label": label, "drop_year": str(yr),
                    "ls_spread_20d": sp, "ic_mean": ic_m, "direction": d,
                })
    loyo_df = pd.DataFrame(loyo_rows)
    loyo_df.to_csv(OUT_DIR / "line1_lambda_loyo.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved loyo ({len(loyo_df)})")

    # ---------- 6. Cross-regime ----------
    logger.info("=== Cross-regime breakdown ===")
    reg_rows = []
    for tier_name, tier_val in [("liq_50m", 5e7)]:
        base_mask = liquidity_mask(df, tier_val)
        for sc, label in active_scores.items():
            d = score_dir[sc]
            for reg in ["low_vol", "mid_vol", "high_vol"]:
                m = base_mask & (df["regime"] == reg)
                if m.sum() < 2000:
                    continue
                ic = ic_summary(daily_ic(df, sc, "fwd_20d", m))
                summ = compute_decile_returns(df, sc, "fwd_20d", m)
                sp = decile_spread(summ, d)
                mono = monotonic_score(summ)
                reg_rows.append({
                    "score": sc, "label": label, "regime": reg,
                    "ic_mean": ic["mean"], "ic_t": ic["t"],
                    "ls_spread_20d": sp, "monotonic_rho": mono,
                    "direction": d, "n_obs": int(m.sum()),
                })
    reg_df = pd.DataFrame(reg_rows)
    reg_df.to_csv(OUT_DIR / "line1_lambda_regime.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved regime ({len(reg_df)})")

    # ---------- 7. Deflated Sharpe ----------
    # N_trials = 測過的獨立變體數：3 訊號(level/diverge/signed) × 3 horizon × 4 tier × 2 dir
    #            這裡用實際納入評估的 score×horizon×tier×direction-test 組合數估算
    logger.info("=== Deflated Sharpe ===")
    n_trials = len(active_scores) * len(HORIZONS) * len(LIQUIDITY_TIERS) * 2
    ds_rows = []
    for tier_name, tier_val in [("liq_50m", 5e7), ("all", None)]:
        mask = liquidity_mask(df, tier_val)
        for sc, label in active_scores.items():
            d = score_dir[sc]
            for h in HORIZONS:
                series = ls_daily_series(df, sc, f"fwd_{h}d", mask, d)
                if len(series) < 60:
                    continue
                m, s = series.mean(), series.std(ddof=1)
                sr_pp = m / s if s > 0 else np.nan          # per-period Sharpe
                sk = float(stats.skew(series))
                ku = float(stats.kurtosis(series, fisher=False))
                ds = deflated_sharpe(sr_pp, n_trials, len(series), skew=sk, kurt=ku)
                ds_rows.append({
                    "score": sc, "label": label, "liquidity": tier_name,
                    "horizon": h, "direction": d, "n_obs": len(series),
                    "sr_per_period": sr_pp,
                    "sharpe_ann": sr_pp * np.sqrt(252 / h) if not np.isnan(sr_pp) else np.nan,
                    "n_trials": n_trials, "sr0_benchmark": ds["sr0"],
                    "psr": ds["psr"], "dsr_pass": ds["dsr_pass"],
                })
    ds_df = pd.DataFrame(ds_rows)
    ds_df.to_csv(OUT_DIR / "line1_lambda_deflated_sharpe.csv", index=False, encoding="utf-8-sig")
    logger.info(f"Saved deflated_sharpe ({len(ds_df)})")

    # ---------- Console summary ----------
    _print_summary(ic_df, decile_df, net_df, wf_df, loyo_df, reg_df, ds_df,
                   active_scores, score_dir, b_has_pulse,
                   load_panel.n_zerovol, load_panel.n_badprice, df)

    # ---------- Markdown report ----------
    _write_md(ic_df, decile_df, net_df, wf_df, loyo_df, reg_df, ds_df,
              active_scores, score_dir, b_has_pulse,
              load_panel.n_zerovol, load_panel.n_badprice, df, args.sample)

    logger.info(f"Total time: {(time.time()-t0)/60:.1f} min")


def _print_summary(ic_df, decile_df, net_df, wf_df, loyo_df, reg_df, ds_df,
                   active_scores, score_dir, b_has_pulse, n_zerovol, n_badprice, df):
    print("\n" + "=" * 100)
    print(f"  格1 lambda 驗證 — V<=0 剔除 {n_zerovol:,} 列 / Close<=0 剔除 {n_badprice:,} 列")
    print(f"  panel: {df['yf_ticker'].nunique()} tickers, {df['date'].min().date()}~{df['date'].max().date()}")
    print("=" * 100)

    print("\n  IC MATRIX (mean IC / IR / t / win%)")
    print(f"  {'score':<28}{'tier':<16}{'H':>4}{'IC':>9}{'IR':>8}{'t':>8}{'win%':>8}")
    for _, r in ic_df.sort_values(["score", "liquidity", "horizon"]).iterrows():
        print(f"  {r['score']:<28}{r['liquidity']:<16}{int(r['horizon']):>4}"
              f"{r['ic_mean']:>+9.4f}{r['ic_ir']:>+8.3f}{r['ic_t']:>+8.2f}{r['ic_win']:>7.1f}%")

    print("\n  DECILE D1/D10/spread/mono (h=20)")
    print(f"  {'score':<28}{'tier':<16}{'D1%':>9}{'D10%':>9}{'D10-D1%':>10}{'mono':>8}")
    for sc in active_scores:
        for tier_name, _ in LIQUIDITY_TIERS:
            sub = decile_df[(decile_df.score == sc) & (decile_df.liquidity == tier_name)
                            & (decile_df.horizon == 20)].sort_values("decile")
            if sub.empty:
                continue
            d1 = sub[sub.decile == 1]["mean_ret"]
            d10 = sub[sub.decile == 10]["mean_ret"]
            if d1.empty or d10.empty:
                continue
            d1, d10 = d1.iloc[0] * 100, d10.iloc[0] * 100
            mono = monotonic_score(sub)
            print(f"  {sc:<28}{tier_name:<16}{d1:>+9.3f}{d10:>+9.3f}{d10-d1:>+10.3f}{mono:>+8.3f}")

    print("\n  NET SPREAD (h=20, cost-adj, long-short)")
    print(f"  {'score':<28}{'tier':<16}{'gross%':>9}{'cost':>7}{'net%':>9}{'mono':>8}{'signOK':>8}")
    for _, r in net_df[net_df.horizon == 20].iterrows():
        print(f"  {r['score']:<28}{r['liquidity']:<16}{r['gross_spread']*100:>+9.3f}"
              f"{r['cost_roundtrip']*100:>6.2f}%{r['net_spread']*100:>+9.3f}"
              f"{r['monotonic_rho']:>+8.3f}{str(r['ic_spread_sign_ok']):>8}")

    print("\n  WALK-FORWARD (signed-IC / LS-spread positive years), liq_50m")
    for sc in active_scores:
        sub = wf_df[(wf_df.score == sc) & (wf_df.liquidity == "liq_50m")]
        if sub.empty:
            continue
        d = score_dir[sc]
        pos_ic = (sub["ic_mean"] * (1 if d == "top" else -1) > 0).sum()
        pos_sp = (sub["ls_spread_20d"] > 0).sum()
        print(f"  {sc:<28} signed-IC+ {pos_ic}/{len(sub)} yrs | LS-spread+ {pos_sp}/{len(sub)} yrs")

    print("\n  LOYO (full vs drop-year LS-spread, liq_50m)")
    for sc in active_scores:
        sub = loyo_df[loyo_df.score == sc]
        if sub.empty:
            continue
        full = sub[sub.drop_year == "none(full)"]["ls_spread_20d"]
        full = float(full.iloc[0]) if len(full) else np.nan
        dropped = sub[sub.drop_year != "none(full)"]
        sp2020 = dropped[dropped.drop_year == "2020"]["ls_spread_20d"]
        sp2020 = float(sp2020.iloc[0]) if len(sp2020) else np.nan
        worst = dropped["ls_spread_20d"].min()
        best = dropped["ls_spread_20d"].max()
        print(f"  {sc:<28} full={full*100:+.3f}% | ex-2020={sp2020*100:+.3f}% | "
              f"drop-range [{worst*100:+.3f}%, {best*100:+.3f}%]")

    print("\n  CROSS-REGIME (liq_50m h=20)")
    print(f"  {'score':<28}{'regime':<10}{'IC':>9}{'t':>7}{'spread%':>10}{'mono':>8}")
    for _, r in reg_df.iterrows():
        print(f"  {r['score']:<28}{r['regime']:<10}{r['ic_mean']:>+9.4f}{r['ic_t']:>+7.2f}"
              f"{r['ls_spread_20d']*100:>+10.3f}{r['monotonic_rho']:>+8.3f}")

    print("\n  DEFLATED SHARPE (N_trials = multiple-comparison count)")
    print(f"  {'score':<28}{'tier':<10}{'H':>4}{'SR_ann':>9}{'sr0':>8}{'PSR':>8}{'pass':>7}")
    for _, r in ds_df.iterrows():
        print(f"  {r['score']:<28}{r['liquidity']:<10}{int(r['horizon']):>4}"
              f"{r['sharpe_ann']:>+9.3f}{r['sr0_benchmark']:>+8.3f}{r['psr']:>8.3f}"
              f"{str(r['dsr_pass']):>7}")


def _write_md(ic_df, decile_df, net_df, wf_df, loyo_df, reg_df, ds_df,
              active_scores, score_dir, b_has_pulse, n_zerovol, n_badprice, df, sample):
    """寫 reports/line1_lambda_divergence.md (人讀結論)。verdict 由主 session 填血肉，
    這裡先把所有關鍵數字 + 自動 verdict heuristic 落地，避免 markdown 與 csv 不同步。"""
    lines = []
    lines.append("# 格1：量價彈性 (Kyle lambda / Amihud) 背離 — SOP-14 gauntlet 結果\n")
    lines.append(f"_產出 {pd.Timestamp.now():%Y-%m-%d %H:%M} / "
                 f"{'SAMPLE ' + str(sample) if sample else 'FULL universe'}_\n")
    lines.append("## 資料 caveat\n")
    lines.append(f"- panel `data_cache/backtest/ohlcv_tw.parquet`：{df['yf_ticker'].nunique()} tickers, "
                 f"{df['date'].min().date()}~{df['date'].max().date()}")
    lines.append(f"- **Volume<=0 剔除 {n_zerovol:,} 列** (停牌凍結列；lambda Amihud 分母除零地雷) + "
                 f"Close<=0 剔除 {n_badprice:,} 列")
    lines.append("- lambda_raw cross-section winsorize 1%/99% 後 rank/z")
    lines.append("- **survivor-biased**：此 panel PIT 不完整 (~46% 缺價 backlog)、下市股被排除，"
                 "回測結果偏樂觀；**不可宣稱 PIT-clean**\n")

    def fmt_ic(sc):
        sub = ic_df[(ic_df.score == sc) & (ic_df.liquidity == "liq_50m")].sort_values("horizon")
        rows = []
        for _, r in sub.iterrows():
            rows.append(f"| {int(r['horizon'])} | {r['ic_mean']:+.4f} | {r['ic_ir']:+.3f} "
                        f"| {r['ic_t']:+.2f} | {r['ic_win']:.1f}% |")
        return rows

    for sc, label in active_scores.items():
        lines.append(f"\n## 訊號 `{sc}` — {label}\n")
        lines.append(f"方向 (D10>D1?): **{score_dir[sc]}**\n")
        lines.append("IC matrix (liq_50m):\n")
        lines.append("| H | IC | IR | t | win% |")
        lines.append("|---|---|---|---|---|")
        lines.extend(fmt_ic(sc))
        # decile spread h=20 across tiers
        lines.append("\nDecile (h=20):\n")
        lines.append("| tier | D1% | D10% | D10-D1% | mono_rho |")
        lines.append("|---|---|---|---|---|")
        for tier_name, _ in LIQUIDITY_TIERS:
            sub = decile_df[(decile_df.score == sc) & (decile_df.liquidity == tier_name)
                            & (decile_df.horizon == 20)].sort_values("decile")
            if sub.empty:
                continue
            d1 = sub[sub.decile == 1]["mean_ret"]
            d10 = sub[sub.decile == 10]["mean_ret"]
            if d1.empty or d10.empty:
                continue
            d1v, d10v = d1.iloc[0] * 100, d10.iloc[0] * 100
            mono = monotonic_score(sub)
            lines.append(f"| {tier_name} | {d1v:+.3f} | {d10v:+.3f} | {d10v-d1v:+.3f} | {mono:+.3f} |")
        # net spread h=20 liq_50m
        ns = net_df[(net_df.score == sc) & (net_df.liquidity == "liq_50m")
                    & (net_df.horizon == 20)]
        if not ns.empty:
            lines.append("\nNet spread (h=20 liq_50m, long-short 2 腿):\n")
            lines.append("| cost RT | gross% | net% | sign_ok |")
            lines.append("|---|---|---|---|")
            for _, r in ns.iterrows():
                lines.append(f"| {r['cost_roundtrip']*100:.2f}% | {r['gross_spread']*100:+.3f} "
                             f"| {r['net_spread']*100:+.3f} | {r['ic_spread_sign_ok']} |")
        # WF / LOYO / regime / DSR
        wf = wf_df[(wf_df.score == sc) & (wf_df.liquidity == "liq_50m")]
        if not wf.empty:
            d = score_dir[sc]
            pos_ic = int((wf["ic_mean"] * (1 if d == "top" else -1) > 0).sum())
            pos_sp = int((wf["ls_spread_20d"] > 0).sum())
            lines.append(f"\n- **Walk-forward** (liq_50m, {len(wf)} 年)：signed-IC 正 {pos_ic}/{len(wf)} 年, "
                         f"LS-spread 正 {pos_sp}/{len(wf)} 年")
        lo = loyo_df[loyo_df.score == sc]
        if not lo.empty:
            full = lo[lo.drop_year == "none(full)"]["ls_spread_20d"]
            full = float(full.iloc[0]) if len(full) else float("nan")
            dropped = lo[lo.drop_year != "none(full)"]
            sp2020 = dropped[dropped.drop_year == "2020"]["ls_spread_20d"]
            sp2020 = float(sp2020.iloc[0]) if len(sp2020) else float("nan")
            worst, best = dropped["ls_spread_20d"].min(), dropped["ls_spread_20d"].max()
            lines.append(f"- **LOYO**：full {full*100:+.3f}% / ex-2020 {sp2020*100:+.3f}% / "
                         f"drop-year range [{worst*100:+.3f}%, {best*100:+.3f}%]")
        rg = reg_df[reg_df.score == sc]
        if not rg.empty:
            cells = ", ".join(f"{r['regime']} IC={r['ic_mean']:+.4f} sp={r['ls_spread_20d']*100:+.2f}%"
                              for _, r in rg.iterrows())
            pos_reg = int((rg["ls_spread_20d"] > 0).sum())
            lines.append(f"- **Cross-regime** (spread 正 {pos_reg}/{len(rg)} cell)：{cells}")
        dsr = ds_df[(ds_df.score == sc) & (ds_df.liquidity == "liq_50m")]
        if not dsr.empty:
            for _, r in dsr.iterrows():
                lines.append(f"- **Deflated Sharpe** h={int(r['horizon'])} (N_trials={int(r['n_trials'])})："
                             f"SR_ann {r['sharpe_ann']:+.3f}, sr0_benchmark {r['sr0_benchmark']:+.3f}, "
                             f"PSR {r['psr']:.3f} -> DSR pass={r['dsr_pass']}")

    lines.append("\n---\n")
    lines.append("## 自動 verdict heuristic (主 session 覆寫最終判定)\n")
    lines.append("規則：IC|t|>2 且 net_spread>0 且 mono>=+0.5 且 WF 正年>=60% 且 DSR pass "
                 "→ production；部分滿足 → informational；spread 與 IC 反號 / mono<0.5 / "
                 "net<0 → reject。\n")
    for sc, label in active_scores.items():
        ic20 = ic_df[(ic_df.score == sc) & (ic_df.liquidity == "liq_50m")
                     & (ic_df.horizon == 20)]
        ns20 = net_df[(net_df.score == sc) & (net_df.liquidity == "liq_50m")
                      & (net_df.horizon == 20)]
        verdict = _auto_verdict(sc, ic_df, decile_df, net_df, wf_df, reg_df, ds_df, score_dir)
        ic_t = float(ic20["ic_t"].iloc[0]) if not ic20.empty else float("nan")
        net_min = float(ns20["net_spread"].min()) if not ns20.empty else float("nan")
        lines.append(f"- `{sc}`：**{verdict}**  (IC_t@20={ic_t:+.2f}, net_spread@20 min={net_min*100:+.3f}%)")

    (OUT_DIR / "line1_lambda_divergence.md").write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"Saved markdown report: {OUT_DIR / 'line1_lambda_divergence.md'}")


def _auto_verdict(sc, ic_df, decile_df, net_df, wf_df, reg_df, ds_df, score_dir):
    """機械 verdict heuristic。"""
    d = score_dir[sc]
    ic20 = ic_df[(ic_df.score == sc) & (ic_df.liquidity == "liq_50m") & (ic_df.horizon == 20)]
    if ic20.empty:
        return "D / reject (no data)"
    ic_t = float(ic20["ic_t"].iloc[0])
    ic_m = float(ic20["ic_mean"].iloc[0])
    # decile mono + spread
    sub = decile_df[(decile_df.score == sc) & (decile_df.liquidity == "liq_50m")
                    & (decile_df.horizon == 20)].sort_values("decile")
    mono = monotonic_score(sub) if not sub.empty else float("nan")
    ns = net_df[(net_df.score == sc) & (net_df.liquidity == "liq_50m") & (net_df.horizon == 20)]
    net_min = float(ns["net_spread"].min()) if not ns.empty else float("nan")
    sign_ok = bool(ns["ic_spread_sign_ok"].all()) if not ns.empty else False
    wf = wf_df[(wf_df.score == sc) & (wf_df.liquidity == "liq_50m")]
    wf_pos_frac = float((wf["ls_spread_20d"] > 0).mean()) if not wf.empty else 0.0
    rg = reg_df[reg_df.score == sc]
    reg_pos = int((rg["ls_spread_20d"] > 0).sum()) if not rg.empty else 0
    dsr = ds_df[(ds_df.score == sc) & (ds_df.liquidity == "liq_50m")]
    dsr_pass = bool(dsr["dsr_pass"].any()) if not dsr.empty else False

    # reverse-artifact: spread 與 IC 反號
    if not sign_ok:
        return "D / reject (spread 與 IC 反號 = reverse-artifact)"
    if not np.isnan(mono) and abs(mono) < 0.5:
        tier = "informational" if (abs(ic_t) > 2 and net_min > 0) else "reject"
        return f"C-D / {tier} (單調性弱 mono={mono:+.2f})"
    if abs(ic_t) > 2 and net_min > 0 and wf_pos_frac >= 0.6 and reg_pos >= 2 and dsr_pass:
        return "A-B / production"
    if abs(ic_t) > 2 and (net_min > 0 or wf_pos_frac >= 0.5):
        return "B-C / informational"
    if net_min <= 0:
        return "C-D / reject (淨 spread <=0)"
    return "C / informational"


if __name__ == "__main__":
    main()
