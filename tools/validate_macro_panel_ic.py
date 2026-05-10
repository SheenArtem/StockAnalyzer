"""
validate_macro_panel_ic.py -- 36 features × 11 年 IC validation per SOP 1-14

流程：
  1. 載入 5 個 panel (FRED / breadth / systemic chip / valuation / ETF flows)
  2. Align 到 ^TWII 日頻
  3. 計算 outcome：未來 60d MDD（從 t 開始 60 個交易日的最低/最高比）
  4. 對每個 feature 算 Spearman IC vs 多 lookback (今日/30d/60d 前) 跟未來 MDD
  5. xcorr 找最佳 lag → 鑑別「真 lead」vs「同期重合」
  6. SOP-12 composite vs best-single：top-N features 加權和對 outcome 的 IC
  7. Walk-forward 70/30：sample 內定權重，sample 外驗證
  8. Output markdown report `reports/macro_panel_ic_validation_2026-05-09.md`

執行：python tools/validate_macro_panel_ic.py
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
MACRO = REPO / "data" / "macro"
BREADTH = REPO / "data" / "breadth"
SENTIMENT = REPO / "data" / "sentiment"
OUT_REPORT = REPO / "reports" / "macro_panel_ic_validation_2026-05-09_v2.md"
OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)


# ============================================================
#  載入 panels
# ============================================================

def load_all_panels() -> pd.DataFrame:
    """合併所有 panel 到日頻，回傳 wide df。"""
    dfs = []

    fred = pd.read_parquet(MACRO / "fred_panel.parquet")
    fred['date'] = pd.to_datetime(fred['date'])
    fred = fred.set_index('date')
    logger.info("FRED: %d rows, %d cols", len(fred), len(fred.columns))
    dfs.append(fred)

    breadth = pd.read_parquet(BREADTH / "tw_breadth.parquet")
    breadth['date'] = pd.to_datetime(breadth['date'])
    breadth = breadth.set_index('date')
    logger.info("Breadth: %d rows, %d cols", len(breadth), len(breadth.columns))
    dfs.append(breadth)

    sys_chip = pd.read_parquet(MACRO / "systemic_chip.parquet")
    sys_chip['date'] = pd.to_datetime(sys_chip['date'])
    sys_chip = sys_chip.set_index('date')
    sys_chip = sys_chip.select_dtypes(include=[np.number])  # drop flag/reason 文字欄
    logger.info("Systemic Chip: %d rows, %d cols", len(sys_chip), len(sys_chip.columns))
    dfs.append(sys_chip)

    val = pd.read_parquet(MACRO / "valuation_panel.parquet")
    val['date'] = pd.to_datetime(val['date'])
    val = val.set_index('date').select_dtypes(include=[np.number])
    logger.info("Valuation: %d rows, %d cols", len(val), len(val.columns))
    dfs.append(val)

    etf = pd.read_parquet(MACRO / "etf_flows.parquet")
    etf['date'] = pd.to_datetime(etf['date'])
    etf = etf.set_index('date').select_dtypes(include=[np.number])
    logger.info("ETF Flows: %d rows, %d cols", len(etf), len(etf.columns))
    dfs.append(etf)

    # Phase 3-C 新增：institutional total
    inst_path = MACRO / "institutional_total.parquet"
    if inst_path.exists():
        inst = pd.read_parquet(inst_path)
        inst['date'] = pd.to_datetime(inst['date'])
        inst = inst.set_index('date').select_dtypes(include=[np.number])
        logger.info("Institutional Total: %d rows, %d cols", len(inst), len(inst.columns))
        dfs.append(inst)

    # 2026-05-10 新增：AAII Sentiment (週頻 1987+) + CNN FGI (日頻 2011+)
    aaii_path = MACRO / "aaii_sentiment.parquet"
    if aaii_path.exists():
        aaii = pd.read_parquet(aaii_path)
        aaii['date'] = pd.to_datetime(aaii['date'])
        aaii = aaii.set_index('date').select_dtypes(include=[np.number])
        logger.info("AAII Sentiment: %d rows, %d cols", len(aaii), len(aaii.columns))
        dfs.append(aaii)

    cnn_fgi_path = SENTIMENT / "cnn_fgi_history.parquet"
    if cnn_fgi_path.exists():
        cnn_fgi = pd.read_parquet(cnn_fgi_path)
        cnn_fgi['date'] = pd.to_datetime(cnn_fgi['date'])
        cnn_fgi = cnn_fgi.set_index('date').select_dtypes(include=[np.number])
        logger.info("CNN FGI: %d rows, %d cols", len(cnn_fgi), len(cnn_fgi.columns))
        dfs.append(cnn_fgi)

    panel = pd.concat(dfs, axis=1).sort_index()
    # rename duplicates: pandas auto-suffixes, but our columns shouldn't overlap; verify
    panel = panel.loc[:, ~panel.columns.duplicated()]

    # forward fill
    panel = panel.ffill()
    return panel


# ============================================================
#  Outcome：未來 60d MDD
# ============================================================

def compute_future_mdd(twii: pd.Series, horizon: int = 60) -> pd.Series:
    """對每個 t，計算 t 到 t+horizon 期間的最大 drawdown (%)。"""
    out = pd.Series(np.nan, index=twii.index)
    n = len(twii)
    arr = twii.values
    for i in range(n - horizon):
        seg = arr[i:i + horizon + 1]
        peak = np.maximum.accumulate(seg)
        dd = (seg - peak) / peak
        out.iloc[i] = dd.min() * 100  # negative = drawdown
    return out


# ============================================================
#  Univariate IC
# ============================================================

def spearman_ic(feat: pd.Series, outcome: pd.Series) -> float:
    """Spearman IC = correlation of ranks. NaN-tolerant."""
    df = pd.concat([feat, outcome], axis=1).dropna()
    if len(df) < 30:
        return np.nan
    rho, _ = stats.spearmanr(df.iloc[:, 0], df.iloc[:, 1])
    return rho


def hit_rate_top_decile(feat: pd.Series, outcome: pd.Series, mdd_threshold: float = -10) -> float:
    """top decile of feature → fraction of times outcome ≤ threshold."""
    df = pd.concat([feat, outcome], axis=1).dropna()
    if len(df) < 100:
        return np.nan
    p90 = df.iloc[:, 0].quantile(0.9)
    p10 = df.iloc[:, 0].quantile(0.1)
    # 不知方向，兩端都試
    top = df[df.iloc[:, 0] >= p90].iloc[:, 1]
    bot = df[df.iloc[:, 0] <= p10].iloc[:, 1]
    rate_top = (top <= mdd_threshold).mean() if len(top) > 0 else np.nan
    rate_bot = (bot <= mdd_threshold).mean() if len(bot) > 0 else np.nan
    return max(rate_top or 0, rate_bot or 0)


def xcorr_peak_lag(feat: pd.Series, outcome: pd.Series, max_lag: int = 60) -> tuple[int, float]:
    """找最佳 lag (-60 to 0, neg = feature 領先 outcome)。"""
    df = pd.concat([feat, outcome], axis=1).dropna()
    if len(df) < 200:
        return 0, np.nan
    f = df.iloc[:, 0].values
    o = df.iloc[:, 1].values
    best_lag, best_rho = 0, 0
    for lag in range(0, max_lag + 1):  # lag ≥ 0 means feat at t-lag predicts outcome at t
        if lag == 0:
            f_shift = f
            o_aligned = o
        else:
            f_shift = f[:-lag]
            o_aligned = o[lag:]
        if len(f_shift) < 100:
            continue
        rho, _ = stats.spearmanr(f_shift, o_aligned)
        if abs(rho) > abs(best_rho):
            best_rho = rho
            best_lag = lag
    return best_lag, best_rho


# ============================================================
#  Composite
# ============================================================

def composite_score(panel: pd.DataFrame, weights: dict, directions: dict) -> pd.Series:
    """各 feature rolling-rank（百分位）× direction × weight 求和。"""
    out = pd.Series(0.0, index=panel.index)
    total_w = 0
    for feat, w in weights.items():
        if feat not in panel.columns:
            continue
        s = panel[feat]
        rank = s.rolling(2520, min_periods=252).rank(pct=True) * 100  # 0-100
        # direction: -1 means hi value = low danger (reverse)
        d = directions.get(feat, 1)
        out = out + rank * d * w
        total_w += abs(w)
    if total_w > 0:
        out = out / total_w
    return out


# ============================================================
#  主流程
# ============================================================

def main():
    panel = load_all_panels()
    logger.info("Combined panel: %d rows, %d cols (date %s ~ %s)",
                len(panel), len(panel.columns), panel.index.min(), panel.index.max())

    # outcome: 未來 60d MDD（取 sp500_close 跟 twii_close 用 twii 較合理當 outcome）
    if 'twii_close' not in panel.columns:
        # build twii from yfinance if missing
        import yfinance as yf
        twii_s = yf.Ticker('^TWII').history(period='15y')['Close']
        twii_s.index = pd.to_datetime(twii_s.index.tz_localize(None).date) if twii_s.index.tz else pd.to_datetime(twii_s.index.date)
        panel['twii_close'] = twii_s.reindex(panel.index).ffill()

    twii = panel['twii_close'].dropna()
    outcome_60d = compute_future_mdd(twii, horizon=60)
    outcome_40d = compute_future_mdd(twii, horizon=40)
    outcome_20d = compute_future_mdd(twii, horizon=20)

    panel['outcome_mdd_60d'] = outcome_60d
    panel['outcome_mdd_40d'] = outcome_40d
    panel['outcome_mdd_20d'] = outcome_20d

    # 排除 outcome 跟 derived 的欄位作為 feature，且排除 close-level price 因為跟 outcome 高度自相關
    EXCLUDE = {
        'outcome_mdd_60d', 'outcome_mdd_40d', 'outcome_mdd_20d',
        'twii_close', 'sp500_close',
        'us_gdp_billion', 'us_nonfin_corp_equity',
        'fed_bs_million_usd',  # raw 數值，用 chg_4w
        'us_initial_claims', 'us_durable_goods_orders',  # raw 月資料
        'us_unemployment_rate', 'us_consumer_sentiment',  # raw 月資料
        # rolling rank already encoded in 'rank' columns
        'hyg_volume', 'jnk_volume', 'lqd_volume', 'tlt_volume', 'spy_volume',
        'hyg_close', 'jnk_close', 'lqd_close', 'tlt_close', 'spy_close',
        'hyg_volume_ma20',
        # binary
        'yield_curve_10y_2y_inverted', 'yield_curve_10y_3m_inverted',
        # margin raw 量級不一致
        'margin_long_total', 'margin_short_total', 'sbl_total',
        # 無 ffill 後可能極端 NaN
        'unchanged', 'advances', 'declines',
        # adl 累計值會被 trend 主導 (用 mcclellan 看就好)
        'adl', 'adl_ma20',
        'foreign_holding_avg',  # raw level 低訊號，已有 _chg_4w
    }
    feature_cols = [c for c in panel.columns
                    if c not in EXCLUDE and pd.api.types.is_numeric_dtype(panel[c])]

    logger.info("Will validate %d features against future MDD", len(feature_cols))

    # Univariate IC
    rows = []
    for col in feature_cols:
        feat = panel[col]
        ic_60 = spearman_ic(feat, outcome_60d)
        ic_40 = spearman_ic(feat, outcome_40d)
        ic_20 = spearman_ic(feat, outcome_20d)
        hit_60 = hit_rate_top_decile(feat, outcome_60d, -10)
        lag, lag_rho = xcorr_peak_lag(feat, outcome_60d, max_lag=60)
        n_valid = panel[[col, 'outcome_mdd_60d']].dropna().shape[0]
        rows.append({
            'feature': col,
            'n_valid': n_valid,
            'ic_60d': ic_60,
            'ic_40d': ic_40,
            'ic_20d': ic_20,
            'hit_top_decile_60d': hit_60,
            'best_lag_60d': lag,
            'best_lag_ic': lag_rho,
            'abs_ic_60d': abs(ic_60) if not pd.isna(ic_60) else 0,
        })

    ic_df = pd.DataFrame(rows).sort_values('abs_ic_60d', ascending=False).reset_index(drop=True)

    # Top features
    top10 = ic_df.head(10)
    logger.info("Top 10 features by |IC 60d|:")
    for _, r in top10.iterrows():
        logger.info("  %s: IC=%+.3f hit=%s lag=%d",
                    r['feature'], r['ic_60d'],
                    f"{r['hit_top_decile_60d']:.2%}" if not pd.isna(r['hit_top_decile_60d']) else 'N/A',
                    r['best_lag_60d'])

    # Composite test (top 10 weighted by abs IC)
    weights = {}
    directions = {}
    for _, r in top10.iterrows():
        f = r['feature']
        ic = r['ic_60d']
        if pd.isna(ic) or abs(ic) < 0.05:
            continue
        weights[f] = abs(ic)
        directions[f] = -1 if ic > 0 else 1
        # IC > 0 means high feature → high (less negative) MDD = LOW danger
        # We want score: high = high danger, so direction = -sign(IC)

    comp = composite_score(panel, weights, directions)
    panel['composite'] = comp

    comp_ic_60 = spearman_ic(comp, outcome_60d)
    comp_ic_40 = spearman_ic(comp, outcome_40d)
    comp_ic_20 = spearman_ic(comp, outcome_20d)

    best_single_ic = top10.iloc[0]['ic_60d']
    best_single_name = top10.iloc[0]['feature']

    logger.info("Composite IC 60d: %+.3f vs best single (%s) IC: %+.3f",
                comp_ic_60, best_single_name, best_single_ic)

    # SOP-12 verdict
    sop12_pass = abs(comp_ic_60) > abs(best_single_ic)
    logger.info("SOP-12 (composite > best single): %s", "PASS" if sop12_pass else "FAIL")

    # ============================================================
    #  Output report
    # ============================================================
    rep = []
    rep.append(f"# Macro Panel IC Validation Report\n")
    rep.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
    rep.append(f"**Panel**: {len(panel)} rows, {len(feature_cols)} features\n")
    rep.append(f"**Date range**: {panel.index.min().date()} ~ {panel.index.max().date()}\n")
    rep.append(f"**Outcome**: future 60d MDD (negative = drawdown)\n\n")

    rep.append("## Top 15 Features by |IC 60d|\n\n")
    rep.append("| Rank | Feature | N | IC 60d | IC 40d | IC 20d | Hit top-10% | Lag (days) | Lag IC |\n")
    rep.append("|------|---------|---|--------|--------|--------|-------------|------------|--------|\n")
    for i, r in ic_df.head(15).iterrows():
        rep.append(f"| {i+1} | `{r['feature']}` | {r['n_valid']} | "
                   f"{r['ic_60d']:+.3f} | {r['ic_40d']:+.3f} | {r['ic_20d']:+.3f} | "
                   f"{r['hit_top_decile_60d']:.1%} | {r['best_lag_60d']} | {r['best_lag_ic']:+.3f} |\n")

    rep.append("\n## Composite Test (SOP-12)\n\n")
    rep.append(f"- **Composite IC 60d**: {comp_ic_60:+.3f}\n")
    rep.append(f"- **Composite IC 40d**: {comp_ic_40:+.3f}\n")
    rep.append(f"- **Composite IC 20d**: {comp_ic_20:+.3f}\n")
    rep.append(f"- **Best single feature**: `{best_single_name}` IC 60d = {best_single_ic:+.3f}\n")
    rep.append(f"- **SOP-12 verdict**: **{'✅ PASS' if sop12_pass else '❌ FAIL'}**\n")
    rep.append(f"  ({'Composite IC' if sop12_pass else 'Best single'} 較強，"
               f"absolute |composite| {abs(comp_ic_60):.3f} vs |best single| {abs(best_single_ic):.3f})\n\n")

    rep.append("## Composite Weights & Directions (Top 10)\n\n")
    rep.append("| Feature | Weight | Direction | Interpretation |\n")
    rep.append("|---------|--------|-----------|----------------|\n")
    for f, w in weights.items():
        d = directions[f]
        interp = "高值=danger" if d == 1 else "高值=safe (反向)"
        rep.append(f"| `{f}` | {w:.3f} | {d:+d} | {interp} |\n")

    rep.append("\n## All Features (Full Table)\n\n")
    rep.append("| Feature | N | IC 60d | IC 40d | IC 20d | Hit top-10% | Lag |\n")
    rep.append("|---------|---|--------|--------|--------|-------------|-----|\n")
    for _, r in ic_df.iterrows():
        rep.append(f"| `{r['feature']}` | {r['n_valid']} | "
                   f"{r['ic_60d']:+.3f} | {r['ic_40d']:+.3f} | {r['ic_20d']:+.3f} | "
                   f"{r['hit_top_decile_60d']:.1%} | {r['best_lag_60d']}d |\n")

    rep.append("\n## Methodology Notes\n\n")
    rep.append("- **IC**: Spearman rank correlation of feature value at time `t` vs future MDD over `[t, t+H]`\n")
    rep.append("- **Hit top-10%**: when feature is in top decile, what fraction of times MDD ≤ -10% within 60d\n")
    rep.append("- **Best lag**: xcorr peak lag in 0-60 days (≥0 means feature precedes outcome)\n")
    rep.append("- **Composite**: top-10 features weighted by |IC|, signed by direction (-1 if IC > 0 else 1)\n")
    rep.append("- **Composite IC interpretation**: positive = high composite → high MDD risk\n\n")

    rep.append("## Caveats (SOP 1-14)\n\n")
    rep.append("- 此驗證為 **continuous outcome** (Spearman IC vs future MDD)，與 System 2 的 N=77 discrete events 互補\n")
    rep.append("- xcorr peak lag 顯示「同期重合 vs 真領先」：lag>0 才是真 leading signal\n")
    rep.append("- 若 SOP-12 FAIL → composite 不接 portfolio gating，僅 informational tier (SOP-14)\n")
    rep.append("- 此驗證 **未做 walk-forward**（in-sample fit）；上線前需另跑 70/30 split\n")

    OUT_REPORT.write_text(''.join(rep), encoding='utf-8')
    logger.info("Report saved -> %s", OUT_REPORT)

    # Also save IC csv for downstream use
    ic_df.to_csv(REPO / "reports" / "macro_panel_ic_2026-05-09_v2.csv", index=False)
    logger.info("IC table saved -> reports/macro_panel_ic_2026-05-09_v2.csv")


if __name__ == '__main__':
    main()
