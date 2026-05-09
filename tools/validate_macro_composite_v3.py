"""
validate_macro_composite_v3.py -- Composite refactor (Phase 4)

V2 SOP-12 FAIL 後 refactor:
  - **Lag-aware weight**：slow features (lag>30) 折減 0.5；coincident (lag=0) 折減 0.7；
    real lead (lag 1-30) 1.0
  - **Pearson cluster dedup**：相似 features (|Pearson|>0.75) 集群只留最強
  - **Top-N filter**：min |IC|>0.15，max 取 8 個
  - **Three composite candidates**:
    1. raw top-N (V2 同一邏輯)
    2. lag-weighted top-N
    3. lag-weighted + dedup top-N

輸出：reports/macro_panel_ic_validation_2026-05-09_v3.md

執行：python tools/validate_macro_composite_v3.py
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
OUT_REPORT = REPO / "reports" / "macro_panel_ic_validation_2026-05-09_v3.md"


def load_all_panels() -> pd.DataFrame:
    dfs = []
    for path, name in [
        (MACRO / "fred_panel.parquet", "FRED"),
        (BREADTH / "tw_breadth.parquet", "Breadth"),
        (MACRO / "systemic_chip.parquet", "Systemic Chip"),
        (MACRO / "valuation_panel.parquet", "Valuation"),
        (MACRO / "etf_flows.parquet", "ETF Flows"),
        (MACRO / "institutional_total.parquet", "Inst Total"),
    ]:
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').select_dtypes(include=[np.number])
        logger.info("%s: %d rows × %d cols", name, len(df), len(df.columns))
        dfs.append(df)
    panel = pd.concat(dfs, axis=1).sort_index()
    panel = panel.loc[:, ~panel.columns.duplicated()]
    panel = panel.ffill()
    return panel


def compute_future_mdd(twii: pd.Series, horizon: int = 60) -> pd.Series:
    out = pd.Series(np.nan, index=twii.index)
    arr = twii.values
    n = len(arr)
    for i in range(n - horizon):
        seg = arr[i:i + horizon + 1]
        peak = np.maximum.accumulate(seg)
        dd = (seg - peak) / peak
        out.iloc[i] = dd.min() * 100
    return out


def spearman_ic(feat: pd.Series, outcome: pd.Series) -> float:
    df = pd.concat([feat, outcome], axis=1).dropna()
    if len(df) < 30:
        return np.nan
    rho, _ = stats.spearmanr(df.iloc[:, 0], df.iloc[:, 1])
    return rho


def xcorr_peak_lag(feat: pd.Series, outcome: pd.Series, max_lag: int = 60) -> tuple[int, float]:
    df = pd.concat([feat, outcome], axis=1).dropna()
    if len(df) < 200:
        return 0, np.nan
    f = df.iloc[:, 0].values
    o = df.iloc[:, 1].values
    best_lag, best_rho = 0, 0.0
    for lag in range(0, max_lag + 1):
        if lag == 0:
            f_shift, o_aligned = f, o
        else:
            f_shift, o_aligned = f[:-lag], o[lag:]
        if len(f_shift) < 100:
            continue
        rho, _ = stats.spearmanr(f_shift, o_aligned)
        if not np.isnan(rho) and abs(rho) > abs(best_rho):
            best_rho, best_lag = rho, lag
    return best_lag, best_rho


def lag_weight(lag: int) -> float:
    """Lag-aware weight：lag 1-30 = 1.0；lag=0 coincident = 0.7；lag>30 slow = 0.5"""
    if 1 <= lag <= 30:
        return 1.0
    if lag == 0:
        return 0.7
    return 0.5


def dedup_features(panel: pd.DataFrame, ic_df: pd.DataFrame,
                   pearson_thresh: float = 0.75) -> pd.DataFrame:
    """同集群只留 |IC| 最強的 1 個。"""
    keep = []
    dropped = {}
    sorted_features = ic_df.sort_values('abs_ic_60d', ascending=False)
    for _, r in sorted_features.iterrows():
        feat = r['feature']
        if feat not in panel.columns:
            continue
        s = panel[feat].dropna()
        if len(s) < 100:
            continue
        # Check Pearson against already-kept
        is_dup = False
        for k in keep:
            ks = panel[k].dropna()
            common = s.index.intersection(ks.index)
            if len(common) < 100:
                continue
            corr = s.loc[common].corr(ks.loc[common])
            if abs(corr) > pearson_thresh:
                dropped[feat] = (k, corr)
                is_dup = True
                break
        if not is_dup:
            keep.append(feat)
    logger.info("Dedup: kept %d / dropped %d (Pearson > %.2f)",
                len(keep), len(dropped), pearson_thresh)
    for f, (k, c) in list(dropped.items())[:10]:
        logger.info("  drop %s (corr %.2f vs %s)", f, c, k)
    return ic_df[ic_df['feature'].isin(keep)].reset_index(drop=True), dropped


def composite_score(panel: pd.DataFrame, ic_table: pd.DataFrame,
                    use_lag_weight: bool = True, top_n: int = 8) -> pd.Series:
    """權重 = |IC| × lag_factor (if use_lag_weight)；direction = -sign(IC)."""
    out = pd.Series(0.0, index=panel.index)
    total_w = 0.0
    rows = ic_table.head(top_n)
    for _, r in rows.iterrows():
        feat = r['feature']
        ic = r['ic_60d']
        if feat not in panel.columns or pd.isna(ic) or abs(ic) < 0.10:
            continue
        s = panel[feat]
        rank = s.rolling(2520, min_periods=252).rank(pct=True) * 100
        direction = -1 if ic > 0 else 1  # positive IC = high feat -> high (less negative) MDD = LOW danger; reverse
        weight = abs(ic)
        if use_lag_weight:
            weight *= lag_weight(int(r.get('best_lag_60d', 0)))
        out = out + rank * direction * weight
        total_w += abs(weight)
    if total_w > 0:
        out = out / total_w
    return out


def main():
    panel = load_all_panels()
    logger.info("Panel: %d rows, %d cols", len(panel), len(panel.columns))

    if 'twii_close' not in panel.columns:
        import yfinance as yf
        twii_s = yf.Ticker('^TWII').history(period='15y')['Close']
        twii_s.index = pd.to_datetime(twii_s.index.tz_localize(None).date) if twii_s.index.tz else pd.to_datetime(twii_s.index.date)
        panel['twii_close'] = twii_s.reindex(panel.index).ffill()

    twii = panel['twii_close'].dropna()
    outcome_60d = compute_future_mdd(twii, horizon=60)
    outcome_40d = compute_future_mdd(twii, horizon=40)
    outcome_20d = compute_future_mdd(twii, horizon=20)

    EXCLUDE = {
        'twii_close', 'sp500_close', 'us_gdp_billion', 'us_nonfin_corp_equity',
        'fed_bs_million_usd', 'us_initial_claims', 'us_durable_goods_orders',
        'us_unemployment_rate', 'us_consumer_sentiment',
        'hyg_volume', 'jnk_volume', 'lqd_volume', 'tlt_volume', 'spy_volume',
        'eem_volume', 'emb_volume', 'fxi_volume', 'ewj_volume', 'move_volume',
        'hyg_close', 'jnk_close', 'lqd_close', 'tlt_close', 'spy_close',
        'hyg_volume_ma20', 'hyg_dollar_flow_ma20',
        'yield_curve_10y_2y_inverted', 'yield_curve_10y_3m_inverted',
        'margin_long_total', 'margin_short_total', 'sbl_total',
        'unchanged', 'advances', 'declines', 'adl', 'adl_ma20',
        'foreign_holding_avg',
        # institutional total raw values 過大尺度
        'foreign_investor_net', 'foreign_dealer_net', 'dealer_self_net',
        'dealer_hedging_net', 'three_majors_total_net', 'foreign_total_net',
        'trust_net', 'dealer_total_net', 'foreign_trust_divergence',
    }
    feature_cols = [c for c in panel.columns
                    if c not in EXCLUDE and pd.api.types.is_numeric_dtype(panel[c])]
    logger.info("Features to validate: %d", len(feature_cols))

    rows = []
    for col in feature_cols:
        feat = panel[col]
        ic_60 = spearman_ic(feat, outcome_60d)
        ic_40 = spearman_ic(feat, outcome_40d)
        ic_20 = spearman_ic(feat, outcome_20d)
        lag, lag_rho = xcorr_peak_lag(feat, outcome_60d, max_lag=60)
        rows.append({
            'feature': col,
            'ic_60d': ic_60, 'ic_40d': ic_40, 'ic_20d': ic_20,
            'best_lag_60d': lag, 'best_lag_ic': lag_rho,
            'abs_ic_60d': abs(ic_60) if not pd.isna(ic_60) else 0,
        })

    ic_df = pd.DataFrame(rows).sort_values('abs_ic_60d', ascending=False).reset_index(drop=True)

    # Filter: |IC| > 0.10
    ic_strong = ic_df[ic_df['abs_ic_60d'] > 0.10].copy()
    logger.info("Filtered to %d features with |IC| > 0.10", len(ic_strong))

    # Dedup
    ic_dedup, dropped = dedup_features(panel, ic_strong)
    logger.info("After dedup: %d features", len(ic_dedup))

    # Three candidate composites
    panel['comp_v2_raw'] = composite_score(panel, ic_strong, use_lag_weight=False, top_n=10)
    panel['comp_v3_lag_weighted'] = composite_score(panel, ic_strong, use_lag_weight=True, top_n=10)
    panel['comp_v3_dedup_top5'] = composite_score(panel, ic_dedup, use_lag_weight=True, top_n=5)
    panel['comp_v3_dedup_top8'] = composite_score(panel, ic_dedup, use_lag_weight=True, top_n=8)

    results = {}
    for name in ['comp_v2_raw', 'comp_v3_lag_weighted',
                 'comp_v3_dedup_top5', 'comp_v3_dedup_top8']:
        ic_60 = spearman_ic(panel[name], outcome_60d)
        ic_40 = spearman_ic(panel[name], outcome_40d)
        ic_20 = spearman_ic(panel[name], outcome_20d)
        results[name] = {'60d': ic_60, '40d': ic_40, '20d': ic_20}
        logger.info("%s: 60d=%+.3f 40d=%+.3f 20d=%+.3f",
                    name, ic_60, ic_40, ic_20)

    best_single = ic_df.iloc[0]
    logger.info("Best single: %s IC 60d=%+.3f", best_single['feature'], best_single['ic_60d'])

    # SOP-12 verdicts
    verdicts = {}
    for name, vals in results.items():
        verdicts[name] = abs(vals['60d']) > abs(best_single['ic_60d'])
        logger.info("%s SOP-12: %s", name, "PASS" if verdicts[name] else "FAIL")

    # ============================================================
    #  Output report
    # ============================================================
    rep = []
    rep.append(f"# Macro Panel IC Validation V3 (Composite Refactor)\n\n")
    rep.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
    rep.append(f"**Panel**: {len(panel)} rows × {len(feature_cols)} features\n")
    rep.append(f"**Outcome**: future 60d/40d/20d MDD\n\n")

    rep.append("## Composite Comparison (4 variants vs best single)\n\n")
    rep.append("| Variant | IC 60d | IC 40d | IC 20d | SOP-12 (60d) |\n")
    rep.append("|---|---|---|---|---|\n")
    for name, vals in results.items():
        rep.append(f"| `{name}` | {vals['60d']:+.3f} | {vals['40d']:+.3f} | "
                   f"{vals['20d']:+.3f} | {'✅ PASS' if verdicts[name] else '❌ FAIL'} |\n")
    rep.append(f"| **best single (`{best_single['feature']}`)** | "
               f"{best_single['ic_60d']:+.3f} | {best_single['ic_40d']:+.3f} | "
               f"{best_single['ic_20d']:+.3f} | (baseline) |\n\n")

    rep.append("## Composite Configuration\n\n")
    rep.append("- **comp_v2_raw**: top-10, weight=|IC|, no lag adjustment (V2 logic for comparison)\n")
    rep.append("- **comp_v3_lag_weighted**: top-10, weight=|IC| × lag_factor (slow 0.5 / coincident 0.7 / lead 1.0)\n")
    rep.append("- **comp_v3_dedup_top5**: dedup Pearson>0.75 + lag-weighted, top-5 only\n")
    rep.append("- **comp_v3_dedup_top8**: dedup Pearson>0.75 + lag-weighted, top-8\n\n")

    rep.append("## Top 10 by |IC 60d| (after filter |IC|>0.10)\n\n")
    rep.append("| Rank | Feature | IC 60d | Lag | Lag-Weight | Cluster |\n")
    rep.append("|---|---|---|---|---|---|\n")
    for i, r in ic_strong.head(15).iterrows():
        lw = lag_weight(int(r['best_lag_60d']))
        in_dedup = r['feature'] in ic_dedup['feature'].values
        rep.append(f"| {i+1} | `{r['feature']}` | {r['ic_60d']:+.3f} | "
                   f"{int(r['best_lag_60d'])}d | {lw:.1f} | "
                   f"{'KEEP' if in_dedup else 'DROP (dup)'} |\n")

    if dropped:
        rep.append("\n## Dedup Drops (similar features removed)\n\n")
        rep.append("| Feature dropped | Pearson | Kept (stronger) |\n")
        rep.append("|---|---|---|\n")
        for f, (k, c) in list(dropped.items())[:15]:
            rep.append(f"| `{f}` | {c:+.2f} | `{k}` |\n")

    rep.append("\n## Verdict\n\n")
    best_variant = max(results.items(), key=lambda x: abs(x[1]['60d']))
    rep.append(f"**Best variant**: `{best_variant[0]}` IC 60d = {best_variant[1]['60d']:+.3f}\n")
    rep.append(f"**vs best single ({best_single['feature']})**: {best_single['ic_60d']:+.3f}\n")
    if verdicts[best_variant[0]]:
        rep.append(f"\n✅ **SOP-12 PASS** — composite 救回，可進 Banner v4 production\n")
    else:
        rep.append(f"\n❌ **SOP-12 FAIL** — composite 仍敗 best single；建議放棄 composite，"
                   f"直接用 best single (`{best_single['feature']}`) 作 informational tier\n")

    OUT_REPORT.write_text(''.join(rep), encoding='utf-8')
    logger.info("Report saved -> %s", OUT_REPORT)


if __name__ == '__main__':
    main()
