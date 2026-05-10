"""
validate_macro_composite_v4.py -- V4 IC re-run after S2 systemic_chip rebuild

Same logic as V3 (Pearson dedup + lag-aware weight + top-N filter)，
但 panel 含 S2 新加的 systemic_chip 欄位:
  - trust_buy_streak (Group C)
  - trust_5d_zscore (Group C)
  - option_top1_concentration (Group D)
  - pcr_oi (Group D，V3 silent-fail 修復)
  - foreign_fut_net_chg_4w (Group A)
  - hyg_volume_z_252d / tlt_spy_chg_4w (Group E，etf_flows 已含，會 dedup)

Verdict 對照 V3 baseline:
  V3 dedup_top8: 60d=-0.422 / 40d=-0.348 / 20d=-0.246  ✅ PASS

PASS+ : V4 任一 horizon ≥ 0.05 absolute IC improvement → 升級 banner v4
NEUTRAL: V4 ≈ V3 (差距 < 0.05 全 horizon) → 不升級，新 features informational
PASS- : V4 比 V3 弱 → 砍拖累 features

輸出: reports/macro_panel_ic_validation_2026-05-09_v4.md
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
OUT_REPORT = REPO / "reports" / "macro_panel_ic_validation_2026-05-09_v4.md"

# V3 baseline for comparison
V3_BASELINE = {
    'best_variant': 'comp_v3_dedup_top8',
    'ic_60d': -0.422, 'ic_40d': -0.348, 'ic_20d': -0.246,
    'best_single_feature': 'buffett_indicator_us',
    'best_single_ic_60d': -0.371, 'best_single_ic_40d': -0.329, 'best_single_ic_20d': -0.281,
    'panel_features': 75,
    'top8_keep': ['buffett_indicator_us', 'us_buffett_strict_rank', 'us_durable_yoy',
                  'fed_bs_trillion', 'st_louis_fsi', 'buffett_rank_tw',
                  'hyg_dollar_flow', 'usdjpy_close'],  # from V3 report Top 10
}

# New features S2 added to systemic_chip (true V4 deltas)
V4_NEW_FEATURES = {
    'trust_buy_streak': 'Group C — 投信連買日數',
    'trust_5d_zscore': 'Group C — 投信 5d 連買 z-score',
    'option_top1_concentration': 'Group D — 選擇權 top1 集中度',
    'pcr_oi': 'Group D — Put/Call OI 比 (V3 silent-fail 修復)',
    'foreign_fut_net_chg_4w': 'Group A — 外資台指期淨 4w 變動',
    'foreign_net_oi': 'Group A — 外資台指期淨 OI (raw)',
}


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
    if 1 <= lag <= 30:
        return 1.0
    if lag == 0:
        return 0.7
    return 0.5


def dedup_features(panel: pd.DataFrame, ic_df: pd.DataFrame,
                   pearson_thresh: float = 0.75) -> tuple[pd.DataFrame, dict]:
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
    logger.info("Dedup: kept %d / dropped %d", len(keep), len(dropped))
    return ic_df[ic_df['feature'].isin(keep)].reset_index(drop=True), dropped


def composite_score(panel: pd.DataFrame, ic_table: pd.DataFrame,
                    use_lag_weight: bool = True, top_n: int = 8) -> pd.Series:
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
        direction = -1 if ic > 0 else 1
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
    ic_strong = ic_df[ic_df['abs_ic_60d'] > 0.10].copy()
    logger.info("Filtered to %d features with |IC| > 0.10", len(ic_strong))

    ic_dedup, dropped = dedup_features(panel, ic_strong)
    logger.info("After dedup: %d features", len(ic_dedup))

    panel['comp_v2_raw'] = composite_score(panel, ic_strong, use_lag_weight=False, top_n=10)
    panel['comp_v3_lag_weighted'] = composite_score(panel, ic_strong, use_lag_weight=True, top_n=10)
    panel['comp_v3_dedup_top5'] = composite_score(panel, ic_dedup, use_lag_weight=True, top_n=5)
    panel['comp_v4_dedup_top8'] = composite_score(panel, ic_dedup, use_lag_weight=True, top_n=8)

    results = {}
    for name in ['comp_v2_raw', 'comp_v3_lag_weighted', 'comp_v3_dedup_top5', 'comp_v4_dedup_top8']:
        ic_60 = spearman_ic(panel[name], outcome_60d)
        ic_40 = spearman_ic(panel[name], outcome_40d)
        ic_20 = spearman_ic(panel[name], outcome_20d)
        results[name] = {'60d': ic_60, '40d': ic_40, '20d': ic_20}
        logger.info("%s: 60d=%+.3f 40d=%+.3f 20d=%+.3f", name, ic_60, ic_40, ic_20)

    best_single = ic_df.iloc[0]
    logger.info("Best single: %s IC 60d=%+.3f", best_single['feature'], best_single['ic_60d'])

    # SOP-12 verdicts
    verdicts = {}
    for name, vals in results.items():
        verdicts[name] = abs(vals['60d']) > abs(best_single['ic_60d'])

    # ============================================================
    #  V4 vs V3 comparison: did new features help?
    # ============================================================
    v4_best = results['comp_v4_dedup_top8']
    delta_60 = abs(v4_best['60d']) - abs(V3_BASELINE['ic_60d'])
    delta_40 = abs(v4_best['40d']) - abs(V3_BASELINE['ic_40d'])
    delta_20 = abs(v4_best['20d']) - abs(V3_BASELINE['ic_20d'])

    if max(delta_60, delta_40, delta_20) >= 0.05:
        v4_verdict = 'PASS+ (升級建議)'
    elif min(delta_60, delta_40, delta_20) <= -0.05:
        v4_verdict = 'PASS- (V4 弱化，建議砍新 features)'
    else:
        v4_verdict = 'NEUTRAL (差距 < 0.05，新 features 標 informational)'

    # Top-N keep list V4
    v4_top8 = ic_dedup.head(8)['feature'].tolist()
    v3_top8 = V3_BASELINE['top8_keep']
    new_in_v4 = [f for f in v4_top8 if f not in v3_top8]
    dropped_from_v3 = [f for f in v3_top8 if f not in v4_top8]

    # New V4 systemic_chip features — did any of them rank in top?
    new_feat_ic = ic_df[ic_df['feature'].isin(V4_NEW_FEATURES.keys())].copy()

    # ============================================================
    #  Output report
    # ============================================================
    rep = []
    rep.append(f"# Macro Panel IC Validation V4 (vs V3)\n\n")
    rep.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
    rep.append(f"**Panel**: {len(panel)} rows × {len(feature_cols)} features (V3 had 75)\n")
    rep.append(f"**Outcome**: future 60d/40d/20d MDD\n\n")

    rep.append("## V4 vs V3 Verdict\n\n")
    rep.append(f"**Verdict**: {v4_verdict}\n\n")
    rep.append("| Horizon | V3 dedup_top8 | V4 dedup_top8 | Delta |abs IC| |\n")
    rep.append("|---|---|---|---|\n")
    rep.append(f"| 60d | {V3_BASELINE['ic_60d']:+.3f} | {v4_best['60d']:+.3f} | "
               f"{delta_60:+.3f} |\n")
    rep.append(f"| 40d | {V3_BASELINE['ic_40d']:+.3f} | {v4_best['40d']:+.3f} | "
               f"{delta_40:+.3f} |\n")
    rep.append(f"| 20d | {V3_BASELINE['ic_20d']:+.3f} | {v4_best['20d']:+.3f} | "
               f"{delta_20:+.3f} |\n\n")

    rep.append("## All V4 Composite Variants vs Best Single\n\n")
    rep.append("| Variant | IC 60d | IC 40d | IC 20d | SOP-12 (60d) |\n")
    rep.append("|---|---|---|---|---|\n")
    for name, vals in results.items():
        rep.append(f"| `{name}` | {vals['60d']:+.3f} | {vals['40d']:+.3f} | "
                   f"{vals['20d']:+.3f} | {'PASS' if verdicts[name] else 'FAIL'} |\n")
    rep.append(f"| **best single (`{best_single['feature']}`)** | "
               f"{best_single['ic_60d']:+.3f} | {best_single['ic_40d']:+.3f} | "
               f"{best_single['ic_20d']:+.3f} | (baseline) |\n\n")

    rep.append("## V4 New Features (S2 systemic_chip rebuild) — IC Performance\n\n")
    rep.append("| Feature | Group | IC 60d | IC 40d | IC 20d | Best Lag | Pass |IC|>0.10? |\n")
    rep.append("|---|---|---|---|---|---|---|\n")
    for _, r in new_feat_ic.iterrows():
        feat = r['feature']
        passes = abs(r['ic_60d']) > 0.10 if not pd.isna(r['ic_60d']) else False
        group = V4_NEW_FEATURES.get(feat, '-')
        rep.append(f"| `{feat}` | {group} | {r['ic_60d']:+.3f} | "
                   f"{r['ic_40d']:+.3f} | {r['ic_20d']:+.3f} | "
                   f"{int(r['best_lag_60d'])}d | {'YES' if passes else 'NO'} |\n")
    rep.append("\n")

    rep.append("## V4 dedup_top8 Keep List (final composite features)\n\n")
    rep.append("| Rank | Feature | IC 60d | Lag | New vs V3? |\n")
    rep.append("|---|---|---|---|---|\n")
    for i, r in ic_dedup.head(8).iterrows():
        new_flag = 'NEW' if r['feature'] not in v3_top8 else ''
        is_v4_new = r['feature'] in V4_NEW_FEATURES
        flag = 'NEW (S2 V4 feature)' if is_v4_new else new_flag
        rep.append(f"| {i+1} | `{r['feature']}` | {r['ic_60d']:+.3f} | "
                   f"{int(r['best_lag_60d'])}d | {flag} |\n")
    rep.append("\n")

    if new_in_v4:
        rep.append(f"**New entries in V4 top-8** (not in V3): {', '.join(f'`{f}`' for f in new_in_v4)}\n\n")
    if dropped_from_v3:
        rep.append(f"**Dropped from V3 top-8**: {', '.join(f'`{f}`' for f in dropped_from_v3)}\n\n")

    rep.append("## Top 15 by |IC 60d| (V4 panel, after filter |IC|>0.10)\n\n")
    rep.append("| Rank | Feature | IC 60d | Lag | LW | Cluster |\n")
    rep.append("|---|---|---|---|---|---|\n")
    for i, r in ic_strong.head(15).iterrows():
        lw = lag_weight(int(r['best_lag_60d']))
        in_dedup = r['feature'] in ic_dedup['feature'].values
        rep.append(f"| {i+1} | `{r['feature']}` | {r['ic_60d']:+.3f} | "
                   f"{int(r['best_lag_60d'])}d | {lw:.1f} | "
                   f"{'KEEP' if in_dedup else 'DROP'} |\n")
    rep.append("\n")

    rep.append("## Conclusion & Recommendation\n\n")
    if v4_verdict.startswith('PASS+'):
        rep.append(f"**Upgrade banner v4 SLOW_FEATURES**: V4 dedup_top8 改善 ≥ 0.05 IC，建議升級。\n\n")
        rep.append(f"- 新 SLOW_FEATURES list: {v4_top8}\n")
        rep.append(f"- 新加入 V4 systemic_chip features: {[f for f in v4_top8 if f in V4_NEW_FEATURES]}\n")
    elif v4_verdict.startswith('PASS-'):
        rep.append(f"**Don't upgrade**: V4 弱化，需檢查哪個新 feature 拖累。\n\n")
    else:
        rep.append(f"**Don't upgrade banner**: V4 與 V3 差距 < 0.05，新 features 推薦標 informational tier。\n\n")
        rep.append(f"- V3 dedup_top8 維持作為 banner v4 SLOW_FEATURES production\n")
        rep.append(f"- V4 新 systemic_chip features 個別表現 (絕對 IC 60d):\n")
        for _, r in new_feat_ic.iterrows():
            rep.append(f"  - `{r['feature']}`: |IC 60d|={abs(r['ic_60d']):.3f}\n")

    OUT_REPORT.write_text(''.join(rep), encoding='utf-8')
    logger.info("Report saved -> %s", OUT_REPORT)
    logger.info("V4 verdict: %s", v4_verdict)


if __name__ == '__main__':
    main()
