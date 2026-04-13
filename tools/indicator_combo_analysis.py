"""
Phase 2b: 組合技 IC 分析

基於 Phase 2a 結果（大部分指標負 IC + 僅 RVOL 正 IC），評估:
1. 兩兩相關矩陣（找冗餘指標）
2. 手選組合技 (sign-flip negatives + 經典 pattern)
3. 現行 scanner 3-group median baseline（含反向版）
4. OLS 線性最佳權重（單 horizon）

用法:
    python tools/indicator_combo_analysis.py              # 全量
    python tools/indicator_combo_analysis.py --sample 300 # 測試
    python tools/indicator_combo_analysis.py --horizon 20 # 指定 horizon (預設全跑)
"""
import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from tools.indicator_ic_analysis import (
    load_ohlcv, compute_all_indicators, add_fwd_returns, add_regime,
    add_universe_flags, compute_daily_ic, summarize_ic,
    SIGNAL_COLS, SIGNAL_LABELS, HORIZONS, MIN_CROSS_SECTION,
)

OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_COMBO = OUT_DIR / "indicator_combo_ic.csv"
OUT_CORR = OUT_DIR / "indicator_pairwise_corr.csv"
OUT_OLS = OUT_DIR / "indicator_ols_weights.csv"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("combo")


# ============================================================
# Signal direction (based on Phase 2a IC sign)
# 若指標 IC 為負（均值回歸），我們 flip 符號讓它變「看多訊號」。
# +1 = Phase 2a 結果已是正 IC
# -1 = Phase 2a 負 IC，用來當「反向訊號」
# ============================================================
SIGNAL_DIRECTION = {
    'sig_ma20_dev':    -1,  # 離 MA20 越遠 → 越要回歸
    'sig_ma_align':    -1,
    'sig_supertrend':  -1,
    'sig_adx_dir':     -1,
    'sig_vwap_dev':    -1,
    'sig_macd_hist':   -1,
    'sig_rsi_dev':     -1,
    'sig_kd_diff':     -1,
    'sig_efi':         -1,
    'sig_td_setup':    +1,  # td_setup 定義是 sell - buy，本身就是反向
    'sig_rvol_log':    +1,  # ✅ 唯一穩定正 IC
    'sig_obv_mom':     -1,
    'sig_bb_pos':      -1,
    'sig_atr_pct':     -1,  # 低波動溢酬
    'sig_squeeze':     +1,  # 弱正 IC
}


# ============================================================
# 1. 每日 rank-normalize (使不同 scale 指標可加總)
# ============================================================
def rank_normalize_signals(df):
    """每日橫截面 rank 正規化，範圍 [-1, +1]。"""
    logger.info("Rank-normalizing signals per day...")
    for col in SIGNAL_COLS:
        # rank within each date, then scale to [-1, 1]
        df[f'{col}_rn'] = df.groupby('date')[col].transform(
            lambda s: (s.rank(pct=True) - 0.5) * 2
        )
    return df


# ============================================================
# 2. Pairwise correlation
# ============================================================
def pairwise_corr(df):
    """用 rank-normalized signals 算兩兩 Spearman corr（實為 Pearson on ranks，等價）。"""
    logger.info("Computing pairwise correlation matrix...")
    cols = [f'{c}_rn' for c in SIGNAL_COLS]
    sub = df[cols].dropna()
    corr = sub.corr()
    # 換成 label
    label_map = {f'{c}_rn': SIGNAL_LABELS[c] for c in SIGNAL_COLS}
    corr.index = corr.index.map(label_map)
    corr.columns = corr.columns.map(label_map)
    return corr


# ============================================================
# 3. Combo factor construction
# ============================================================
def build_combos(df):
    """
    建構 combo 欄位到 df。回傳 {combo_name: description} 的 dict。
    所有 combo 都是基於 rank-normalized signals，sign-flipped 到看多方向。
    """
    logger.info("Building combo factors...")
    combos = {}

    # ---- Directional composite (sign-flipped) ----
    # 所有訊號都轉成「正值 = 看多」
    for col, d in SIGNAL_DIRECTION.items():
        df[f'{col}_dir'] = df[f'{col}_rn'] * d

    # Combo 1: 全指標等權 (ex RVOL, Squeeze 已正方向 + 其他 flip)
    dir_cols = [f'{c}_dir' for c in SIGNAL_COLS]
    df['combo_all_equal'] = df[dir_cols].mean(axis=1)
    combos['combo_all_equal'] = '全 15 指標等權（方向 flipped）'

    # Combo 2: 3-group median (現行 scanner 邏輯 baseline，未 flip)
    trend_cols = [f'{c}_rn' for c in
                  ['sig_ma20_dev', 'sig_ma_align', 'sig_supertrend', 'sig_adx_dir', 'sig_vwap_dev']]
    mom_cols = [f'{c}_rn' for c in
                ['sig_macd_hist', 'sig_rsi_dev', 'sig_kd_diff', 'sig_efi', 'sig_td_setup']]
    vol_cols = [f'{c}_rn' for c in ['sig_rvol_log', 'sig_obv_mom']]

    df['combo_3group_median_raw'] = (
        df[trend_cols].median(axis=1)
        + df[mom_cols].median(axis=1)
        + df[vol_cols].median(axis=1)
    ) / 3
    combos['combo_3group_median_raw'] = '現行 scanner: 3-group median（未 flip）'

    # Combo 3: 3-group median (flipped 版本)
    trend_dir = [f'{c}_dir' for c in
                 ['sig_ma20_dev', 'sig_ma_align', 'sig_supertrend', 'sig_adx_dir', 'sig_vwap_dev']]
    mom_dir = [f'{c}_dir' for c in
               ['sig_macd_hist', 'sig_rsi_dev', 'sig_kd_diff', 'sig_efi', 'sig_td_setup']]
    vol_dir = [f'{c}_dir' for c in ['sig_rvol_log', 'sig_obv_mom']]

    df['combo_3group_median_flip'] = (
        df[trend_dir].median(axis=1)
        + df[mom_dir].median(axis=1)
        + df[vol_dir].median(axis=1)
    ) / 3
    combos['combo_3group_median_flip'] = '現行 scanner: 3-group median（方向 flipped）'

    # Combo 4: RVOL + low ATR (低波動放量)
    df['combo_rvol_lowatr'] = (
        df['sig_rvol_log_rn'] - df['sig_atr_pct_rn']
    ) / 2
    combos['combo_rvol_lowatr'] = '低波動放量 (RVOL - ATR%)'

    # Combo 5: RVOL × mean-reversion composite
    mr_composite = df[[f'{c}_dir' for c in
                       ['sig_ma20_dev', 'sig_rsi_dev', 'sig_kd_diff', 'sig_efi', 'sig_bb_pos']]].mean(axis=1)
    df['combo_rvol_meanrev'] = (df['sig_rvol_log_rn'] + mr_composite) / 2
    combos['combo_rvol_meanrev'] = 'RVOL + 均值回歸 composite'

    # Combo 6: Low-vol + mean-reversion (純反向)
    df['combo_meanrev_pure'] = df[[f'{c}_dir' for c in
                                   ['sig_ma20_dev', 'sig_rsi_dev', 'sig_kd_diff',
                                    'sig_efi', 'sig_bb_pos', 'sig_atr_pct']]].mean(axis=1)
    combos['combo_meanrev_pure'] = '純反向均值回歸 (6 個 flipped)'

    # Combo 7: 單 RVOL（baseline，比對個別指標）
    df['combo_rvol_only'] = df['sig_rvol_log_rn']
    combos['combo_rvol_only'] = '只用 RVOL（baseline）'

    # Combo 8: Squeeze + RVOL (爆量突破)
    df['combo_squeeze_rvol'] = (df['sig_squeeze_rn'] + df['sig_rvol_log_rn']) / 2
    combos['combo_squeeze_rvol'] = 'Squeeze + RVOL'

    # Combo 9: 趨勢反向 + RVOL (逆勢找撈底)
    df['combo_counter_trend_rvol'] = (
        df['sig_ma20_dev_dir'] + df['sig_rvol_log_rn']
    ) / 2
    combos['combo_counter_trend_rvol'] = '逆勢撈底 (-MA20dev + RVOL)'

    # Combo 10: 動能 flip + 量能正向
    mom_flip = df[[f'{c}_dir' for c in mom_cols_inner()]].mean(axis=1)
    vol_pos = df[[f'{c}_rn' for c in ['sig_rvol_log', 'sig_obv_mom']]].mean(axis=1)
    df['combo_momflip_volpos'] = (mom_flip + vol_pos) / 2
    combos['combo_momflip_volpos'] = '動能 flip + 量能正向'

    return combos


def mom_cols_inner():
    return ['sig_macd_hist', 'sig_rsi_dev', 'sig_kd_diff', 'sig_efi', 'sig_td_setup']


# ============================================================
# 4. OLS 最佳權重
# ============================================================
def fit_ols_weights(df, horizon=20, universe='all'):
    """
    用全樣本 OLS 對 fwd_{h}d 擬合 15 個 rank-normalized signals，
    回傳權重 + in-sample IC。

    注意：in-sample 會 overfit，實務要做 walk-forward，但 v1 先看量級。
    """
    logger.info(f"Fitting OLS weights (horizon={horizon}d, universe={universe})...")
    rn_cols = [f'{c}_rn' for c in SIGNAL_COLS]
    X_df = df[rn_cols + [f'fwd_{horizon}d']].dropna()
    if universe == 'momentum':
        X_df = X_df[df.loc[X_df.index, 'in_momentum_universe'] == True]

    X = X_df[rn_cols].values
    y = X_df[f'fwd_{horizon}d'].values

    # Simple OLS via normal equation
    # 加 intercept
    X_intercept = np.column_stack([np.ones(len(X)), X])
    try:
        beta, *_ = np.linalg.lstsq(X_intercept, y, rcond=None)
    except Exception as e:
        logger.warning(f"OLS failed: {e}")
        return None

    intercept = beta[0]
    weights = dict(zip(SIGNAL_COLS, beta[1:]))

    # In-sample predicted signal
    pred = X @ beta[1:] + intercept
    # IC of predicted vs actual (in-sample)
    ic_is, _ = stats.spearmanr(pred, y)

    return {
        'intercept': float(intercept),
        'weights': {k: float(v) for k, v in weights.items()},
        'in_sample_ic': float(ic_is),
        'n_obs': len(X),
    }


# ============================================================
# 5. Run IC matrix for combos
# ============================================================
def run_combo_ic(df, combos, universes, horizons):
    results = []
    for combo_name, combo_desc in combos.items():
        for h in horizons:
            for uni_name, uni_col in universes:
                ic_series = compute_daily_ic(
                    df, combo_name, f'fwd_{h}d',
                    universe_filter=uni_col, regime=None,
                )
                stats_dict = summarize_ic(ic_series)
                results.append({
                    'combo': combo_name,
                    'description': combo_desc,
                    'horizon': h,
                    'universe': uni_name,
                    **stats_dict,
                })
    return pd.DataFrame(results)


# ============================================================
# Main
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample', type=int, default=None)
    parser.add_argument('--since', type=str, default=None)
    parser.add_argument('--horizon', type=int, default=None,
                        help='Run only this horizon (else all 1/5/10/20)')
    args = parser.parse_args()

    t0 = time.time()
    df = load_ohlcv(sample=args.sample, since=args.since)
    df = compute_all_indicators(df)
    df = add_fwd_returns(df)
    df = add_regime(df)
    df = add_universe_flags(df)
    df = rank_normalize_signals(df)

    # ----- Pairwise correlation -----
    corr = pairwise_corr(df)
    corr.to_csv(OUT_CORR, encoding='utf-8-sig')
    logger.info(f"Correlation matrix saved: {OUT_CORR}")

    print("\n=== Pairwise Correlation (rank-normalized, |corr| > 0.5 標示) ===")
    # 找出高相關對
    pairs = []
    for i, r in enumerate(corr.index):
        for j, c in enumerate(corr.columns):
            if j <= i:
                continue
            v = corr.iloc[i, j]
            if abs(v) >= 0.5:
                pairs.append((r, c, v))
    pairs.sort(key=lambda x: abs(x[2]), reverse=True)
    for r, c, v in pairs[:20]:
        flag = '!!' if abs(v) > 0.7 else '. '
        print(f"  {flag} {r:<15} -- {c:<15}  corr={v:+.3f}")

    # ----- Combo IC -----
    combos = build_combos(df)
    horizons = [args.horizon] if args.horizon else HORIZONS
    universes = [('all', None), ('momentum', 'in_momentum_universe')]

    t_ic = time.time()
    combo_df = run_combo_ic(df, combos, universes, horizons)
    logger.info(f"Combo IC done in {time.time()-t_ic:.1f}s")

    combo_df = combo_df.sort_values(['horizon', 'universe', 'mean'],
                                    ascending=[True, True, False])
    combo_df.to_csv(OUT_COMBO, index=False, encoding='utf-8-sig')
    logger.info(f"Combo IC saved: {OUT_COMBO}")

    # Print combo results
    print("\n" + "=" * 110)
    print("  Combo IC Results (universe=all)")
    print("=" * 110)
    print(f"  {'Combo':<34} {'H':>3} {'mean_IC':>9} {'IR':>7} {'95% CI':<20} "
          f"{'p':>8} {'win%':>6} {'n':>6}")
    print("  " + "-" * 106)
    for h in horizons:
        sub = combo_df[(combo_df['universe'] == 'all') & (combo_df['horizon'] == h)]
        for _, r in sub.iterrows():
            name = r['combo'][:34]
            ci_str = f"[{r['ci_low']:+.3f}, {r['ci_high']:+.3f}]" if pd.notna(r['ci_low']) else ""
            flag = '*' if r['p'] < 0.05 else ' '
            print(f"  {name:<34} {h:>3d} {r['mean']:>+9.4f} {r['ir']:>+7.3f} "
                  f"{ci_str:<20} {r['p']:>8.4f} {r['win_rate']:>5.1f}% {r['n']:>6d} {flag}")
        print()

    # ----- OLS weights -----
    print("=" * 110)
    print("  OLS In-Sample Optimal Weights (warning: 過擬合風險高，僅看量級)")
    print("=" * 110)
    ols_rows = []
    for h in horizons:
        for uni_name, uni_col in [('all', None), ('momentum', 'in_momentum_universe')]:
            res = fit_ols_weights(df, horizon=h, universe=uni_name)
            if res:
                print(f"\n  h={h}d, universe={uni_name}, in-sample IC={res['in_sample_ic']:+.4f}, n={res['n_obs']:,}")
                print(f"  {'指標':<20} {'權重':>10}  {'方向':>5}")
                for sig, w in sorted(res['weights'].items(), key=lambda x: abs(x[1]), reverse=True)[:8]:
                    sign = '→多' if w > 0 else '→空'
                    print(f"    {SIGNAL_LABELS[sig]:<16} {w:>+10.5f}  {sign}")
                for sig, w in res['weights'].items():
                    ols_rows.append({
                        'horizon': h, 'universe': uni_name,
                        'indicator': sig, 'label': SIGNAL_LABELS[sig],
                        'weight': w, 'in_sample_ic': res['in_sample_ic'],
                    })
    if ols_rows:
        pd.DataFrame(ols_rows).to_csv(OUT_OLS, index=False, encoding='utf-8-sig')
        logger.info(f"OLS weights saved: {OUT_OLS}")

    print(f"\nTotal time: {(time.time()-t0)/60:.1f} min")


if __name__ == '__main__':
    main()
