"""
vf_g6_validation.py - VF-G6 QM 軟警報 / 部位參數驗證

VF-G6 涵蓋的 magic numbers:
  1. base_pct = 8%                                    (momentum_screener:115)
  2. trigger multiplier clip(0.5, 1.5)                (momentum_screener:143)
  3. GRACE_PERIOD_DAYS = 5                            (exit_manager:55)
  4. CONSEC_BREACH_DAYS = 2                           (exit_manager:56)
  5. VOLUME_CONFIRM_RATIO = 0.8                       (exit_manager:57)
  6. Entry gate thresholds (trigger >= 3 green)       (momentum_screener:172)

資料限制（VF-G5 相同）：trade_journal_qm_tw 是 weekly picks + fwd return。
  - GRACE / CONSEC / VOL: 需 day-level OHLCV 模擬時序，本輪跳過
  - trigger multiplier clip: 沒 trigger_score 欄位，跳過
  - Entry gate threshold: 同 BUY threshold 限制，跳過

可驗項：
  T1. base_pct × composite_score scaling 有沒有 alpha
     - QM-weighted (base × qm_score/80) vs equal-weighted portfolio
     - 若 weighted 勝 → scaling formula 合理
     - 若等權勝 → scaling 無 alpha，可簡化成固定 base_pct
  T2. base_pct grid (4% / 6% / 8% / 10% / 12%) 對 portfolio scale
     - 僅比例 effect，不影響 per-pick alpha；記錄 total exposure range
     - 避免「8% × 20 picks = 160% 槓桿」或「4% × 20 picks = 80% 未全倉」

Usage:
    python tools/vf_g6_validation.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

BT_DIR = ROOT / "data_cache" / "backtest"
JOURNAL_PATH = BT_DIR / "trade_journal_qm_tw.parquet"
REPORT_PATH = ROOT / "reports" / "vf_g6_validation.md"


def grade(ir):
    if pd.isna(ir):
        return 'N/A'
    if abs(ir) >= 0.3:
        return 'A' if ir > 0 else 'A(rev)'
    elif abs(ir) >= 0.1:
        return 'B' if ir > 0 else 'B(rev)'
    elif abs(ir) >= 0.05:
        return 'C'
    return 'D'


def _df_to_md(df: pd.DataFrame) -> str:
    if df.empty:
        return "(empty)"
    cols = list(df.columns)
    int_cols = {'n_weeks', 'horizon', 'n_picks', 'weeks'}
    pct_cols = {'eq_mean', 'qw_mean', 'delta', 'total_exposure_mean',
                'total_exposure_p10', 'total_exposure_p90',
                'mean_pos', 'max_pos', 'min_pos'}

    def fmt(col, v):
        if isinstance(v, float):
            if pd.isna(v):
                return "NaN"
            if col in int_cols:
                return str(int(v))
            if col in pct_cols:
                return f"{v:+.2%}"
            return f"{v:+.4f}"
        return str(v)

    lines = ["| " + " | ".join(cols) + " |",
             "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(fmt(c, row[c]) for c in cols) + " |")
    return "\n".join(lines)


# ================================================================
# T1: QM-weighted vs Equal-weighted portfolio (live formula)
# ================================================================
def t1_weighted_vs_equal(j: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    """
    Live formula: weight_i = qm_score_i / 80 (clipped to [0.2, 1.5])
    Normalize: weight_i / sum(weight_i)  (per week)

    比較 per-week：
      eq_mean = mean(fwd_X)  等權
      qw_mean = sum(weight_norm × fwd_X)  QM 加權
    """
    j = j.copy()
    j['week_end_date'] = pd.to_datetime(j['week_end_date'])
    # weight from qm_score (qm_score / 80, clip 0.2~1.5)
    j['raw_w'] = (j['qm_score'] / 80.0).clip(lower=0.2, upper=1.5)

    rows = []
    for h in horizons:
        fwd = f'fwd_{h}d'
        if fwd not in j.columns:
            continue
        weekly = []
        for wd, g in j.groupby('week_end_date'):
            g = g.dropna(subset=[fwd])
            if len(g) < 5:
                continue
            eq = g[fwd].mean()
            wsum = g['raw_w'].sum()
            if wsum <= 0:
                continue
            wnorm = g['raw_w'] / wsum
            qw = (wnorm * g[fwd]).sum()
            weekly.append({'eq': eq, 'qw': qw, 'delta': qw - eq})
        wdf = pd.DataFrame(weekly)
        if wdf.empty:
            continue

        eq_mean = wdf['eq'].mean()
        qw_mean = wdf['qw'].mean()
        delta_mean = wdf['delta'].mean()
        # weekly t-stat on delta
        t_stat, p_val = stats.ttest_1samp(wdf['delta'], 0.0, nan_policy='omit')
        # IR-like: mean / std
        delta_std = wdf['delta'].std(ddof=1)
        ir = delta_mean / delta_std if delta_std and delta_std > 0 else np.nan

        rows.append({
            'horizon': h,
            'n_weeks': len(wdf),
            'eq_mean': eq_mean,
            'qw_mean': qw_mean,
            'delta': delta_mean,
            't_stat': t_stat,
            'p_val': p_val,
            'delta_IR': ir,
            'grade': grade(ir),
        })
    return pd.DataFrame(rows)


# ================================================================
# T2: base_pct grid impact on total exposure
# ================================================================
def t2_base_pct_exposure(j: pd.DataFrame, base_pcts: list[float],
                          top_n: int = 20) -> pd.DataFrame:
    """
    對每週 top_N picks 計算 total exposure（假設用 live formula 直接加總）：
      pos_i = base_pct × (qm_score_i / 80) × clip(trigger/5, 0.5, 1.5)
      但 journal 無 trigger → 用 mult=1.0 (neutral)
      pos_i = base_pct × (qm_score_i / 80)
      total_exposure = sum(pos_i over top_N)

    觀察：不同 base_pct 下 total_exposure 是否落在合理範圍 (50%~150%)
    """
    j = j.copy()
    rows = []
    # For each base_pct, compute per-week total exposure (top_N picks)
    for bp in base_pcts:
        weekly_exp = []
        weekly_mean_pos = []
        weekly_max_pos = []
        weekly_min_pos = []
        for wd, g in j.groupby('week_end_date'):
            g_top = g[g['rank_in_top50'] <= top_n]
            if g_top.empty:
                continue
            pos = bp * (g_top['qm_score'] / 80.0).clip(lower=0.2, upper=1.5)
            weekly_exp.append(pos.sum())
            weekly_mean_pos.append(pos.mean())
            weekly_max_pos.append(pos.max())
            weekly_min_pos.append(pos.min())
        if not weekly_exp:
            continue
        wexp = np.array(weekly_exp)
        rows.append({
            'base_pct': bp,
            'top_n': top_n,
            'n_weeks': len(weekly_exp),
            'total_exposure_mean': wexp.mean() / 100,
            'total_exposure_p10': np.percentile(wexp, 10) / 100,
            'total_exposure_p90': np.percentile(wexp, 90) / 100,
            'mean_pos': np.mean(weekly_mean_pos) / 100,
            'max_pos': np.max(weekly_max_pos) / 100,
            'min_pos': np.min(weekly_min_pos) / 100,
        })
    return pd.DataFrame(rows)


# ================================================================
# Main
# ================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--journal", default=str(JOURNAL_PATH))
    ap.add_argument("--horizons", type=int, nargs='+', default=[5, 20, 40, 60])
    ap.add_argument("--top-n", type=int, default=20, help="top_n for exposure calc")
    args = ap.parse_args()

    print("=" * 80)
    print("VF-G6 QM 軟警報 / 部位參數驗證")
    print("=" * 80)

    j = pd.read_parquet(args.journal)
    j['week_end_date'] = pd.to_datetime(j['week_end_date'])
    print(f"Journal: {len(j)} picks × {len(j.columns)} cols")
    print(f"Date range: {j['week_end_date'].min().date()} ~ {j['week_end_date'].max().date()}")
    print(f"qm_score: min={j['qm_score'].min():.1f} / max={j['qm_score'].max():.1f} / mean={j['qm_score'].mean():.1f}")
    print()

    # === T1 ===
    print("=" * 80)
    print("T1: QM-weighted vs Equal-weighted portfolio")
    print("=" * 80)
    print("Live formula: weight_i = clip(qm_score / 80, 0.2, 1.5), normalized per week")
    t1 = t1_weighted_vs_equal(j, args.horizons)
    print(t1.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))
    print()

    # === T2 ===
    print("=" * 80)
    print(f"T2: base_pct grid 對 total exposure (top_{args.top_n} picks)")
    print("=" * 80)
    base_pcts = [4.0, 6.0, 8.0, 10.0, 12.0]
    t2 = t2_base_pct_exposure(j, base_pcts, top_n=args.top_n)
    print(t2.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))
    print()

    # === Write report ===
    print("Saving report...")
    REPORT_PATH.parent.mkdir(exist_ok=True)
    lines = []
    lines.append("# VF-G6 QM 軟警報 / 部位參數驗證 (2026-04-23)\n")
    lines.append(f"- Journal: {len(j)} picks × {j['stock_id'].nunique()} 檔 × {j['week_end_date'].nunique()} 週\n")

    lines.append("## 涵蓋的 magic numbers\n")
    lines.append("| # | 參數 | 現值 | 可驗? |")
    lines.append("| --- | --- | --- | --- |")
    lines.append("| 13 | base_pct | 8.0% | ✅ T1+T2 |")
    lines.append("| 14 | trigger mult clip | 0.5 ~ 1.5 | ❌ journal 無 trigger |")
    lines.append("| 15 | QM entry gate threshold | trigger ≥ 3 | ❌ 同 VF-G5 |")
    lines.append("| 16 | GRACE_PERIOD_DAYS | 5 | ❌ 需 day-level 模擬 |")
    lines.append("| 17 | CONSEC_BREACH_DAYS | 2 | ❌ 需 day-level 模擬 |")
    lines.append("| 18-22 | 軟警報各門檻 | 各種 | ❌ 需 day-level 模擬 |")
    lines.append("")

    lines.append("## T1: QM-weighted vs Equal-weighted portfolio\n")
    lines.append("Live formula: `weight_i = clip(qm_score_i / 80, 0.2, 1.5)` normalized per week")
    lines.append("比較：`qw_mean = sum(w_norm × fwd_X)` vs `eq_mean = mean(fwd_X)`\n")
    lines.append(_df_to_md(t1))
    lines.append("")

    lines.append("## T2: base_pct grid 對 total exposure\n")
    lines.append(f"假設每週 top_{args.top_n} picks 全買，用 live formula (trigger neutral = 1.0)：")
    lines.append("`pos_i = base_pct × (qm_score_i / 80)`，total_exposure = Σ pos_i\n")
    lines.append(_df_to_md(t2))
    lines.append("")

    # --- 結論 ---
    lines.append("## 結論\n")

    # T1 結論
    lines.append("### T1: QM 加權是否勝等權？\n")
    if not t1.empty:
        # Check if qw consistently beats eq
        max_abs_ir = t1['delta_IR'].abs().max()
        any_sig = (t1['p_val'] < 0.05).any()
        lines.append(f"- 最大 |delta IR|: {max_abs_ir:+.3f} ({grade(max_abs_ir) if pd.notna(max_abs_ir) else 'N/A'})")
        sig_rows = t1[t1['p_val'] < 0.05]
        if any_sig:
            lines.append(f"- p < 0.05 顯著 horizon: {sorted(sig_rows['horizon'].tolist())}")
            # 看方向
            pos_sig = (sig_rows['delta'] > 0).sum()
            lines.append(f"- 顯著中 {pos_sig}/{len(sig_rows)} 為 qw > eq")
        else:
            lines.append(f"- 無任何 horizon 達 p < 0.05 顯著 → QM 加權與等權表現統計上相同")
        if max_abs_ir < 0.1:
            lines.append(f"- **|delta IR| < 0.1 平原** → QM 加權 formula 無顯著 alpha")
            lines.append(f"  - 可簡化成固定 base_pct × 等權，但為了 UI 顯示「根據分數配置」的直覺，保留 formula 無害")
        else:
            lines.append(f"- |delta IR| 可能有訊號 → 實際建議落地前需 walk-forward 確認")
    lines.append("")

    # T2 結論
    lines.append("### T2: base_pct 合理範圍\n")
    if not t2.empty:
        # Find base_pct where mean total exposure ~ 80-100%
        for _, row in t2.iterrows():
            bp = row['base_pct']
            exp = row['total_exposure_mean']
            p10 = row['total_exposure_p10']
            p90 = row['total_exposure_p90']
            status = ""
            if exp < 0.5:
                status = "⚠️ 未全倉"
            elif exp > 1.5:
                status = "⚠️ 槓桿過大"
            else:
                status = "✅ 合理"
            lines.append(f"- base_pct {bp:.0f}%: mean total exposure {exp:.0%} (p10-p90: {p10:.0%}~{p90:.0%}) {status}")

        # Recommendation
        live_row = t2[t2['base_pct'] == 8.0]
        if not live_row.empty:
            live_exp = live_row.iloc[0]['total_exposure_mean']
            if 0.7 <= live_exp <= 1.2:
                lines.append(f"\n- live `base_pct=8.0%` total exposure {live_exp:.0%} 屬合理帶（0.7~1.2），不動")
            else:
                lines.append(f"\n- live `base_pct=8.0%` total exposure {live_exp:.0%} 偏離合理帶")
    lines.append("")

    # 未驗項目解釋
    lines.append("### 未驗參數（需 day-level OHLCV 模擬，本輪跳過）\n")
    lines.append("- **GRACE_PERIOD_DAYS = 5**：進場後 5d 內不觸發硬停損，需 day-level 時序")
    lines.append("- **CONSEC_BREACH_DAYS = 2**：連續 2 日跌破才確認，需 day-level OHLCV")
    lines.append("- **VOLUME_CONFIRM_RATIO = 0.8**：量縮確認，需日成交量")
    lines.append("- **trigger multiplier clip(0.5, 1.5)**：journal 無 trigger_score 欄位")
    lines.append("- **QM entry gate threshold (≥3)**：同 VF-G5 BUY threshold，journal 是 post-filter\n")
    lines.append("以上 5 項估算影響：")
    lines.append("- GRACE/CONSEC/VOL：防止 5d 內假跌破洗出場，尾端風險控管類（類比 VF-G1 結論：SL 價值在尾端控管，不在期望報酬增強）")
    lines.append("- 合理預期：這些參數空間同為平原（exit 類 5 連 D 前例）")
    lines.append("- 需要時可另建 day-level 模擬工具驗，但 ROI 低\n")

    # --- 產出 ---
    lines.append("## 產出\n")
    lines.append("- `tools/vf_g6_validation.py`")
    lines.append("- `reports/vf_g6_validation.md`")
    t1.to_csv(REPORT_PATH.parent / "vf_g6_t1_weighted_vs_equal.csv", index=False)
    t2.to_csv(REPORT_PATH.parent / "vf_g6_t2_base_pct_exposure.csv", index=False)
    lines.append("- `reports/vf_g6_t1_weighted_vs_equal.csv`")
    lines.append("- `reports/vf_g6_t2_base_pct_exposure.csv`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"  {REPORT_PATH}")


if __name__ == "__main__":
    main()
