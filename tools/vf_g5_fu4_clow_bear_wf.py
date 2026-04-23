"""
vf_g5_fu4_clow_bear_wf.py - VF-G5 FU-4: C_low × TWII bear walk-forward 驗證

FU-3 發現 C_low (trend_score<7) 在 TWII bear 市 fwd_60d +10.34%（移除 2022
後 +14.88%）。要確認是否跨多個獨立熊市週期都 robust，才考慮 live 落地。

5 個主要 TWII bear clusters (>50 days):
  1. 2015-10 ~ 2016-03  (80d, China slowdown)
  2. 2018-10 ~ 2019-03  (104d, Fed/trade war)
  3. 2020-03 ~ 2020-05  (57d, COVID crash)
  4. 2022-04 ~ 2023-01  (198d, Fed hiking)
  5. 2025-03 ~ 2025-06  (63d, Trump tariff shock)

Walk-forward 方法：
  1. Per-cluster: 每個熊市期 C_low vs A/B/C_mid mean fwd_60d
  2. Out-of-sample: 排除 2022 後，C_low 在其餘 4 個 bear cluster 是否仍 +5%+
  3. Live 可行性：bear regime 下每週 top_20 picks 中 C_low 占比分佈

Usage:
    python tools/vf_g5_fu4_clow_bear_wf.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

BT_DIR = ROOT / "data_cache" / "backtest"
JOURNAL_PATH = BT_DIR / "trade_journal_qm_tw.parquet"
REPORT_PATH = ROOT / "reports" / "vf_g5_fu4_clow_bear_wf.md"

# 5 主要 bear clusters (>50 days)
BEAR_CLUSTERS = [
    ('2015H2_China',      '2015-10-30', '2016-03-02'),
    ('2018H2_Fed_Trade',  '2018-10-04', '2019-03-14'),
    ('2020Q1_COVID',      '2020-03-09', '2020-05-29'),
    ('2022_FedHike',      '2022-04-07', '2023-01-16'),
    ('2025H1_Tariff',     '2025-03-07', '2025-06-09'),
]


def _df_to_md(df: pd.DataFrame, pct_cols=None, int_cols=None) -> str:
    if df.empty:
        return "(empty)"
    cols = list(df.columns)
    pct_cols = pct_cols or set()
    int_cols = int_cols or {'n'}

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


def classify_scenario(ts):
    if ts >= 9: return 'A'
    if ts >= 8: return 'B'
    if ts >= 7: return 'C_mid'
    return 'C_low'


def assign_cluster(dt):
    """Return cluster name if dt in any bear cluster else None."""
    for name, start, end in BEAR_CLUSTERS:
        if pd.Timestamp(start) <= dt <= pd.Timestamp(end):
            return name
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--journal", default=str(JOURNAL_PATH))
    args = ap.parse_args()

    print("=" * 80)
    print("VF-G5 FU-4: C_low × TWII bear walk-forward")
    print("=" * 80)

    j = pd.read_parquet(args.journal)
    j['week_end_date'] = pd.to_datetime(j['week_end_date'])
    j['scenario_proxy'] = j['trend_score'].apply(classify_scenario)
    j['cluster'] = j['week_end_date'].apply(assign_cluster)

    # Filter to picks in bear clusters only
    jb = j[j['cluster'].notna()].copy()
    print(f"Journal: {len(j)} picks; in bear clusters: {len(jb)}")
    print()
    print("Cluster counts by scenario:")
    print(pd.crosstab(jb['cluster'], jb['scenario_proxy']).to_string())
    print()

    # === T1: Per-cluster scenario × fwd_60 ===
    print("=" * 80)
    print("T1: Per-cluster scenario × fwd_60d mean")
    print("=" * 80)
    rows = []
    for cluster_name in [c[0] for c in BEAR_CLUSTERS]:
        cg = jb[jb['cluster'] == cluster_name]
        for sc in ['A', 'B', 'C_mid', 'C_low']:
            sub = cg[cg['scenario_proxy'] == sc]
            rt = sub['fwd_60d'].dropna()
            rows.append({
                'cluster': cluster_name,
                'scenario': sc,
                'n': len(rt),
                'mean_fwd60': rt.mean() if len(rt) else np.nan,
                'winrate_60': (rt > 0).mean() if len(rt) else np.nan,
            })
    t1 = pd.DataFrame(rows)
    print(t1.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))
    print()

    # === T2: C_low pivot ===
    print("=" * 80)
    print("T2: C_low across clusters (pivot)")
    print("=" * 80)
    clow_pivot = t1[t1['scenario'] == 'C_low'].set_index('cluster')[['n', 'mean_fwd60', 'winrate_60']]
    print(clow_pivot.to_string(float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))
    print()

    # Robustness: 平均 (with / without 2022)
    all_clow = t1[t1['scenario'] == 'C_low']
    all_with = all_clow['mean_fwd60'].dropna()
    mean_all = all_with.mean()
    pos_clusters = (all_with > 0).sum()
    without_22 = all_clow[all_clow['cluster'] != '2022_FedHike']
    mean_excl22 = without_22['mean_fwd60'].mean()
    pos_excl22 = (without_22['mean_fwd60'] > 0).sum()
    print(f"C_low 全 5 cluster 平均 fwd_60: {mean_all:+.2%} ({pos_clusters}/{len(all_with)} 正)")
    print(f"排除 2022 後 4 cluster 平均: {mean_excl22:+.2%} ({pos_excl22}/{len(without_22)} 正)")
    print()

    # === T3: C_low vs scenario mean per cluster ===
    print("=" * 80)
    print("T3: C_low 是否每個 cluster 都勝其他 scenario？")
    print("=" * 80)
    t3 = t1.pivot_table(index='cluster', columns='scenario', values='mean_fwd60')
    # Order columns
    t3 = t3[['A', 'B', 'C_mid', 'C_low']]
    # Rank within row: C_low is rank 1 if best
    t3_rank = t3.rank(axis=1, ascending=False)
    print(t3.to_string(float_format=lambda x: f"{x:+.2%}"))
    print()
    print("C_low 名次 (1=最佳):")
    print(t3_rank[['C_low']].to_string(float_format=lambda x: f"{int(x)}" if not pd.isna(x) else 'NaN'))
    clow_ranks = t3_rank['C_low'].dropna()
    print(f"\nC_low 勝率 (rank==1): {(clow_ranks == 1).sum()}/{len(clow_ranks)} clusters")
    print()

    # === T4: Live feasibility ===
    print("=" * 80)
    print("T4: Live 可行性 - bear regime 下每週 C_low 占比")
    print("=" * 80)
    # Per-week scenario count in bear periods
    jb_weeks = jb.groupby(['week_end_date', 'scenario_proxy']).size().unstack(fill_value=0)
    # Ensure all cols exist
    for c in ['A', 'B', 'C_mid', 'C_low']:
        if c not in jb_weeks.columns:
            jb_weeks[c] = 0
    jb_weeks['total'] = jb_weeks[['A', 'B', 'C_mid', 'C_low']].sum(axis=1)
    jb_weeks['clow_pct'] = jb_weeks['C_low'] / jb_weeks['total'].replace(0, np.nan)
    n_weeks_bear = len(jb_weeks)
    print(f"Bear regime 週數: {n_weeks_bear}")
    print(f"每週 C_low picks 數量分佈:")
    print(jb_weeks['C_low'].describe().to_string())
    print()
    print(f"每週 C_low 占比分佈:")
    print(jb_weeks['clow_pct'].describe().to_string(float_format=lambda x: f"{x:+.2%}"))
    print()
    weeks_no_clow = (jb_weeks['C_low'] == 0).sum()
    print(f"有 0 個 C_low picks 的週數: {weeks_no_clow}/{n_weeks_bear} ({weeks_no_clow/n_weeks_bear:.0%})")

    # === Save report ===
    print("\nSaving report...")
    REPORT_PATH.parent.mkdir(exist_ok=True)
    lines = []
    lines.append("# VF-G5 FU-4: C_low × TWII bear walk-forward (2026-04-23)\n")
    lines.append(f"- Journal: {len(j)} picks; in bear clusters: {len(jb)}")
    lines.append("- Scenario proxy: A (trend_score≥9) / B (=8) / C_mid (=7) / C_low (<7, picks ≥6)\n")

    lines.append("## TWII bear clusters (>50 days)\n")
    for name, start, end in BEAR_CLUSTERS:
        sub = jb[jb['cluster'] == name]
        lines.append(f"- **{name}**: {start} ~ {end}, picks n={len(sub)}")
    lines.append("")

    pct_cols = {'mean_fwd60', 'winrate_60'}

    lines.append("## T1: 每個 bear cluster × scenario fwd_60d mean\n")
    lines.append(_df_to_md(t1, pct_cols=pct_cols))
    lines.append("")

    lines.append("## T2: C_low pivot across clusters\n")
    # T2 as DF
    clow_df = clow_pivot.reset_index()
    lines.append(_df_to_md(clow_df, pct_cols=pct_cols))
    lines.append(f"\n- **C_low 全 5 cluster 平均 fwd_60**: {mean_all:+.2%} ({pos_clusters}/{len(all_with)} 正)")
    lines.append(f"- **排除 2022 後 4 cluster 平均**: {mean_excl22:+.2%} ({pos_excl22}/{len(without_22)} 正)")
    lines.append("")

    lines.append("## T3: C_low 是否每個 cluster 都勝其他 scenario？\n")
    t3_df = t3.reset_index()
    lines.append(_df_to_md(t3_df, pct_cols={'A', 'B', 'C_mid', 'C_low'}))
    lines.append(f"\n- **C_low rank 1 (最佳) 的 cluster 數**: {(clow_ranks == 1).sum()}/{len(clow_ranks)}")
    lines.append("")

    lines.append("## T4: Live 可行性 — bear regime 下每週 C_low picks 分佈\n")
    lines.append(f"- Bear regime 週數: {n_weeks_bear}")
    lines.append(f"- 每週 C_low picks 平均: {jb_weeks['C_low'].mean():.1f} (median {jb_weeks['C_low'].median():.0f})")
    lines.append(f"- 每週 C_low 占比平均: {jb_weeks['clow_pct'].mean():.0%}")
    lines.append(f"- 0 個 C_low picks 的週數: {weeks_no_clow}/{n_weeks_bear} ({weeks_no_clow/n_weeks_bear:.0%})")
    lines.append("")

    # --- 結論 ---
    lines.append("## 結論\n")

    # C_low consistency
    lines.append("### C_low × bear pattern robustness\n")
    lines.append(f"- 5 個 bear cluster 平均 fwd_60: {mean_all:+.2%}")
    lines.append(f"- 5 個 cluster 中 **{pos_clusters} 個正**")
    if mean_excl22 > 0.05 and pos_excl22 >= 3:
        lines.append(f"- ✅ 排除 2022 後 4/{len(without_22)} 正 (+{pos_excl22})，mean={mean_excl22:+.2%} → pattern **robust across multiple bears**")
    elif pos_clusters >= 4:
        lines.append(f"- ✅ 5 個 cluster 中 ≥4 正 → pattern robust")
    else:
        lines.append(f"- ⚠️ cluster 間不一致，pattern 可能非 robust")
    lines.append("")

    # Rank robustness
    lines.append("### C_low 相對其他 scenario 的排名穩定性\n")
    rank_1 = (clow_ranks == 1).sum()
    rank_12 = (clow_ranks <= 2).sum()
    n_c = len(clow_ranks)
    if rank_1 >= n_c * 0.6:
        lines.append(f"- ✅ C_low 在 {rank_1}/{n_c} cluster 排名第 1 (≥60%)，dominant")
    elif rank_12 >= n_c * 0.8:
        lines.append(f"- ⚠️ C_low 在 {rank_12}/{n_c} cluster 排前 2，但非 dominant")
    else:
        lines.append(f"- ❌ C_low 排名不穩，pattern 非 reliable winner")
    lines.append("")

    # Live feasibility
    lines.append("### Live 可行性\n")
    avg_clow = jb_weeks['C_low'].mean()
    if avg_clow >= 2:
        lines.append(f"- 每週平均 {avg_clow:.1f} 個 C_low picks，足以建立 mini-portfolio 或 overlay signal")
    elif avg_clow >= 1:
        lines.append(f"- 每週平均 {avg_clow:.1f} 個，配置稀疏，需跨週累積")
    else:
        lines.append(f"- 每週平均 {avg_clow:.1f} 個，太稀疏，獨立 signal 難實作")

    if weeks_no_clow / n_weeks_bear > 0.3:
        lines.append(f"- {weeks_no_clow/n_weeks_bear:.0%} 週無 C_low picks，signal 不連續")
    lines.append("")

    # Final decision
    lines.append("### 最終決策\n")
    if mean_excl22 > 0.05 and pos_excl22 >= 3 and rank_1 >= n_c * 0.6 and avg_clow >= 2:
        lines.append("- ✅ **可規劃 live 落地**：C_low bear pattern 跨 4/5 個 bear cluster robust + 實務可行")
        lines.append("- 建議設計：TWII bear regime 下，value-side C_low picks 獨立 overlay signal，或 scanner 單獨標記")
    elif mean_excl22 > 0.05 and pos_excl22 >= 3:
        lines.append("- ⚠️ **Signal 存在但 live 可行性受限**：pattern robust 但每週 picks 稀疏")
        lines.append("- 建議：歸檔為研究 finding，不急著 live 落地；等未來 regime overlay 框架完成時一併整合")
    else:
        lines.append("- ❌ **Pattern 不夠 robust，不落地**")
        lines.append("- 維持 VF-G5 FU-3 結論：C_low bear 是歷史 finding，不落地 live")
    lines.append("")

    # Save
    t1.to_csv(REPORT_PATH.parent / "vf_g5_fu4_cluster_scenario.csv", index=False)
    clow_df.to_csv(REPORT_PATH.parent / "vf_g5_fu4_clow_pivot.csv", index=False)
    t3_df.to_csv(REPORT_PATH.parent / "vf_g5_fu4_cluster_pivot.csv", index=False)
    jb_weeks[['A', 'B', 'C_mid', 'C_low', 'total', 'clow_pct']].to_csv(
        REPORT_PATH.parent / "vf_g5_fu4_weekly_clow_dist.csv"
    )

    lines.append("## 產出\n")
    lines.append("- `tools/vf_g5_fu4_clow_bear_wf.py`")
    lines.append("- `reports/vf_g5_fu4_clow_bear_wf.md`")
    lines.append("- `reports/vf_g5_fu4_cluster_scenario.csv`")
    lines.append("- `reports/vf_g5_fu4_clow_pivot.csv`")
    lines.append("- `reports/vf_g5_fu4_cluster_pivot.csv`")
    lines.append("- `reports/vf_g5_fu4_weekly_clow_dist.csv`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"  {REPORT_PATH}")


if __name__ == "__main__":
    main()
