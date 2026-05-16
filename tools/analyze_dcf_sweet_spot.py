"""分析 Q4 sweet spot 配置 — DCF MOS 在中度低估帶報酬最高的假說

用 reports/dcf_ic_historical_panel.parquet 跑 4 個 portfolio:
  1. Baseline (全 universe)
  2. Filter MOS > 0% (Phase 3 既有)
  3. Sweet spot Q4 [+10%, +50%] 「中度低估」
  4. 嚴格寬版 [+5%, +80%]

對每年 FY-end+90d 進場 equal-weight hold 252d, 算 yearly mean ret + cross-year Sharpe + n_avg.
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np

REPO = Path(__file__).resolve().parent.parent
PANEL = REPO / "reports" / "dcf_ic_historical_panel.parquet"
OUT = REPO / "reports" / "dcf_sweet_spot.md"


def main():
    df = pd.read_parquet(PANEL).dropna(subset=["base_mos", "fwd_252d_ret"])
    print(f"Loaded {len(df)} rows")

    # 4 portfolio definitions: (label, predicate)
    configs = [
        ("Baseline (全 universe)", lambda d: d),
        ("MOS > 0% (既有)", lambda d: d[d["base_mos"] > 0]),
        ("Sweet Q4 [+10%, +50%]", lambda d: d[(d["base_mos"] >= 0.10) & (d["base_mos"] <= 0.50)]),
        ("寬版 [+5%, +80%]", lambda d: d[(d["base_mos"] >= 0.05) & (d["base_mos"] <= 0.80)]),
        ("極窄 [+15%, +30%]", lambda d: d[(d["base_mos"] >= 0.15) & (d["base_mos"] <= 0.30)]),
    ]

    yearly_rows = []
    summary_rows = []

    for label, pred in configs:
        yearly_ret = []
        yearly_n = []
        for fy in sorted(df["fy_end"].unique()):
            sub = df[df["fy_end"] == fy]
            f = pred(sub)
            if len(f) >= 5:
                yearly_ret.append({"label": label, "fy": fy, "n": len(f),
                                   "ret": f["fwd_252d_ret"].mean()})
                yearly_n.append(len(f))
            else:
                yearly_ret.append({"label": label, "fy": fy, "n": len(f), "ret": np.nan})
        yearly_rows.extend(yearly_ret)
        valid = [r["ret"] for r in yearly_ret if not np.isnan(r["ret"])]
        if not valid:
            continue
        mean_ret = np.mean(valid)
        std_ret = np.std(valid, ddof=1) if len(valid) > 1 else 0
        sharpe = mean_ret / std_ret if std_ret > 0 else 0
        # vs 2330 buy-and-hold 對比？這裡先不加，只跟 baseline 比
        summary_rows.append({
            "label": label, "mean_ret": mean_ret, "std_ret": std_ret,
            "sharpe": sharpe, "n_avg": np.mean(yearly_n) if yearly_n else 0,
            "n_min": min(yearly_n) if yearly_n else 0,
            "yearly_consistency": sum(1 for r in valid if r > 0) / len(valid),
        })

    yr_df = pd.DataFrame(yearly_rows)
    sum_df = pd.DataFrame(summary_rows)

    lines = []
    lines.append("# DCF Q4 Sweet Spot Portfolio 驗證")
    lines.append("\n假說：Decile spread 顯示 Q4 (MOS +17% mean) 報酬 peak +43%，遠勝 Q1 (-65% MOS) +12.5% 與 Q10 (+754% MOS) +35%。")
    lines.append("→ 中度低估 [+10%, +50%] 是 sweet spot，極端值 (Q1 高估 / Q10 FCF artifact) 都應避開。\n")
    lines.append("策略：每年 FY-end+90d 進場 equal-weight，hold 252d，重新平衡至下年。\n")
    lines.append("---\n")

    lines.append("## Yearly returns by portfolio\n")
    pivot = yr_df.pivot(index="fy", columns="label", values="ret")
    pivot_n = yr_df.pivot(index="fy", columns="label", values="n")
    portfolios = [c[0] for c in configs]
    lines.append("| FY | " + " | ".join(portfolios) + " |")
    lines.append("|---|" + "---:|" * len(portfolios))
    for fy in pivot.index:
        cells = [f"{fy}"]
        for p in portfolios:
            r = pivot.loc[fy, p]
            n = int(pivot_n.loc[fy, p]) if not pd.isna(pivot_n.loc[fy, p]) else 0
            if pd.isna(r):
                cells.append(f"n={n} (skip)")
            else:
                cells.append(f"{r:+.2%} (n={n})")
        lines.append("| " + " | ".join(cells) + " |")

    lines.append("\n## Cross-year summary\n")
    lines.append("| Portfolio | Mean Ret | Std | Sharpe | n_avg/yr | n_min/yr | Yrs > 0 |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    base_sharpe = sum_df[sum_df["label"] == configs[0][0]]["sharpe"].iloc[0]
    for r in sum_df.itertuples():
        delta = r.sharpe - base_sharpe if "Baseline" not in r.label else 0
        delta_s = f" (Δ={delta:+.2f})" if "Baseline" not in r.label else ""
        lines.append(f"| {r.label} | {r.mean_ret:+.2%} | {r.std_ret:.2%} | "
                     f"{r.sharpe:+.2f}{delta_s} | {r.n_avg:.0f} | {r.n_min} | "
                     f"{r.yearly_consistency*100:.0f}% |")

    # Statistical significance: paired t-test sweet portfolio vs baseline yearly diff
    lines.append("\n---\n## Statistical significance (paired diff vs Baseline)\n")
    base_yr = yr_df[yr_df["label"] == configs[0][0]].set_index("fy")["ret"]
    sig_rows = []
    for label, _ in configs[1:]:
        port_yr = yr_df[yr_df["label"] == label].set_index("fy")["ret"]
        # 對齊年份 + 雙方都非 NaN
        joined = pd.concat([base_yr.rename("base"), port_yr.rename("port")], axis=1).dropna()
        n_valid = len(joined)
        if n_valid < 3:
            sig_rows.append({"label": label, "n_valid_yr": n_valid,
                             "mean_diff": np.nan, "std_diff": np.nan, "t": np.nan})
            continue
        diffs = joined["port"] - joined["base"]
        m = diffs.mean()
        s = diffs.std(ddof=1)
        t = m / (s / np.sqrt(n_valid)) if s > 0 else 0
        sig_rows.append({"label": label, "n_valid_yr": n_valid,
                         "mean_diff": m, "std_diff": s, "t": t})

    lines.append("| Portfolio | 有效年數 | Mean Yearly Diff vs Base | Std Diff | t-stat | p-judgment |")
    lines.append("|---|---:|---:|---:|---:|---|")
    for r in sig_rows:
        if pd.isna(r["t"]):
            lines.append(f"| {r['label']} | {r['n_valid_yr']} | n/a | n/a | n/a | "
                         f"⚠️ 樣本太小 (n<3) |")
            continue
        t = r["t"]
        if abs(t) >= 2.78:   # 95% CI 雙尾, df=4
            p = "✅ p<0.05 (顯著)"
        elif abs(t) >= 2.13:  # 90% CI 雙尾, df=4
            p = "⚠️ p<0.10 (邊際)"
        else:
            p = "❌ p>0.10 (noise)"
        lines.append(f"| {r['label']} | {r['n_valid_yr']} | {r['mean_diff']:+.2%} | "
                     f"{r['std_diff']:.2%} | {t:+.2f} | {p} |")
    lines.append("\n⚠️ 5 個 yearly observation 統計力極弱 — t-stat 需 >2.78 才能 95% 排除 noise。")

    # Verdict
    lines.append("\n---\n## Verdict\n")
    sweet = sum_df[sum_df["label"] == "Sweet Q4 [+10%, +50%]"].iloc[0]
    wide = sum_df[sum_df["label"] == "寬版 [+5%, +80%]"].iloc[0]
    narrow = sum_df[sum_df["label"] == "極窄 [+15%, +30%]"].iloc[0]
    baseline = sum_df[sum_df["label"] == "Baseline (全 universe)"].iloc[0]

    lines.append(f"- **Baseline Sharpe**: {baseline.sharpe:+.2f} | Mean ret {baseline.mean_ret:+.2%}")
    lines.append(f"- **Sweet Q4 [+10%, +50%]** Sharpe: {sweet.sharpe:+.2f} "
                 f"(Δ={sweet.sharpe-baseline.sharpe:+.2f}), n_avg={sweet.n_avg:.0f}")
    lines.append(f"- **寬版 [+5%, +80%]** Sharpe: {wide.sharpe:+.2f} "
                 f"(Δ={wide.sharpe-baseline.sharpe:+.2f}), n_avg={wide.n_avg:.0f}")
    lines.append(f"- **極窄 [+15%, +30%]** Sharpe: {narrow.sharpe:+.2f} "
                 f"(Δ={narrow.sharpe-baseline.sharpe:+.2f}), n_avg={narrow.n_avg:.0f}")

    best = sum_df.iloc[sum_df["sharpe"].idxmax()]
    lines.append(f"\n**最佳 portfolio**: {best.label} (Sharpe {best.sharpe:+.2f})")
    if best.label == configs[0][0]:
        lines.append("→ Baseline 最強，sweet spot 假說 **不成立**，DCF 過濾全部 D 級。")
    else:
        gain = best.sharpe - baseline.sharpe
        if gain >= 0.15:
            lines.append(f"→ Sharpe Δ={gain:+.2f}，**強訊號可上線**")
        elif gain >= 0.05:
            lines.append(f"→ Sharpe Δ={gain:+.2f}，**弱訊號 B 級**，建議 shadow 1-2 季再決定")
        else:
            lines.append(f"→ Sharpe Δ={gain:+.2f}，**邊際 C 級**，可能 noise")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved {OUT}")
    print("\n=== Summary ===")
    print(sum_df.to_string(index=False))


if __name__ == "__main__":
    main()
