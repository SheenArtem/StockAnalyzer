"""Niche 用法 B 驗證 — DCF MOS 當 position sizing modifier

策略對比 (universe = all 274 candidates):
  1. Baseline: equal weight
  2. MOS bucket bonus: 落在 Q4 area [+10%, +50%] 給 1.5x 權重，其他 0.5x（其他不剔除，只 weight 變小）
  3. MOS linear (clipped): weight ∝ clip(MOS, -50%, +100%) + 1.0 (避免 Q10 outlier 主導)
  4. MOS rank: 按 base_mos rank percentile 線性給 weight (Top 0%=2.0, Bot 100%=0.0)

對每年 FY-end+90d 進場，hold 252d，weighted return = Σ w_i × ret_i / Σ w_i
跟 baseline (equal weight, n>=10) 比 mean ret + Sharpe + t-test。
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np

REPO = Path(__file__).resolve().parent.parent
PANEL = REPO / "reports" / "dcf_ic_historical_panel.parquet"
OUT = REPO / "reports" / "dcf_position_sizing.md"


def weighted_ret(df: pd.DataFrame, weights: pd.Series) -> float:
    w = weights.values
    r = df["fwd_252d_ret"].values
    valid = ~np.isnan(r) & (w > 0)
    if not valid.any():
        return np.nan
    return float((w[valid] * r[valid]).sum() / w[valid].sum())


def main():
    df = pd.read_parquet(PANEL).dropna(subset=["base_mos", "fwd_252d_ret"])
    yrs = sorted(df["fy_end"].unique())

    rows = {label: [] for label in
            ["Baseline (eq)", "MOS bucket", "MOS linear", "MOS rank"]}

    for fy in yrs:
        sub = df[df["fy_end"] == fy].copy().reset_index(drop=True)
        n = len(sub)

        # 1. Baseline equal
        rows["Baseline (eq)"].append({"fy": fy, "n": n,
                                      "ret": sub["fwd_252d_ret"].mean()})

        # 2. Bucket bonus: Q4 area 1.5x, others 0.5x
        w_bucket = sub["base_mos"].apply(
            lambda m: 1.5 if 0.10 <= m <= 0.50 else 0.5
        )
        rows["MOS bucket"].append({"fy": fy, "n": n,
                                   "ret": weighted_ret(sub, w_bucket)})

        # 3. Linear (clipped to [-50%, +100%] to avoid Q10 outlier)
        clipped = sub["base_mos"].clip(lower=-0.5, upper=1.0)
        w_linear = clipped + 1.0  # shift so all positive (range 0.5 ~ 2.0)
        rows["MOS linear"].append({"fy": fy, "n": n,
                                   "ret": weighted_ret(sub, w_linear)})

        # 4. Rank percentile: top MOS rank → weight 2.0, bottom → 0.0
        ranks = sub["base_mos"].rank(pct=True)
        w_rank = ranks * 2.0   # weight ∈ [0, 2]
        rows["MOS rank"].append({"fy": fy, "n": n,
                                 "ret": weighted_ret(sub, w_rank)})

    lines = []
    lines.append("# Niche B: DCF MOS Position Sizing 驗證")
    lines.append("\n4 種 weight scheme 應用同一 universe (n_avg=132)，hold 252d:")
    lines.append("- **Baseline (eq)**: 等權")
    lines.append("- **MOS bucket**: Q4 area [+10%, +50%] 1.5x weight，其他 0.5x")
    lines.append("- **MOS linear**: weight = clip(MOS, -50%, +100%) + 1.0 (避 Q10 outlier 主導)")
    lines.append("- **MOS rank**: weight = base_mos rank percentile × 2.0\n")

    lines.append("## Yearly returns\n")
    lines.append("| FY | " + " | ".join(rows.keys()) + " |")
    lines.append("|---|" + "---:|" * len(rows))
    for i, fy in enumerate(yrs):
        cells = [fy]
        for label in rows.keys():
            r = rows[label][i]["ret"]
            cells.append(f"{r:+.2%}" if not np.isnan(r) else "n/a")
        lines.append("| " + " | ".join(cells) + " |")

    lines.append("\n## Cross-year summary\n")
    lines.append("| Scheme | Mean Ret | Std | Sharpe | Δ vs Base | t-stat | p |")
    lines.append("|---|---:|---:|---:|---:|---:|---|")
    base_rets = [r["ret"] for r in rows["Baseline (eq)"]]
    base_mean = np.mean(base_rets)
    base_std = np.std(base_rets, ddof=1)
    base_sharpe = base_mean / base_std if base_std > 0 else 0
    lines.append(f"| Baseline (eq) | {base_mean:+.2%} | {base_std:.2%} | "
                 f"{base_sharpe:+.2f} | — | — | — |")
    for label in rows.keys():
        if label == "Baseline (eq)":
            continue
        rets = [r["ret"] for r in rows[label]]
        m = np.mean(rets)
        s = np.std(rets, ddof=1)
        sh = m / s if s > 0 else 0
        diffs = np.array(rets) - np.array(base_rets)
        d_mean = diffs.mean()
        d_std = diffs.std(ddof=1)
        t = d_mean / (d_std / np.sqrt(len(diffs))) if d_std > 0 else 0
        if abs(t) >= 2.78:
            p = "✅ <0.05"
        elif abs(t) >= 2.13:
            p = "⚠️ <0.10"
        else:
            p = "❌ noise"
        lines.append(f"| {label} | {m:+.2%} | {s:.2%} | {sh:+.2f} | "
                     f"{sh-base_sharpe:+.2f} | {t:+.2f} | {p} |")

    lines.append("\n⚠️ 5 個 yearly observation, t > 2.78 才能 95% 排除 noise (df=4, two-tail)\n")
    lines.append("---\n## Verdict\n")

    # Find best non-baseline
    best_label = max((l for l in rows.keys() if l != "Baseline (eq)"),
                     key=lambda l: np.mean([r["ret"] for r in rows[l]]))
    best_rets = [r["ret"] for r in rows[best_label]]
    best_sh = np.mean(best_rets) / np.std(best_rets, ddof=1)
    diff_mean = np.mean(np.array(best_rets) - np.array(base_rets))
    diff_std = np.std(np.array(best_rets) - np.array(base_rets), ddof=1)
    t_best = diff_mean / (diff_std / np.sqrt(len(best_rets))) if diff_std > 0 else 0

    lines.append(f"- 最佳 scheme: **{best_label}**, Sharpe {best_sh:+.2f} "
                 f"(Δ {best_sh-base_sharpe:+.2f}), t={t_best:+.2f}")
    if abs(t_best) >= 2.78:
        lines.append(f"- ✅ p<0.05 顯著，可上線當 position sizing layer")
    elif abs(t_best) >= 2.13:
        lines.append(f"- ⚠️ p<0.10 邊際，shadow 累積 OOS 再決定")
    else:
        lines.append(f"- ❌ p>0.10 noise，跟 sweet spot 同結論：方向對但樣本不足，不上線")
    lines.append(f"\n結論同 selection 驗證：DCF 訊號 directional 存在但 5-yr sample 統計力不足。")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved {OUT}")
    # Print summary
    for label in rows.keys():
        rets = [r["ret"] for r in rows[label]]
        print(f"{label:<20} mean={np.mean(rets):+.2%}  sharpe={np.mean(rets)/np.std(rets,ddof=1):+.2f}")


if __name__ == "__main__":
    main()
