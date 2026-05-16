"""分析 reports/dcf_ic_historical_panel.parquet -> reports/dcf_ic_validation.md

Phase 1: Pearson + Spearman IC by year / overall + IR
Phase 2: Decile spread (Q10-Q1) + monotonicity (Spearman of decile vs ret)
Phase 3: 方案 A 過濾回測 (3 thresholds: -20%, 0%, +20%)
Final verdict A/B grades 各自 + 落地建議
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from scipy.stats import pearsonr, spearmanr

REPO = Path(__file__).resolve().parent.parent
PANEL = REPO / "reports" / "dcf_ic_historical_panel.parquet"
OUT = REPO / "reports" / "dcf_ic_validation.md"


def grade_ic(ic, ir):
    if abs(ic) >= 0.03 and abs(ir) >= 0.3:
        return "A"
    if abs(ic) >= 0.02 and abs(ir) >= 0.2:
        return "B"
    if abs(ic) >= 0.01:
        return "C"
    return "D"


def main():
    df = pd.read_parquet(PANEL)
    df = df.dropna(subset=["base_mos", "fwd_252d_ret"])
    print(f"Loaded {len(df)} rows with non-null fwd_252d_ret")
    print(f"FY distribution:\n{df['fy_end'].value_counts().sort_index()}\n")

    lines = []
    lines.append("# DCF Base MOS IC 驗證報告")
    lines.append(f"\n資料: {len(df)} (stock, fy_end) panels；universe top300 扣金融/公用後 274 candidate，"
                 f"含 5 FY ({sorted(df['fy_end'].unique())})\n")
    lines.append("---\n")

    # ============ Phase 1: IC ============
    lines.append("## Phase 1: Cross-sectional IC (Pearson + Spearman)\n")
    yearly = []
    for fy in sorted(df["fy_end"].unique()):
        sub = df[df["fy_end"] == fy]
        if len(sub) < 10:
            continue
        pr, _ = pearsonr(sub["base_mos"], sub["fwd_252d_ret"])
        sp, _ = spearmanr(sub["base_mos"], sub["fwd_252d_ret"])
        yearly.append({"fy": fy, "n": len(sub), "pearson": pr, "spearman": sp,
                       "mean_fwd_ret": sub["fwd_252d_ret"].mean()})

    yr_df = pd.DataFrame(yearly)
    lines.append("| FY end | n | Pearson IC | Spearman IC | Mean Fwd 252d Ret |")
    lines.append("|---|---:|---:|---:|---:|")
    for r in yr_df.itertuples():
        lines.append(f"| {r.fy} | {r.n} | {r.pearson:+.4f} | {r.spearman:+.4f} | {r.mean_fwd_ret:+.2%} |")

    pmean, pstd = yr_df["pearson"].mean(), yr_df["pearson"].std()
    smean, sstd = yr_df["spearman"].mean(), yr_df["spearman"].std()
    pir = pmean / pstd if pstd > 0 else 0
    sir = smean / sstd if sstd > 0 else 0
    grade_p = grade_ic(pmean, pir)
    grade_s = grade_ic(smean, sir)

    lines.append(f"\n**Overall IC**: Pearson **{pmean:+.4f}** (IR={pir:+.2f}, grade **{grade_p}**) | "
                 f"Spearman **{smean:+.4f}** (IR={sir:+.2f}, grade **{grade_s}**)\n")

    # ============ Phase 2: Decile ============
    lines.append("## Phase 2: Decile spread (Base MOS deciles → fwd 252d ret)\n")
    # Pool across years for decile分布 + 也做 per-year
    df["decile"] = pd.qcut(df["base_mos"].rank(method="first"), 10, labels=False) + 1
    dec = df.groupby("decile").agg(
        n=("base_mos", "size"),
        mos_mean=("base_mos", "mean"),
        ret_mean=("fwd_252d_ret", "mean"),
        ret_median=("fwd_252d_ret", "median"),
    ).reset_index()
    lines.append("| Decile | n | Mean MOS | Mean Fwd Ret | Median Fwd Ret |")
    lines.append("|---:|---:|---:|---:|---:|")
    for r in dec.itertuples():
        lines.append(f"| Q{r.decile} | {r.n} | {r.mos_mean:+.2%} | {r.ret_mean:+.2%} | {r.ret_median:+.2%} |")

    q10_q1 = dec.iloc[-1]["ret_mean"] - dec.iloc[0]["ret_mean"]
    mono, _ = spearmanr(dec["decile"], dec["ret_mean"])
    lines.append(f"\n**Q10 - Q1 spread**: {q10_q1:+.2%} | "
                 f"**Monotonicity (Spearman)**: {mono:+.3f}\n")

    if abs(mono) < 0.3:
        mono_note = "⚠️ 非單調 — 可能 false signal (參考 ROIC F3 inverted-U 教訓)"
    elif mono > 0:
        mono_note = "✅ 正向單調 — Base MOS 高（低估）→ 報酬高（符合 DCF 理論）"
    else:
        mono_note = "❌ 反向單調 — Base MOS 高反而報酬低（與 DCF 理論矛盾）"
    lines.append(f"{mono_note}\n")

    # ============ Phase 3: 方案 A 過濾回測 ============
    lines.append("## Phase 3: 方案 A 過濾回測\n")
    lines.append("策略：每年 FY-end+90d 進場，equal-weight, hold 252d。")
    lines.append("Baseline = 全 universe；Filtered = Base MOS > threshold。\n")

    thresholds = [-0.20, 0.0, 0.20]
    results = []
    by_year_baseline = []
    by_year_filter = {t: [] for t in thresholds}

    for fy in sorted(df["fy_end"].unique()):
        sub = df[df["fy_end"] == fy].dropna(subset=["fwd_252d_ret"])
        if len(sub) < 10:
            continue
        ret_baseline = sub["fwd_252d_ret"].mean()
        by_year_baseline.append({"fy": fy, "n": len(sub), "ret": ret_baseline})
        for t in thresholds:
            f = sub[sub["base_mos"] > t]
            if len(f) >= 5:
                by_year_filter[t].append({"fy": fy, "n": len(f), "ret": f["fwd_252d_ret"].mean()})

    bs = pd.DataFrame(by_year_baseline)
    lines.append("| FY entry (FY+90d) | n_universe | Baseline ret (eq-weight)" +
                 "".join([f" | MOS>{int(t*100):+}% n / ret" for t in thresholds]) + " |")
    lines.append("|---|---:|---:|" + "|---:|" * len(thresholds))

    for r in bs.itertuples():
        row_parts = [f"{r.fy}", f"{r.n}", f"{r.ret:+.2%}"]
        for t in thresholds:
            fr = [x for x in by_year_filter[t] if x["fy"] == r.fy]
            if fr:
                row_parts.append(f"{fr[0]['n']} / {fr[0]['ret']:+.2%}")
            else:
                row_parts.append("n<5")
        lines.append("| " + " | ".join(row_parts) + " |")

    lines.append("")
    lines.append("**Yearly summary (mean across 5 FY)**:\n")
    lines.append("| Strategy | Mean Ret | Stdev | Sharpe-like | n_avg/yr |")
    lines.append("|---|---:|---:|---:|---:|")
    bm = bs["ret"].mean()
    bs_std = bs["ret"].std()
    lines.append(f"| Baseline | {bm:+.2%} | {bs_std:.2%} | "
                 f"{bm/bs_std if bs_std>0 else 0:+.2f} | {bs['n'].mean():.0f} |")
    for t in thresholds:
        fl = pd.DataFrame(by_year_filter[t])
        if fl.empty:
            lines.append(f"| MOS>{int(t*100):+}% | N/A (樣本太少) | — | — | — |")
            continue
        m = fl["ret"].mean()
        s = fl["ret"].std()
        lines.append(f"| MOS>{int(t*100):+}% | {m:+.2%} | {s:.2%} | "
                     f"{m/s if s>0 else 0:+.2f} | {fl['n'].mean():.0f} |")

    # ============ 最終 Verdict ============
    lines.append("\n---\n## 最終 Verdict\n")

    lines.append(f"### 方案 B (軟加分 — composite_score 加 MOS 維度)\n")
    lines.append(f"- Pearson IC = {pmean:+.4f} (IR={pir:+.2f}) → **{grade_p}**")
    lines.append(f"- Spearman IC = {smean:+.4f} (IR={sir:+.2f}) → **{grade_s}**")
    lines.append(f"- Decile Q10-Q1 spread = {q10_q1:+.2%}, monotonicity = {mono:+.3f}")
    if max(grade_p, grade_s) in ("A", "B"):
        lines.append(f"- **落地建議**：上線 composite_score (建議權重 5-10 分先 shadow)")
    elif "C" in (grade_p, grade_s):
        lines.append(f"- **落地建議**：弱訊號 C 級，shadow 觀察 1-2 季再決定")
    else:
        lines.append(f"- **落地建議**：D 級 / 反向，**不上線**，歸檔。"
                     "符合 ROIC/CCC/GM level/CAPE 等同類絕對估值因子全 D 的歷史規律")

    lines.append(f"\n### 方案 A (硬過濾 — 強勢股池 + MOS > threshold)\n")

    best_filter = None
    best_sharpe_gain = -999
    baseline_sharpe = bm/bs_std if bs_std>0 else 0
    for t in thresholds:
        fl = pd.DataFrame(by_year_filter[t])
        if fl.empty:
            continue
        m = fl["ret"].mean()
        s = fl["ret"].std()
        sharpe = m/s if s>0 else 0
        gain = sharpe - baseline_sharpe
        if gain > best_sharpe_gain:
            best_sharpe_gain = gain
            best_filter = (t, m, s, sharpe, fl["n"].mean())

    if best_filter and best_sharpe_gain > 0.1:
        t, m, s, sh, n = best_filter
        lines.append(f"- 最佳 threshold MOS > {int(t*100):+}%: Sharpe-like {sh:+.2f} "
                     f"vs baseline {baseline_sharpe:+.2f} (Δ={best_sharpe_gain:+.2f}), "
                     f"n_avg={n:.0f} 檔/年")
        if n >= 30:
            lines.append(f"- **落地建議**：Sharpe 提升 ≥0.1 且樣本 ≥30 檔 → **可上線**過濾")
        else:
            lines.append(f"- **落地建議**：Sharpe 有提升但樣本不足 (<30 檔/年)，"
                         "上線會大幅縮減 universe，不建議當主篩")
    else:
        lines.append(f"- 所有 threshold 都無法擊敗 baseline Sharpe (best gain={best_sharpe_gain:+.2f})")
        lines.append(f"- **落地建議**：**不上線**，過濾後 risk-adjusted 報酬反而變差或無提升")

    lines.append(f"\n### 觀察重點 (Multi-bull bias 警示)\n")
    # 2022 是 bear year
    fy22 = yr_df[yr_df["fy"] == "2022-12-31"]
    if not fy22.empty:
        lines.append(f"- 2022 FY (bear year): Pearson IC = {fy22.iloc[0]['pearson']:+.4f}, "
                     f"Mean Fwd Ret = {fy22.iloc[0]['mean_fwd_ret']:+.2%}")
        if fy22.iloc[0]['pearson'] * pmean < 0:
            lines.append(f"  ⚠️ Bear year 方向與整體相反 — 警惕 multi-bull bias")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nSaved: {OUT}")
    print(f"\n=== Headline ===")
    print(f"方案 B (IC): Pearson {grade_p} ({pmean:+.4f}) / Spearman {grade_s} ({smean:+.4f}) / Decile mono {mono:+.3f}")
    print(f"方案 A (filter): best_gain={best_sharpe_gain:+.2f} ", end="")
    if best_filter:
        print(f"@ MOS>{int(best_filter[0]*100):+}%")


if __name__ == "__main__":
    main()
