"""跨 15 quarters 的 IC + sweet spot + position sizing 整合驗證

讀 reports/dcf_quarterly_panel.parquet (1926 rows, fwd_60d_ret 不重疊)
輸出 reports/dcf_quarterly_validation.md

跟 yearly 版本 (5 obs) 對比：quarterly 15 obs, df=14, t > 2.14 才 p<0.05
"""
from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr

REPO = Path(__file__).resolve().parent.parent
PANEL = REPO / "reports" / "dcf_quarterly_panel.parquet"
OUT = REPO / "reports" / "dcf_quarterly_validation.md"

T_95 = 2.14   # df=14 two-tail
T_90 = 1.76
T_50_OBS_95 = 2.78  # df=4 (yearly)


def grade_ic(ic, ir):
    if abs(ic) >= 0.03 and abs(ir) >= 0.3: return "A"
    if abs(ic) >= 0.02 and abs(ir) >= 0.2: return "B"
    if abs(ic) >= 0.01: return "C"
    return "D"


def main():
    df = pd.read_parquet(PANEL).dropna(subset=["base_mos", "fwd_60d_ret"])
    print(f"Loaded {len(df)} rows across {df['q_end'].nunique()} quarters")

    qs = sorted(df["q_end"].unique())
    lines = ["# DCF Quarterly Panel 驗證 (擴大樣本)\n"]
    lines.append(f"Sample: **{len(df)} rows × {len(qs)} quarters** (Q2/Q3/Q4 × 5 yr 2019-2023)")
    lines.append(f"Forward 60d return 不重疊 → 獨立 obs. df=14, t > **2.14** 才 p<0.05 (vs yearly 5 obs t>2.78).\n---\n")

    # ============ Phase 1: IC ============
    lines.append("## Phase 1: Cross-sectional IC (per quarter)\n")
    yearly = []
    for q in qs:
        sub = df[df["q_end"] == q]
        if len(sub) < 10: continue
        pr, _ = pearsonr(sub["base_mos"], sub["fwd_60d_ret"])
        sp, _ = spearmanr(sub["base_mos"], sub["fwd_60d_ret"])
        yearly.append({"q": q, "n": len(sub), "pearson": pr, "spearman": sp,
                       "mean_fwd": sub["fwd_60d_ret"].mean()})

    yr = pd.DataFrame(yearly)
    lines.append("| Q end | n | Pearson IC | Spearman IC | Mean Fwd 60d |")
    lines.append("|---|---:|---:|---:|---:|")
    for r in yr.itertuples():
        lines.append(f"| {r.q} | {r.n} | {r.pearson:+.4f} | {r.spearman:+.4f} | {r.mean_fwd:+.2%} |")

    pmean, pstd = yr["pearson"].mean(), yr["pearson"].std()
    smean, sstd = yr["spearman"].mean(), yr["spearman"].std()
    pir = pmean / pstd if pstd > 0 else 0
    sir = smean / sstd if sstd > 0 else 0
    gp, gs = grade_ic(pmean, pir), grade_ic(smean, sir)

    # paired t for IC > 0 (one-sample t against 0)
    n_ic = len(yr)
    tp = pmean / (pstd / np.sqrt(n_ic)) if pstd > 0 else 0
    ts = smean / (sstd / np.sqrt(n_ic)) if sstd > 0 else 0

    lines.append(f"\n**Overall Pearson IC** = {pmean:+.4f} (IR={pir:+.2f}, **t={tp:+.2f}**, grade **{gp}**)")
    lines.append(f"**Overall Spearman IC** = {smean:+.4f} (IR={sir:+.2f}, **t={ts:+.2f}**, grade **{gs}**)\n")

    yrs_pos_ic = sum(1 for x in yr["pearson"] if x > 0)
    lines.append(f"Positive Pearson IC quarters: {yrs_pos_ic}/{len(yr)} ({yrs_pos_ic/len(yr)*100:.0f}%)")
    lines.append(f"Positive Spearman IC quarters: {sum(1 for x in yr['spearman'] if x > 0)}/{len(yr)}\n")

    # ============ Phase 2: Decile ============
    lines.append("## Phase 2: Decile spread (Pool across quarters)\n")
    df_d = df.copy()
    df_d["decile"] = pd.qcut(df_d["base_mos"].rank(method="first"), 10, labels=False) + 1
    dec = df_d.groupby("decile").agg(
        n=("base_mos","size"), mos_mean=("base_mos","mean"),
        ret_mean=("fwd_60d_ret","mean"), ret_median=("fwd_60d_ret","median")
    ).reset_index()
    lines.append("| Decile | n | Mean MOS | Mean Fwd 60d | Median Fwd 60d |")
    lines.append("|---:|---:|---:|---:|---:|")
    for r in dec.itertuples():
        lines.append(f"| Q{r.decile} | {r.n} | {r.mos_mean:+.2%} | {r.ret_mean:+.2%} | {r.ret_median:+.2%} |")
    spread = dec.iloc[-1]["ret_mean"] - dec.iloc[0]["ret_mean"]
    mono, _ = spearmanr(dec["decile"], dec["ret_mean"])
    lines.append(f"\n**Q10-Q1 spread**: {spread:+.2%} | **Monotonicity**: {mono:+.3f}\n")

    # ============ Phase 3: Sweet spot + Sizing ============
    lines.append("## Phase 3: Sweet spot bucket portfolios + position sizing\n")

    def weighted_ret(sub, w):
        valid = (w > 0) & sub["fwd_60d_ret"].notna()
        if not valid.any(): return np.nan
        return float((w[valid] * sub["fwd_60d_ret"][valid]).sum() / w[valid].sum())

    configs = [
        ("Baseline (eq)", lambda s: pd.Series(1.0, index=s.index)),
        ("Filter MOS>0%", lambda s: pd.Series(np.where(s["base_mos"] > 0, 1.0, 0.0), index=s.index)),
        ("Sweet Q4 [+10%,+50%]", lambda s: pd.Series(np.where(
            (s["base_mos"] >= 0.10) & (s["base_mos"] <= 0.50), 1.0, 0.0), index=s.index)),
        ("寬版 [+5%,+80%]", lambda s: pd.Series(np.where(
            (s["base_mos"] >= 0.05) & (s["base_mos"] <= 0.80), 1.0, 0.0), index=s.index)),
        ("Bucket 1.5x/0.5x", lambda s: pd.Series(np.where(
            (s["base_mos"] >= 0.10) & (s["base_mos"] <= 0.50), 1.5, 0.5), index=s.index)),
        ("Rank weighted", lambda s: pd.Series(s["base_mos"].rank(pct=True).values * 2.0, index=s.index)),
    ]

    qly = {label: [] for label, _ in configs}
    for q in qs:
        sub = df[df["q_end"] == q].copy()
        for label, fn in configs:
            w = fn(sub)
            if (w > 0).sum() < 5:
                qly[label].append({"q": q, "n": int((w>0).sum()), "ret": np.nan})
                continue
            qly[label].append({"q": q, "n": int((w>0).sum()), "ret": weighted_ret(sub, w)})

    base_rets = np.array([r["ret"] for r in qly["Baseline (eq)"]])
    base_mean = np.nanmean(base_rets)
    base_std = np.nanstd(base_rets, ddof=1)
    base_sh = base_mean / base_std if base_std > 0 else 0

    lines.append("| Portfolio | Mean (60d) | Std | Sharpe | Δ vs Base | t-stat | p (df=14) |")
    lines.append("|---|---:|---:|---:|---:|---:|---|")
    lines.append(f"| Baseline (eq) | {base_mean:+.2%} | {base_std:.2%} | {base_sh:+.2f} | — | — | — |")
    for label, _ in configs:
        if label == "Baseline (eq)": continue
        rets = np.array([r["ret"] for r in qly[label]])
        valid_mask = ~np.isnan(rets) & ~np.isnan(base_rets)
        if valid_mask.sum() < 3:
            lines.append(f"| {label} | n/a | — | — | — | — | n<3 |")
            continue
        m = np.nanmean(rets[valid_mask])
        s = np.nanstd(rets[valid_mask], ddof=1)
        sh = m / s if s > 0 else 0
        diffs = rets[valid_mask] - base_rets[valid_mask]
        dm = diffs.mean()
        ds = diffs.std(ddof=1)
        n_eff = valid_mask.sum()
        t = dm / (ds / np.sqrt(n_eff)) if ds > 0 else 0
        if abs(t) >= T_95: p = "✅ <0.05"
        elif abs(t) >= T_90: p = "⚠️ <0.10"
        else: p = "❌ noise"
        lines.append(f"| {label} | {m:+.2%} | {s:.2%} | {sh:+.2f} | "
                     f"{sh-base_sh:+.2f} | {t:+.2f} | {p} |")

    # ============ Verdict ============
    lines.append("\n---\n## Final Verdict (quarterly sample)\n")
    if abs(tp) >= T_95:
        ic_v = f"✅ Pearson IC t={tp:+.2f} p<0.05 顯著"
    elif abs(tp) >= T_90:
        ic_v = f"⚠️ Pearson IC t={tp:+.2f} p<0.10 邊際"
    else:
        ic_v = f"❌ Pearson IC t={tp:+.2f} noise"
    lines.append(f"- {ic_v}")
    if abs(ts) >= T_95:
        ic_s = f"✅ Spearman IC t={ts:+.2f} p<0.05 顯著"
    elif abs(ts) >= T_90:
        ic_s = f"⚠️ Spearman IC t={ts:+.2f} p<0.10 邊際"
    else:
        ic_s = f"❌ Spearman IC t={ts:+.2f} noise"
    lines.append(f"- {ic_s}")
    lines.append(f"- Decile Q10-Q1 spread: {spread:+.2%}, monotonicity {mono:+.3f}")
    lines.append("\n比 yearly version (5 obs) 樣本 3x，t 門檻從 2.78 降到 2.14。")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved {OUT}")
    print(f"\nPearson overall IC = {pmean:+.4f}, IR = {pir:+.2f}, t = {tp:+.2f} → grade {gp}")
    print(f"Spearman overall IC = {smean:+.4f}, IR = {sir:+.2f}, t = {ts:+.2f} → grade {gs}")


if __name__ == "__main__":
    main()
