"""dcf_ic_analyze.py -- DCF Base MOS IC / Decile / Filtered backtest é©—è?

??reports/dcf_ic_historical_panel.parquetï¼Œè¼¸??reports/dcf_ic_validation.md

Phase 1: IC (Pearson + Spearman) cross-sectional by FY; IR = mean(IC) / std(IC)
Phase 2: 10-decile spread forward 252d; monotonicity check
Phase 3: ?¹æ? A ?æ¿¾?æ¸¬ (baseline = equal-weight ??panel; filter = MOS > threshold)
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr

REPO = Path(__file__).resolve().parent.parent
PANEL_PATH = REPO / "reports" / "dcf_ic_historical_panel.parquet"
OUT_MD = REPO / "reports" / "dcf_ic_validation.md"

N_DECILES = 10
THRESHOLDS = [-0.20, 0.0, 0.20]  # ?¹æ? A: Base MOS > threshold


def ic_table(df: pd.DataFrame, factor_col: str, ret_col: str) -> pd.DataFrame:
    rows = []
    for fy, g in df.groupby("fy_end"):
        g = g[[factor_col, ret_col]].replace([np.inf, -np.inf], np.nan).dropna()
        if len(g) < 10:
            continue
        pe_ic, pe_p = pearsonr(g[factor_col], g[ret_col])
        sp_ic, sp_p = spearmanr(g[factor_col], g[ret_col])
        rows.append({
            "fy_end": fy, "n": len(g),
            "pearson_ic": pe_ic, "pearson_p": pe_p,
            "spearman_ic": sp_ic, "spearman_p": sp_p,
            "mean_ret": float(g[ret_col].mean()),
        })
    df_ic = pd.DataFrame(rows)
    if df_ic.empty:
        return df_ic
    overall = {
        "fy_end": "OVERALL",
        "n": df_ic["n"].sum(),
        "pearson_ic": df_ic["pearson_ic"].mean(),
        "pearson_p": np.nan,
        "spearman_ic": df_ic["spearman_ic"].mean(),
        "spearman_p": np.nan,
        "mean_ret": df_ic["mean_ret"].mean(),
    }
    overall["pearson_ir"] = overall["pearson_ic"] / df_ic["pearson_ic"].std(ddof=0) \
        if df_ic["pearson_ic"].std() > 0 else 0
    overall["spearman_ir"] = overall["spearman_ic"] / df_ic["spearman_ic"].std(ddof=0) \
        if df_ic["spearman_ic"].std() > 0 else 0
    df_ic = pd.concat([df_ic, pd.DataFrame([overall])], ignore_index=True)
    return df_ic


def decile_spread(df: pd.DataFrame, factor_col: str, ret_col: str) -> pd.DataFrame:
    """æ¯å¹´??10 deciles, ?‹å? decile å¹³å? forward return, ??cross-FY å¹³å?"""
    out_rows = []
    for fy, g in df.groupby("fy_end"):
        g = g[[factor_col, ret_col]].replace([np.inf, -np.inf], np.nan).dropna().copy()
        if len(g) < 50:  # ä¸å???10 ç­?            continue
        g["decile"] = pd.qcut(g[factor_col].rank(method="first"), N_DECILES,
                              labels=False, duplicates="drop") + 1
        for d, gg in g.groupby("decile"):
            out_rows.append({
                "fy_end": fy, "decile": int(d), "n": len(gg),
                "mean_ret": float(gg[ret_col].mean()),
                "median_ret": float(gg[ret_col].median()),
            })
    long = pd.DataFrame(out_rows)
    if long.empty:
        return long
    # è·¨å¹´å¹³å?ï¼ˆper decileï¼?    agg = long.groupby("decile").agg(
        n_total=("n", "sum"),
        mean_ret_avg=("mean_ret", "mean"),
        mean_ret_std=("mean_ret", "std"),
    ).reset_index()
    return agg


def monotonicity_spearman(decile_agg: pd.DataFrame) -> float:
    """Spearman corr of (decile, mean_ret_avg) ??1.0 å®Œç??®èª¿"""
    if len(decile_agg) < 3:
        return np.nan
    r, _ = spearmanr(decile_agg["decile"], decile_agg["mean_ret_avg"])
    return r


def filter_backtest(df: pd.DataFrame, factor_col: str, ret_col: str,
                    thresholds: list[float]) -> pd.DataFrame:
    """?¹æ? Aï¼šæ? FY å»ºç? portfolio (baseline = all panel; filtered = MOS > threshold)
    ??CAGR / Sharpe / MDD / n_stocks per threshold
    """
    g = df[["fy_end", factor_col, ret_col]].replace([np.inf, -np.inf], np.nan).dropna()
    if g.empty:
        return pd.DataFrame()

    rows = []
    # Baseline: equal-weight ??panel
    baseline_yearly = g.groupby("fy_end")[ret_col].agg(["mean", "count"]).reset_index()
    baseline_yearly.columns = ["fy_end", "mean_ret", "n"]
    rows.append({
        "scenario": "baseline_all",
        "n_avg": float(baseline_yearly["n"].mean()),
        "mean_ret_yearly": baseline_yearly["mean_ret"].tolist(),
        "fy_ends": baseline_yearly["fy_end"].tolist(),
    })

    for th in thresholds:
        filt = g[g[factor_col] > th]
        if filt.empty:
            continue
        yearly = filt.groupby("fy_end")[ret_col].agg(["mean", "count"]).reset_index()
        yearly.columns = ["fy_end", "mean_ret", "n"]
        rows.append({
            "scenario": f"mos>{th:+.0%}",
            "n_avg": float(yearly["n"].mean()),
            "mean_ret_yearly": yearly["mean_ret"].tolist(),
            "fy_ends": yearly["fy_end"].tolist(),
        })

    # Compute summary stats per scenario
    summary = []
    for r in rows:
        rets = np.array(r["mean_ret_yearly"])
        if len(rets) < 2:
            continue
        cagr = float(np.prod(1 + rets) ** (1 / len(rets)) - 1)
        sharpe = float(rets.mean() / rets.std(ddof=0)) if rets.std() > 0 else 0
        # Max drawdown of compound equity curve
        eq = np.cumprod(1 + rets)
        peak = np.maximum.accumulate(eq)
        mdd = float(((eq / peak) - 1).min())
        summary.append({
            "scenario": r["scenario"],
            "n_avg": round(r["n_avg"], 1),
            "n_years": len(rets),
            "yearly_mean_ret": rets.mean(),
            "cagr": cagr,
            "sharpe": sharpe,
            "mdd": mdd,
            "yearly_returns": [f"{x*100:+.1f}%" for x in rets],
            "fy_ends_short": [f.replace("-12-31", "") for f in r["fy_ends"]],
        })
    return pd.DataFrame(summary)


def grade_ic(ic: float, ir: float) -> str:
    if ic > 0.05 and ir > 0.5:
        return "A"
    if ic > 0.03 and ir > 0.3:
        return "A"
    if ic > 0.02:
        return "B"
    if ic > 0.01:
        return "C"
    return "D"


def main():
    df = pd.read_parquet(PANEL_PATH)
    print(f"Loaded panel: {len(df)} rows; FYs: {df['fy_end'].nunique()}; stocks: {df['stock_id'].nunique()}")
    print(df.groupby("fy_end").size())
    print()

    md = []
    md.append("# DCF Base MOS ??IC / Decile / Filter Validation")
    md.append("")
    md.append(f"Panel: `{PANEL_PATH.relative_to(REPO)}` | Rows: {len(df)} | "
              f"FYs: {df['fy_end'].nunique()} | Stocks: {df['stock_id'].nunique()}")
    md.append(f"Sectors covered: {sorted(df['sector_key'].unique().tolist())}")
    md.append("")

    md.append("## Sample by FY")
    md.append("")
    md.append("| FY end | n | base_mos median | base_mos mean | fwd_252d mean |")
    md.append("|---|---|---|---|---|")
    sample = df.groupby("fy_end").agg(
        n=("stock_id", "size"),
        med=("base_mos", "median"),
        mean=("base_mos", "mean"),
        ret=("fwd_252d_ret", "mean"),
    ).reset_index()
    for _, r in sample.iterrows():
        md.append(f"| {r['fy_end']} | {r['n']} | {r['med']:+.1%} | {r['mean']:+.1%} | {r['ret']:+.1%} |")
    md.append("")

    # ============ Phase 1: IC ============
    md.append("## Phase 1: IC (Pearson + Spearman) ??base_mos vs fwd_252d_ret")
    md.append("")
    ic_252 = ic_table(df, "base_mos", "fwd_252d_ret")
    print("IC 252d:")
    print(ic_252.to_string(index=False))
    md.append("| FY end | n | Pearson IC | p | Spearman IC | p | mean fwd ret |")
    md.append("|---|---|---|---|---|---|---|")
    for _, r in ic_252.iterrows():
        md.append(f"| {r['fy_end']} | {r['n']} | "
                  f"{r['pearson_ic']:+.4f} | {r.get('pearson_p', np.nan):.3f} | "
                  f"{r['spearman_ic']:+.4f} | {r.get('spearman_p', np.nan):.3f} | "
                  f"{r['mean_ret']:+.1%} |")
    md.append("")
    overall_row = ic_252[ic_252["fy_end"] == "OVERALL"].iloc[0]
    overall_pe = overall_row["pearson_ic"]
    overall_sp = overall_row["spearman_ic"]
    pe_ir = overall_row.get("pearson_ir", np.nan)
    sp_ir = overall_row.get("spearman_ir", np.nan)
    md.append(f"**Overall**: Pearson IC = {overall_pe:+.4f} (IR={pe_ir:+.2f}) | "
              f"Spearman IC = {overall_sp:+.4f} (IR={sp_ir:+.2f})")
    md.append("")

    # 60d
    md.append("### IC vs fwd_60d (sanity)")
    md.append("")
    ic_60 = ic_table(df, "base_mos", "fwd_60d_ret")
    md.append("| FY end | n | Pearson | Spearman | mean ret |")
    md.append("|---|---|---|---|---|")
    for _, r in ic_60.iterrows():
        md.append(f"| {r['fy_end']} | {r['n']} | {r['pearson_ic']:+.4f} | "
                  f"{r['spearman_ic']:+.4f} | {r['mean_ret']:+.1%} |")
    md.append("")

    grade_b = grade_ic(abs(overall_sp), abs(sp_ir))
    md.append(f"**Grade for ?¹æ? B (composite weight)**: **{grade_b}** "
              f"(based on |Spearman IC|={abs(overall_sp):.4f}, |IR|={abs(sp_ir):.2f})")
    md.append("")

    # ============ Phase 2: Decile spread ============
    md.append("## Phase 2: Decile spread ??base_mos sorted into 10 deciles")
    md.append("")
    md.append("Q1 = lowest MOS (most overvalued); Q10 = highest MOS (deepest discount)")
    md.append("")
    decile = decile_spread(df, "base_mos", "fwd_252d_ret")
    print("Decile spread:")
    print(decile.to_string(index=False))
    md.append("| Decile | n total | avg fwd_252d ret | std across FYs |")
    md.append("|---|---|---|---|")
    for _, r in decile.iterrows():
        md.append(f"| Q{int(r['decile'])} | {int(r['n_total'])} | {r['mean_ret_avg']:+.1%} | "
                  f"{r['mean_ret_std']:+.1%} |")
    md.append("")
    if not decile.empty:
        spread = decile[decile["decile"] == N_DECILES]["mean_ret_avg"].iloc[0] - \
                 decile[decile["decile"] == 1]["mean_ret_avg"].iloc[0]
        mono = monotonicity_spearman(decile)
        md.append(f"**Q10 - Q1 spread**: {spread:+.1%}")
        md.append(f"**Monotonicity (Spearman of decile vs avg ret)**: {mono:+.3f}")
        md.append("")
        if mono > 0.7:
            mono_verdict = "STRONG monotonic (cheap ??high return)"
        elif mono > 0.3:
            mono_verdict = "PARTIAL monotonic"
        elif mono < -0.3:
            mono_verdict = "INVERTED (cheap ??lower return, bias suspicion)"
        else:
            mono_verdict = "NO monotonicity (likely noise or inverted-U shape)"
        md.append(f"**Verdict**: {mono_verdict}")
        md.append("")

    # ============ Phase 3: ?¹æ? A ?æ¿¾?æ¸¬ ============
    md.append("## Phase 3: ?¹æ? A ??Filter baseline panel by Base MOS threshold")
    md.append("")
    md.append("Baseline = equal-weight all top-200 (post fin/utility filter); ")
    md.append("Filtered = subset where base_mos > threshold. Each FY ??252d forward ret, ")
    md.append("compounded across 5 FYs for CAGR / Sharpe / MDD.")
    md.append("")
    filt = filter_backtest(df, "base_mos", "fwd_252d_ret", THRESHOLDS)
    print("\nFilter backtest:")
    print(filt[["scenario", "n_avg", "n_years", "yearly_mean_ret", "cagr", "sharpe", "mdd"]].to_string(index=False))
    md.append("| Scenario | n avg | n years | yearly_mean | CAGR | Sharpe | MDD | yearly_returns |")
    md.append("|---|---|---|---|---|---|---|---|")
    for _, r in filt.iterrows():
        md.append(f"| {r['scenario']} | {r['n_avg']} | {r['n_years']} | "
                  f"{r['yearly_mean_ret']*100:+.1f}% | {r['cagr']*100:+.1f}% | "
                  f"{r['sharpe']:+.2f} | {r['mdd']*100:+.1f}% | "
                  f"{', '.join(r['yearly_returns'])} |")
    md.append("")
    # Verdict for ?¹æ? A
    baseline_sharpe = filt[filt["scenario"] == "baseline_all"]["sharpe"].iloc[0] if not filt.empty else np.nan
    md.append("### ?¹æ? A verdict")
    md.append("")
    md.append(f"Baseline Sharpe: {baseline_sharpe:+.2f}")
    grade_a_per_th = {}
    for _, r in filt.iterrows():
        if r["scenario"] == "baseline_all":
            continue
        improvement = r["sharpe"] - baseline_sharpe
        n_ok = r["n_avg"] >= 30
        if improvement > 0.1 and n_ok:
            grade = "A"
        elif improvement > 0 and n_ok:
            grade = "B"
        elif improvement < -0.1:
            grade = "D"
        else:
            grade = "C"
        grade_a_per_th[r["scenario"]] = grade
        md.append(f"- **{r['scenario']}**: Sharpe={r['sharpe']:+.2f} (vs baseline "
                  f"{baseline_sharpe:+.2f}, ?={improvement:+.2f}), n_avg={r['n_avg']}, **Grade {grade}**")
    md.append("")
    best_a_grade = max(grade_a_per_th.values(), key=lambda x: "ABCD".index(x)) if grade_a_per_th else "D"
    md.append(f"**?¹æ? A best grade**: **{best_a_grade}**")
    md.append("")

    # ============ Bear-year check ============
    md.append("## Bear-year check (2022)")
    md.append("")
    bear = df[df["fy_end"] == "2022-12-31"]
    if not bear.empty:
        ic_2022 = ic_table(bear, "base_mos", "fwd_252d_ret")
        if not ic_2022.empty:
            r22 = ic_2022.iloc[0]
            md.append(f"2022 FY ??entry 2023-04 ??forward = 2023-04 to 2024-04 (post-recovery bull)")
            md.append(f"- Pearson IC: {r22['pearson_ic']:+.4f}, Spearman IC: {r22['spearman_ic']:+.4f}, "
                      f"mean ret: {r22['mean_ret']:+.1%}")
    md.append("")

    # ============ Final verdict + ?½åœ°å»ºè­° ============
    md.append("## Final verdict & ?½åœ°å»ºè­°")
    md.append("")
    md.append(f"- **?¹æ? A (ç¡¬é?æ¿?**: Grade **{best_a_grade}**")
    md.append(f"- **?¹æ? B (composite ? å?)**: Grade **{grade_b}**")
    md.append("")
    md.append("### å»ºè­°")
    md.append("")
    if best_a_grade in ("A", "B"):
        md.append(f"- ?¹æ? A ?¯ä?ç·šï???`scanner_value` / `whale_picks_screener` ?§å? `base_mos > {THRESHOLDS[0]}` filter")
    elif best_a_grade == "C":
        md.append("- ?¹æ? A ?Šé?ï¼šå¯ä½œç‚º informational tier (UI é¡¯ç¤º?¨ï?ä¸é€?selection)")
    else:
        md.append("- ?¹æ? A ä¸ä?ç·šï?filter å¾?Sharpe æ²’æ??‡æ?æ¨?œ¬å¤ªå? ??æ­¸æ?")
    md.append("")
    if grade_b in ("A", "B"):
        weight_pct = 15 if grade_b == "A" else 8
        md.append(f"- ?¹æ? B ?¯å??†ï?composite_score ??{weight_pct} ?†æ???(rank-normalize base_mos)")
    elif grade_b == "C":
        md.append("- ?¹æ? B ?Šé?ï¼šIC å¼±ï?å»ºè­°??F-Score ??ROIC çµ„å?æ¸?combo IC ?æ±ºå®?)
    else:
        md.append(f"- ?¹æ? B ä¸ä?ç·šï?IC å¾®å¼± (|Spearman|={abs(overall_sp):.4f})ï¼?
                  "ç¬¦å??¢æ? value factor ??D ç´šç?æ­·å²çµè?ï¼Œæ­¸æª”ä?ä¸Šç?")
    md.append("")
    md.append("---")
    md.append(f"_Generated by `tools/dcf_ic_analyze.py` from `{PANEL_PATH.name}`._")
    md.append("")

    OUT_MD.write_text("\n".join(md), encoding="utf-8")
    print(f"\nReport written: {OUT_MD}")


if __name__ == "__main__":
    main()
