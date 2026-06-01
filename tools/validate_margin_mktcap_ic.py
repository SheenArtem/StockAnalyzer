"""
validate_margin_mktcap_ic.py -- 融資餘額佔市值比 IC validation vs ^TWII

驗證 build_market_cap_panel.py 產的兩個 feature 是否真有崩盤/頂部預測力:
  - margin_to_mktcap_pct   (官方融資金額 / 上市總市值 x100, 絕對 level)
  - margin_mktcap_z_252d   (上述比值的 252d z-score, 去趨勢版)

對齊 validate_vol_complex_ic.py 的 SOP-12 + SOP-14 流程，但:
  - TW 本土訊號 (融資餘額為當日盤後 TWSE 官方數)，feature@D vs 自 D+1 起的 fwd MDD，
    無 US->TW T+1 shift。
  - 慢結構訊號改用 20/60/120 交易日 horizon (~1mo/3mo/6mo)，對應「頂部帶」用途。
  - 額外加 fwd-return IC (高槓桿應預測較低未來報酬) 與重大回檔事件研究。

Outcome: ^TWII 自 D+1 起 fwd 20/60/120d 最大回檔 (close-to-min, 負值)。

Verdict (SOP-12 三 gate):
  A. |IC| >= 0.10 + p < 0.05 across 20/60/120d
  B. Decile spread 方向一致 (危險訊號預期: 高 feature -> 更負 MDD -> spread < 0)
  C. |Q10-Q1 median spread| >= 2pp
  3 gate pass -> PASS / 2 -> MARGINAL / <2 -> FAIL
  (FAIL 但危險帶 conditional lift >= 2.0x -> 升 SOP-14 informational)

Usage:
  python tools/validate_margin_mktcap_ic.py
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

MC_PATH = REPO / "data" / "macro" / "market_cap.parquet"
OUT_MD = REPO / "reports" / "margin_mktcap_ic_validation.md"
OUT_CSV = REPO / "reports" / "margin_mktcap_ic_validation.csv"

HORIZONS = [20, 60, 120]

# feature -> conditional-lift thresholds (危險帶值 + 鄰近檔位)
FEATURES = {
    'margin_to_mktcap_pct': [0.43, 0.48, 0.53],   # 0.48 = build 的當代頂部帶下緣
    'margin_mktcap_z_252d': [1.5, 2.0, 2.5],       # 2.5 = build 的自適應急升門檻
}

# 2016-2026 重大 TWII 回檔 (頂部日期, 約略). 測 build「各大頂前 0.43-0.53%」之說。
KNOWN_TOPS = {
    "2018 Q4 selloff":      pd.Timestamp("2018-10-01"),
    "COVID 2020":           pd.Timestamp("2020-01-14"),
    "2022 bear":            pd.Timestamp("2022-01-05"),
    "2024-08 yen carry":    pd.Timestamp("2024-07-11"),
    "2025 tariff":          pd.Timestamp("2025-03-18"),
}


def load_twii() -> pd.Series:
    import yfinance as yf
    df = yf.Ticker('^TWII').history(start='2015-01-01', auto_adjust=False)
    df.index = pd.to_datetime(df.index.date)
    return df['Close'].sort_index().astype(float)


def compute_fwd_mdd(close: pd.Series, horizon: int) -> pd.Series:
    """feature@D vs 自 D+1 起 horizon 交易日內最大回檔 (負值 %)。"""
    arr = close.values
    n = len(arr)
    out = np.full(n, np.nan)
    for i in range(n - horizon):
        seg = arr[i + 1: i + horizon + 1]
        if len(seg) == 0:
            continue
        out[i] = (seg.min() - arr[i]) / arr[i] * 100
    return pd.Series(out, index=close.index)


def compute_fwd_ret(close: pd.Series, horizon: int) -> pd.Series:
    """feature@D vs D+horizon 收盤報酬 (%)。次要視角: 高槓桿是否預測低報酬。"""
    return (close.shift(-horizon) / close - 1.0) * 100


def build_aligned_panel() -> pd.DataFrame:
    mc = pd.read_parquet(MC_PATH)
    mc['date'] = pd.to_datetime(mc['date'])
    mc = mc.set_index('date').sort_index()
    mc = mc.dropna(subset=['margin_to_mktcap_pct'])

    twii = load_twii()
    twii.index = pd.to_datetime(twii.index)

    out = mc[['margin_to_mktcap_pct', 'margin_mktcap_z_252d']].copy()
    out['twii_close'] = twii.reindex(out.index).ffill()
    for h in HORIZONS:
        out[f'fwd_{h}d_mdd'] = compute_fwd_mdd(twii, h).reindex(out.index)
        out[f'fwd_{h}d_ret'] = compute_fwd_ret(twii, h).reindex(out.index)
    return out


def spearman_ic(feat: pd.Series, outcome: pd.Series):
    df = pd.concat([feat, outcome], axis=1).dropna()
    if len(df) < 30:
        return np.nan, np.nan, len(df)
    rho, p = stats.spearmanr(df.iloc[:, 0], df.iloc[:, 1])
    return rho, p, len(df)


def decile_spread(feat: pd.Series, outcome: pd.Series):
    df = pd.concat([feat, outcome], axis=1).dropna()
    df.columns = ['f', 'o']
    if len(df) < 100:
        return {}
    df['dec'] = pd.qcut(df['f'].rank(method='first'), 10, labels=False) + 1
    medians = df.groupby('dec')['o'].median()
    return {
        'q1_median': medians.iloc[0],
        'q10_median': medians.iloc[-1],
        'spread_med': medians.iloc[-1] - medians.iloc[0],
    }


def evaluate_feature(panel: pd.DataFrame, col: str) -> dict:
    f = panel[col]
    out = {'feature': col, 'horizons': {}}
    for h in HORIZONS:
        mdd = panel[f'fwd_{h}d_mdd']
        ret = panel[f'fwd_{h}d_ret']
        rho, p, n = spearman_ic(f, mdd)
        ret_rho, ret_p, _ = spearman_ic(f, ret)
        spread = decile_spread(f, mdd)
        out['horizons'][h] = {'ic': rho, 'pvalue': p, 'n': n,
                              'ret_ic': ret_rho, 'ret_p': ret_p, **spread}
    return out


def conditional_lift(panel: pd.DataFrame, col: str, thresholds: list) -> list[dict]:
    base_n = len(panel)
    base_hit = (panel['fwd_60d_mdd'] <= -10).mean() * 100
    rows = [{
        'cond': 'baseline', 'n': base_n, 'pct': 100.0,
        'fwd20_med': panel['fwd_20d_mdd'].median(),
        'fwd60_med': panel['fwd_60d_mdd'].median(),
        'fwd120_med': panel['fwd_120d_mdd'].median(),
        'hit60_neg10pct': base_hit, 'lift': 1.0,
    }]
    for t in thresholds:
        sub = panel[panel[col] >= float(t)]
        n = len(sub)
        if n < 5:
            continue
        hit = (sub['fwd_60d_mdd'] <= -10).mean() * 100
        rows.append({
            'cond': f'{col} >= {t}', 'n': n, 'pct': n / base_n * 100,
            'fwd20_med': sub['fwd_20d_mdd'].median(),
            'fwd60_med': sub['fwd_60d_mdd'].median(),
            'fwd120_med': sub['fwd_120d_mdd'].median(),
            'hit60_neg10pct': hit,
            'lift': hit / base_hit if base_hit > 0 else 0,
        })
    return rows


def event_study(panel: pd.DataFrame) -> list[dict]:
    rows = []
    for label, top_d in KNOWN_TOPS.items():
        if top_d < panel.index.min() or top_d > panel.index.max():
            continue
        idx = panel.index.searchsorted(top_d)
        if idx >= len(panel):
            continue
        d = panel.index[idx]
        # 頂部前 120 交易日窗口內，feature 達到的最高值 + 危險帶是否觸發
        lb = max(0, idx - 120)
        win = panel.iloc[lb: idx + 1]
        max_pct = win['margin_to_mktcap_pct'].max()
        max_z = win['margin_mktcap_z_252d'].max()
        fired_abs = (win['margin_to_mktcap_pct'] >= 0.48).any()
        fired_z = (win['margin_mktcap_z_252d'] > 2.5).any()
        rows.append({
            'top': label,
            'date': d.strftime('%Y-%m-%d'),
            'pct_at_top': panel.loc[d, 'margin_to_mktcap_pct'],
            'z_at_top': panel.loc[d, 'margin_mktcap_z_252d'],
            'max_pct_120d_before': max_pct,
            'max_z_120d_before': max_z,
            'fired_abs_0.48': fired_abs,
            'fired_z_2.5': fired_z,
        })
    return rows


def sop12_verdict(eval_result: dict) -> tuple[str, list[str]]:
    notes = []
    gate_a = gate_b = gate_c = True
    for h in HORIZONS:
        r = eval_result['horizons'][h]
        ic, p = r['ic'], r['pvalue']
        if pd.isna(ic) or abs(ic) < 0.10 or p > 0.05:
            gate_a = False
            notes.append(f"Gate A FAIL @ {h}d: |IC|={abs(ic):.3f} p={p:.4f}")
    spreads = [eval_result['horizons'][h].get('spread_med', 0) for h in HORIZONS]
    if not (all(s < 0 for s in spreads) or all(s > 0 for s in spreads)):
        gate_b = False
        notes.append(f"Gate B FAIL: spread signs 不一致 {[f'{s:+.2f}' for s in spreads]}")
    for h in HORIZONS:
        spread = eval_result['horizons'][h].get('spread_med')
        if spread is None or pd.isna(spread) or abs(spread) < 2.0:
            gate_c = False
            notes.append(f"Gate C FAIL @ {h}d: |spread|={abs(spread or 0):.2f}pp")
    n_pass = int(gate_a) + int(gate_b) + int(gate_c)
    if n_pass == 3:
        return 'PASS', notes
    if n_pass == 2:
        return 'MARGINAL', notes
    return 'FAIL', notes


def main():
    panel = build_aligned_panel()
    print(f"Aligned panel: {len(panel)} rows {panel.index.min().date()} ~ {panel.index.max().date()}")

    results, verdicts, conds = {}, {}, {}
    for col, thresholds in FEATURES.items():
        results[col] = evaluate_feature(panel, col)
        v, notes = sop12_verdict(results[col])
        cond = conditional_lift(panel, col, thresholds)
        # high-threshold lift 升級 (取該 feature 最高門檻)
        top_t = thresholds[-1]
        red = next((r for r in cond if r['cond'] == f"{col} >= {top_t}"), None)
        if v == 'FAIL' and red and red['lift'] >= 2.0:
            v = 'MARGINAL (informational)'
            notes.append(f"UPGRADE: 危險帶 lift={red['lift']:.2f}x >= 2.0x -> SOP-14 tier")
        verdicts[col] = (v, notes)
        conds[col] = cond

    events = event_study(panel)

    # ---------- print ----------
    print("\n=== Univariate IC (vs fwd MDD) ===")
    for col in FEATURES:
        for h in HORIZONS:
            r = results[col]['horizons'][h]
            print(f"  {col:22s} fwd{h:>3}d  IC_mdd={r['ic']:+.3f} p={r['pvalue']:.4f} "
                  f"spread={r.get('spread_med', float('nan')):+.2f}pp "
                  f"IC_ret={r['ret_ic']:+.3f} n={r['n']}")
        print(f"  -> Verdict {col:22s}: {verdicts[col][0]}\n")

    print("=== Conditional lift (hit fwd60 <= -10%) ===")
    for col in FEATURES:
        print(f"  [{col}]")
        for r in conds[col]:
            print(f"    {r['cond']:32s} n={r['n']:5d} ({r['pct']:5.1f}%) "
                  f"fwd60_med={r['fwd60_med']:+6.2f}% hit={r['hit60_neg10pct']:5.1f}% lift={r['lift']:.2f}x")

    print("\n=== Event study (重大 TWII 頂部) ===")
    for e in events:
        print(f"  {e['top']:20s} {e['date']}  pct@top={e['pct_at_top']:.3f}% z@top={e['z_at_top']:+.2f} "
              f"max_pct_120d={e['max_pct_120d_before']:.3f}% fired(0.48)={e['fired_abs_0.48']} fired(z2.5)={e['fired_z_2.5']}")

    write_report(panel, results, verdicts, conds, events)
    write_csv(results, verdicts)


def write_report(panel, results, verdicts, conds, events):
    today = datetime.now().strftime('%Y-%m-%d')
    md = [
        "# 融資餘額佔市值比 IC Validation vs ^TWII",
        "",
        f"Date: {today}  Panel: {panel.index.min().date()} ~ {panel.index.max().date()} ({len(panel)} rows)",
        "Outcome: ^TWII 自 D+1 起 fwd 20/60/120d 最大回檔 (close-to-min, 負值)",
        "Feature@D 為當日盤後 TWSE 官方融資餘額 / 上市總市值，無 T+1 shift。",
        "",
        "## Verdict 摘要 (SOP-12 3-gate)",
        "",
        "| Feature | Verdict | Best |IC_mdd| |",
        "|---|---|---|",
    ]
    for col in FEATURES:
        best_ic = max((abs(results[col]['horizons'][h]['ic']) for h in HORIZONS
                       if not pd.isna(results[col]['horizons'][h]['ic'])), default=0)
        md.append(f"| `{col}` | {verdicts[col][0]} | {best_ic:.3f} |")

    md += ["", "## Univariate IC (Spearman)", "",
           "| feature | horizon | n | IC vs MDD | p | IC vs ret | Q1 med MDD | Q10 med MDD | Spread (pp) |",
           "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for col in FEATURES:
        for h in HORIZONS:
            r = results[col]['horizons'][h]
            md.append(f"| {col} | {h}d | {r['n']} | {r['ic']:+.3f} | {r['pvalue']:.4f} | "
                      f"{r['ret_ic']:+.3f} | {r.get('q1_median', float('nan')):+.2f}% | "
                      f"{r.get('q10_median', float('nan')):+.2f}% | {r.get('spread_med', float('nan')):+.2f} |")

    md += ["", "## Conditional lift (危險帶門檻; hit = fwd60d MDD <= -10%)", ""]
    for col in FEATURES:
        md.append(f"### `{col}`")
        md += ["", "| Condition | n | % days | fwd20 med | fwd60 med | fwd120 med | hit fwd60<=-10% | lift |",
               "|---|---:|---:|---:|---:|---:|---:|---:|"]
        for r in conds[col]:
            md.append(f"| {r['cond']} | {r['n']} | {r['pct']:.1f}% | {r['fwd20_med']:+.2f}% | "
                      f"{r['fwd60_med']:+.2f}% | {r['fwd120_med']:+.2f}% | {r['hit60_neg10pct']:.1f}% | {r['lift']:.2f}x |")
        notes = verdicts[col][1]
        if notes:
            md += ["", "Gate / upgrade notes:"] + [f"- {n}" for n in notes]
        md.append("")

    md += ["## Event study: 重大 TWII 頂部前 feature 行為", "",
           "測 build_systemic_chip_panel.py 之說「各大頂/崩盤前 0.43-0.53%, >=0.48 為當代頂部帶下緣」。", "",
           "| Top | Date | pct@top | z@top | max pct (前120d) | max z (前120d) | 觸 0.48 | 觸 z2.5 |",
           "|---|---|---:|---:|---:|---:|---|---|"]
    for e in events:
        md.append(f"| {e['top']} | {e['date']} | {e['pct_at_top']:.3f}% | {e['z_at_top']:+.2f} | "
                  f"{e['max_pct_120d_before']:.3f}% | {e['max_z_120d_before']:+.2f} | "
                  f"{'YES' if e['fired_abs_0.48'] else 'no'} | {'YES' if e['fired_z_2.5'] else 'no'} |")

    md += ["", "## 結論與建議", ""]
    pass_any = any(verdicts[col][0] in ('PASS', 'MARGINAL', 'MARGINAL (informational)') for col in FEATURES)
    if not pass_any:
        md += ["**兩 feature 皆 FAIL SOP-12。** 融資佔市值比在 ^TWII 上無顯著 fwd-MDD 預測力。",
               "維持 systemic_chip Group B informational tile，**不接 composite / rebalance gate** (SOP-14)。"]
    else:
        md += ["部分 feature 達 MARGINAL/PASS，見上表；仍維持 SOP-14 informational tier。"]
    md += ["",
           "**Caveats (SOP 1-14)**:",
           "- 重疊窗口: fwd 120d MDD 相鄰日共用 119 天 -> 有效樣本遠小於名目 n, p-value 偏樂觀。",
           "- 結構性下降: `margin_to_mktcap_pct` 絕對 level 2016 ~0.62% -> 2025 ~0.35%，"
           "絕對門檻 IC/lift 受 regime 主導 (2016-18 幾乎恆 >=0.48); z_252d 為去趨勢版，較可信。",
           "- 絕對門檻校準漂移: 0.48 校準於 2024-26，但 2025/26 比值上限僅 0.460/0.385，"
           "危險帶近年幾乎不觸發 -> 該門檻已實質失效，需每 1-2 年 review。"]

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(md), encoding='utf-8')
    print(f"\n[OK] Report -> {OUT_MD}")


def write_csv(results, verdicts):
    rows = []
    for col in FEATURES:
        for h in HORIZONS:
            r = results[col]['horizons'][h]
            rows.append({
                'feature': col, 'horizon_d': h, 'n': r['n'],
                'ic_mdd': r['ic'], 'pvalue': r['pvalue'],
                'ic_ret': r['ret_ic'],
                'q1_median': r.get('q1_median'), 'q10_median': r.get('q10_median'),
                'spread_med': r.get('spread_med'), 'verdict': verdicts[col][0],
            })
    pd.DataFrame(rows).to_csv(OUT_CSV, index=False)
    print(f"[OK] CSV -> {OUT_CSV}")


if __name__ == '__main__':
    main()
