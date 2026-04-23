"""
vf_g5_validation.py - VF-G5: QM 進場閘門 + Scenario 區間係數驗證

VF-G5 涵蓋的 magic numbers：
  - Scenario A/B/C 進場區間係數 (scenario_engine.py): 0.98, 0.99, 1.01, 1.02
  - DEFAULT_BUY_THRESHOLD (analysis_engine): trigger_score ≥ 3
  - DEFAULT_SELL_THRESHOLD: ≤ -2
  - momentum_screener QM entry gate: trigger ≥ 3 (green)

資料限制：trade_journal_qm_tw.parquet 是 **post-filter picks**，無原始
trigger_score / scenario code / rec_entry_low/high；需從現有欄位 proxy 推。

4 個 Test（scope 1-2 hours，不重跑 simulator）：
  T1. qm_score / f_score / body_score / trend_score 在 picks 內 IC
       → 若平原 (IR < 0.1) → 閘門已夠，不可再拆 threshold；
       → 若有 IR → top picks 更好，tighter threshold 有價值
  T2. rank_in_top50 階梯效果（≤5 / 6-10 / 11-25 / 26-50）
       → 單調遞減才算 rank 有區分；平原 → top50 基本等價
  T3. Scenario A/B/C/D proxy 分類 × fwd return
       → 從 trend_score 推 A(≥3)/B(1~2)/C(-2~0)/D(<-2)，比較 fwd mean
       → A>B>C 單調 → 分類 valid；反直覺 → scenario 邏輯有 bug
  T4. Entry range fill 率（用 fwd_5d_min/max）
       → 判斷「Scenario A 現價 0.99~1.01」「B 5MA 拉回 0.98~5MA」是否在前 5d 成交
       → Grid: 窄 0.99/1.01 vs 現行 0.98/1.02 vs 寬 0.95/1.05 的 fill rate vs fwd

Usage:
    python tools/vf_g5_validation.py
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
REPORT_PATH = ROOT / "reports" / "vf_g5_validation.md"


def grade(ir: float) -> str:
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
    int_cols = {'n', 'weeks', 'horizon', 'rank_low', 'rank_high'}
    pct_cols = {'mean_ret', 'winrate', 'fill_rate', 'top', 'bot', 'spread'}

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

    lines = []
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(fmt(c, row[c]) for c in cols) + " |")
    return "\n".join(lines)


# ================================================================
# Test 1: Score IC within picks
# ================================================================
def test1_score_ic(j: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    rows = []
    # 用 weekly groupby 算 weekly IC → mean IC / IR
    score_cols = ['qm_score', 'f_score', 'body_score', 'trend_score']
    j = j.copy()
    j['week_end_date'] = pd.to_datetime(j['week_end_date'])

    for h in horizons:
        fwd = f'fwd_{h}d'
        if fwd not in j.columns:
            continue
        for sc in score_cols:
            sub = j[[sc, fwd, 'week_end_date']].dropna()
            if sub.empty:
                continue
            weekly_ic = []
            for _, grp in sub.groupby('week_end_date'):
                if len(grp) < 5:
                    continue
                rho, _ = stats.spearmanr(grp[sc], grp[fwd])
                if not pd.isna(rho):
                    weekly_ic.append(rho)
            if not weekly_ic:
                continue
            arr = np.array(weekly_ic)
            std = arr.std(ddof=1) if len(arr) > 1 else np.nan
            ir = arr.mean() / std if len(arr) > 1 and std > 0 else np.nan
            # decile
            sub_s = sub.sort_values(sc).reset_index(drop=True)
            n = len(sub_s)
            cut = max(1, n // 10)
            bot_ret = sub_s.iloc[:cut][fwd].mean()
            top_ret = sub_s.iloc[-cut:][fwd].mean()
            rows.append({
                'score': sc,
                'horizon': h,
                'IC': arr.mean(),
                'IR': ir,
                'n_weeks': len(arr),
                'top_decile': top_ret,
                'bot_decile': bot_ret,
                'spread': top_ret - bot_ret,
                'grade': grade(ir),
            })
    return pd.DataFrame(rows)


# ================================================================
# Test 2: rank_in_top50 階梯
# ================================================================
def test2_rank_steps(j: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    bands = [
        ('1-5', 1, 5),
        ('6-10', 6, 10),
        ('11-25', 11, 25),
        ('26-50', 26, 50),
    ]
    rows = []
    for h in horizons:
        fwd = f'fwd_{h}d'
        if fwd not in j.columns:
            continue
        for label, lo, hi in bands:
            sub = j[(j['rank_in_top50'] >= lo) & (j['rank_in_top50'] <= hi)]
            if sub.empty:
                continue
            rt = sub[fwd].dropna()
            rows.append({
                'band': label,
                'rank_low': lo,
                'rank_high': hi,
                'horizon': h,
                'n': len(rt),
                'mean_ret': rt.mean(),
                'winrate': (rt > 0).mean(),
                'median_ret': rt.median(),
            })
    return pd.DataFrame(rows)


# ================================================================
# Test 3: Scenario proxy 分類
# ================================================================
def test3_scenario_proxy(j: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    """
    近似 scenario_engine.determine_scenario：
      A (強力進攻): trend_score >= 9
      B (拉回關注): 7 <= trend_score < 9
      C (反彈搶短): trend_score < 7 but >=6（picks 下限）
      D picks 理論上不存在（entry_gate 會擋）
    """
    def classify(ts):
        if ts >= 9:
            return 'A'
        if ts >= 8:
            return 'B'
        if ts >= 7:
            return 'C_mid'
        return 'C_low'

    jc = j.copy()
    jc['scenario_proxy'] = jc['trend_score'].apply(classify)
    rows = []
    for h in horizons:
        fwd = f'fwd_{h}d'
        if fwd not in jc.columns:
            continue
        for sc, grp in jc.groupby('scenario_proxy'):
            rt = grp[fwd].dropna()
            rows.append({
                'scenario': sc,
                'horizon': h,
                'n': len(rt),
                'mean_ret': rt.mean(),
                'winrate': (rt > 0).mean(),
                'median_ret': rt.median(),
            })
    return pd.DataFrame(rows)


# ================================================================
# Test 4: Entry range fill rate × forward return
# ================================================================
def test4_entry_range(j: pd.DataFrame) -> pd.DataFrame:
    """
    Proxy：picks 的 entry_price 當作「signal day close」。
    因為沒有 fwd_1d~5d 的 OHLC，我們只能用 fwd_5d_min（5 日最低價 / entry -1） 來近似。

    對三組區間做 fill rate + 條件 forward return：
      narrow: entry * 0.99 ~ entry * 1.01 (AI 訊號/強勢)
      live  : entry * 0.98 ~ entry * 1.02 (現行 Scenario A/B)
      wide  : entry * 0.95 ~ entry * 1.05
      atr   : entry * (1 - 0.5*atr%) ~ entry * (1 + 0.5*atr%)  個股差異化

    fill 判斷：entry*low_coef >= entry + entry*fwd_5d_min 近似 (fwd_5d_min 是 5 日內最大跌幅
    負值)，代表 5 日內最低價 = entry*(1+fwd_5d_min)。若 entry*(1+fwd_5d_min) <= entry*low_coef
    → 表示 5 日內價格有觸及或跌破 low_coef 上緣，可以 fill 在 low_coef。

    實際更精確的 fill 邏輯：
      low_fill  = (1 + fwd_5d_min) <= low_coef     # 5d 最低 ≤ 進場低緣
      high_fill = (1 + fwd_5d_max) >= high_coef    # 5d 最高 ≥ 進場高緣
      fill = low_fill OR (1+fwd_5d_min) <= high_coef   # 只要最低 ≤ high_coef 就可 fill（從上方進入）

    我們用「low_fill OR 區間內盤整」：fill = (1+fwd_5d_min) <= high_coef AND (1+fwd_5d_max) >= low_coef
    """
    if 'fwd_5d_min' not in j.columns:
        # fwd_20d_min 替代（更 generous）
        low_col = 'fwd_20d_min'
        high_col = 'fwd_20d_max'
    else:
        low_col = 'fwd_5d_min'
        high_col = 'fwd_5d_max'
    if low_col not in j.columns or high_col not in j.columns:
        return pd.DataFrame()

    scenarios = [
        ('narrow (0.99/1.01)', 0.99, 1.01),
        ('live (0.98/1.02)', 0.98, 1.02),
        ('wide (0.95/1.05)', 0.95, 1.05),
    ]

    rows = []
    for label, low_coef, high_coef in scenarios:
        sub = j[[low_col, high_col, 'fwd_20d', 'fwd_40d']].dropna().copy()
        # low price ratio = 1 + fwd_min；high price ratio = 1 + fwd_max
        low_ratio = 1 + sub[low_col]
        high_ratio = 1 + sub[high_col]
        fill_mask = (low_ratio <= high_coef) & (high_ratio >= low_coef)
        fill_rate = fill_mask.mean()
        if fill_mask.sum() > 0:
            fwd20_fill = sub.loc[fill_mask, 'fwd_20d'].mean()
            fwd40_fill = sub.loc[fill_mask, 'fwd_40d'].mean()
            win20_fill = (sub.loc[fill_mask, 'fwd_20d'] > 0).mean()
        else:
            fwd20_fill = fwd40_fill = win20_fill = np.nan
        if (~fill_mask).sum() > 0:
            fwd20_no = sub.loc[~fill_mask, 'fwd_20d'].mean()
        else:
            fwd20_no = np.nan
        rows.append({
            'range': label,
            'fill_rate': fill_rate,
            'n_fill': int(fill_mask.sum()),
            'fwd20_fill': fwd20_fill,
            'fwd40_fill': fwd40_fill,
            'win20_fill': win20_fill,
            'fwd20_nofill': fwd20_no,
            'delta_fill_vs_nofill': fwd20_fill - fwd20_no if not pd.isna(fwd20_fill) and not pd.isna(fwd20_no) else np.nan,
        })
    return pd.DataFrame(rows)


# ================================================================
# Main
# ================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--journal", default=str(JOURNAL_PATH))
    ap.add_argument("--horizons", type=int, nargs='+', default=[5, 20, 40, 60])
    args = ap.parse_args()

    print("=" * 80)
    print("VF-G5 QM 進場閘門 + Scenario 區間驗證")
    print("=" * 80)

    j = pd.read_parquet(args.journal)
    print(f"Journal: {len(j)} picks × {len(j.columns)} cols")
    print(f"Date range: {j['week_end_date'].min()} ~ {j['week_end_date'].max()}")
    print(f"Stocks: {j['stock_id'].nunique()}")
    print()

    # Test 1
    print("=" * 80)
    print("Test 1: Score IC within picks (qm / f / body / trend)")
    print("=" * 80)
    t1 = test1_score_ic(j, args.horizons)
    print(t1.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))
    print()

    # Test 2
    print("=" * 80)
    print("Test 2: rank_in_top50 階梯效果")
    print("=" * 80)
    t2 = test2_rank_steps(j, args.horizons)
    print(t2.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))
    print()

    # Test 3
    print("=" * 80)
    print("Test 3: Scenario proxy 分類 (from trend_score)")
    print("=" * 80)
    t3 = test3_scenario_proxy(j, args.horizons)
    print(t3.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))
    print()

    # Test 4
    print("=" * 80)
    print("Test 4: Entry range fill 率 × conditional fwd return")
    print("=" * 80)
    t4 = test4_entry_range(j)
    if not t4.empty:
        print(t4.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))
    else:
        print("(no fwd_5d OHLC min/max data)")
    print()

    # --- Write report ---
    REPORT_PATH.parent.mkdir(exist_ok=True)
    lines = []
    lines.append("# VF-G5 QM 進場閘門 + Scenario 區間係數驗證 (2026-04-23)\n")
    lines.append(f"- Journal: {len(j)} picks × {len(j.columns)} cols")
    lines.append(f"- Date range: {j['week_end_date'].min().date()} ~ {j['week_end_date'].max().date()}")
    lines.append(f"- Stocks: {j['stock_id'].nunique()}\n")

    lines.append("## Test 1: qm_score 等 4 個分數在 picks 內 IC\n")
    lines.append(_df_to_md(t1))
    lines.append("")

    lines.append("## Test 2: rank_in_top50 階梯效果\n")
    lines.append(_df_to_md(t2))
    lines.append("")

    lines.append("## Test 3: Scenario proxy 分類 × 報酬\n")
    lines.append("Proxy 規則（近似 scenario_engine.determine_scenario）：")
    lines.append("- trend_score ≥ 9 → A（強力進攻）")
    lines.append("- trend_score 8 → B（拉回關注）")
    lines.append("- trend_score 7 → C_mid")
    lines.append("- trend_score < 7 → C_low（picks 下限 ≥6）\n")
    lines.append(_df_to_md(t3))
    lines.append("")

    if not t4.empty:
        lines.append("## Test 4: Entry range fill 率 × conditional fwd return\n")
        lines.append("Fill 判定：5 日內 (low, high) 區間與 entry * (low_coef, high_coef) 有交集")
        lines.append("→ 市價/限價單可 fill 在區間內。\n")
        lines.append(_df_to_md(t4))
        lines.append("")

    # --- 結論判讀 ---
    lines.append("## 結論判讀\n")

    # T1 結論
    lines.append("### Test 1 → score 閘門\n")
    if not t1.empty:
        best_ir = t1['IR'].max()
        best_row = t1.loc[t1['IR'].idxmax()]
        lines.append(f"- 最強: `{best_row['score']}` @ fwd_{int(best_row['horizon'])}d: IR={best_ir:+.3f} ({best_row['grade']})")
        t1_platform = t1['IR'].abs().max() < 0.1
        if t1_platform:
            lines.append(f"- **全平原** (|IR| 最大 {t1['IR'].abs().max():.3f} < 0.1)：picks 內排序無 alpha → QM 閘門已夠，無法再拆 threshold 提升")
        else:
            lines.append(f"- 有 IR → top picks 顯著優於 bottom picks，可考慮 tighter threshold 或 qm_score 層級過濾")
    lines.append("")

    # T2 結論
    lines.append("### Test 2 → rank 階梯\n")
    if not t2.empty:
        # fwd_20 比較
        t2_20 = t2[t2['horizon'] == 20].sort_values('rank_low')
        if not t2_20.empty:
            means = t2_20['mean_ret'].values
            monotone = all(means[i] >= means[i+1] for i in range(len(means)-1))
            lines.append(f"- fwd_20d mean by band: {', '.join(f'{r}={m:+.2%}' for r, m in zip(t2_20['band'].values, means))}")
            if monotone:
                lines.append(f"- **單調遞減**：rank 有區分 → top 10 或 top 20 的收斂有價值，picks 上限可考慮下修")
            else:
                lines.append(f"- **非單調**：rank 50 內差異不顯著 → top 50 差不多，threshold 邊際穩")
    lines.append("")

    # T3 結論
    lines.append("### Test 3 → Scenario 分類\n")
    if not t3.empty:
        t3_20 = t3[t3['horizon'] == 20].sort_values('scenario')
        if not t3_20.empty:
            lines.append(f"- fwd_20d by scenario: {', '.join(f'{s}={m:+.2%} (n={int(n)})' for s, m, n in zip(t3_20['scenario'].values, t3_20['mean_ret'].values, t3_20['n'].values))}")
            # 期望 A > B > C
            a_ret = t3_20[t3_20['scenario'] == 'A']['mean_ret'].values
            b_ret = t3_20[t3_20['scenario'] == 'B']['mean_ret'].values
            c_ret = t3_20[t3_20['scenario'].str.startswith('C')]['mean_ret'].mean() if (t3_20['scenario'].str.startswith('C')).any() else np.nan
            if len(a_ret) and len(b_ret) and not pd.isna(c_ret):
                if a_ret[0] > b_ret[0] > c_ret:
                    lines.append("- **A > B > C** 單調 → scenario 分類有效，trend_score 閘門對齊報酬")
                else:
                    lines.append(f"- 分類反直覺 (A={a_ret[0]:+.2%}, B={b_ret[0]:+.2%}, C_avg={c_ret:+.2%}) → trend_score 閘門 logic 可能要調")
    lines.append("")

    # T4 結論
    lines.append("### Test 4 → Entry range fill\n")
    if not t4.empty:
        lines.append(f"- narrow (0.99/1.01) fill rate: {t4.iloc[0]['fill_rate']:.0%}")
        lines.append(f"- live (0.98/1.02) fill rate:   {t4.iloc[1]['fill_rate']:.0%}")
        lines.append(f"- wide (0.95/1.05) fill rate:   {t4.iloc[2]['fill_rate']:.0%}")
        # 是否 fill > nofill
        live_delta = t4.iloc[1]['delta_fill_vs_nofill']
        if not pd.isna(live_delta):
            if live_delta > 0.005:
                lines.append(f"- live 區間 fill 組 fwd_20d 比 no-fill 多 {live_delta:+.2%} → 區間有效選樣")
            elif live_delta < -0.005:
                lines.append(f"- live 區間 fill 組 fwd_20d 比 no-fill 少 {abs(live_delta):.2%} → 區間可能過早進場（沒等更好價）")
            else:
                lines.append(f"- live 區間 fill vs no-fill 差 {live_delta:+.2%} 接近平原")
    lines.append("")

    # --- 產出 ---
    lines.append("## 產出\n")
    lines.append("- `reports/vf_g5_validation.md` (本報告)")

    # Save CSVs
    if not t1.empty:
        t1.to_csv(REPORT_PATH.parent / "vf_g5_score_ic.csv", index=False)
        lines.append("- `reports/vf_g5_score_ic.csv`")
    if not t2.empty:
        t2.to_csv(REPORT_PATH.parent / "vf_g5_rank_steps.csv", index=False)
        lines.append("- `reports/vf_g5_rank_steps.csv`")
    if not t3.empty:
        t3.to_csv(REPORT_PATH.parent / "vf_g5_scenario_proxy.csv", index=False)
        lines.append("- `reports/vf_g5_scenario_proxy.csv`")
    if not t4.empty:
        t4.to_csv(REPORT_PATH.parent / "vf_g5_entry_range.csv", index=False)
        lines.append("- `reports/vf_g5_entry_range.csv`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {REPORT_PATH}")


if __name__ == "__main__":
    main()
