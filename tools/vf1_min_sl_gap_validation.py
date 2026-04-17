"""
VF-1 MIN_SL_GAP 動態化驗證

比較兩版 min_sl_gap 公式的假停損率：
  版 A (現行): min_sl_gap = 0.03 固定
  版 B (提案): min_sl_gap = max(0.03, atr_pct * 1.5 / 100)

邏輯 (exit_manager.py:119-123):
  if weekly_ma20 < entry_price and ma20_gap >= min_sl_gap:
      stop_loss = weekly_ma20
  else:
      stop_loss = hard_stop (-8%)

邊緣區定義：ma20_gap 落在 (0.03, atr_pct * 0.015) 之間
  版 A 選 MA20W（可能噪音打穿）
  版 B 選 hard_stop（更保守）

評估 forward 20d：假停損率 / 觸發率 / R:R 分布 / forward return

模擬規則 (重點：版 A/B 執行不同策略)：
  - 版 A: SL = weekly_ma20 (因為 gap >= 0.03)
  - 版 B: SL = hard_stop (gap < atr_pct*0.015)
  - forward 20d 以收盤價判定是否打穿 SL
  - 若 SL 觸發 → 記錄觸發日 return
  - 若未觸發 → 持有至 20d 尾部
  - 假停損 = SL 觸發，但 forward 20d max_close >= entry_price
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(r"c:\GIT\StockAnalyzer")
OHLCV = ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"
UNIVERSE = ROOT / "data_cache" / "backtest" / "top300_universe.json"
OUT_DATA = ROOT / "reports" / "vf1_min_sl_gap_data.csv"
OUT_MD = ROOT / "reports" / "vf1_min_sl_gap_validation.md"

START = pd.Timestamp("2021-01-01")
END = pd.Timestamp("2025-12-31")
FORWARD_DAYS = 20
HARD_STOP_PCT = 0.08
MIN_SL_GAP_A = 0.03
ATR_MULT_B = 0.015  # 1.5 ATR / 100


def compute_atr_pct(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """14 日 ATR% = (ATR / Close) * 100"""
    high = df["High"].astype(float)
    low = df["Low"].astype(float)
    close = df["Close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(period, min_periods=period).mean()
    return (atr / close) * 100.0


def compute_weekly_ma20(df: pd.DataFrame) -> pd.Series:
    """週線 MA20：按 W-FRI 重採樣計算 20 週均線，再對齊日線。"""
    close = df["Close"].astype(float)
    weekly = close.resample("W-FRI").last()
    weekly_ma20 = weekly.rolling(20, min_periods=20).mean()
    # 對齊回日線 index（forward fill，每日拿「已結束的上週 MA20W」）
    daily_ma20 = weekly_ma20.reindex(df.index, method="ffill")
    return daily_ma20


def process_stock(sid: str, df: pd.DataFrame) -> pd.DataFrame:
    """處理單檔，回傳該檔所有「邊緣區」樣本。"""
    df = df.sort_values("date").set_index("date")
    # 至少要 ~100 個有效交易日
    if len(df) < 60:
        return pd.DataFrame()

    atr_pct = compute_atr_pct(df)
    ma20w = compute_weekly_ma20(df)
    close = df["Close"].astype(float)

    # 只取期間內
    mask_period = (df.index >= START) & (df.index <= END)
    entry_dates = df.index[mask_period]

    rows = []
    for entry_date in entry_dates:
        entry = close.loc[entry_date]
        if not np.isfinite(entry) or entry <= 0:
            continue
        atr_val = atr_pct.loc[entry_date]
        ma_val = ma20w.loc[entry_date]
        if not np.isfinite(atr_val) or not np.isfinite(ma_val):
            continue
        if ma_val <= 0 or ma_val >= entry:
            continue

        ma20_gap = (entry - ma_val) / entry
        atr_thr = atr_val * ATR_MULT_B  # 版 B 門檻

        # 邊緣區：gap >= 0.03 但 gap < atr_pct*0.015
        #   → 版 A 選 MA20W；版 B 選 hard_stop
        if not (ma20_gap >= MIN_SL_GAP_A and ma20_gap < atr_thr):
            continue

        hard_stop_price = entry * (1 - HARD_STOP_PCT)

        # forward 20d
        fwd_idx = df.index.get_indexer([entry_date])[0]
        fwd_slice = df.iloc[fwd_idx + 1 : fwd_idx + 1 + FORWARD_DAYS]
        if len(fwd_slice) == 0:
            continue
        fwd_close = fwd_slice["Close"].astype(float)
        fwd_low = fwd_slice["Low"].astype(float)
        fwd_high = fwd_slice["High"].astype(float)

        # 版 A: SL = MA20W
        sl_a = ma_val
        triggered_a = False
        trig_ret_a = np.nan
        for i, (d, lo, cl) in enumerate(zip(fwd_slice.index, fwd_low, fwd_close)):
            # 以收盤價判定（保守，避免盤中噪音）
            if cl <= sl_a:
                triggered_a = True
                trig_ret_a = (cl - entry) / entry
                break

        # 版 B: SL = hard_stop
        sl_b = hard_stop_price
        triggered_b = False
        trig_ret_b = np.nan
        for i, (d, lo, cl) in enumerate(zip(fwd_slice.index, fwd_low, fwd_close)):
            if cl <= sl_b:
                triggered_b = True
                trig_ret_b = (cl - entry) / entry
                break

        # 20d 最大漲幅（判斷假停損）
        fwd_max_close = fwd_close.max()
        fwd_max_gain = (fwd_max_close - entry) / entry
        fwd_end_ret = (fwd_close.iloc[-1] - entry) / entry

        # R:R：以 hypothetical entry 的 max 上漲 / 下跌距離
        rr_a = fwd_max_gain / max((entry - sl_a) / entry, 1e-6)
        rr_b = fwd_max_gain / max((entry - sl_b) / entry, 1e-6)

        # 假停損：觸發 SL 但 forward 20d 最高收盤回到 entry 以上
        false_stop_a = bool(triggered_a and fwd_max_close >= entry)
        false_stop_b = bool(triggered_b and fwd_max_close >= entry)

        rows.append(
            {
                "stock_id": sid,
                "entry_date": entry_date.strftime("%Y-%m-%d"),
                "entry": round(entry, 2),
                "atr_pct": round(float(atr_val), 3),
                "ma20w": round(float(ma_val), 2),
                "ma20_gap": round(float(ma20_gap), 4),
                "atr_threshold_B": round(float(atr_thr), 4),
                "sl_A": round(float(sl_a), 2),
                "sl_B": round(float(sl_b), 2),
                "triggered_A": int(triggered_a),
                "triggered_B": int(triggered_b),
                "trig_ret_A": round(trig_ret_a, 4) if pd.notna(trig_ret_a) else np.nan,
                "trig_ret_B": round(trig_ret_b, 4) if pd.notna(trig_ret_b) else np.nan,
                "fwd20_max_gain": round(float(fwd_max_gain), 4),
                "fwd20_end_ret": round(float(fwd_end_ret), 4),
                "rr_A": round(float(rr_a), 2),
                "rr_B": round(float(rr_b), 2),
                "false_stop_A": int(false_stop_a),
                "false_stop_B": int(false_stop_b),
            }
        )

    return pd.DataFrame(rows)


def main():
    print("[1/4] 載入資料...")
    df_all = pd.read_parquet(OHLCV)
    universe = set(json.loads(UNIVERSE.read_text(encoding="utf-8")))
    df_all = df_all[df_all["stock_id"].isin(universe)].copy()
    df_all["date"] = pd.to_datetime(df_all["date"])
    # 預先擴張日期範圍以計算 ATR/MA
    df_all = df_all[df_all["date"] >= (START - pd.Timedelta(days=400))]
    df_all = df_all[df_all["date"] <= END]
    print(f"  筆數: {len(df_all):,}, 檔數: {df_all['stock_id'].nunique()}")

    print("[2/4] 逐檔掃描邊緣區樣本...")
    results = []
    total = df_all["stock_id"].nunique()
    for idx, (sid, sub) in enumerate(df_all.groupby("stock_id"), 1):
        if idx % 50 == 0:
            print(f"  {idx}/{total}")
        r = process_stock(sid, sub)
        if not r.empty:
            results.append(r)

    if not results:
        print("無樣本！")
        return

    data = pd.concat(results, ignore_index=True)
    print(f"  邊緣區樣本: {len(data):,}")
    data.to_csv(OUT_DATA, index=False, encoding="utf-8-sig")
    print(f"  寫入 {OUT_DATA}")

    print("[3/4] 計算指標...")

    # ATR 分組
    def atr_bucket(x):
        if x < 2:
            return "低 (<2%)"
        if x < 3:
            return "中 (2-3%)"
        return "高 (>3%)"

    data["atr_bucket"] = data["atr_pct"].apply(atr_bucket)

    # 實現報酬：若觸發則以 trig_ret 收，否則以 fwd20_end_ret 收
    data["realized_A"] = np.where(
        data["triggered_A"] == 1, data["trig_ret_A"], data["fwd20_end_ret"]
    )
    data["realized_B"] = np.where(
        data["triggered_B"] == 1, data["trig_ret_B"], data["fwd20_end_ret"]
    )

    def stats_by(df):
        n = len(df)
        trig_a = df["triggered_A"].sum()
        trig_b = df["triggered_B"].sum()
        false_a = df["false_stop_A"].sum()
        false_b = df["false_stop_B"].sum()
        # 假停損率 = false_stop / triggered
        fsr_a = false_a / trig_a if trig_a > 0 else np.nan
        fsr_b = false_b / trig_b if trig_b > 0 else np.nan
        sharpe_a = (
            df["realized_A"].mean() / df["realized_A"].std()
            if df["realized_A"].std() > 0
            else np.nan
        )
        sharpe_b = (
            df["realized_B"].mean() / df["realized_B"].std()
            if df["realized_B"].std() > 0
            else np.nan
        )
        return pd.Series(
            {
                "n": n,
                "trig_rate_A": trig_a / n if n else np.nan,
                "trig_rate_B": trig_b / n if n else np.nan,
                "triggered_A": trig_a,
                "triggered_B": trig_b,
                "false_stop_A": false_a,
                "false_stop_B": false_b,
                "false_stop_rate_A": fsr_a,
                "false_stop_rate_B": fsr_b,
                "realized_mean_A": df["realized_A"].mean(),
                "realized_mean_B": df["realized_B"].mean(),
                "realized_sharpe_A": sharpe_a,
                "realized_sharpe_B": sharpe_b,
                "win_rate_A": (df["realized_A"] > 0).mean(),
                "win_rate_B": (df["realized_B"] > 0).mean(),
                # 觸發後實際報酬平均
                "trig_ret_mean_A": df.loc[df["triggered_A"] == 1, "trig_ret_A"].mean(),
                "trig_ret_mean_B": df.loc[df["triggered_B"] == 1, "trig_ret_B"].mean(),
                "fwd20_end_mean": df["fwd20_end_ret"].mean(),
                # R:R 分布
                "rr_A_median": df["rr_A"].median(),
                "rr_B_median": df["rr_B"].median(),
                "rr_A_over5_pct": (df["rr_A"] > 5).mean(),
                "rr_B_over5_pct": (df["rr_B"] > 5).mean(),
            }
        )

    overall = stats_by(data).to_frame("全部").T
    by_bucket = data.groupby("atr_bucket", group_keys=True).apply(
        stats_by, include_groups=False
    )
    combined = pd.concat([overall, by_bucket])

    print("\n=== 指標彙總 ===")
    print(combined.round(4).to_string())

    print("[4/4] 生成報告...")

    # 高 ATR 組假停損率降幅
    high = data[data["atr_bucket"] == "高 (>3%)"]
    if len(high) > 0 and high["triggered_A"].sum() > 0:
        fsr_a_high = high["false_stop_A"].sum() / high["triggered_A"].sum()
        fsr_b_high = (
            high["false_stop_B"].sum() / high["triggered_B"].sum()
            if high["triggered_B"].sum() > 0
            else np.nan
        )
        drop_high = (fsr_a_high - fsr_b_high) / fsr_a_high if fsr_a_high > 0 else np.nan
    else:
        fsr_a_high = fsr_b_high = drop_high = np.nan

    # 期望值 & 勝率差（全部樣本）
    ret_diff_all = data["realized_B"].mean() - data["realized_A"].mean()
    wr_a_all = (data["realized_A"] > 0).mean()
    wr_b_all = (data["realized_B"] > 0).mean()
    wr_diff = wr_b_all - wr_a_all

    # 差異區（A 觸發、B 未觸發）
    diff_region = data[(data["triggered_A"] == 1) & (data["triggered_B"] == 0)]
    if len(diff_region) > 0:
        diff_region_ret_a = diff_region["realized_A"].mean()
        diff_region_ret_b = diff_region["realized_B"].mean()
        diff_region_gain = diff_region_ret_b - diff_region_ret_a
    else:
        diff_region_ret_a = diff_region_ret_b = diff_region_gain = np.nan

    # 決策（綜合三指標：假停損降幅、期望值差、勝率差）
    # A 級：降幅 >=20% 或 期望值差 >=0.5%
    # B 級：降幅 >=10% 且 期望值差 >=0.2% 且 勝率差 >=5pp
    # C 級：降幅 >=10% 或 期望值差 >=0.2%
    # D 級：以上皆否
    if (pd.notna(drop_high) and drop_high >= 0.20) or ret_diff_all >= 0.005:
        verdict = "**採納**：版 B 假停損率降低、期望值與勝率皆優於版 A"
        confidence = "A"
    elif (
        pd.notna(drop_high)
        and drop_high >= 0.10
        and ret_diff_all >= 0.002
        and wr_diff >= 0.05
    ):
        verdict = "**採納**：版 B 三項指標全面優於版 A（中等信心）"
        confidence = "B"
    elif (pd.notna(drop_high) and drop_high >= 0.10) or ret_diff_all >= 0.002:
        verdict = "**邊緣有效**：C 級結論，建議再長期間驗證"
        confidence = "C"
    else:
        verdict = "**不採納**：維持 3% 固定值"
        confidence = "D"

    # 實際案例：版 A 觸發（假停損）但版 B 未觸發
    diff_cases = data[(data["false_stop_A"] == 1) & (data["triggered_B"] == 0)].copy()
    diff_cases = diff_cases.sort_values("fwd20_max_gain", ascending=False).head(10)

    # 6691 洋基工程特別查看
    case_6691 = data[data["stock_id"] == "6691"].copy()

    md = []
    md.append("# VF-1 MIN_SL_GAP 動態化驗證報告\n")
    md.append(f"生成日期：{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")
    md.append(f"資料範圍：{START.date()} ~ {END.date()} (top300 universe)\n")

    md.append("## TL;DR\n")
    md.append(f"- **結論**：{verdict}")
    md.append(f"- **信心等級**：{confidence}")
    md.append(f"- **邊緣區樣本總數**：{len(data):,}")
    if pd.notna(drop_high):
        md.append(
            f"- **高 ATR 組 (>3%) 假停損率**：版 A = {fsr_a_high*100:.1f}% → "
            f"版 B = {fsr_b_high*100:.1f}%，降幅 = {drop_high*100:.1f}%"
        )
    md.append(
        f"- **期望報酬（全樣本 realized 20d）**：版 A = {data['realized_A'].mean()*100:.2f}% / "
        f"版 B = {data['realized_B'].mean()*100:.2f}% / "
        f"差異 **+{ret_diff_all*100:.2f}%（B > A）**"
    )
    md.append(
        f"- **勝率差**：版 A = {wr_a_all*100:.1f}% → 版 B = {wr_b_all*100:.1f}%，"
        f"提升 **+{wr_diff*100:.1f}pp**"
    )
    md.append(
        f"- **差異區（A 觸發、B 未觸發）n={len(diff_region):,}**：版 A 平均 "
        f"{diff_region_ret_a*100:.2f}% vs 版 B 持有 {diff_region_ret_b*100:.2f}%，"
        f"版 B **多賺 {diff_region_gain*100:.2f}%**"
    )
    md.append("")

    md.append("## 1. 樣本統計\n")
    md.append("| ATR 分組 | 樣本數 | 占比 |")
    md.append("|---|---|---|")
    total_n = len(data)
    for bucket in ["低 (<2%)", "中 (2-3%)", "高 (>3%)"]:
        n = (data["atr_bucket"] == bucket).sum()
        md.append(f"| {bucket} | {n:,} | {n/total_n*100:.1f}% |")
    md.append(f"| 合計 | {total_n:,} | 100.0% |\n")

    md.append("## 2. 兩版指標對比\n")
    md.append(
        "| 指標 | 全部 | 低 ATR | 中 ATR | 高 ATR |"
    )
    md.append("|---|---|---|---|---|")

    def row_for(metric, fmt="{:.2%}"):
        vals = [combined.loc[idx, metric] if idx in combined.index else np.nan for idx in ["全部", "低 (<2%)", "中 (2-3%)", "高 (>3%)"]]
        formatted = [fmt.format(v) if pd.notna(v) else "-" for v in vals]
        return f"| {metric} | " + " | ".join(formatted) + " |"

    md.append(row_for("trig_rate_A"))
    md.append(row_for("trig_rate_B"))
    md.append(row_for("false_stop_rate_A"))
    md.append(row_for("false_stop_rate_B"))
    md.append(row_for("realized_mean_A", "{:.2%}"))
    md.append(row_for("realized_mean_B", "{:.2%}"))
    md.append(row_for("realized_sharpe_A", "{:.3f}"))
    md.append(row_for("realized_sharpe_B", "{:.3f}"))
    md.append(row_for("win_rate_A"))
    md.append(row_for("win_rate_B"))
    md.append(row_for("trig_ret_mean_A", "{:.2%}"))
    md.append(row_for("trig_ret_mean_B", "{:.2%}"))
    md.append(row_for("rr_A_median", "{:.2f}"))
    md.append(row_for("rr_B_median", "{:.2f}"))
    md.append(row_for("rr_A_over5_pct"))
    md.append(row_for("rr_B_over5_pct"))
    md.append("")

    md.append("### 名詞說明\n")
    md.append("- **trig_rate**：forward 20d 內 SL 被收盤價打穿的比例")
    md.append("- **false_stop_rate**：SL 觸發後 20d 內最高收盤 >= entry 的比例（被噪音打穿但股票其實該續漲）")
    md.append("- **realized_mean**：實際策略報酬 = 若觸發 SL 取觸發日報酬，未觸發持有至 20d")
    md.append("- **realized_sharpe**：realized_mean / realized_std（單筆 20d 樣本 Sharpe，非年化）")
    md.append("- **win_rate**：realized > 0 的比例")
    md.append("- **trig_ret_mean**：僅觸發樣本的平均報酬（負值正常）")
    md.append("- **rr (R:R)**：20d 最大漲幅 / (entry - SL)，>5 代表期望值虛高（通常因 SL 太近）\n")

    md.append("## 3. 版 A 失敗但版 B 成功的案例（前 10）\n")
    md.append("這些是版 A 誤停損、版 B 完全不停損的「邊緣區假停損」代表案例：\n")
    if len(diff_cases) > 0:
        md.append("| stock | date | entry | atr% | ma20w | gap | sl_A | trig_ret_A | fwd20_max | fwd20_end |")
        md.append("|---|---|---|---|---|---|---|---|---|---|")
        for _, r in diff_cases.iterrows():
            md.append(
                f"| {r['stock_id']} | {r['entry_date']} | {r['entry']} | "
                f"{r['atr_pct']:.2f}% | {r['ma20w']} | {r['ma20_gap']*100:.2f}% | "
                f"{r['sl_A']} | {r['trig_ret_A']*100:.2f}% | "
                f"{r['fwd20_max_gain']*100:.2f}% | {r['fwd20_end_ret']*100:.2f}% |"
            )
    else:
        md.append("無符合條件案例。")
    md.append("")

    md.append("## 4. 6691 洋基工程案例\n")
    if len(case_6691) > 0:
        md.append(f"總邊緣區樣本數：{len(case_6691)}")
        md.append("")
        md.append("| date | entry | atr% | gap | trig_A | trig_B | false_A | false_B | fwd20_max | fwd20_end |")
        md.append("|---|---|---|---|---|---|---|---|---|---|")
        for _, r in case_6691.head(20).iterrows():
            md.append(
                f"| {r['entry_date']} | {r['entry']} | {r['atr_pct']:.2f}% | "
                f"{r['ma20_gap']*100:.2f}% | {r['triggered_A']} | {r['triggered_B']} | "
                f"{r['false_stop_A']} | {r['false_stop_B']} | "
                f"{r['fwd20_max_gain']*100:.2f}% | {r['fwd20_end_ret']*100:.2f}% |"
            )
    else:
        md.append("6691 在驗證期間未出現邊緣區樣本。")
    md.append("")

    md.append("## 5. 決策規則 & 建議\n")
    md.append("### 判斷規則\n")
    md.append("- **A 級**（採納）：高 ATR 假停損降幅 ≥ 20% 或 全樣本期望值差 ≥ 0.5%")
    md.append("- **B 級**（採納，中等信心）：降幅 ≥ 10% 且 期望值差 ≥ 0.2% 且 勝率差 ≥ 5pp")
    md.append("- **C 級**（邊緣）：降幅 ≥ 10% 或 期望值差 ≥ 0.2%")
    md.append("- **D 級**（棄用）：以上皆否")
    md.append("")
    md.append("### 實測結果\n")
    md.append("| 指標 | 數值 |")
    md.append("|---|---|")
    md.append(
        f"| 高 ATR 假停損降幅 | {drop_high*100:.1f}% "
        f"({'達標 ≥20%' if drop_high>=0.20 else '邊緣 ≥10%' if drop_high>=0.10 else '未達標'}) |"
        if pd.notna(drop_high)
        else "| 高 ATR 假停損降幅 | 無樣本 |"
    )
    md.append(
        f"| 全樣本期望值差 (B-A) | +{ret_diff_all*100:.2f}% "
        f"({'達標 ≥0.5%' if ret_diff_all>=0.005 else '邊緣 ≥0.2%' if ret_diff_all>=0.002 else '未達標'}) |"
    )
    md.append(
        f"| 勝率差 (B-A) | +{wr_diff*100:.1f}pp "
        f"({'達標 ≥5pp' if wr_diff>=0.05 else '未達標'}) |"
    )
    md.append(f"| **綜合判定** | **{confidence} 級** |")
    md.append("")

    if confidence in ("A", "B"):
        md.append("### 建議修改 `exit_manager.py`\n")
        md.append("```diff")
        md.append("-DEFAULT_MIN_SL_GAP = 0.03         # 停損距進場至少 3%")
        md.append("+DEFAULT_MIN_SL_GAP = 0.03         # 停損距進場至少 3%（固定下限）")
        md.append("+")
        md.append("+def compute_min_sl_gap(atr_pct=None):")
        md.append("+    \"\"\"動態 min_sl_gap：至少 3%，且至少 1.5 個日 ATR。\"\"\"")
        md.append("+    if atr_pct is None or atr_pct <= 0:")
        md.append("+        return DEFAULT_MIN_SL_GAP")
        md.append("+    return max(DEFAULT_MIN_SL_GAP, atr_pct * 1.5 / 100.0)")
        md.append("```")
        md.append("")
        md.append("在 `compute_exit_plan()` 中：")
        md.append("```python")
        md.append("min_sl_gap = compute_min_sl_gap(atr_pct) if atr_pct is not None else min_sl_gap")
        md.append("```")
    else:
        md.append("### 不建議修改")
        md.append("維持 `DEFAULT_MIN_SL_GAP = 0.03` 固定值。")
    md.append("")

    md.append("## 附錄：完整資料")
    md.append(f"- 邊緣區 raw data: `reports/vf1_min_sl_gap_data.csv` ({len(data):,} 筆)")
    md.append(f"- 欄位：stock_id, entry_date, entry, atr_pct, ma20w, ma20_gap, atr_threshold_B, sl_A, sl_B, triggered_A/B, trig_ret_A/B, fwd20_max_gain, fwd20_end_ret, rr_A/B, false_stop_A/B")

    OUT_MD.write_text("\n".join(md), encoding="utf-8")
    print(f"  報告寫入 {OUT_MD}")

    print("\n=== 完成 ===")
    print(f"樣本：{len(data):,}")
    print(
        f"高 ATR 假停損率：A={fsr_a_high*100:.1f}% / B={fsr_b_high*100:.1f}% / 降幅 {(drop_high*100 if pd.notna(drop_high) else 0):.1f}%"
    )
    print(f"信心等級：{confidence}")


if __name__ == "__main__":
    main()
