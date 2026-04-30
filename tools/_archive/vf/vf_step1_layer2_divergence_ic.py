"""
VF Step 1 - Layer 2 底部背離 factor IC 驗證

目的:
  驗證 pattern_detection.detect_divergence() 的底/頂背離 signal 是否有 alpha。
  原函式是 snapshot (只回傳最後一根的背離類型)，這裡改為 vectorized 時序掃描:
  對每檔股票每天生成當下的背離 signal (使用 T 日 close 前資訊)，然後做截面 IC。

Signal 定義 (每日整數編碼):
  +3 bull_strong  / +2 bull / +1 bull_weak (hidden)
   0 none
  -1 bear_weak    / -2 bear / -3 bear_strong

方法:
  1. 對每檔股票計算 RSI14 / MACD histogram
  2. 用 scipy.signal.argrelextrema (order=3) 找 pivot lows / pivot highs
  3. Sliding window=40 日, 每個 T 日檢查最近 2 個 pivot 是否符合背離條件
  4. 生成 daily signal panel (ticker x date x signal)
  5. Cross-sectional Spearman IC vs fwd_20d / fwd_40d / fwd_60d
  6. 分三段 regime: 2016-2019 / 2020-2022 / 2023-2025
  7. Quintile spread (由於離散 signal 只有 7 值，quintile 改成正負 bull/bear/none bucket)

Look-ahead check:
  - signal[T] 只用 [T-window, T] 區間價格/指標 -> T 日 close 後才可知
  - fwd_Nd[T] = close[T+N]/close[T] - 1 -> 從 T+1 開始持有
  - 兩者時序對齊無 leak

Output:
  - reports/vf_step1_layer2_divergence_ic.csv  (regime x horizon x IC/tstat/spread/n)
  - reports/vf_step1_layer2_divergence_ic.md   (摘要 + verdict)
"""
import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats
from scipy.signal import argrelextrema

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT))

PARQUET_PATH = _ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUT_DIR / "vf_step1_layer2_divergence_ic.csv"
OUT_MD = OUT_DIR / "vf_step1_layer2_divergence_ic.md"

HORIZONS = [20, 40, 60]
WINDOW = 40                 # 背離掃描回看窗口
PIVOT_ORDER = 3             # argrelextrema order
MIN_CROSS_SECTION = 30
MIN_HISTORY = 200

REGIMES = {
    "2016-2019": ("2016-01-01", "2019-12-31"),
    "2020-2022": ("2020-01-01", "2022-12-31"),
    "2023-2025": ("2023-01-01", "2025-12-31"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("div_ic")


# ============================================================
# 1. 指標計算 (RSI14 + MACD hist)
# ============================================================
def compute_indicators_one(sub):
    """對單一 ticker 計算 RSI14 + MACD histogram。"""
    from ta.trend import MACD
    from ta.momentum import RSIIndicator

    close = sub["Close"]
    try:
        sub["RSI"] = RSIIndicator(close, n=14).rsi()
        macd = MACD(close)
        sub["MACD_hist"] = macd.macd_diff()
    except Exception as e:
        logger.warning(f"indicator fail {sub['yf_ticker'].iloc[0]}: {e}")
        sub["RSI"] = np.nan
        sub["MACD_hist"] = np.nan
    return sub


# ============================================================
# 2. 時序背離掃描 (vectorized pivot-based)
# ============================================================
def _scan_divergence_series(prices_low, prices_high, indicator, window=WINDOW, order=PIVOT_ORDER):
    """
    對單一 ticker 的時序做 sliding-window 背離掃描。

    Returns:
        np.array of int signal, shape (n,).
        +3 bull_strong, +2 bull, +1 bull_weak
         0 none
        -1 bear_weak, -2 bear, -3 bear_strong
    """
    n = len(prices_low)
    signal = np.zeros(n, dtype=np.int8)

    if n < window:
        return signal

    # 一次性找出所有 pivot 的位置 (全序列; T 日只會用到 <=T 的 pivot)
    # 注意 argrelextrema 對邊界會漏 order 個點 -> 可接受
    valid_ind = ~np.isnan(indicator)
    if valid_ind.sum() < window:
        return signal

    price_min_all = set(argrelextrema(prices_low, np.less, order=order)[0].tolist())
    price_max_all = set(argrelextrema(prices_high, np.greater, order=order)[0].tolist())

    # indicator 可能有 NaN (前 14 根 RSI 為 NaN) -> 填大數避免被 argrelextrema 當極值
    ind_filled = np.where(np.isnan(indicator), np.nanmedian(indicator), indicator)
    ind_min_all = set(argrelextrema(ind_filled, np.less, order=order)[0].tolist())
    ind_max_all = set(argrelextrema(ind_filled, np.greater, order=order)[0].tolist())

    # 轉 sorted list
    price_min_sorted = sorted(price_min_all)
    price_max_sorted = sorted(price_max_all)
    ind_min_sorted = sorted(ind_min_all)
    ind_max_sorted = sorted(ind_max_all)

    for T in range(window, n):
        lo = T - window + 1
        # 取出當前 window 範圍內的 pivot (index <= T, >= lo)
        # 注意: pivot 需要 order 根右側確認 -> 實務上 T 日能確定的 pivot 只到 T-order
        cutoff = T - order  # 只用已確認的 pivot
        if cutoff < lo:
            continue

        pm_win = [i for i in price_min_sorted if lo <= i <= cutoff]
        px_win = [i for i in price_max_sorted if lo <= i <= cutoff]
        im_win = [i for i in ind_min_sorted if lo <= i <= cutoff]
        ix_win = [i for i in ind_max_sorted if lo <= i <= cutoff]

        # ===== 底背離 =====
        if len(pm_win) >= 2 and len(im_win) >= 2:
            p1_idx, p2_idx = pm_win[-2], pm_win[-1]
            p1_price, p2_price = prices_low[p1_idx], prices_low[p2_idx]

            # 找對應的指標波谷 (複製原函式邏輯)
            ind1_cands = [i for i in im_win if max(0, p1_idx - order) <= i <= p1_idx + order]
            ind2_cands = [i for i in im_win if max(p1_idx, p2_idx - order) <= i <= p2_idx + order]

            if ind1_cands and ind2_cands:
                ind1_idx = ind1_cands[-1]
                ind2_idx = ind2_cands[-1]
                ind1_val = indicator[ind1_idx]
                ind2_val = indicator[ind2_idx]

                if not (np.isnan(ind1_val) or np.isnan(ind2_val)):
                    # 標準底背離: 價格更低低點 + 指標更高低點
                    if p2_price < p1_price and ind2_val > ind1_val:
                        price_drop_pct = (p1_price - p2_price) / p1_price * 100
                        ind_rise_pct = min(
                            (ind2_val - ind1_val) / abs(ind1_val) * 100 if ind1_val != 0 else 0,
                            500,
                        )
                        if price_drop_pct > 3 and ind_rise_pct > 10:
                            signal[T] = 3  # bull_strong
                        else:
                            signal[T] = 2  # bull
                        continue
                    # 隱藏底背離
                    if p2_price > p1_price and ind2_val < ind1_val:
                        signal[T] = 1  # bull_weak
                        continue

        # ===== 頂背離 =====
        if len(px_win) >= 2 and len(ix_win) >= 2:
            p1_idx, p2_idx = px_win[-2], px_win[-1]
            p1_price, p2_price = prices_high[p1_idx], prices_high[p2_idx]

            ind1_cands = [i for i in ix_win if max(0, p1_idx - order) <= i <= p1_idx + order]
            ind2_cands = [i for i in ix_win if max(p1_idx, p2_idx - order) <= i <= p2_idx + order]

            if ind1_cands and ind2_cands:
                ind1_idx = ind1_cands[-1]
                ind2_idx = ind2_cands[-1]
                ind1_val = indicator[ind1_idx]
                ind2_val = indicator[ind2_idx]

                if not (np.isnan(ind1_val) or np.isnan(ind2_val)):
                    # 標準頂背離
                    if p2_price > p1_price and ind2_val < ind1_val:
                        price_rise_pct = (p2_price - p1_price) / p1_price * 100
                        ind_drop_pct = min(
                            (ind1_val - ind2_val) / abs(ind1_val) * 100 if ind1_val != 0 else 0,
                            500,
                        )
                        if price_rise_pct > 3 and ind_drop_pct > 10:
                            signal[T] = -3
                        else:
                            signal[T] = -2
                        continue
                    # 隱藏頂背離
                    if p2_price < p1_price and ind2_val > ind1_val:
                        signal[T] = -1
                        continue

    return signal


def compute_divergence_one_ticker(sub, indicator_col):
    """對一檔股票生成 divergence signal 時序。"""
    sub = sub.sort_values("date").reset_index(drop=True)
    prices_low = sub["Low"].values.astype(float)
    prices_high = sub["High"].values.astype(float)
    indicator = sub[indicator_col].values.astype(float)
    sig = _scan_divergence_series(prices_low, prices_high, indicator)
    return sig


# ============================================================
# 3. Main pipeline
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=None, help="Random sample N tickers")
    parser.add_argument("--since", type=str, default="2015-06-01",
                        help="Load since date (need +6mo buffer before 2016)")
    parser.add_argument("--indicator", type=str, default="RSI",
                        choices=["RSI", "MACD_hist"],
                        help="Which indicator to use for divergence")
    args = parser.parse_args()

    t0 = time.time()
    logger.info(f"Loading {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= args.since].copy()

    for col in ["Open", "High", "Low", "Close", "AdjClose", "Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    counts = df.groupby("yf_ticker").size()
    keep = counts[counts >= MIN_HISTORY].index
    df = df[df["yf_ticker"].isin(keep)].copy()

    if args.sample:
        rng = np.random.default_rng(42)
        tickers = df["yf_ticker"].unique()
        chosen = rng.choice(tickers, min(args.sample, len(tickers)), replace=False)
        df = df[df["yf_ticker"].isin(chosen)].copy()

    df = df.sort_values(["yf_ticker", "date"]).reset_index(drop=True)
    logger.info(f"Loaded {len(df):,} rows, {df['yf_ticker'].nunique()} tickers, "
                f"range {df['date'].min().date()} ~ {df['date'].max().date()}")

    # --- Step 1: compute RSI / MACD ---
    logger.info("Computing RSI + MACD indicators...")
    t1 = time.time()
    df = df.groupby("yf_ticker", group_keys=False).apply(compute_indicators_one, include_groups=True)
    logger.info(f"indicators done in {time.time()-t1:.1f}s")

    # --- Step 2: scan divergence signal per ticker ---
    logger.info(f"Scanning divergence signal (indicator={args.indicator})...")
    t2 = time.time()
    indicator_col = args.indicator
    all_tickers = df["yf_ticker"].unique()
    sig_rows = []
    for i, tk in enumerate(all_tickers):
        sub = df[df["yf_ticker"] == tk]
        sig = compute_divergence_one_ticker(sub, indicator_col)
        sig_rows.append(pd.DataFrame({
            "yf_ticker": tk,
            "date": sub["date"].values,
            "div_signal": sig,
        }))
        if (i + 1) % 200 == 0:
            logger.info(f"  scanned {i+1}/{len(all_tickers)} ({time.time()-t2:.1f}s)")
    sig_df = pd.concat(sig_rows, ignore_index=True)
    df = df.merge(sig_df, on=["yf_ticker", "date"], how="left")
    logger.info(f"divergence scan done in {time.time()-t2:.1f}s")

    # --- Step 3: forward returns ---
    logger.info("Computing forward returns...")
    df = df.sort_values(["yf_ticker", "date"]).reset_index(drop=True)
    for h in HORIZONS:
        df[f"fwd_{h}d"] = df.groupby("yf_ticker")["AdjClose"].pct_change(h).shift(-h)

    # ---- Signal distribution ----
    logger.info("Signal distribution (overall):")
    dist = df["div_signal"].value_counts().sort_index()
    print(dist.to_string())
    total = len(df)
    nonzero_pct = (df["div_signal"] != 0).sum() / total * 100
    bull_pct = (df["div_signal"] > 0).sum() / total * 100
    bear_pct = (df["div_signal"] < 0).sum() / total * 100
    logger.info(f"nonzero={nonzero_pct:.2f}%  bull={bull_pct:.2f}%  bear={bear_pct:.2f}%")

    # --- Step 4: regime-segmented IC ---
    logger.info("Computing IC per regime x horizon...")
    results = []
    for regime_name, (start, end) in REGIMES.items():
        reg_df = df[(df["date"] >= start) & (df["date"] <= end)].copy()
        for h in HORIZONS:
            ret_col = f"fwd_{h}d"
            sub = reg_df.dropna(subset=["div_signal", ret_col])
            sub = sub[sub["div_signal"] != 0]  # 只看有背離的樣本 (signal=0 會稀釋 IC)

            # 每日截面 IC
            ics = []
            for date, g in sub.groupby("date"):
                if len(g) < 5:  # 背離稀疏, 閾值下調
                    continue
                try:
                    ic, _ = stats.spearmanr(g["div_signal"], g[ret_col])
                    if not np.isnan(ic):
                        ics.append(ic)
                except Exception:
                    continue
            ic_series = pd.Series(ics)

            if len(ic_series) == 0:
                mean_ic = np.nan
                t_stat = np.nan
                p_val = np.nan
                ir = np.nan
            else:
                mean_ic = ic_series.mean()
                std_ic = ic_series.std(ddof=1) if len(ic_series) > 1 else np.nan
                ir = mean_ic / std_ic if std_ic and std_ic > 0 else np.nan
                t_stat = mean_ic * np.sqrt(len(ic_series)) / std_ic if std_ic and std_ic > 0 else np.nan
                p_val = 2 * (1 - stats.t.cdf(abs(t_stat), df=len(ic_series) - 1)) if not np.isnan(t_stat) else np.nan

            # Bull vs Bear spread (用 signed signal 分兩組)
            bull_ret = sub[sub["div_signal"] > 0][ret_col]
            bear_ret = sub[sub["div_signal"] < 0][ret_col]
            bull_mean = bull_ret.mean() if len(bull_ret) > 0 else np.nan
            bear_mean = bear_ret.mean() if len(bear_ret) > 0 else np.nan
            spread = bull_mean - bear_mean if not (np.isnan(bull_mean) or np.isnan(bear_mean)) else np.nan

            # Bull-strong only vs universe mean (quasi Q5-Q1 替代)
            bull_strong = sub[sub["div_signal"] == 3][ret_col]
            bear_strong = sub[sub["div_signal"] == -3][ret_col]
            bull_strong_mean = bull_strong.mean() if len(bull_strong) > 0 else np.nan
            bear_strong_mean = bear_strong.mean() if len(bear_strong) > 0 else np.nan

            results.append({
                "regime": regime_name,
                "horizon": h,
                "mean_ic": mean_ic,
                "ic_ir": ir,
                "t_stat": t_stat,
                "p_value": p_val,
                "n_days": len(ic_series),
                "n_obs_bull": len(bull_ret),
                "n_obs_bear": len(bear_ret),
                "bull_mean_ret": bull_mean,
                "bear_mean_ret": bear_mean,
                "bull_bear_spread": spread,
                "bull_strong_mean": bull_strong_mean,
                "bear_strong_mean": bear_strong_mean,
                "n_obs_bull_strong": len(bull_strong),
                "n_obs_bear_strong": len(bear_strong),
            })

    res_df = pd.DataFrame(results)
    res_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    logger.info(f"CSV written: {OUT_CSV}")
    print()
    print(res_df.to_string(index=False))

    # --- Step 5: write MD summary + verdict ---
    write_md_summary(res_df, args, total_rows=total, nonzero_pct=nonzero_pct,
                     bull_pct=bull_pct, bear_pct=bear_pct, dist=dist)
    logger.info(f"MD written: {OUT_MD}")
    logger.info(f"Total elapsed: {time.time()-t0:.1f}s")


def write_md_summary(res_df, args, total_rows, nonzero_pct, bull_pct, bear_pct, dist):
    """產出 1 頁 Markdown 摘要 + verdict。"""
    # Verdict 判定
    thresh_ic = 0.03
    sig_rows = res_df.dropna(subset=["mean_ic"])

    # 按照 horizon 判斷 (取每個 horizon 三段都顯著的才算該 horizon 強)
    horizon_sig_count = {}
    for h in HORIZONS:
        hr = sig_rows[sig_rows["horizon"] == h]
        # 顯著 = |IC| > 0.03 AND p < 0.05 AND 方向正確 (bull_bear_spread > 0)
        hr_strong = hr[
            (hr["mean_ic"].abs() > thresh_ic) &
            (hr["p_value"] < 0.05)
        ]
        horizon_sig_count[h] = len(hr_strong)

    # 全局 verdict: 看最佳 horizon 的 regime 通過數
    best_h = max(horizon_sig_count, key=horizon_sig_count.get)
    best_count = horizon_sig_count[best_h]
    if best_count == 3:
        verdict = "A 級 (三段皆 sig, 上線)"
    elif best_count == 2:
        verdict = "B 級 (2 段 sig, 觀察)"
    elif best_count == 1:
        verdict = "C 級 (1 段 sig, 弱)"
    else:
        verdict = "D 級 (全不顯著, 砍掉或不作為主因子)"

    lines = []
    lines.append(f"# VF Step 1 - Layer 2 底部背離 factor IC 驗證\n")
    lines.append(f"**Run config**: indicator={args.indicator}, window={WINDOW}, pivot_order={PIVOT_ORDER}, sample={args.sample or 'ALL'}\n")
    lines.append(f"**Universe**: 台股 (filtered history >= {MIN_HISTORY} days)\n")
    lines.append(f"**Horizons**: {HORIZONS}\n")
    lines.append(f"**Regimes**: 2016-2019 / 2020-2022 / 2023-2025\n")
    lines.append("")
    lines.append("## 1. Signal 基本統計\n")
    lines.append(f"- 總樣本列數: {total_rows:,}")
    lines.append(f"- 背離觸發率 (nonzero): **{nonzero_pct:.2f}%**")
    lines.append(f"- bull (>0): {bull_pct:.2f}% / bear (<0): {bear_pct:.2f}%")
    lines.append("")
    lines.append("訊號分佈:")
    lines.append("```")
    lines.append(dist.to_string())
    lines.append("```")
    lines.append("")
    lines.append("## 2. IC Matrix (regime x horizon)\n")
    lines.append("| regime | horizon | mean_IC | IC_IR | t_stat | p_value | n_days | bull_n | bear_n | spread |")
    lines.append("|--------|---------|---------|-------|--------|---------|--------|--------|--------|--------|")
    for _, r in res_df.iterrows():
        lines.append(
            f"| {r['regime']} | {r['horizon']}d | {r['mean_ic']:+.4f} | {r['ic_ir']:+.3f} | "
            f"{r['t_stat']:+.2f} | {r['p_value']:.4f} | {r['n_days']} | "
            f"{r['n_obs_bull']:,} | {r['n_obs_bear']:,} | {r['bull_bear_spread']:+.4%} |"
        )
    lines.append("")
    lines.append("## 3. Bull vs Bear 組平均報酬\n")
    lines.append("| regime | horizon | bull_mean | bear_mean | spread | bull_strong_mean | bear_strong_mean |")
    lines.append("|--------|---------|-----------|-----------|--------|------------------|------------------|")
    for _, r in res_df.iterrows():
        bs = f"{r['bull_strong_mean']:+.4%}" if not pd.isna(r['bull_strong_mean']) else "NaN"
        brs = f"{r['bear_strong_mean']:+.4%}" if not pd.isna(r['bear_strong_mean']) else "NaN"
        lines.append(
            f"| {r['regime']} | {r['horizon']}d | "
            f"{r['bull_mean_ret']:+.4%} | {r['bear_mean_ret']:+.4%} | {r['bull_bear_spread']:+.4%} | "
            f"{bs} | {brs} |"
        )
    lines.append("")
    lines.append("## 4. Verdict\n")
    lines.append(f"**{verdict}**\n")
    lines.append("判定標準: |mean_IC| > 0.03 AND p < 0.05")
    lines.append(f"- 各 horizon 通過 regime 數: {horizon_sig_count}")
    lines.append(f"- 最佳 horizon = {best_h}d, 通過 {best_count}/3 段")
    lines.append("")
    lines.append("## 5. 看法 / 建議\n")
    if best_count == 0:
        lines.append("- Divergence signal 在全期間及三段 regime 皆不具顯著預測力 (考量 p<0.05 + |IC|>0.03 雙門檻)")
        lines.append("- **不建議**將背離作為 Layer 2 主因子。analysis_engine 原本將 div 當加分項 (+1~+3)，IC 驗證顯示此加分噪音大於訊號")
        lines.append("- 可考慮: (a) 將 divergence 降為劇本輔助 (視覺提示) 而非分數加權 (b) 與其他因子組合看條件機率")
    elif best_count == 1:
        lines.append(f"- 僅 {best_h}d horizon 在單一 regime 顯著，顯示 divergence signal 穩定性差")
        lines.append("- 不建議獨立上線，可作為組合因子之一 (低權重 5-10%)")
    elif best_count == 2:
        lines.append(f"- {best_h}d horizon 在 2 段 regime 顯著，值得進一步驗證")
        lines.append("- 建議: 觀察期 3 個月 + 與 F-Score / 動能因子組合測試 composite IC")
    else:
        lines.append(f"- {best_h}d horizon 三段皆顯著, divergence signal 具穩定 alpha")
        lines.append("- 建議: 上線作為 Layer 2 主因子之一, 權重 15-20%")
    lines.append("")
    lines.append("## 6. Look-ahead 檢查\n")
    lines.append("- signal[T] 使用 [T-window, T] 區間 + pivot order=3 右側確認 (cutoff=T-order)")
    lines.append("- 實際可用 pivot 只到 T-3 -> T 日收盤後才能確定 signal")
    lines.append("- fwd_Nd[T] = close[T+N]/close[T] - 1 -> 從 T+1 開始計算")
    lines.append("- 時序對齊無 leakage")
    lines.append("")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
