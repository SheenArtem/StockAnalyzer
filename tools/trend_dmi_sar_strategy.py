"""
tools/trend_dmi_sar_strategy.py

量化交易系統實作 + 回測 (2026-05-22)

策略規格 (依使用者 2026-05-22 提供的 spec)
==========================================

模組一  指標
    MA_60     SMA(60)
    DMI_14    +DI, -DI, ADX (14)
    RSI_14    14 週期相對強弱
    ATR_14    14 週期平均真實區間
    SAR       Wilder Parabolic SAR, step=0.02, max=0.2

模組二  進場 (日收盤判定，全部 True 才產生買進訊號)
    1) Close > MA60
    2) +DI > -DI
    3) ADX > 25 且 ADX > ADX[-1]
    4) RSI < 70
    Action：訊號日收盤價 = Entry_Price (依 spec 字面)

模組三  初始停損
    Initial_SL = Entry_Price - 1.5 × ATR_at_entry  (進場日 ATR 鎖死)

模組四  動態出場
    Phase A 硬性防守：Close < Initial_SL → 停損出場
    切換 Phase B 條件：Close > Entry + ATR_at_entry  (獲利達 1 ATR)
    Phase B SAR 移動停利：Close < SAR_today → 停利出場

回測假設
    - 訊號日收盤進場 / 訊號日收盤出場 (close-to-close, spec 字面)
    - 全現金 (all-in single ticker)，無槓桿、無重疊持倉
    - 台股費用：手續費 0.1425% (買賣各收)、證交稅 0.3% (賣出收)
    - 多頭 only (做多單向，spec 沒提空單)

CLI
---
單檔回測 (2330 近 12 年)：
    python tools/trend_dmi_sar_strategy.py --ticker 2330 --start 2014-01-01

多檔批次：
    python tools/trend_dmi_sar_strategy.py --universe 2330,2454,0050,2317,2412 --start 2014-01-01

輸出
----
- stdout 績效摘要 (CAGR / Sharpe / MDD / 勝率 / 交易次數 / 平均持有天數)
- 預設寫入 reports/trend_dmi_sar_<ticker>_<start>_<end>.csv (交易明細)
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "data_cache"
PANEL_TW = ROOT / "data_cache/backtest/ohlcv_tw.parquet"
REPORTS_DIR = ROOT / "reports"

FEE_RATE = 0.001425
TAX_RATE = 0.003

# ---------------------------------------------------------------------------
# 資料載入
# ---------------------------------------------------------------------------

def load_ohlcv(ticker: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    """
    優先序：
      1) data_cache/<ticker>_price.csv  (live cache，最新到昨日)
      2) data_cache/backtest/ohlcv_tw.parquet  (panel，截至 2026-04-30)
    """
    csv_path = CACHE_DIR / f"{ticker}_price.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        df = df[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
    elif PANEL_TW.exists():
        panel = pd.read_parquet(PANEL_TW)
        sub = panel[panel["stock_id"] == ticker]
        if sub.empty:
            raise ValueError(f"Ticker {ticker} 在 panel 與 CSV 都找不到")
        df = sub.set_index("date")[["Open", "High", "Low", "Close", "Volume"]].sort_index()
    else:
        raise FileNotFoundError(f"找不到 {ticker} 資料 (既無 {csv_path} 也無 panel)")

    df = df[~df.index.duplicated(keep="last")].sort_index()
    if start:
        df = df[df.index >= pd.Timestamp(start)]
    if end:
        df = df[df.index <= pd.Timestamp(end)]
    return df


# ---------------------------------------------------------------------------
# 指標
# ---------------------------------------------------------------------------

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """計算 MA60 / DMI14 / RSI14 / ATR14 / SAR"""
    out = df.copy()
    high, low, close = out["High"], out["Low"], out["Close"]

    # MA60
    out["MA60"] = close.rolling(60).mean()

    # ATR14 (Wilder smoothing — 用 RMA / EMA(alpha=1/14)；這裡用 14 SMA 維持與專案 technical_analysis 一致)
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    out["ATR14"] = tr.rolling(14).mean()

    # DMI14 (+DI, -DI, ADX)
    up = high.diff()
    down = -low.diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr_smooth = tr.rolling(14).mean().replace(0, np.nan)
    out["+DI"] = 100 * (pd.Series(plus_dm, index=out.index).rolling(14).mean() / tr_smooth)
    out["-DI"] = 100 * (pd.Series(minus_dm, index=out.index).rolling(14).mean() / tr_smooth)
    di_sum = (out["+DI"] + out["-DI"]).replace(0, np.nan)
    dx = 100 * (out["+DI"] - out["-DI"]).abs() / di_sum
    out["ADX"] = dx.rolling(14).mean()

    # RSI14
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    out["RSI14"] = (100 - 100 / (1 + rs)).fillna(100)

    # SAR (Wilder Parabolic, step=0.02, max=0.2)
    out["SAR"] = _parabolic_sar(high.values, low.values, step=0.02, max_step=0.2)
    return out


def _parabolic_sar(high: np.ndarray, low: np.ndarray, step: float = 0.02, max_step: float = 0.2) -> np.ndarray:
    """
    標準 Wilder Parabolic SAR。
    回傳長度 = len(high) 的陣列；首兩根 warm-up 為 nan。
    """
    n = len(high)
    sar = np.full(n, np.nan)
    if n < 2:
        return sar

    # Warm-up：用前兩根決定初始趨勢
    if high[1] >= high[0]:
        trend = 1  # uptrend
        sar_t = low[0]
        ep = high[1]
    else:
        trend = -1
        sar_t = high[0]
        ep = low[1]
    af = step
    sar[1] = sar_t

    for i in range(2, n):
        prev_sar = sar_t
        # 1) tentative SAR for today (using yesterday's trend/EP/AF)
        sar_t = prev_sar + af * (ep - prev_sar)

        # 2) clamp by prior two bars
        if trend == 1:
            sar_t = min(sar_t, low[i - 1], low[i - 2])
        else:
            sar_t = max(sar_t, high[i - 1], high[i - 2])

        # 3) check reversal by today's price
        if trend == 1:
            if low[i] < sar_t:
                # reverse to down
                trend = -1
                sar_t = ep            # new SAR = old EP
                ep = low[i]
                af = step
            else:
                if high[i] > ep:
                    ep = high[i]
                    af = min(af + step, max_step)
                # else keep af/ep
        else:
            if high[i] > sar_t:
                trend = 1
                sar_t = ep
                ep = high[i]
                af = step
            else:
                if low[i] < ep:
                    ep = low[i]
                    af = min(af + step, max_step)

        sar[i] = sar_t

    return sar


# ---------------------------------------------------------------------------
# 進出場 state machine + 回測
# ---------------------------------------------------------------------------

@dataclass
class Trade:
    ticker: str
    entry_date: pd.Timestamp
    entry_price: float
    initial_sl: float
    atr_at_entry: float
    exit_date: pd.Timestamp
    exit_price: float
    exit_reason: str          # 'stop_loss' / 'trailing_stop' / 'end_of_data'
    phase_at_exit: str        # 'A' / 'B'
    holding_days: int
    gross_return_pct: float   # 不含費用
    net_return_pct: float     # 含費用


def run_backtest(
    df_ind: pd.DataFrame,
    ticker: str,
    switch_atr_mult: float = 1.0,
    exit_on_adx_drop: bool = False,
) -> List[Trade]:
    """
    輸入：含指標的 df (compute_indicators 結果)
    參數變體：
      switch_atr_mult : Phase A → Phase B 切換閾值倍數 (default 1.0 = spec)
      exit_on_adx_drop: True 時 Phase A/B 若 ADX < 25 直接出場 (額外風控)
    輸出：成交清單
    """
    trades: List[Trade] = []

    state = "flat"            # 'flat' / 'A' / 'B'
    entry_price = np.nan
    entry_date = None
    initial_sl = np.nan
    atr_at_entry = np.nan

    # 從第 61 列開始 (MA60 需要 60 根 warm-up，DMI/ADX 需 14+14=28 根，更鬆)
    df = df_ind.dropna(subset=["MA60", "ADX", "RSI14", "ATR14", "SAR"]).copy()
    if df.empty:
        return trades

    # 預先取 ADX shift(1) 比較動能向上
    df["ADX_prev"] = df["ADX"].shift(1)

    for dt, row in df.iterrows():
        close = row["Close"]

        if state == "flat":
            # 進場判定 (4 條全 True)
            if (
                close > row["MA60"]
                and row["+DI"] > row["-DI"]
                and row["ADX"] > 25
                and pd.notna(row["ADX_prev"]) and row["ADX"] > row["ADX_prev"]
                and row["RSI14"] < 70
            ):
                state = "A"
                entry_price = close
                entry_date = dt
                atr_at_entry = row["ATR14"]
                initial_sl = entry_price - 1.5 * atr_at_entry

        elif state == "A":
            # 同根 K 線判斷順序：停損優先 → ADX 退場 → 切換 Phase B
            if close < initial_sl:
                trades.append(_close_trade(ticker, entry_date, entry_price, initial_sl,
                                           atr_at_entry, dt, close, "stop_loss", "A"))
                state = "flat"
            elif exit_on_adx_drop and row["ADX"] < 25:
                trades.append(_close_trade(ticker, entry_date, entry_price, initial_sl,
                                           atr_at_entry, dt, close, "adx_drop", "A"))
                state = "flat"
            elif close > entry_price + switch_atr_mult * atr_at_entry:
                state = "B"

        elif state == "B":
            if close < row["SAR"]:
                trades.append(_close_trade(ticker, entry_date, entry_price, initial_sl,
                                           atr_at_entry, dt, close, "trailing_stop", "B"))
                state = "flat"
            elif exit_on_adx_drop and row["ADX"] < 25:
                trades.append(_close_trade(ticker, entry_date, entry_price, initial_sl,
                                           atr_at_entry, dt, close, "adx_drop", "B"))
                state = "flat"

    # 收尾：若資料結束仍持倉，按最後收盤強制平倉 (end_of_data)
    if state in ("A", "B"):
        last_dt = df.index[-1]
        last_close = df.iloc[-1]["Close"]
        trades.append(_close_trade(ticker, entry_date, entry_price, initial_sl,
                                   atr_at_entry, last_dt, last_close, "end_of_data", state))

    return trades


def _close_trade(ticker, ed, ep, sl, atr, xd, xp, reason, phase) -> Trade:
    gross = (xp - ep) / ep * 100
    # 費用：買入 fee，賣出 fee + tax
    cost_pct = (FEE_RATE + FEE_RATE + TAX_RATE) * 100
    net = gross - cost_pct
    return Trade(
        ticker=ticker,
        entry_date=ed,
        entry_price=round(ep, 4),
        initial_sl=round(sl, 4),
        atr_at_entry=round(atr, 4),
        exit_date=xd,
        exit_price=round(xp, 4),
        exit_reason=reason,
        phase_at_exit=phase,
        holding_days=(xd - ed).days,
        gross_return_pct=round(gross, 3),
        net_return_pct=round(net, 3),
    )


# ---------------------------------------------------------------------------
# 績效統計 (含複利資金曲線)
# ---------------------------------------------------------------------------

@dataclass
class PerfStats:
    ticker: str
    n_trades: int
    win_rate_pct: float
    avg_return_pct: float
    avg_win_pct: float
    avg_loss_pct: float
    profit_factor: float
    total_return_pct: float       # 複利
    cagr_pct: float
    sharpe: float                 # 以「逐筆 net return」為樣本，年化 ~13 trades/yr 假設 (動態)
    max_drawdown_pct: float
    avg_holding_days: float
    median_holding_days: float
    bh_total_return_pct: float    # buy & hold 比較
    bh_cagr_pct: float
    bh_max_drawdown_pct: float


def perf_summary(trades: List[Trade], df_ind: pd.DataFrame, ticker: str) -> PerfStats:
    if not trades:
        return PerfStats(ticker, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    rets = np.array([t.net_return_pct / 100 for t in trades])
    wins = rets[rets > 0]
    losses = rets[rets <= 0]
    win_rate = len(wins) / len(rets) * 100

    avg_ret = rets.mean() * 100
    avg_win = wins.mean() * 100 if len(wins) else 0.0
    avg_loss = losses.mean() * 100 if len(losses) else 0.0
    pf = (wins.sum() / abs(losses.sum())) if losses.sum() != 0 else float("inf")

    # 複利 equity curve（每筆交易後乘 (1+net_return)）
    equity = np.cumprod(1 + rets)
    total = (equity[-1] - 1) * 100

    # CAGR
    span_days = (trades[-1].exit_date - trades[0].entry_date).days
    yrs = span_days / 365.25 if span_days > 0 else 1
    cagr = (equity[-1] ** (1 / yrs) - 1) * 100 if equity[-1] > 0 else -100.0

    # Sharpe：以逐筆 net return 為樣本，估每年期望 trades 次
    n_per_year = len(trades) / yrs if yrs > 0 else len(trades)
    if rets.std() > 0:
        sharpe = (rets.mean() / rets.std()) * np.sqrt(n_per_year)
    else:
        sharpe = 0.0

    # Max DD
    peaks = np.maximum.accumulate(equity)
    dd = (equity - peaks) / peaks
    mdd = dd.min() * 100

    # Holding days
    hds = np.array([t.holding_days for t in trades])

    # B&H baseline (從第一個訊號日到最後出場日)
    bh_window = df_ind.loc[trades[0].entry_date:trades[-1].exit_date]
    if not bh_window.empty:
        bh_close = bh_window["Close"].dropna().values
        bh_ret = (bh_close[-1] / bh_close[0] - 1) * 100
        bh_cagr = ((bh_close[-1] / bh_close[0]) ** (1 / yrs) - 1) * 100 if yrs > 0 else 0
        bh_equity = bh_close / bh_close[0]
        bh_peaks = np.maximum.accumulate(bh_equity)
        bh_dd = (bh_equity - bh_peaks) / bh_peaks
        bh_mdd = bh_dd.min() * 100
    else:
        bh_ret = bh_cagr = bh_mdd = 0.0

    return PerfStats(
        ticker=ticker,
        n_trades=len(trades),
        win_rate_pct=round(win_rate, 2),
        avg_return_pct=round(avg_ret, 3),
        avg_win_pct=round(avg_win, 3),
        avg_loss_pct=round(avg_loss, 3),
        profit_factor=round(pf, 3) if pf != float("inf") else float("inf"),
        total_return_pct=round(total, 2),
        cagr_pct=round(cagr, 2),
        sharpe=round(sharpe, 3),
        max_drawdown_pct=round(mdd, 2),
        avg_holding_days=round(hds.mean(), 1),
        median_holding_days=round(float(np.median(hds)), 1),
        bh_total_return_pct=round(bh_ret, 2),
        bh_cagr_pct=round(bh_cagr, 2),
        bh_max_drawdown_pct=round(bh_mdd, 2),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def run_one(
    ticker: str,
    start: Optional[str],
    end: Optional[str],
    save_trades: bool = True,
    switch_atr_mult: float = 1.0,
    exit_on_adx_drop: bool = False,
) -> PerfStats:
    df_raw = load_ohlcv(ticker, start=start, end=end)
    df_ind = compute_indicators(df_raw)
    trades = run_backtest(df_ind, ticker,
                          switch_atr_mult=switch_atr_mult,
                          exit_on_adx_drop=exit_on_adx_drop)
    stats = perf_summary(trades, df_ind, ticker)

    if save_trades and trades:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        s = start or df_raw.index[0].strftime("%Y%m%d")
        e = end or df_raw.index[-1].strftime("%Y%m%d")
        out = REPORTS_DIR / f"trend_dmi_sar_{ticker}_{s}_{e}.csv"
        pd.DataFrame([asdict(t) for t in trades]).to_csv(out, index=False)
        print(f"  → 交易明細: {out}")

    return stats


def print_stats(stats: PerfStats) -> None:
    print(f"\n=== {stats.ticker} ===")
    print(f"  交易次數      : {stats.n_trades}")
    print(f"  勝率          : {stats.win_rate_pct:.2f}%")
    print(f"  平均報酬/筆   : {stats.avg_return_pct:+.3f}%  (win {stats.avg_win_pct:+.2f}% / loss {stats.avg_loss_pct:+.2f}%)")
    print(f"  Profit Factor : {stats.profit_factor}")
    print(f"  總報酬 (複利) : {stats.total_return_pct:+.2f}%")
    print(f"  CAGR          : {stats.cagr_pct:+.2f}%")
    print(f"  Sharpe        : {stats.sharpe:.3f}")
    print(f"  Max DD        : {stats.max_drawdown_pct:.2f}%")
    print(f"  平均持有      : {stats.avg_holding_days:.1f} 天 (median {stats.median_holding_days:.1f})")
    print(f"  --- B&H 比較 ---")
    print(f"  B&H 總報酬    : {stats.bh_total_return_pct:+.2f}%")
    print(f"  B&H CAGR      : {stats.bh_cagr_pct:+.2f}%")
    print(f"  B&H Max DD    : {stats.bh_max_drawdown_pct:.2f}%")


def main():
    ap = argparse.ArgumentParser(description="Trend-DMI-SAR strategy backtest")
    ap.add_argument("--ticker", type=str, help="單檔回測 (e.g. 2330)")
    ap.add_argument("--universe", type=str, help="多檔逗號分隔 (e.g. 2330,2454,0050)")
    ap.add_argument("--start", type=str, default="2014-01-01")
    ap.add_argument("--end", type=str, default=None)
    ap.add_argument("--no-save-trades", action="store_true")
    args = ap.parse_args()

    if not args.ticker and not args.universe:
        ap.error("--ticker 或 --universe 至少要給一個")

    tickers = [args.ticker] if args.ticker else [t.strip() for t in args.universe.split(",")]
    all_stats = []
    for t in tickers:
        try:
            s = run_one(t, args.start, args.end, save_trades=not args.no_save_trades)
            all_stats.append(s)
            print_stats(s)
        except Exception as e:
            print(f"[FAIL] {t}: {e}")

    if len(all_stats) > 1:
        print("\n=== Universe summary ===")
        df_summary = pd.DataFrame([asdict(s) for s in all_stats])
        print(df_summary[[
            "ticker", "n_trades", "win_rate_pct", "total_return_pct", "cagr_pct",
            "sharpe", "max_drawdown_pct", "bh_total_return_pct", "bh_cagr_pct", "bh_max_drawdown_pct"
        ]].to_string(index=False))
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORTS_DIR / f"trend_dmi_sar_summary_{args.start}_{args.end or 'latest'}.csv"
        df_summary.to_csv(out, index=False)
        print(f"\n→ Universe summary: {out}")


if __name__ == "__main__":
    main()
