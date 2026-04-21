"""
market_regime_logger.py
=======================
每日 scan 時執行，計算當天 market regime 並 append 到 regime_log.jsonl。

用途：VF-G4 shadow run — 不改 scanner 邏輯，但累積 regime log 供事後比對
「如果只 volatile 時 scan」的表現。

Regime rules（對齊 qm_historical_simulator.build_regime_series）：
  - trending:  20d return > 5%
  - volatile:  20d high-low range / avg > 8%
  - ranging:   abs(20d ret) < 2% and range <= 8%
  - neutral:   其他

Market proxy：等權 top300（對齊 VF-G4 驗證邏輯）。

Output: data/tracking/regime_log.jsonl
  {"date":"2026-04-21","regime":"volatile","ret_20d":0.024,"range_20d":0.092,"sharpe_60d":1.8}
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("regime_log")

OHLCV_PATH = ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"
LOG_PATH = ROOT / "data" / "tracking" / "regime_log.jsonl"


def load_top300():
    """從 top300_universe.json 讀 universe。"""
    p = ROOT / "data_cache" / "backtest" / "top300_universe.json"
    if p.exists():
        return json.loads(p.read_text(encoding='utf-8'))
    logger.warning("top300_universe.json 不存在，fallback 用 qm_result.json 內所有 picks")
    qm = ROOT / "data" / "latest" / "qm_result.json"
    if qm.exists():
        data = json.loads(qm.read_text(encoding='utf-8'))
        return [p['stock_id'] for p in data.get('results', data.get('picks', []))[:300]]
    return []


def _twse_trading_days_between(start: pd.Timestamp, end: pd.Timestamp) -> list:
    """回傳 start < d <= end 之間的 TWSE 交易日（含 end，排除 start 當日）。
    用 pandas 工作日（週一到五）近似；台灣假日較多但 TWSE bulk 會回 404 自動跳過。"""
    days = pd.bdate_range(start=start + pd.Timedelta(days=1), end=end)
    return [d for d in days]


def _fetch_todays_close_from_twse(universe: list, dates: list) -> pd.Series:
    """對每個日期呼叫 TWSE bulk，過濾 universe 算等權 close。
    回傳 Series indexed by date。"""
    from twse_api import TWSEOpenData
    api = TWSEOpenData()
    result = {}
    univ_set = set(universe)
    for d in dates:
        try:
            df = api.get_market_daily_all(date=d.to_pydatetime())
            if df is None or df.empty:
                logger.debug("  TWSE bulk %s: empty (假日?)", d.strftime('%Y-%m-%d'))
                continue
            # 過濾 universe
            df = df[df['stock_id'].astype(str).isin(univ_set)]
            if df.empty:
                continue
            # 等權 close
            close = pd.to_numeric(df['close'], errors='coerce').dropna()
            if close.empty:
                continue
            avg = float(close.mean())
            result[d] = avg
            logger.info("  TWSE bulk %s: %d stocks matched, avg=%.2f",
                        d.strftime('%Y-%m-%d'), len(close), avg)
        except Exception as e:
            logger.warning("  TWSE bulk %s failed: %s", d.strftime('%Y-%m-%d'), e)
    return pd.Series(result).sort_index() if result else pd.Series(dtype='float64')


def compute_today_regime() -> dict:
    """Compute today's market regime from cached OHLCV + TWSE live supplement."""
    logger.info("Loading OHLCV: %s", OHLCV_PATH)
    ohlcv = pd.read_parquet(OHLCV_PATH)
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])

    universe = load_top300()
    if not universe:
        raise RuntimeError("Cannot determine universe for market proxy")
    logger.info("Universe: %d stocks", len(universe))

    proxy = ohlcv[ohlcv['stock_id'].isin(universe)].copy()
    # Equal-weight daily close index
    daily_avg = proxy.groupby('date')['Close'].mean().sort_index()

    if len(daily_avg) < 60:
        raise RuntimeError(f"Insufficient history: {len(daily_avg)} days")

    # 若 parquet 落後，從 TWSE bulk 補齊到今天
    parquet_latest = daily_avg.index[-1]
    today = pd.Timestamp.now().normalize()
    missing_days = _twse_trading_days_between(parquet_latest, today)
    if missing_days:
        logger.info("Parquet latest=%s, today=%s → fetching %d missing days from TWSE",
                    parquet_latest.date(), today.date(), len(missing_days))
        supplement = _fetch_todays_close_from_twse(universe, missing_days)
        if not supplement.empty:
            daily_avg = pd.concat([daily_avg, supplement]).sort_index()
            logger.info("Extended daily_avg: %d → %d days (now through %s)",
                        len(daily_avg) - len(supplement), len(daily_avg),
                        daily_avg.index[-1].date())
    else:
        logger.info("Parquet is up to date (latest=%s)", parquet_latest.date())

    # Rolling windows
    ret20 = daily_avg.pct_change(20)
    rolling_max = daily_avg.rolling(20, min_periods=10).max()
    rolling_min = daily_avg.rolling(20, min_periods=10).min()
    rolling_avg = daily_avg.rolling(20, min_periods=10).mean()
    range20 = (rolling_max - rolling_min) / rolling_avg.replace(0, np.nan)
    daily_ret = daily_avg.pct_change()
    sharpe60 = (
        daily_ret.rolling(60, min_periods=30).mean() /
        daily_ret.rolling(60, min_periods=30).std().replace(0, np.nan) *
        np.sqrt(60)
    )

    # 今天（最後 1 日）
    today = daily_avg.index[-1]
    r20 = ret20.iloc[-1]
    rng20 = range20.iloc[-1]
    s60 = sharpe60.iloc[-1]

    # Rule-based classification（對齊 VF-G4 驗證）
    regime = 'neutral'
    if range20.iloc[-1] > 0.08:
        regime = 'volatile'
    elif ret20.iloc[-1] > 0.05:
        regime = 'trending'
    elif abs(ret20.iloc[-1]) < 0.02 and range20.iloc[-1] <= 0.08:
        regime = 'ranging'

    return {
        'date': today.strftime('%Y-%m-%d'),
        'regime': regime,
        'ret_20d': round(float(r20), 4) if not pd.isna(r20) else None,
        'range_20d': round(float(rng20), 4) if not pd.isna(rng20) else None,
        'sharpe_60d': round(float(s60), 3) if not pd.isna(s60) else None,
        'proxy': 'equal_weight_top300',
    }


def append_log(entry: dict) -> bool:
    """Append to regime_log.jsonl；若 date 已存在則覆蓋。"""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Read existing
    existing = {}
    if LOG_PATH.exists():
        for line in LOG_PATH.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                existing[rec['date']] = rec
            except Exception:
                continue

    # Upsert
    replaced = entry['date'] in existing
    existing[entry['date']] = entry

    # Write back sorted
    with open(LOG_PATH, 'w', encoding='utf-8') as f:
        for date in sorted(existing.keys()):
            f.write(json.dumps(existing[date], ensure_ascii=False) + '\n')

    return replaced


def main():
    entry = compute_today_regime()
    replaced = append_log(entry)
    logger.info("Regime: %s (ret_20d=%s, range_20d=%s)  [%s]",
                entry['regime'], entry['ret_20d'], entry['range_20d'],
                'replaced' if replaced else 'new')
    logger.info("Log: %s", LOG_PATH)


if __name__ == "__main__":
    main()
