"""
build_systemic_chip_panel.py -- 機構撤退訊號 (Systemic Chip)

從 data_cache 內逐檔 chip CSV 聚合到大盤層級，產出：
  data/macro/systemic_chip.parquet

5 組訊號：
  Group A 外資撤退：foreign_holding_chg_4w / sbl_change_4w_pct / foreign_fut_net_oi
  Group B 籌碼鬆動：margin_to_index_ratio (zscore) / short_to_long_ratio
  Group C 投信動能：trust_buy_streak (per-stock chip 太短，先 stub)
  Group D 期權對沖：pcr_oi (從 pcr_history) / top5_top10_oi_diff (TAIFEX, stub)
  Group E ETF 流動：etf_redemption_streak (stub)

Phase 1 實作 A/B 兩組（資料齊），C/D 部分用既有 sentiment/pcr_history，E 留 stub。

執行：python tools/build_systemic_chip_panel.py
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
CACHE = REPO / "data_cache"
SENT = REPO / "data" / "sentiment"
OUT = REPO / "data" / "macro" / "systemic_chip.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)


def _safe_read_csv(path: Path, parse_dates: bool = True) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(path, index_col=0)
        df.index = pd.to_datetime(df.index, errors='coerce')
        df = df[~df.index.isna()]
        return df if not df.empty else None
    except Exception:
        return None


def aggregate_sbl() -> pd.Series:
    """合併所有股票 SBL 借券賣出餘額 → 大盤總額。"""
    files = sorted(CACHE.glob("*_sbl_chip.csv"))
    logger.info("Aggregating SBL from %d files", len(files))
    daily_sum = {}
    for f in files:
        df = _safe_read_csv(f)
        if df is None or '借券賣出餘額' not in df.columns:
            continue
        s = pd.to_numeric(df['借券賣出餘額'], errors='coerce').dropna()
        for d, v in s.items():
            daily_sum[d] = daily_sum.get(d, 0) + v
    return pd.Series(daily_sum, name='sbl_total').sort_index()


def aggregate_margin() -> pd.DataFrame:
    """合併所有股票 融資/融券餘額 → 大盤總額。"""
    files = sorted(CACHE.glob("*_margin_chip.csv"))
    logger.info("Aggregating margin from %d files", len(files))
    long_sum = {}
    short_sum = {}
    for f in files:
        df = _safe_read_csv(f)
        if df is None:
            continue
        if '融資餘額' in df.columns:
            s = pd.to_numeric(df['融資餘額'], errors='coerce').dropna()
            for d, v in s.items():
                long_sum[d] = long_sum.get(d, 0) + v
        if '融券餘額' in df.columns:
            s = pd.to_numeric(df['融券餘額'], errors='coerce').dropna()
            for d, v in s.items():
                short_sum[d] = short_sum.get(d, 0) + v
    df = pd.DataFrame({
        'margin_long_total': pd.Series(long_sum),
        'margin_short_total': pd.Series(short_sum),
    }).sort_index()
    return df


def aggregate_foreign_holding() -> pd.Series:
    """外資持股率市場 median（per-stock 樣本一致版）。

    Bug fix 2026-05-09: 原版每天 mean across 103 stocks 但 stocks 不一致
    (e.g. 新股加入或下市)，造成 4w chg 突然 +12pp 偽訊號。新版：
      1. pivot 成 date × ticker matrix
      2. ffill 每檔股票 (handles 缺值)
      3. 只取連續 252 天都有資料的 stocks 子集做 median
      4. median 比 mean 更 robust to outliers
    """
    files = sorted(CACHE.glob("*_shareholding_chip.csv"))
    logger.info("Aggregating foreign holding from %d files", len(files))

    series_list = []
    for f in files:
        df = _safe_read_csv(f)
        if df is None or 'ForeignHoldingRatio' not in df.columns:
            continue
        ticker = f.stem.replace('_shareholding_chip', '')
        s = pd.to_numeric(df['ForeignHoldingRatio'], errors='coerce').dropna()
        if len(s) < 100:
            continue
        s.name = ticker
        series_list.append(s)

    wide = pd.concat(series_list, axis=1).sort_index()
    wide = wide.ffill(limit=10)  # ffill up to 10 days

    # 對每天計算 median，但要求該檔股票過去 252 天有 ≥ 200 個非 NaN
    has_data_252d = wide.rolling(252, min_periods=200).count() >= 200
    valid = wide.where(has_data_252d)
    median_series = valid.median(axis=1)

    median_series.name = 'foreign_holding_median'
    logger.info("Median panel: %d days, sample stocks last day = %d",
                len(median_series), int(has_data_252d.iloc[-1].sum()))
    return median_series


def load_pcr_history() -> pd.DataFrame:
    p = SENT / "pcr_history.parquet"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_parquet(p)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()
    return df


def build_panel() -> pd.DataFrame:
    sbl = aggregate_sbl()
    margin = aggregate_margin()
    foreign = aggregate_foreign_holding()
    pcr = load_pcr_history()

    panel = pd.DataFrame(index=sbl.index)
    panel['sbl_total'] = sbl
    panel = panel.join(margin, how='outer')
    # 注意：欄名保留 foreign_holding_avg 但實際是 stable-sample median (2026-05-09 fix)
    panel['foreign_holding_avg'] = foreign
    panel = panel.sort_index()

    # 加 TAIEX 收盤（用 ^TWII 從 yfinance）
    try:
        import yfinance as yf
        twii = yf.Ticker('^TWII').history(period='15y')['Close']
        twii.index = twii.index.tz_localize(None) if twii.index.tz else twii.index
        twii.index = pd.to_datetime(twii.index.date)
        panel = panel.join(twii.rename('twii_close'), how='left')
        panel['twii_close'] = panel['twii_close'].ffill()
    except Exception as e:
        logger.warning("Failed to fetch ^TWII: %s", e)
        panel['twii_close'] = np.nan

    # PCR
    if not pcr.empty and 'pcr_oi' in pcr.columns:
        panel = panel.join(pcr[['pcr_oi']], how='left')

    # forward fill
    for col in ['sbl_total', 'margin_long_total', 'margin_short_total',
                'foreign_holding_avg', 'pcr_oi']:
        if col in panel.columns:
            panel[col] = panel[col].ffill()

    # ============================================================
    # Derived signals
    # ============================================================

    # Group A 外資撤退：
    panel['foreign_holding_chg_4w'] = panel['foreign_holding_avg'].diff(20)
    panel['sbl_change_4w_pct'] = panel['sbl_total'].pct_change(20) * 100

    # Group B 籌碼鬆動：
    if 'twii_close' in panel.columns:
        panel['margin_to_index_ratio'] = panel['margin_long_total'] / panel['twii_close']
        panel['margin_ratio_z_252d'] = (
            (panel['margin_to_index_ratio'] - panel['margin_to_index_ratio'].rolling(252).mean()) /
            panel['margin_to_index_ratio'].rolling(252).std()
        )
    panel['short_to_long_ratio'] = (
        panel['margin_short_total'] / panel['margin_long_total'].replace(0, np.nan)
    )

    # ============================================================
    # Flags (簡化版規則：未經 IC 驗證；下一階段 Phase B 再校準)
    # ============================================================

    def flag_a(row):
        h = row.get('foreign_holding_chg_4w')
        sbl = row.get('sbl_change_4w_pct')
        reasons = []
        if h is not None and not pd.isna(h) and h < -0.3:
            reasons.append(f"外資持股率 4w {h:+.2f}pp")
        if sbl is not None and not pd.isna(sbl) and sbl > 15:
            reasons.append(f"借券賣出 4w +{sbl:.0f}%")
        if len(reasons) >= 2:
            return 'high', ' / '.join(reasons)
        if len(reasons) == 1:
            return 'mid', reasons[0]
        return 'low', ''

    def flag_b(row):
        z = row.get('margin_ratio_z_252d')
        sl = row.get('short_to_long_ratio')
        reasons = []
        if z is not None and not pd.isna(z) and z > 1.5:
            reasons.append(f"融資/指數 z {z:+.2f}")
        if sl is not None and not pd.isna(sl) and sl < 0.05:
            reasons.append(f"短/多比 {sl:.3f} (極低)")
        if len(reasons) >= 2:
            return 'high', ' / '.join(reasons)
        if len(reasons) == 1:
            return 'mid', reasons[0]
        return 'low', ''

    def flag_d(row):
        pcr = row.get('pcr_oi')
        if pcr is None or pd.isna(pcr):
            return 'low', ''
        if pcr > 1.3:
            return 'high', f"PCR_OI {pcr:.2f} (避險高)"
        if pcr > 1.1:
            return 'mid', f"PCR_OI {pcr:.2f}"
        return 'low', ''

    flags_a = panel.apply(flag_a, axis=1)
    flags_b = panel.apply(flag_b, axis=1)
    flags_d = panel.apply(flag_d, axis=1)
    panel['group_a_flag'] = [x[0] for x in flags_a]
    panel['group_a_reason'] = [x[1] for x in flags_a]
    panel['group_b_flag'] = [x[0] for x in flags_b]
    panel['group_b_reason'] = [x[1] for x in flags_b]
    panel['group_c_flag'] = 'low'  # stub
    panel['group_d_flag'] = [x[0] for x in flags_d]
    panel['group_e_flag'] = 'low'  # stub

    panel = panel.reset_index().rename(columns={'index': 'date'})
    panel['date'] = pd.to_datetime(panel['date'])
    return panel


def main():
    panel = build_panel()
    logger.info("Panel rows=%d cols=%d", len(panel), len(panel.columns))
    logger.info("Date range: %s ~ %s", panel['date'].min(), panel['date'].max())
    last = panel.iloc[-1].to_dict()
    logger.info("Last row keys with values:")
    for k, v in last.items():
        logger.info("  %s = %s", k, v)
    panel.to_parquet(OUT, index=False)
    logger.info("Saved -> %s", OUT)


if __name__ == '__main__':
    main()
