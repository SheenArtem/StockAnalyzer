"""
build_systemic_chip_panel.py -- 機構撤退訊號 (Systemic Chip)

從 data_cache 內逐檔 chip CSV 聚合到大盤層級，產出：
  data/macro/systemic_chip.parquet

5 組訊號：
  Group A 外資撤退：foreign_holding_chg_4w / sbl_change_4w_pct / foreign_fut_net_chg_4w
  Group B 籌碼鬆動：margin_to_index_ratio (zscore) / short_to_long_ratio
  Group C 投信動能：trust_buy_streak / trust_5d_zscore (data/macro/institutional_total.parquet)
  Group D 期權對沖：pcr_oi / option_top1_concentration (data/sentiment/atm_put_premium.parquet)
  Group E ETF 流動：hyg_volume_z_252d / tlt_spy_chg_4w (data/macro/etf_flows.parquet)

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
MACRO = REPO / "data" / "macro"
OUT = MACRO / "systemic_chip.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)


def _safe_read_csv(path: Path, parse_dates: bool = True) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(path, index_col=0)
        df.index = pd.to_datetime(df.index, errors='coerce')
        df = df[~df.index.isna()]
        return df if not df.empty else None
    except Exception:
        return None


def _aggregate_consistent_sample(
    glob_pattern: str, col_name: str, file_suffix: str,
) -> pd.Series:
    """通用：合併所有股票某欄位的市場總和，用 stable-sample 防漂移。

    Bug fix 2026-05-09 (sbl + margin)：原版每天簡單 sum 但 ticker 集合不一致，
    任何一檔當天沒更新就把 total 拉低，造成 sbl_total 5/4 591M → 5/8 5.4M
    -99% 偽訊號。新版同 aggregate_foreign_holding：
      1. pivot 成 date × ticker matrix
      2. ffill 每檔股票（補單日缺值，limit=10）
      3. 只 sum 過去 252 天 ≥ 200 個交易日有資料的 ticker 子集（穩定樣本）
    """
    files = sorted(CACHE.glob(glob_pattern))
    logger.info("Aggregating %s from %d files", col_name, len(files))
    series_list = []
    for f in files:
        df = _safe_read_csv(f)
        if df is None or col_name not in df.columns:
            continue
        ticker = f.stem.replace(file_suffix, '')
        s = pd.to_numeric(df[col_name], errors='coerce').dropna()
        if len(s) < 100:
            continue
        s.name = ticker
        series_list.append(s)
    if not series_list:
        return pd.Series(dtype=float)

    wide = pd.concat(series_list, axis=1).sort_index()
    wide = wide.ffill(limit=10)

    # 穩定樣本：過去 252d 至少 200 天有資料才納入該日總和
    has_data_252d = wide.rolling(252, min_periods=200).count() >= 200
    valid = wide.where(has_data_252d)
    total = valid.sum(axis=1, min_count=1)  # min_count=1 避免空 row 變 0

    sample_size_last = int(has_data_252d.iloc[-1].sum())
    logger.info("  %s: %d days, sample stocks last day = %d",
                col_name, len(total), sample_size_last)
    return total


def aggregate_sbl() -> pd.Series:
    """SBL 借券賣出餘額大盤總額（穩定樣本版，2026-05-09 fix）。"""
    s = _aggregate_consistent_sample('*_sbl_chip.csv', '借券賣出餘額', '_sbl_chip')
    s.name = 'sbl_total'
    return s


def aggregate_margin() -> pd.DataFrame:
    """融資/融券餘額大盤總額（穩定樣本版，2026-05-09 fix）。"""
    long_total = _aggregate_consistent_sample('*_margin_chip.csv', '融資餘額', '_margin_chip')
    short_total = _aggregate_consistent_sample('*_margin_chip.csv', '融券餘額', '_margin_chip')
    df = pd.DataFrame({
        'margin_long_total': long_total,
        'margin_short_total': short_total,
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
    else:
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
    # 統一欄名：pc_ratio_oi -> pcr_oi
    if 'pc_ratio_oi' in df.columns and 'pcr_oi' not in df.columns:
        df = df.rename(columns={'pc_ratio_oi': 'pcr_oi'})
    return df


def load_futures_institutional() -> pd.DataFrame:
    """Group A S2-A：外資期貨淨部位，data/sentiment/futures_institutional.parquet"""
    p = SENT / "futures_institutional.parquet"
    if not p.exists():
        logger.warning("futures_institutional.parquet not found, skipping S2-A")
        return pd.DataFrame()
    df = pd.read_parquet(p)
    df['data_date'] = pd.to_datetime(df['data_date'])
    df = df.set_index('data_date').sort_index()
    return df


def load_institutional_total() -> pd.DataFrame:
    """Group C：大盤三大法人買賣，data/macro/institutional_total.parquet"""
    p = MACRO / "institutional_total.parquet"
    if not p.exists():
        logger.warning("institutional_total.parquet not found, skipping Group C")
        return pd.DataFrame()
    df = pd.read_parquet(p)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()
    return df


def load_atm_put_premium() -> pd.DataFrame:
    """Group D S2-D：期權 OI 集中度，data/sentiment/atm_put_premium.parquet"""
    p = SENT / "atm_put_premium.parquet"
    if not p.exists():
        logger.warning("atm_put_premium.parquet not found, skipping S2-D")
        return pd.DataFrame()
    df = pd.read_parquet(p)
    df['data_date'] = pd.to_datetime(df['data_date'])
    df = df.set_index('data_date').sort_index()
    return df


def load_etf_flows() -> pd.DataFrame:
    """Group E：ETF 流動，data/macro/etf_flows.parquet"""
    p = MACRO / "etf_flows.parquet"
    if not p.exists():
        logger.warning("etf_flows.parquet not found, skipping Group E")
        return pd.DataFrame()
    df = pd.read_parquet(p)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()
    return df


def build_panel() -> pd.DataFrame:
    sbl = aggregate_sbl()
    margin = aggregate_margin()
    foreign = aggregate_foreign_holding()
    pcr = load_pcr_history()
    fut_inst = load_futures_institutional()
    inst_total = load_institutional_total()
    atm_put = load_atm_put_premium()
    etf = load_etf_flows()

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

    # Group A S2-A：外資期貨淨部位 (futures_institutional)
    if not fut_inst.empty and 'foreign_net_oi' in fut_inst.columns:
        panel = panel.join(fut_inst[['foreign_net_oi']], how='left')
    else:
        panel['foreign_net_oi'] = np.nan

    # Group C：投信動能 (institutional_total)
    # trust_buy_streak 已在 institutional_total 預計算；trust_net 用於 z-score
    if not inst_total.empty:
        c_cols = []
        if 'trust_buy_streak' in inst_total.columns:
            c_cols.append('trust_buy_streak')
        if 'trust_net' in inst_total.columns:
            c_cols.append('trust_net')
        if c_cols:
            panel = panel.join(inst_total[c_cols], how='left')

    # Group D S2-D：option OI 集中度 (atm_put_premium)
    # top1 concentration = top_oi_oi_1 / sum(top_oi_oi_1~5)
    if not atm_put.empty:
        oi_cols = [f'top_oi_oi_{i}' for i in range(1, 6)
                   if f'top_oi_oi_{i}' in atm_put.columns]
        if oi_cols:
            atm_put['top5_oi_sum'] = atm_put[oi_cols].sum(axis=1)
            atm_put['option_top1_concentration'] = (
                atm_put['top_oi_oi_1'] / atm_put['top5_oi_sum'].replace(0, np.nan)
            )
            panel = panel.join(atm_put[['option_top1_concentration']], how='left')
        else:
            panel['option_top1_concentration'] = np.nan
    else:
        panel['option_top1_concentration'] = np.nan

    # Group E：ETF 流動 (etf_flows) — 直接接既有 derived 欄位
    if not etf.empty:
        e_cols = []
        if 'hyg_volume_z_252d' in etf.columns:
            e_cols.append('hyg_volume_z_252d')
        if 'tlt_spy_chg_4w' in etf.columns:
            e_cols.append('tlt_spy_chg_4w')
        if e_cols:
            panel = panel.join(etf[e_cols], how='left')

    # forward fill
    ffill_cols = [
        'sbl_total', 'margin_long_total', 'margin_short_total',
        'foreign_holding_avg', 'pcr_oi', 'foreign_net_oi',
        'trust_buy_streak', 'trust_net',
        'option_top1_concentration',
        'hyg_volume_z_252d', 'tlt_spy_chg_4w',
    ]
    for col in ffill_cols:
        if col in panel.columns:
            panel[col] = panel[col].ffill()

    # ============================================================
    # Derived signals
    # ============================================================

    # Group A 外資撤退：
    panel['foreign_holding_chg_4w'] = panel['foreign_holding_avg'].diff(20)
    panel['sbl_change_4w_pct'] = panel['sbl_total'].pct_change(20) * 100
    # S2-A: 外資期貨淨部位 4w 變動（raw contracts，非 pct）
    if 'foreign_net_oi' in panel.columns:
        panel['foreign_fut_net_chg_4w'] = panel['foreign_net_oi'].diff(20)
    else:
        panel['foreign_fut_net_chg_4w'] = np.nan

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

    # Group C 投信動能：trust_5d_zscore
    if 'trust_net' in panel.columns:
        trust_5d = panel['trust_net'].rolling(5).sum()
        trust_5d_mean = trust_5d.rolling(252).mean()
        trust_5d_std = trust_5d.rolling(252).std()
        panel['trust_5d_zscore'] = (trust_5d - trust_5d_mean) / trust_5d_std.replace(0, np.nan)
    else:
        panel['trust_5d_zscore'] = np.nan

    # ============================================================
    # Flags (簡化版規則：未經 IC 驗證；下一階段 Phase B 再校準)
    # ============================================================

    def flag_a(row):
        h = row.get('foreign_holding_chg_4w')
        sbl = row.get('sbl_change_4w_pct')
        fut_chg = row.get('foreign_fut_net_chg_4w')
        reasons = []
        if h is not None and not pd.isna(h) and h < -0.3:
            reasons.append(f"外資持股率 4w {h:+.2f}pp")
        if sbl is not None and not pd.isna(sbl) and sbl > 15:
            reasons.append(f"借券賣出 4w +{sbl:.0f}%")
        if fut_chg is not None and not pd.isna(fut_chg) and fut_chg < -20000:
            reasons.append(f"外資期貨淨部位 4w {fut_chg:+.0f}口")
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

    def flag_c(row):
        streak = row.get('trust_buy_streak')
        z = row.get('trust_5d_zscore')
        reasons = []
        if streak is not None and not pd.isna(streak):
            if streak >= 5:
                reasons.append(f"投信連買 {int(streak)}天")
            elif streak >= 3:
                reasons.append(f"投信連買 {int(streak)}天 (短)")
        if z is not None and not pd.isna(z) and z > 1.5:
            reasons.append(f"投信5d zscore {z:.2f}")
        if len(reasons) >= 2:
            return 'high', ' / '.join(reasons)
        if len(reasons) == 1:
            # streak >= 5 alone -> high; streak 3-4 alone -> mid
            if streak is not None and not pd.isna(streak) and streak >= 5 and 'zscore' not in reasons[0]:
                return 'high', reasons[0]
            return 'mid', reasons[0]
        return 'low', ''

    def flag_d(row):
        pcr_val = row.get('pcr_oi')
        conc = row.get('option_top1_concentration')
        reasons = []
        if pcr_val is not None and not pd.isna(pcr_val):
            if pcr_val > 1.3:
                reasons.append(f"PCR_OI {pcr_val:.2f} (避險高)")
            elif pcr_val > 1.1:
                reasons.append(f"PCR_OI {pcr_val:.2f}")
        if conc is not None and not pd.isna(conc) and conc > 0.4:
            reasons.append(f"Put OI top1集中度 {conc:.2f}")
        if len(reasons) >= 2:
            return 'high', ' / '.join(reasons)
        if len(reasons) == 1:
            return 'mid', reasons[0]
        return 'low', ''

    def flag_e(row):
        hyg_z = row.get('hyg_volume_z_252d')
        tlt_spy = row.get('tlt_spy_chg_4w')
        reasons = []
        # hyg_volume_z_252d 高位（|z| > 1.5）代表恐慌性拋售或追捧，方向看 hyg 本身
        if hyg_z is not None and not pd.isna(hyg_z) and abs(hyg_z) > 1.5:
            dir_str = "放量" if hyg_z > 0 else "縮量"
            reasons.append(f"HYG成交量z {hyg_z:.2f} ({dir_str})")
        # tlt_spy_chg_4w > 0 代表資金避險（TLT 跑贏 SPY）
        if tlt_spy is not None and not pd.isna(tlt_spy) and tlt_spy > 3:
            reasons.append(f"TLT/SPY 4w {tlt_spy:+.2f}% (避險)")
        if len(reasons) >= 2:
            return 'high', ' / '.join(reasons)
        if len(reasons) == 1:
            return 'mid', reasons[0]
        return 'low', ''

    flags_a = panel.apply(flag_a, axis=1)
    flags_b = panel.apply(flag_b, axis=1)
    flags_c = panel.apply(flag_c, axis=1)
    flags_d = panel.apply(flag_d, axis=1)
    flags_e = panel.apply(flag_e, axis=1)
    panel['group_a_flag'] = [x[0] for x in flags_a]
    panel['group_a_reason'] = [x[1] for x in flags_a]
    panel['group_b_flag'] = [x[0] for x in flags_b]
    panel['group_b_reason'] = [x[1] for x in flags_b]
    panel['group_c_flag'] = [x[0] for x in flags_c]
    panel['group_c_reason'] = [x[1] for x in flags_c]
    panel['group_d_flag'] = [x[0] for x in flags_d]
    panel['group_d_reason'] = [x[1] for x in flags_d]
    panel['group_e_flag'] = [x[0] for x in flags_e]
    panel['group_e_reason'] = [x[1] for x in flags_e]

    panel = panel.reset_index().rename(columns={'index': 'date'})
    panel['date'] = pd.to_datetime(panel['date'])
    return panel


def main():
    panel = build_panel()
    logger.info("Panel rows=%d cols=%d", len(panel), len(panel.columns))
    logger.info("Date range: %s ~ %s", panel['date'].min(), panel['date'].max())

    # Last 5 rows flag summary
    flag_cols = [c for c in panel.columns if c.endswith('_flag')]
    reason_cols = [c for c in panel.columns if c.endswith('_reason')]
    last5 = panel.tail(5)[['date'] + flag_cols + reason_cols]
    logger.info("Last 5 rows flag summary:")
    for _, row in last5.iterrows():
        date_str = str(row['date'].date()) if hasattr(row['date'], 'date') else str(row['date'])[:10]
        flags_str = ' | '.join(f"{c.replace('group_','').replace('_flag','')}={row[c]}" for c in flag_cols)
        logger.info("  %s  %s", date_str, flags_str)

    # Flag distribution across full history
    logger.info("Flag distribution (full history):")
    for c in flag_cols:
        if c in panel.columns:
            dist = panel[c].value_counts().to_dict()
            logger.info("  %s: %s", c, dist)

    panel.to_parquet(OUT, index=False)
    logger.info("Saved -> %s", OUT)


if __name__ == '__main__':
    main()
