"""
build_systemic_chip_panel.py -- 機構撤退訊號 (Systemic Chip)

從 data_cache 內逐檔 chip CSV 聚合到大盤層級，產出：
  data/macro/systemic_chip.parquet

5 組訊號：
  Group A 外資撤退：foreign_holding_chg_4w / sbl_change_4w_pct / foreign_fut_net_chg_4w
  Group B 籌碼鬆動：margin_to_index_ratio (zscore) / short_to_long_ratio /
                    margin_to_mktcap_pct (融資餘額佔市值比, 官方金額/上市總市值) + z
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
CHIP_HISTORY = CACHE / "chip_history"  # chip_history_dl.py 輸出 {margin,short_sale,institutional}.parquet
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


def _aggregate_consistent_sample_parquet(
    parquet_path: Path, value_col: str,
) -> pd.Series:
    """同 _aggregate_consistent_sample，但讀 chip_history long-format parquet
    (date, stock_id, <value_col>, ...) 而非 legacy per-stock CSV。

    Repoint 2026-05-30 (df53942 後續)：legacy *_margin_chip.csv / *_sbl_chip.csv
    自 2026-05-23 scanner 瘦身後停更（per-stock chip 抓取搭在已停的 QM/Value 掃描
    裡），而 chip_history/{margin,short_sale}.parquet 由 TDCC weekly bat (margin/
    short_sale --resume) + daily scanner 刷到最新。改吃 parquet 才不會卡舊資料。
    universe 較大 (1842 vs 106) 但 stable-sample 邏輯不變；parquet 起點 2021-04，
    早於此的 margin/sbl 值為 NaN（dashboard 看現值，252d 視窗 2022+ 即完整）。
    """
    if not parquet_path.exists():
        logger.warning("  %s not found, returning empty", parquet_path.name)
        return pd.Series(dtype=float)
    df = pd.read_parquet(parquet_path, columns=['date', 'stock_id', value_col])
    df['date'] = pd.to_datetime(df['date'])
    # pivot date x stock_id（aggfunc=last 防同日重複）
    wide = df.pivot_table(index='date', columns='stock_id', values=value_col,
                          aggfunc='last').sort_index()
    wide = wide.ffill(limit=10)
    # 穩定樣本：過去 252d 至少 200 天有資料才納入該日總和（同 CSV 版）
    has_data_252d = wide.rolling(252, min_periods=200).count() >= 200
    valid = wide.where(has_data_252d)
    total = valid.sum(axis=1, min_count=1)
    sample_size_last = int(has_data_252d.iloc[-1].sum()) if len(has_data_252d) else 0
    logger.info("  %s (parquet): %d days, sample stocks last day = %d",
                value_col, len(total), sample_size_last)
    return total


def aggregate_sbl() -> pd.Series:
    """SBL 借券賣出餘額大盤總額（穩定樣本版；2026-05-30 改吃 chip_history parquet）。"""
    s = _aggregate_consistent_sample_parquet(
        CHIP_HISTORY / "short_sale.parquet", 'sbl_balance')
    s.name = 'sbl_total'
    return s


def aggregate_margin() -> pd.DataFrame:
    """融資/融券餘額大盤總額（穩定樣本版；2026-05-30 改吃 chip_history parquet）。
    margin_balance=融資餘額 / short_balance=融券餘額（已對 2330 cross-check 同尺度）。"""
    long_total = _aggregate_consistent_sample_parquet(
        CHIP_HISTORY / "margin.parquet", 'margin_balance')
    short_total = _aggregate_consistent_sample_parquet(
        CHIP_HISTORY / "margin.parquet", 'short_balance')
    df = pd.DataFrame({
        'margin_long_total': long_total,
        'margin_short_total': short_total,
    }).sort_index()
    return df


# 0050 固定 universe（FTSE Russell TW50 index Q1-2026 x data_cache 交集）
# 來源：元大 0050 官方季度調整公告 2025-Q4 + TWSE 成分股申報
# 時間戳：2026-05-09；大型藍籌變動慢，下次調整建議 2027-Q1 前複查
# 共 23 檔（完整 50 檔中有 27 檔不在 scanner 追蹤範圍，不在 data_cache）
TW0050_FIXED_UNIVERSE = [
    '2330', '2317', '2454', '2308', '2382', '2303', '3711',
    '1303', '1326', '2357', '2376', '2408', '2327', '2301',
    '3017', '2344', '2345', '2383', '6669', '6770', '3231',
    '2409', '3443',
]


def aggregate_foreign_holding(
    universe: list[str] | None = None,
) -> pd.Series:
    """外資持股率大盤 median（0050 固定 universe 版）。

    Fix 2026-05-09: 舊版 stable-sample filter 仍有週末跳問題：
      - chips_history_dl 每週更新，帶入新 ticker -> universe 每日不同
      - stable-sample 252d 過濾 per-day 通過/落選 -> 成分組合仍漂移
      - 結果：4w chg 出現 +12pp 偽訊號

    新版改用 0050 固定 23 檔 large-cap universe：
      1. 從 TW0050_FIXED_UNIVERSE 拉 ForeignHoldingRatio
      2. pivot -> date x ticker matrix
      3. ffill limit=30（覆蓋 6 週 chip refresh 間隔）
      4. 每日 median across FIXED 23 tickers（universe 不漂移）

    驗證：2026-01-01 後零週末跳；全期 4w chg std=1.31pp（舊版 2.03pp）
    """
    if universe is None:
        universe = TW0050_FIXED_UNIVERSE

    logger.info("aggregate_foreign_holding: fixed universe %d tickers", len(universe))
    series_list = []
    missing = []
    for ticker in universe:
        f = CACHE / f"{ticker}_shareholding_chip.csv"
        if not f.exists():
            missing.append(ticker)
            continue
        df = _safe_read_csv(f)
        if df is None or 'ForeignHoldingRatio' not in df.columns:
            missing.append(ticker)
            continue
        s = pd.to_numeric(df['ForeignHoldingRatio'], errors='coerce').dropna()
        if len(s) < 100:
            missing.append(ticker)
            continue
        s.name = ticker
        series_list.append(s)

    if missing:
        logger.warning("  Missing from cache (will be excluded): %s", missing)
    if not series_list:
        logger.error("  No ForeignHoldingRatio data found for any universe ticker")
        return pd.Series(dtype=float, name='foreign_holding_median')

    wide = pd.concat(series_list, axis=1).sort_index()
    # ffill limit=30: 覆蓋 ~6 週缺口（chip_history_dl 週度更新頻率）
    wide = wide.ffill(limit=30)

    median_series = wide.median(axis=1)
    median_series.name = 'foreign_holding_median'

    n_last = wide.iloc[-1].notna().sum()
    logger.info("  Median panel: %d days, n_tickers last day = %d / %d",
                len(median_series), n_last, len(series_list))
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


def load_market_cap() -> pd.DataFrame:
    """上市總市值 + 融資餘額佔市值比 (data/macro/market_cap.parquet,
    由 build_market_cap_panel.py 產：官方 MI_MARGN 融資金額 / Sum(收盤x發行股數))。
    回 DataFrame index=date, cols=[margin_to_mktcap_pct, margin_mktcap_z_252d, total_market_cap]。"""
    p = MACRO / "market_cap.parquet"
    if not p.exists():
        logger.warning("  market_cap.parquet not found (run tools/build_market_cap_panel.py)")
        return pd.DataFrame()
    df = pd.read_parquet(p)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date').sort_index()


def build_panel() -> pd.DataFrame:
    sbl = aggregate_sbl()
    margin = aggregate_margin()
    foreign = aggregate_foreign_holding()
    pcr = load_pcr_history()
    fut_inst = load_futures_institutional()
    inst_total = load_institutional_total()
    atm_put = load_atm_put_premium()
    etf = load_etf_flows()

    # 2026-05-30: panel index 改用所有來源的 union，不再只錨定 sbl。
    # sbl/margin 改吃 chip_history parquet 後起點 2021-04，若仍錨 sbl.index 會把
    # institutional_total(2014+) 等較長歷史一起截斷（2532->1245 rows）。union 後
    # margin/sbl 在 2021 前自然為 NaN，其餘 group 保留完整歷史。
    # union 只取核心籌碼序列 (sbl/margin/foreign/institutional)，不含 pcr/etf/fut
    # (那些是 left-join 補充，pcr 有 2002+ 深歷史會把 panel 灌成稀疏 6000+ 列)。
    # 給出 ~2014+ (institutional_total 起點)，接近原版 2016 起點。
    idx = sbl.index
    for _src in (margin, foreign, inst_total):
        if _src is not None and len(getattr(_src, 'index', [])):
            idx = idx.union(_src.index)
    panel = pd.DataFrame(index=idx.sort_values())
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
        # fail loud: yfinance ^TWII 偶爾延遲一天 → 最後幾列 ffill 會是「假收盤」
        # (2026-06-04 案例: panel 6/4 列被填 6/3 收盤, 大盤位階乖離率失真)
        if len(panel) and twii.index.max() < panel.index.max():
            logger.warning(
                "^TWII last bar %s < panel spine last %s -> tail twii_close is "
                "ffill-stale, dist_ma* on those rows uses a stale close",
                twii.index.max().date(), panel.index.max().date())
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

    # Group C：投信動能 + 外資現貨日買賣超 (institutional_total)
    # trust_* = 投信動能；foreign_* = 外資現貨流量(快流量)，與 foreign_net_oi(期貨)
    # 併看可直接驗證「多現貨、空期貨」避險結構 vs 方向性看空 (報告 §3 核心論點)
    if not inst_total.empty:
        c_cols = [c for c in (
            'trust_buy_streak', 'trust_net',
            'foreign_investor_net', 'foreign_total_net',
            'foreign_buy_streak', 'foreign_sell_streak',
            'foreign_cum_5d', 'foreign_cum_20d',
        ) if c in inst_total.columns]
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
        if 'hyg_dollar_flow_z_252d' in etf.columns:  # 報告 §5: 金額流向比成交量 z 更直接的資金流訊號
            e_cols.append('hyg_dollar_flow_z_252d')
        if e_cols:
            panel = panel.join(etf[e_cols], how='left')

    # forward fill
    ffill_cols = [
        'sbl_total', 'margin_long_total', 'margin_short_total',
        'foreign_holding_avg', 'pcr_oi', 'foreign_net_oi',
        'trust_buy_streak', 'trust_net',
        'foreign_investor_net', 'foreign_total_net',
        'foreign_buy_streak', 'foreign_sell_streak',
        'foreign_cum_5d', 'foreign_cum_20d',
        'option_top1_concentration',
        'hyg_volume_z_252d', 'tlt_spy_chg_4w', 'hyg_dollar_flow_z_252d',
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

    # Group B 補：融資餘額佔市值比 (官方融資金額 / 上市總市值, build_market_cap_panel.py)
    # 真實可讀 % (vs margin_to_index_ratio 因分母是指數點數只能看 z)。
    # margin_mktcap_z_252d 為其 252d z-score；訊號上與 margin_ratio_z_252d 高度一致。
    # margin_maintenance_pct (2026-06-04 加): 大盤融資維持率 = 擔保品市值/官方融資金額,
    # 急跌時驟降 = 斷頭賣壓 (informational, 非 IC composite)。
    mcap = load_market_cap()
    for _c in ('margin_to_mktcap_pct', 'margin_mktcap_z_252d', 'total_market_cap',
               'margin_maintenance_pct'):
        if not mcap.empty and _c in mcap.columns:
            panel[_c] = mcap[_c].reindex(panel.index).ffill()
        else:
            panel[_c] = np.nan

    # Group C 投信動能：trust_5d_zscore
    if 'trust_net' in panel.columns:
        trust_5d = panel['trust_net'].rolling(5).sum()
        trust_5d_mean = trust_5d.rolling(252).mean()
        trust_5d_std = trust_5d.rolling(252).std()
        panel['trust_5d_zscore'] = (trust_5d - trust_5d_mean) / trust_5d_std.replace(0, np.nan)
    else:
        panel['trust_5d_zscore'] = np.nan

    # 指數均線乖離率 (twii_close vs 20/50/200 日均線) -- 補「trigger price」缺口
    # 乖離率 = (close - MA) / MA * 100；公式同 system3 ma_dist。panel 已 date-sorted
    # (上方 margin_ratio_z_252d 用 rolling(252) 已隱含)。MA 絕對值供報告引用具體點位。
    if 'twii_close' in panel.columns:
        c = panel['twii_close']
        for w in (20, 50, 200):
            ma = c.rolling(w, min_periods=max(5, w // 2)).mean()
            panel[f'twii_ma{w}'] = ma
            panel[f'twii_dist_ma{w}'] = (c - ma) / ma.replace(0, np.nan) * 100

    # 絕對百分位 (rolling 10yr rank, 無 look-ahead) -- 供 flag 的「絕對極值閘」。
    # 報告 §5: A/D/E 原規則只看 4w 變化 / 固定絕對門檻, 會漏掉史上極值(外資期淨OI 0%、
    # 選擇權集中度 95%); 補百分位閘讓燈號反映史上尾部。pattern 同 hy_oas_rank。
    if 'foreign_net_oi' in panel.columns:  # 期貨史長(2016+), min_periods=252 足夠
        panel['foreign_net_oi_pctile'] = (
            panel['foreign_net_oi'].rolling(2520, min_periods=252).rank(pct=True) * 100
        )
    # option_top1_concentration 資料稀疏(atm_put_premium 僅近期數十日)，min_periods=252 會全 NaN
    # 害集中度閘永遠不觸發；改用其自身有效史 min_periods=10 計 rank (此 leg 資料薄,informational)
    if 'option_top1_concentration' in panel.columns:
        _conc = panel['option_top1_concentration'].dropna()
        if len(_conc) >= 10:
            _cr = _conc.rolling(2520, min_periods=10).rank(pct=True) * 100
            panel['option_concentration_pctile'] = _cr.reindex(panel.index)
        else:
            panel['option_concentration_pctile'] = np.nan
    else:
        panel['option_concentration_pctile'] = np.nan

    # ============================================================
    # Flags (簡化版規則：未經 IC 驗證；下一階段 Phase B 再校準)
    # 變化型門檻維持原值(未校準,informational); 報告 §5 補「絕對百分位閘」避免漏史上極值。
    # ============================================================

    def flag_a(row):
        h = row.get('foreign_holding_chg_4w')
        sbl = row.get('sbl_change_4w_pct')
        fut_chg = row.get('foreign_fut_net_chg_4w')
        oi_pct = row.get('foreign_net_oi_pctile')
        reasons = []  # 變化型 (門檻維持原值)
        if h is not None and not pd.isna(h) and h < -0.3:
            reasons.append(f"外資持股率 4w {h:+.2f}pp")
        if sbl is not None and not pd.isna(sbl) and sbl > 15:
            reasons.append(f"借券賣出 4w +{sbl:.0f}%")
        if fut_chg is not None and not pd.isna(fut_chg) and fut_chg < -20000:
            reasons.append(f"外資期貨淨部位 4w {fut_chg:+.0f}口")
        n_change = len(reasons)
        # 絕對極值閘 (報告 §5)：外資期淨OI <= 5 百分位(史上極空) -> 至少 mid，補「只看 4w 變化」之漏
        ext = oi_pct is not None and not pd.isna(oi_pct) and oi_pct <= 5
        if ext:
            reasons.append(f"外資期淨OI {oi_pct:.0f} 百分位(史上極空)")
        if n_change >= 2:
            return 'high', ' / '.join(reasons)
        if n_change == 1 or ext:
            return 'mid', ' / '.join(reasons)
        return 'low', '外資持股/借券/外資期貨淨部位 4w 變化、外資期淨OI 百分位 均未達撤退門檻'

    def flag_b(row):
        z = row.get('margin_ratio_z_252d')
        sl = row.get('short_to_long_ratio')
        mpct = row.get('margin_to_mktcap_pct')
        mz = row.get('margin_mktcap_z_252d')
        mmaint = row.get('margin_maintenance_pct')
        reasons = []
        if z is not None and not pd.isna(z) and z > 1.5:
            reasons.append(f"融資/指數 z {z:+.2f}")
        if sl is not None and not pd.isna(sl) and sl < 0.05:
            reasons.append(f"短/多比 {sl:.3f} (極低)")
        # 融資餘額佔市值比 危險帶 (informational, 非 IC composite)。實測 2018-2025 各大頂
        # /崩盤前 0.43-0.53%, >=0.48 為當代頂部帶下緣。⚠️ 絕對門檻為 2024-2026 校準, 此比值
        # 長期結構性下降(2000 ~4% -> 現 0.38%), 需每 1-2 年 review; mz>2.5 (相對 252d z) 為
        # 自適應備援, 不與絕對門檻雙計(elif)。當前 0.38%/z1.87 兩者皆未觸發。
        if mpct is not None and not pd.isna(mpct) and mpct >= 0.48:
            reasons.append(f"融資佔市值 {mpct:.2f}% (>=0.48 當代頂部帶)")
        elif mz is not None and not pd.isna(mz) and mz > 2.5:
            reasons.append(f"融資佔市值 z {mz:+.2f} (急升)")
        # 大盤融資維持率 追繳壓力區 (informational, 非 IC composite)。<140 為斷頭賣壓升溫:
        # 2022-10-28 谷 123.1 / 2025-04-09 谷 124.9 / 2024-08-05 谷 142.4 (擦邊未觸)。
        # 與 0.48 絕對門檻不同, 130 法定追繳線 + 166.7 新倉基準 (融資成數6成) 為規則錨定,
        # 分子分母同 book 自正規化, 無結構性漂移問題, 不需定期下調。
        if mmaint is not None and not pd.isna(mmaint) and mmaint < 140:
            reasons.append(f"融資維持率 {mmaint:.0f}% (<140 追繳壓力區, 法定斷頭線 130)")
        if len(reasons) >= 2:
            return 'high', ' / '.join(reasons)
        if len(reasons) == 1:
            return 'mid', reasons[0]
        return 'low', '融資/指數 z、券資比、融資佔市值比、融資維持率 均未達鬆動門檻'

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
        return 'low', '投信連買天數與 5 日 z-score 均未達動能門檻'

    def flag_d(row):
        pcr_val = row.get('pcr_oi')
        conc = row.get('option_top1_concentration')
        conc_pct = row.get('option_concentration_pctile')
        reasons = []  # 變化/絕對值型 (門檻維持原值)
        if pcr_val is not None and not pd.isna(pcr_val):
            if pcr_val > 1.3:
                reasons.append(f"PCR_OI {pcr_val:.2f} (避險高)")
            elif pcr_val > 1.1:
                reasons.append(f"PCR_OI {pcr_val:.2f}")
        if conc is not None and not pd.isna(conc) and conc > 0.4:
            reasons.append(f"Put OI top1集中度 {conc:.2f}")
        n_base = len(reasons)
        # 集中度絕對極值閘 (報告 §5)：集中度 >= 90 百分位 -> 至少 mid(持倉脆弱)，
        # 補原規則「只看絕對值 0.4」會漏掉高百分位(如集中度 95% 卻仍 low)之缺
        ext = conc_pct is not None and not pd.isna(conc_pct) and conc_pct >= 90
        if ext:
            reasons.append(f"選擇權集中度 {conc_pct:.0f} 百分位(持倉脆弱)")
        if n_base >= 2:
            return 'high', ' / '.join(reasons)
        if n_base == 1 or ext:
            return 'mid', ' / '.join(reasons)
        return 'low', 'PCR-OI、選擇權集中度(絕對值與百分位) 均未達對沖門檻'

    def flag_e(row):
        hyg_z = row.get('hyg_volume_z_252d')
        tlt_spy = row.get('tlt_spy_chg_4w')
        flow_z = row.get('hyg_dollar_flow_z_252d')
        reasons = []
        n_change = 0
        # hyg_volume_z_252d 高位（|z| > 1.5）代表恐慌性拋售或追捧，方向看 hyg 本身
        if hyg_z is not None and not pd.isna(hyg_z) and abs(hyg_z) > 1.5:
            reasons.append(f"HYG成交量z {hyg_z:.2f} ({'放量' if hyg_z > 0 else '縮量'})")
            n_change += 1
        # tlt_spy_chg_4w > 0 代表資金避險（TLT 跑贏 SPY）
        if tlt_spy is not None and not pd.isna(tlt_spy) and tlt_spy > 3:
            reasons.append(f"TLT/SPY 4w {tlt_spy:+.2f}% (避險)")
            n_change += 1
        # HY 金額流向 z (報告 §5)：比成交量 z 更直接的資金流訊號。z<=-1.5 強流出->high；
        # z in [-1.5,-0.75] 輕度外流->mid (現 -0.99 應為 mid, 原規則漏接)
        flow_level = None
        if flow_z is not None and not pd.isna(flow_z):
            if flow_z <= -1.5:
                reasons.append(f"HYG金額流向z {flow_z:.2f} (強流出)")
                flow_level = 'high'
            elif flow_z <= -0.75:
                reasons.append(f"HYG金額流向z {flow_z:.2f} (資金外流)")
                flow_level = 'mid'
        if n_change >= 2 or flow_level == 'high':
            return 'high', ' / '.join(reasons)
        if n_change == 1 or flow_level == 'mid':
            return 'mid', ' / '.join(reasons)
        return 'low', 'HYG 成交量z、TLT/SPY 4週變化、HYG 金額流向z 均未達流動門檻'

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
