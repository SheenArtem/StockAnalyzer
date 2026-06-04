"""
build_market_cap_panel.py -- 上市總市值 + 融資餘額佔市值比 + 大盤融資維持率 (大盤層級, 上市 only)

產出 data/macro/market_cap.parquet:
  date, total_market_cap (上市總市值, 元),
  margin_value (官方融資金額, 元), margin_to_mktcap_pct (融資金額/總市值 x100),
  margin_mktcap_z_252d (252日 z-score),
  margin_collateral_value (融資擔保品市值, 元), margin_maintenance_pct (大盤融資維持率, %)

資料源 (全免費, 零重複抓取):
  - 分母 總市值: 重用 data_cache/backtest/ohlcv_tw.parquet (refresh_universe_prices.py
    每日更新, run_scanner.bat:357) x TWSE OpenAPI t187ap03_L 發行股數
    (cache 在 data/macro/listed_shares.parquet, 月刷一次)。
    NOTE: 歷史用「當前發行股數 snapshot」近似 (資本變動慢, 對 252d z 影響小)。
  - 分子 融資金額: TWSE MI_MARGN selectType=MS 官方融資金額今日餘額 (仟元)。
    !! 用官方金額, 不自己 (融資張數 x 收盤) 重建 -- 後者會高估 ~2x
    (融資金額含融資成數 ~6成 + 用融資成本價, 非現價x100%)。
  - 融資維持率 (2026-06-04 加): 業界標準近似 (XQ/M平方/CMoney 同類自算, 主管機關不公布)
      大盤融資維持率 = Σ(個股融資餘額張 x 1000 x 收盤價) / 官方融資金額 x 100%
    分子個股融資餘額: MI_MARGN selectType=ALL 全表 -> data/macro/margin_units.parquet。
    !! 不用 chip_history/margin.parquet -- 它有 universe filter, 缺 218 檔 ETF
    (槓桿/反向/主動/債 ETF, 2026-05-29 實測佔官方總張數 23.4%, 且佔比逐年成長
    = 時變性低估偏差); MI_MARGN ALL 與分母 MS 同 endpoint 同日對齊, 每日新鮮。
    ETF 等不在 ohlcv_tw 的價格: yfinance 批次補 -> data/macro/margin_gap_prices.parquet
    (獨立檔, 不塞 ohlcv_tw 以免污染 breadth 等下游)。已下市股 (如 2888 新光金) yfinance
    無價 -> 覆蓋率守門 0.95 (張數) 容忍, 缺的多為低價股, 市值面誤差 <1%。

執行:
  python tools/build_market_cap_panel.py                       # 日更 (append 最新交易日)
  python tools/build_market_cap_panel.py --refresh-shares      # 強制重抓 t187ap03_L 股數
  python tools/build_market_cap_panel.py --backfill-margin 504 # 一次性回填 ~2yr 融資金額
  python tools/build_market_cap_panel.py --backfill-units 1300 # 一次性回填融資張數全表 (~33min)
"""
from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
OHLCV_TW = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"
MACRO = REPO / "data" / "macro"
MARGIN_UNITS = MACRO / "margin_units.parquet"      # MI_MARGN ALL 全表融資餘額 (張)
GAP_PRICES = MACRO / "margin_gap_prices.parquet"   # ohlcv_tw 缺的 ETF 等收盤價
SHARES_CACHE = MACRO / "listed_shares.parquet"
OUT = MACRO / "market_cap.parquet"
MACRO.mkdir(parents=True, exist_ok=True)

_HEADERS = {'User-Agent': 'Mozilla/5.0'}
_THROTTLE_SEC = 1.5


# --------------------------------------------------------------------------- #
#  發行股數 (t187ap03_L, 上市) -- cache 月刷
# --------------------------------------------------------------------------- #
def load_listed_shares(refresh: bool = False, max_age_days: int = 25) -> pd.Series:
    """上市公司在外流通普通股數 (股)。回 Series index=stock_id。
    cache 在 listed_shares.parquet, 超過 max_age_days 或 --refresh-shares 才重抓。"""
    if SHARES_CACHE.exists() and not refresh:
        age_days = (pd.Timestamp.now() - pd.Timestamp(SHARES_CACHE.stat().st_mtime, unit='s')).days
        if age_days <= max_age_days:
            s = pd.read_parquet(SHARES_CACHE)['shares']
            logger.info("listed_shares: %d 檔 (cache, %dd old)", len(s), age_days)
            return s
    logger.info("Fetching TWSE t187ap03_L 上市公司基本資料 (發行股數)...")
    url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
    rows = requests.get(url, headers=_HEADERS, verify=False, timeout=30).json()
    shares = {}
    for row in rows:
        sid = str(row.get('公司代號', '')).strip()
        raw = str(row.get('已發行普通股數或TDR原股發行股數', '')).replace(',', '')
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        if sid and val > 0:
            shares[sid] = val
    s = pd.Series(shares, name='shares')
    s.index.name = 'stock_id'
    s.to_frame().to_parquet(SHARES_CACHE)
    logger.info("listed_shares: %d 檔 (fresh -> %s)", len(s), SHARES_CACHE.name)
    return s


# --------------------------------------------------------------------------- #
#  總市值 (上市) -- 重用 ohlcv_tw x shares
# --------------------------------------------------------------------------- #
def build_market_cap_series(shares: pd.Series) -> pd.Series:
    """每日上市總市值 (元) = sum(收盤 x 發行股數)。重用 ohlcv_tw.parquet, 零抓取。"""
    if not OHLCV_TW.exists():
        raise FileNotFoundError(f"{OHLCV_TW} 不存在 (應由 refresh_universe_prices.py 維護)")
    df = pd.read_parquet(OHLCV_TW, columns=['stock_id', 'date', 'Close'])
    df['stock_id'] = df['stock_id'].astype(str)
    df = df[df['stock_id'].isin(shares.index)]  # 上市 only
    # 防呆：剔除 NaN 收盤 (yfinance 偶有「有量無價」列, e.g. 2026-06-01)。若不剔，
    # NaN*股數=NaN 被 sum 當 0 跳過 -> 總市值塌陷，但下方 cnt 仍把該列計入 ->
    # MIN_STOCKS 覆蓋守門失效, 寫出 0 總市值 / inf 比值。剔掉後 cnt 反映真實有價檔數,
    # partial/全 NaN 日自然落在 MIN_STOCKS floor 之下被砍成 NaN。
    df = df.dropna(subset=['Close'])
    df['cap'] = df['Close'] * df['stock_id'].map(shares)
    grp = df.groupby('date')
    mktcap = grp['cap'].sum().sort_index()
    cnt = grp['stock_id'].count().sort_index()
    mktcap.index = pd.to_datetime(mktcap.index)
    cnt.index = pd.to_datetime(cnt.index)
    # 覆蓋率守門：partial-update 日 (檔數 << 近期中位數) 會給偏低總市值 -> 砍成 NaN，
    # 避免假的 margin/mktcap 比值尖峰污染 z-score (e.g. 2025-08-01 僅 452 檔 vs ~900)。
    # (a) 絕對覆蓋 floor：上市市值覆蓋 2016+ 才達 ~86-97%；2015 僅 ~511 檔 (~54% 覆蓋)
    #     -> 比值假性偏高 (2015-10~2016-01 衝到 0.9-1.0% 是 artifact 非真實) -> 砍 NaN。
    #     ⚠️ 即便 2016+ 仍 ~86-97% 覆蓋 (比值略偏高 ~10-16%, 逐年收斂)，但與危險帶校準
    #     同基礎故可用；2015 (54%) 失真過大必砍。
    MIN_STOCKS = 800
    low_abs = cnt < MIN_STOCKS
    if low_abs.any():
        logger.info("  絕對覆蓋 floor: 砍 %d 個 <%d 檔的日 (市值覆蓋不足, e.g. 2015)", int(low_abs.sum()), MIN_STOCKS)
        mktcap[low_abs] = np.nan
    # (b) 相對守門：partial-update 日 (檔數 << 近期中位數) 也砍
    med = cnt.rolling(60, min_periods=20).median()
    low_cov = cnt < (0.7 * med)
    if low_cov.any():
        logger.info("  覆蓋率守門: 砍 %d 個低覆蓋日 (檔數 < 0.7x 近60日中位數)", int(low_cov.sum()))
        mktcap[low_cov] = np.nan
    logger.info("total_market_cap: %d days, last(%s) = %.2f 兆 (%d 上市檔)",
                len(mktcap), mktcap.index[-1].date(), mktcap.iloc[-1] / 1e12, len(shares))
    return mktcap.rename('total_market_cap')


# --------------------------------------------------------------------------- #
#  融資餘額張數全表 (MI_MARGN selectType=ALL) -- 維持率分子原料
# --------------------------------------------------------------------------- #
def fetch_margin_units_one_day(date: pd.Timestamp) -> pd.DataFrame | None:
    """TWSE MI_MARGN ALL 全表: 每檔融資今日餘額 (張)。含 ETF/TDR, 無 universe filter。
    回 DataFrame(date, stock_id, margin_units); 非交易日 / 抓失敗回 None。"""
    url = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
    params = {'response': 'json', 'date': date.strftime('%Y%m%d'), 'selectType': 'ALL'}
    try:
        d = requests.get(url, params=params, headers=_HEADERS, verify=False, timeout=30).json()
    except Exception as e:
        logger.warning("  MI_MARGN ALL %s fetch err: %s", date.date(), e)
        return None
    if d.get('stat') != 'OK':
        return None  # 非交易日
    tables = d.get('tables', [])
    if len(tables) < 2:
        return None
    recs = []
    for row in tables[1].get('data', []):  # tables[1] = 融資融券彙總: [0]代號 ... [6]融資今日餘額
        try:
            sid = str(row[0]).strip()
            units = float(str(row[6]).replace(',', ''))
        except (ValueError, IndexError, TypeError):
            continue
        if sid:
            recs.append({'date': date, 'stock_id': sid, 'margin_units': units})
    return pd.DataFrame(recs) if recs else None


def update_margin_units(trade_dates: list, backfill_n: int) -> pd.DataFrame:
    """維護 margin_units.parquet: 對最近 backfill_n 個交易日中尚無資料者抓 MI_MARGN ALL。
    與分母 margin_value (MS) 同 endpoint 同日對齊。回全量 DataFrame。"""
    if MARGIN_UNITS.exists():
        existing = pd.read_parquet(MARGIN_UNITS)
        existing['date'] = pd.to_datetime(existing['date'])
        have = set(existing['date'].unique())
    else:
        existing = pd.DataFrame(columns=['date', 'stock_id', 'margin_units'])
        have = set()
    todo = [d for d in trade_dates[-backfill_n:] if d not in have]
    logger.info("margin_units backfill: %d 待抓 / %d 已有日", len(todo), len(have))
    new_frames = []
    for i, d in enumerate(todo, 1):
        df = fetch_margin_units_one_day(d)
        if df is not None:
            new_frames.append(df)
        if i % 20 == 0:
            logger.info("  ... %d/%d (last %s: %s 檔)", i, len(todo), d.date(),
                        len(df) if df is not None else 'None')
        time.sleep(_THROTTLE_SEC)
    if new_frames:
        parts = ([existing] if len(existing) else []) + new_frames
        existing = pd.concat(parts, ignore_index=True)
        existing = existing.drop_duplicates(subset=['date', 'stock_id'], keep='last')
        existing = existing.sort_values(['date', 'stock_id']).reset_index(drop=True)
        existing.to_parquet(MARGIN_UNITS, index=False)
        logger.info("margin_units: +%d 日 -> %s (%d rows)", len(new_frames), MARGIN_UNITS.name, len(existing))
    return existing


# --------------------------------------------------------------------------- #
#  ETF gap 收盤價 (margin_units 有融資但 ohlcv_tw 無價格者) -- yfinance 批次
# --------------------------------------------------------------------------- #
def update_gap_prices(units: pd.DataFrame, px: pd.DataFrame) -> pd.DataFrame:
    """維護 margin_gap_prices.parquet。gap 兩型:
    (a) 代號缺席型: margin_units 出現過但 ohlcv_tw 完全沒有 (槓桿/反向/主動/債 ETF) -> 抓全史
    (b) 尾巴斷更型: 在 ohlcv_tw 但近 10 個 units 交易日有缺價 (e.g. 00981A 被 universe
        改版踢出後停更 / yfinance partial-batch 漏日) -> top-up 近 14 日
    獨立檔案, 不寫進 ohlcv_tw (避免污染 breadth 等下游)。"""
    import yfinance as yf
    ohlcv_ids = set(px['stock_id'].unique())
    absent_ids = sorted(set(units['stock_id'].unique()) - ohlcv_ids)
    recent_dates = sorted(units['date'].unique())[-10:]
    ru = units[units['date'].isin(recent_dates)][['date', 'stock_id']]
    rp = px[px['date'].isin(recent_dates)][['date', 'stock_id']].drop_duplicates()
    chk = ru.merge(rp.assign(_has=1), on=['date', 'stock_id'], how='left')
    stale_ids = sorted(set(chk.loc[chk['_has'].isna(), 'stock_id']) - set(absent_ids))
    gap_ids = absent_ids + stale_ids
    if not gap_ids:
        return pd.DataFrame(columns=['date', 'stock_id', 'Close'])
    units_min = units['date'].min()
    if GAP_PRICES.exists():
        existing = pd.read_parquet(GAP_PRICES)
        existing['date'] = pd.to_datetime(existing['date'])
        # 自癒: 既有代號若資料起點比 units 起點晚 >30 天 (e.g. 日更先建檔後才 backfill
        # units 深歷史), 視同新代號重抓全史; 上市日晚於 units 起點者重抓一次無害 (冪等)
        first_dt = existing.groupby('stock_id')['date'].min()
        shallow = set(first_dt[first_dt > units_min + pd.Timedelta(days=30)].index)
        known = set(existing['stock_id'].unique()) - shallow
    else:
        existing = pd.DataFrame(columns=['date', 'stock_id', 'Close'])
        known = set()
    # 缺席型且不 known -> 全史; 尾巴斷更型 (在 ohlcv_tw 有歷史) 一律只 top-up
    new_ids = [s for s in absent_ids if s not in known]
    old_ids = [s for s in gap_ids if s in known] + [s for s in stale_ids if s not in known]
    start_full = (units_min - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
    start_topup = (pd.Timestamp.now() - pd.Timedelta(days=14)).strftime('%Y-%m-%d')

    def _batch(ids: list, start: str) -> list[pd.DataFrame]:
        frames = []
        for j in range(0, len(ids), 100):
            chunk = ids[j:j + 100]
            try:
                raw = yf.download([f"{s}.TW" for s in chunk], start=start, interval='1d',
                                  progress=False, auto_adjust=False, threads=True,
                                  group_by='ticker')
            except Exception as e:
                logger.warning("  gap_prices yf batch err [%s..]: %s", chunk[0], e)
                continue
            for s in chunk:
                try:
                    close = raw[f"{s}.TW"]['Close'].dropna()
                except (KeyError, TypeError):
                    continue
                if close.empty:
                    continue
                idx = pd.to_datetime(close.index)
                idx = idx.tz_localize(None) if getattr(idx, 'tz', None) is not None else idx
                frames.append(pd.DataFrame({
                    'date': pd.to_datetime(idx.date), 'stock_id': s, 'Close': close.values}))
        return frames

    frames = []
    if new_ids:
        logger.info("gap_prices: %d 新代號抓全史 (start=%s)", len(new_ids), start_full)
        frames += _batch(new_ids, start_full)
    if old_ids:
        frames += _batch(old_ids, start_topup)
    if frames:
        parts = ([existing] if len(existing) else []) + frames
        existing = pd.concat(parts, ignore_index=True)
        existing = existing.drop_duplicates(subset=['date', 'stock_id'], keep='last')
        existing = existing.sort_values(['date', 'stock_id']).reset_index(drop=True)
        existing.to_parquet(GAP_PRICES, index=False)
    logger.info("gap_prices: %d 代號 / %d rows -> %s",
                existing['stock_id'].nunique() if len(existing) else 0, len(existing), GAP_PRICES.name)
    return existing


# --------------------------------------------------------------------------- #
#  融資擔保品市值 (Σ 張 x 1000 x 收盤) -- 維持率分子, 元
# --------------------------------------------------------------------------- #
def build_margin_collateral_series(trade_dates: pd.DatetimeIndex, backfill_units: int) -> pd.Series:
    """每日上市融資擔保品市值 (元) = Σ(個股融資餘額張 x 1000 x 收盤價)。
    餘額: MI_MARGN ALL (含 ETF) / 價格: ohlcv_tw + margin_gap_prices 聯集。
    覆蓋率守門: 有價張數 <95% -> NaN (缺多為已下市低價股, 市值面誤差 <1%;
    更低 = 價格 panel partial-update 壞日, 寧可 NaN fail loud)。"""
    units = update_margin_units(list(trade_dates), backfill_units)
    if units.empty:
        logger.warning("margin_units 無資料, 跳過維持率分子")
        return pd.Series(dtype=float, name='margin_collateral_value')

    px = pd.read_parquet(OHLCV_TW, columns=['stock_id', 'date', 'Close'])
    px['stock_id'] = px['stock_id'].astype(str)
    px['date'] = pd.to_datetime(px['date'])
    px = px.dropna(subset=['Close'])
    gap = update_gap_prices(units, px)
    if len(gap):
        px = pd.concat([px[['date', 'stock_id', 'Close']], gap[['date', 'stock_id', 'Close']]],
                       ignore_index=True)
    # (date,stock) 唯一化: gap 與 ohlcv_tw 對尾巴斷更型代號會重疊, 不 dedup 會讓
    # left-join 複製 units 列 -> 擔保市值重複計算
    px = px.drop_duplicates(subset=['date', 'stock_id'], keep='last')

    mm = units.merge(px, on=['date', 'stock_id'], how='left')
    mm['collateral'] = mm['margin_units'] * 1000.0 * mm['Close']  # 張 -> 股 x 收盤 = 元
    g = mm.groupby('date')
    collateral = g['collateral'].sum()
    total_units = g['margin_units'].sum()
    matched_units = mm.loc[mm['Close'].notna()].groupby('date')['margin_units'].sum()
    cov = (matched_units.reindex(collateral.index).fillna(0)
           / total_units.replace(0, np.nan))
    bad = cov < 0.95
    if bad.any():
        logger.info("  維持率分子守門: 砍 %d 個低價格覆蓋日 (有價張數 <95%%, min cov=%.2f)",
                    int(bad.sum()), float(cov.min()))
        collateral[bad] = np.nan
    collateral = collateral.where(total_units > 0)
    collateral.index = pd.to_datetime(collateral.index)
    collateral = collateral.reindex(trade_dates)
    ok = collateral.dropna()
    if len(ok):
        logger.info("margin_collateral: %d days, last(%s) = %.0f 億 (cov %.1f%%)",
                    len(ok), ok.index[-1].date(), ok.iloc[-1] / 1e8,
                    float(cov.dropna().iloc[-1]) * 100 if len(cov.dropna()) else float('nan'))
    return collateral.rename('margin_collateral_value')


# --------------------------------------------------------------------------- #
#  融資金額 (官方 MI_MARGN selectType=MS) -- 元
# --------------------------------------------------------------------------- #
def fetch_margin_value(date: pd.Timestamp) -> float | None:
    """TWSE 官方上市融資金額今日餘額 (元)。MI_MARGN selectType=MS, 來源仟元 x1000。"""
    url = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
    params = {'response': 'json', 'date': date.strftime('%Y%m%d'), 'selectType': 'MS'}
    try:
        d = requests.get(url, params=params, headers=_HEADERS, verify=False, timeout=25).json()
    except Exception as e:
        logger.warning("  MI_MARGN %s fetch err: %s", date.date(), e)
        return None
    if d.get('stat') != 'OK':
        return None  # 非交易日 / 無資料
    for t in d.get('tables', []):
        for row in t.get('data', []):
            if row and '融資金額' in str(row[0]):
                try:
                    return float(str(row[-1]).replace(',', '')) * 1000.0  # 今日餘額, 仟元 -> 元
                except (ValueError, IndexError):
                    return None
    return None


def backfill_margin_values(dates: list[pd.Timestamp], existing: pd.Series) -> pd.Series:
    """對 dates 中尚無資料者抓官方融資金額 (throttled)。回更新後 Series。"""
    out = existing.copy()
    todo = [d for d in dates if d not in out.index or pd.isna(out.get(d))]
    logger.info("融資金額 backfill: %d 待抓 / %d 已有", len(todo), len(out))
    for i, d in enumerate(todo, 1):
        v = fetch_margin_value(d)
        if v is not None:
            out.loc[d] = v
        if i % 20 == 0:
            logger.info("  ... %d/%d (last %s = %s)", i, len(todo), d.date(),
                        f"{v/1e8:.0f}億" if v else "None")
        time.sleep(_THROTTLE_SEC)
    return out.sort_index()


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--refresh-shares', action='store_true', help='強制重抓 t187ap03_L 股數')
    ap.add_argument('--backfill-margin', type=int, default=0,
                    help='回填最近 N 個交易日的官方融資金額 (default 0 = 只抓最新交易日)')
    ap.add_argument('--backfill-units', type=int, default=0,
                    help='回填最近 N 個交易日的融資餘額張數全表 (維持率分子, default 0 = 只抓最新交易日)')
    args = ap.parse_args()

    shares = load_listed_shares(refresh=args.refresh_shares)
    mktcap = build_market_cap_series(shares)

    # 既有融資金額 series (從現有 panel 讀, 否則空)
    if OUT.exists():
        prev = pd.read_parquet(OUT)
        prev['date'] = pd.to_datetime(prev['date'])
        margin_val = prev.set_index('date')['margin_value'].dropna()
    else:
        margin_val = pd.Series(dtype=float, name='margin_value')

    # 決定要抓哪些日: 最新交易日 (日更) 或最近 N 日 (backfill)
    trade_dates = list(mktcap.index)
    n = args.backfill_margin if args.backfill_margin > 0 else 1
    target_dates = trade_dates[-n:]
    margin_val = backfill_margin_values(target_dates, margin_val)

    # margin_value 異常守門：融資餘額具黏性 (單日變動通常 <5%)，相對 10 日中位數
    # deviation > 20% = 抓取雜訊 (backfill 暫時性壞回應, e.g. 2025-02-07 曾存成 1727億
    # vs 官方 3060億) -> 砍 NaN，避免污染 z-score。真實崩盤是漸進去槓桿不會單日 -20%。
    if len(margin_val) >= 5:
        mv_med = margin_val.rolling(10, center=True, min_periods=3).median()
        bad = (margin_val - mv_med).abs() > 0.20 * mv_med
        if bad.any():
            logger.info("margin_value 守門: 砍 %d 個異常值 (單日 deviation > 20%% vs 10日中位數): %s",
                        int(bad.sum()), [str(d.date()) for d in margin_val.index[bad]])
            margin_val = margin_val.mask(bad)

    # 組 panel: 對齊到有總市值的交易日
    panel = mktcap.to_frame()
    panel['margin_value'] = margin_val.reindex(panel.index)
    panel['margin_to_mktcap_pct'] = panel['margin_value'] / panel['total_market_cap'] * 100.0
    # 防呆雙重保險：total_market_cap=0 (理應已被 build_market_cap_series 的 floor 砍成 NaN)
    # 會讓比值 inf。一律轉 NaN，絕不寫 inf 進 parquet 污染 z-score / dashboard tile。
    panel['margin_to_mktcap_pct'] = panel['margin_to_mktcap_pct'].replace([np.inf, -np.inf], np.nan)

    # 大盤融資維持率 = 擔保品市值 / 官方融資金額 x 100 (業界標準近似, 見檔頭)
    n_units = args.backfill_units if args.backfill_units > 0 else 1
    panel['margin_collateral_value'] = build_margin_collateral_series(panel.index, n_units)
    panel['margin_maintenance_pct'] = (
        panel['margin_collateral_value'] / panel['margin_value'] * 100.0
    ).replace([np.inf, -np.inf], np.nan)
    # 252d z-score (只在 ratio 有值處)
    # z-score: min_periods=120 (需 ~6 月歷史才算, 避免 startup 低變異窗口放大成假極端 z;
    # 此比值很穩定 0.3-0.5%, 早期窗口 std 過小會噴極端值)
    r = panel['margin_to_mktcap_pct']
    panel['margin_mktcap_z_252d'] = (
        (r - r.rolling(252, min_periods=120).mean()) / r.rolling(252, min_periods=120).std()
    )
    panel = panel.reset_index().rename(columns={'index': 'date'})

    panel.to_parquet(OUT, index=False)
    last = panel.dropna(subset=['margin_to_mktcap_pct']).iloc[-1] if panel['margin_to_mktcap_pct'].notna().any() else None
    if last is not None:
        mm = last.get('margin_maintenance_pct')
        logger.info("OK -> %s | %s: 總市值 %.2f兆 / 融資 %.0f億 / 佔比 %.3f%% / z=%.2f / 維持率 %s",
                    OUT.name, pd.Timestamp(last['date']).date(),
                    last['total_market_cap'] / 1e12, last['margin_value'] / 1e8,
                    last['margin_to_mktcap_pct'],
                    last['margin_mktcap_z_252d'] if pd.notna(last['margin_mktcap_z_252d']) else float('nan'),
                    f"{mm:.1f}%" if mm is not None and pd.notna(mm) else "NaN")
    else:
        logger.warning("OK -> %s | 尚無融資金額 (跑 --backfill-margin N 補歷史)", OUT.name)


if __name__ == '__main__':
    main()
