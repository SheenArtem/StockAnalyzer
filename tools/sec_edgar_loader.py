"""
SEC EDGAR Company Facts loader — VF-L1b Phase 2 POC

功能：
  A. get_cik(ticker)             — ticker -> 10-digit padded CIK
  B. fetch_company_facts()       — 下載 Company Facts JSON (rate-limited + cache)
  C. extract_piotroski_fields()  — XBRL tag -> Piotroski 9-項 long-format DataFrame
  D. build_panel()               — 批次建立 panel parquet

輸出 schema:  [ticker, date, statement, line_item, value]
  與 financials_us.parquet 相同，可共用 compute_us_fscore.py

Usage:
    python tools/sec_edgar_loader.py --tickers AAPL MSFT GOOGL --out poc
    python tools/sec_edgar_loader.py --tickers AAPL MSFT  # default out path
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

import pandas as pd
import numpy as np
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / 'data_cache' / 'sec_edgar'
RAW_DIR = CACHE_DIR / 'raw'
CIK_MAP_PATH = CACHE_DIR / 'cik_mapping.json'
CIK_MAP_TTL_DAYS = 7

# SEC EDGAR API endpoints
TICKERS_URL = 'https://www.sec.gov/files/company_tickers.json'
FACTS_URL = 'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'

# SEC 要求帶 User-Agent，否則 403
USER_AGENT = 'StockAnalyzer Research sheenshih@any-cast.com'
RATE_SLEEP = 0.12  # 秒，SEC 上限 10 req/s，留餘裕

# Piotroski 9 項 XBRL tag mapping
# key = 標準欄位名稱（line_item），value = XBRL tag 清單（依優先順序）
PIOTROSKI_TAG_MAP: dict[str, dict] = {
    # --- Income Statement ---
    'NetIncome': {
        'statement': 'income',
        'tags': [
            'NetIncomeLoss',
            'ProfitLoss',
            'NetIncomeLossAvailableToCommonStockholdersBasic',
        ],
        'is_instant': False,  # flow item (qtrs=1)
    },
    'Revenue': {
        'statement': 'income',
        'tags': [
            'RevenueFromContractWithCustomerExcludingAssessedTax',
            'SalesRevenueNet',
            'Revenues',
            'SalesRevenueGoodsNet',
            'RevenueFromContractWithCustomerIncludingAssessedTax',
        ],
        'is_instant': False,
    },
    'GrossProfit': {
        'statement': 'income',
        'tags': ['GrossProfit'],
        'is_instant': False,
    },
    'CostOfRevenue': {
        'statement': 'income',
        'tags': [
            'CostOfGoodsAndServicesSold',
            'CostOfRevenue',
            'CostOfGoodsSold',
        ],
        'is_instant': False,
    },
    # --- Cash Flow ---
    'CFO': {
        'statement': 'cashflow',
        'tags': [
            'NetCashProvidedByUsedInOperatingActivities',
            'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
        ],
        'is_instant': False,
    },
    # --- Balance Sheet ---
    'TotalAssets': {
        'statement': 'balance',
        'tags': ['Assets'],
        'is_instant': True,  # stock item
    },
    'LongTermDebt': {
        'statement': 'balance',
        'tags': [
            'LongTermDebt',
            'LongTermDebtNoncurrent',
            'LongTermDebtAndCapitalLeaseObligations',
        ],
        'is_instant': True,
    },
    'CurrentAssets': {
        'statement': 'balance',
        'tags': ['AssetsCurrent'],
        'is_instant': True,
    },
    'CurrentLiabilities': {
        'statement': 'balance',
        'tags': ['LiabilitiesCurrent'],
        'is_instant': True,
    },
    'SharesOutstanding': {
        'statement': 'balance',
        'tags': [
            'CommonStockSharesOutstanding',       # instant (balance)
            'WeightedAverageNumberOfSharesOutstandingBasic',  # flow (income)
        ],
        'is_instant': True,  # CommonStockSharesOutstanding 是 instant；
                             # WeightedAverage 有 start，但取 instant tag 優先
    },
    # --- P/B 計算所需欄位 ---
    'StockholdersEquity': {
        'statement': 'balance',
        'tags': [
            'StockholdersEquity',
            'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
            'CommonStockholdersEquity',
        ],
        'is_instant': True,
    },
    'Liabilities': {
        'statement': 'balance',
        'tags': [
            'Liabilities',
        ],
        'is_instant': True,
    },
}


# ---------------------------------------------------------------------------
# A. CIK mapping
# ---------------------------------------------------------------------------

def _load_cik_map() -> dict[str, str]:
    """從 SEC 下載 ticker->CIK 對照，回 {ticker_upper: '0000123456'}。"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # TTL 檢查
    if CIK_MAP_PATH.exists():
        age_days = (time.time() - CIK_MAP_PATH.stat().st_mtime) / 86400
        if age_days < CIK_MAP_TTL_DAYS:
            with open(CIK_MAP_PATH, encoding='utf-8') as f:
                return json.load(f)
            logger.debug('CIK map loaded from cache')

    logger.info('Downloading CIK mapping from SEC...')
    resp = requests.get(TICKERS_URL, headers={'User-Agent': USER_AGENT}, timeout=30)
    resp.raise_for_status()
    raw = resp.json()  # {0: {cik_str, ticker, title}, 1: ...}

    mapping = {}
    for v in raw.values():
        ticker = str(v.get('ticker', '')).upper().strip()
        cik = str(v.get('cik_str', '')).strip().zfill(10)
        if ticker:
            mapping[ticker] = cik

    with open(CIK_MAP_PATH, 'w', encoding='utf-8') as f:
        json.dump(mapping, f)
    logger.info('CIK map: %d entries saved to %s', len(mapping), CIK_MAP_PATH)
    return mapping


_cik_map_cache: dict[str, str] | None = None


def get_cik(ticker: str) -> str | None:
    """回 10-digit padded CIK，找不到回 None。"""
    global _cik_map_cache
    if _cik_map_cache is None:
        _cik_map_cache = _load_cik_map()
    return _cik_map_cache.get(ticker.upper().strip())


# ---------------------------------------------------------------------------
# B. Company Facts 抓取
# ---------------------------------------------------------------------------

def fetch_company_facts(ticker: str, cache: bool = True) -> dict | None:
    """
    下載 SEC Company Facts JSON。
    永久 cache 到 RAW_DIR/CIK{cik}.json。
    回 dict 或 None（找不到 CIK / 網路失敗）。
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    cik = get_cik(ticker)
    if not cik:
        logger.warning('%s: CIK not found in mapping', ticker)
        return None

    cache_path = RAW_DIR / f'CIK{cik}.json'
    if cache and cache_path.exists():
        with open(cache_path, encoding='utf-8') as f:
            return json.load(f)

    url = FACTS_URL.format(cik=cik)
    time.sleep(RATE_SLEEP)
    try:
        resp = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if cache:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f)
        return data
    except requests.HTTPError as e:
        logger.warning('%s (CIK=%s): HTTP %s', ticker, cik, e.response.status_code if e.response else '?')
        return None
    except Exception as e:
        logger.warning('%s (CIK=%s): %s', ticker, cik, e)
        return None


# ---------------------------------------------------------------------------
# C. Piotroski 欄位抽取
# ---------------------------------------------------------------------------

def _derive_quarterly_from_ytd(
    entries: list[dict],
) -> dict[str, float]:
    """
    從 YTD 累積條目推算單季數字。
    適用於 AAPL/MSFT 等以累積方式申報的公司。

    邏輯：
      start = 財年起始（通常 Oct-01 或 Jul-01）
      Q1 (diff ~90d) = 直接取
      Q2 = YTD_2Q - Q1
      Q3 = YTD_3Q - YTD_2Q
      Q4 = FY_annual - YTD_3Q

    回 {end_date: quarterly_val}
    """
    from datetime import datetime

    # 先按財年起始日分組（start date 相同 = 同一財年）
    fy_groups: dict[str, list[dict]] = {}
    for e in entries:
        start_str = e.get('start')
        if start_str is None:
            continue
        if start_str not in fy_groups:
            fy_groups[start_str] = []
        fy_groups[start_str].append(e)

    result: dict[str, float] = {}
    for fy_start, group in fy_groups.items():
        # 以 diff 天數排序：Q1 < H1 < 9M < FY
        parsed = []
        for e in group:
            try:
                diff = (datetime.strptime(e['end'], '%Y-%m-%d')
                        - datetime.strptime(fy_start, '%Y-%m-%d')).days
            except ValueError:
                continue
            if e.get('val') is not None:
                parsed.append((diff, e['end'], float(e['val'])))
        if not parsed:
            continue
        parsed.sort()

        # 取最後 filed 版本（同 diff 取最後）
        by_diff: dict[int, tuple[str, float]] = {}
        for diff, end_str, val in parsed:
            # bucket：Q1~105d, H1~220d, 9M~320d, FY~400d
            bucket = round(diff / 95)  # 1=Q1, 2=H1, 3=9M, 4=FY
            by_diff[bucket] = (end_str, val)

        # 推算各季
        ytd: dict[int, float] = {b: v for b, (_, v) in by_diff.items()}
        end_dates: dict[int, str] = {b: end for b, (end, _) in by_diff.items()}

        for q_bucket in sorted(ytd.keys()):
            end_str = end_dates[q_bucket]
            ytd_val = ytd[q_bucket]
            prev_bucket = q_bucket - 1
            if prev_bucket == 0 or prev_bucket not in ytd:
                q_val = ytd_val  # Q1 or standalone annual
            else:
                q_val = ytd_val - ytd[prev_bucket]
            result[end_str] = q_val  # 後蓋前

    return result


def _get_tag_unit_values(us_gaap: dict, tag: str) -> list[dict]:
    """從 us-gaap 取指定 tag 的 entries list（USD > shares > pure > 第一個）。"""
    if tag not in us_gaap:
        return []
    units_dict = us_gaap[tag].get('units', {})
    for unit_key in ('USD', 'shares', 'pure'):
        if unit_key in units_dict:
            return units_dict[unit_key]
    for entries in units_dict.values():
        return entries
    return []


def _extract_tag_series(us_gaap: dict, tags: list[str], is_instant: bool) -> list[tuple[str, float]]:
    """
    從 us-gaap namespace 抓指定 tag 清單，回 [(end_date, value), ...] 依優先順序。

    SEC Company Facts 結構：
      flow items (Income/CFO):  每條有 start + end；單季 = diff 約 60~95 天
      instant items (Balance):  每條只有 end（無 start）

    公司申報方式：
      方式 A（直接單季）：每個 10-Q 條目 diff = 90d，直接取
      方式 B（YTD 累積）：Q1=90d, Q2=181d, Q3=272d, Q4(10-K)=363d
                         需要從 YTD 推算單季遞增差

    合併策略：遍歷 tags 清單（優先順序高 → 低），各 tag 提取資料後，
    以 tags 清單順序為優先（先抓到的 date 不覆蓋），再補後面 tag 沒有的 dates。
    這樣可以處理 AAPL 類型：新 tag 覆蓋 2017+，舊 tag 覆蓋 2007-2018。
    """
    from datetime import datetime

    # 每個 tag 獨立提取，然後 merge（tags 清單靠前的優先）
    merged: dict[str, float] = {}  # date -> value，先進先占

    for tag in tags:
        unit_values = _get_tag_unit_values(us_gaap, tag)
        if not unit_values:
            continue

        result_for_tag: dict[str, float] = {}

        if is_instant:
            # Balance sheet: 只有 end，取 10-Q / 10-K
            for e in unit_values:
                form = e.get('form', '')
                if form in ('10-Q', '10-K') and e.get('val') is not None:
                    result_for_tag[e['end']] = float(e['val'])
        else:
            # Flow items: 先嘗試直接單季（diff 60~105d），再 YTD 推算
            valid = [e for e in unit_values
                     if e.get('form') in ('10-Q', '10-K')
                     and e.get('val') is not None
                     and e.get('start') is not None]

            single_q: dict[str, float] = {}
            for e in valid:
                try:
                    diff = (datetime.strptime(e['end'], '%Y-%m-%d')
                            - datetime.strptime(e['start'], '%Y-%m-%d')).days
                except ValueError:
                    continue
                if 60 <= diff <= 105:
                    single_q[e['end']] = float(e['val'])

            # YTD 推算補充 single_q 沒有的日期
            derived = _derive_quarterly_from_ytd(valid)
            for d_str, val in derived.items():
                if d_str not in single_q:
                    single_q[d_str] = val

            result_for_tag = single_q

        # 合併：tags 清單靠前的優先（先佔的日期不被後面覆蓋）
        for date_str, val in result_for_tag.items():
            if date_str not in merged:
                merged[date_str] = val

    if merged:
        return sorted(merged.items())  # 按日期升序
    return []


def extract_piotroski_fields(facts_json: dict, ticker: str) -> pd.DataFrame:
    """
    從 Company Facts JSON 抽取 Piotroski 9 項所需欄位。
    回 long format: [ticker, date, statement, line_item, value]
    """
    us_gaap = facts_json.get('facts', {}).get('us-gaap', {})
    if not us_gaap:
        logger.warning('%s: no us-gaap facts found', ticker)
        return pd.DataFrame(columns=['ticker', 'date', 'statement', 'line_item', 'value'])

    rows: list[dict] = []
    for field_name, cfg in PIOTROSKI_TAG_MAP.items():
        series = _extract_tag_series(us_gaap, cfg['tags'], cfg['is_instant'])
        for date_str, val in series:
            rows.append({
                'ticker': ticker,
                'date': date_str,
                'statement': cfg['statement'],
                'line_item': field_name,
                'value': val,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # GrossProfit fallback: Revenue - CostOfRevenue（若 GrossProfit tag 缺）
    gp_dates = set(df.loc[df['line_item'] == 'GrossProfit', 'date'])
    rev_df = df[df['line_item'] == 'Revenue'].set_index('date')['value']
    cor_df = df[df['line_item'] == 'CostOfRevenue'].set_index('date')['value']
    common_dates = set(rev_df.index) & set(cor_df.index)
    missing_gp = common_dates - gp_dates
    if missing_gp:
        calc_rows = []
        for d in missing_gp:
            gp_val = rev_df.loc[d] - cor_df.loc[d]
            calc_rows.append({
                'ticker': ticker,
                'date': d,
                'statement': 'income',
                'line_item': 'GrossProfit',
                'value': gp_val,
            })
        if calc_rows:
            df = pd.concat([df, pd.DataFrame(calc_rows)], ignore_index=True)
            logger.debug('%s: computed GrossProfit for %d dates from Rev-CoR', ticker, len(calc_rows))

    return df.sort_values(['line_item', 'date']).reset_index(drop=True)


# ---------------------------------------------------------------------------
# D. Panel builder
# ---------------------------------------------------------------------------

DEFAULT_OUT = ROOT / 'data_cache' / 'backtest' / 'financials_us_edgar.parquet'


CHECKPOINT_EVERY = 100  # 每 100 檔 flush 一次 parquet


def _flush_panel(all_dfs: list, out_path: Path) -> None:
    """合併 all_dfs 並落盤 parquet（覆蓋式 checkpoint）。"""
    if not all_dfs or out_path is None:
        return
    panel = pd.concat(all_dfs, ignore_index=True)
    panel['value'] = pd.to_numeric(panel['value'], errors='coerce')
    out_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(out_path, index=False)
    size_mb = out_path.stat().st_size / 1024 / 1024
    logger.info('Checkpoint: %d rows, %d tickers -> %s (%.2f MB)',
                len(panel), panel['ticker'].nunique(), out_path, size_mb)


def build_panel(
    tickers: list[str],
    out_path: str | Path | None = DEFAULT_OUT,
    refresh_cache: bool = False,
    resume: bool = True,
) -> pd.DataFrame:
    """
    批次抓取 + 抽取，合併成 panel DataFrame。

    out_path 給 Path 就落盤 parquet；None 只回傳 DataFrame。
    resume=True: 跳過已有 raw/CIK*.json 且 parquet 中已含該 ticker 的筆（中斷重跑）。
    每 CHECKPOINT_EVERY 檔 flush 一次 parquet。
    """
    out_path = Path(out_path) if out_path is not None else None
    all_dfs: list[pd.DataFrame] = []
    t0 = time.time()
    ok, fail, api_fail, skipped_resume = 0, 0, 0, 0

    # --resume: 載入已存在的 parquet，跳過已處理的 ticker
    already_done: set[str] = set()
    if resume and out_path is not None and out_path.exists():
        try:
            existing = pd.read_parquet(out_path)
            already_done = set(existing['ticker'].unique())
            all_dfs.append(existing)
            logger.info('Resume: loaded %d existing tickers from %s', len(already_done), out_path.name)
        except Exception as e:
            logger.warning('Resume: failed to load existing parquet (%s), starting fresh', e)

    for i, ticker in enumerate(tickers, 1):
        if resume and ticker in already_done:
            skipped_resume += 1
            continue

        logger.info('[%d/%d] %s ...', i, len(tickers), ticker)
        try:
            facts = fetch_company_facts(ticker, cache=(not refresh_cache))
            if facts is None:
                api_fail += 1
                fail += 1
                continue
            df_ticker = extract_piotroski_fields(facts, ticker)
            if df_ticker.empty:
                logger.warning('%s: extracted 0 rows', ticker)
                fail += 1
            else:
                all_dfs.append(df_ticker)
                ok += 1
                logger.info('  -> %d rows, date range %s ~ %s',
                            len(df_ticker), df_ticker['date'].min(), df_ticker['date'].max())
        except Exception as e:
            logger.error('%s: unexpected error: %s', ticker, e)
            fail += 1

        # Checkpoint: 每 CHECKPOINT_EVERY 新抓的檔 flush 一次
        if out_path is not None and ok > 0 and ok % CHECKPOINT_EVERY == 0:
            _flush_panel(all_dfs, out_path)

    elapsed = time.time() - t0
    logger.info('Done: ok=%d fail=%d (api_fail=%d) skipped_resume=%d elapsed=%.1fs',
                ok, fail, api_fail, skipped_resume, elapsed)

    if not all_dfs:
        logger.error('No data extracted.')
        return pd.DataFrame()

    panel = pd.concat(all_dfs, ignore_index=True)
    panel['value'] = pd.to_numeric(panel['value'], errors='coerce')

    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        panel.to_parquet(out_path, index=False)
        size_mb = out_path.stat().st_size / 1024 / 1024
        logger.info('Saved %d rows, %d tickers -> %s (%.2f MB)', len(panel), panel['ticker'].nunique(), out_path, size_mb)

    return panel


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

POC_TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'META', 'NVDA', 'AMZN', 'TSLA', 'JPM', 'JNJ', 'V']

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='SEC EDGAR Piotroski panel builder')
    ap.add_argument('--tickers', nargs='+', default=POC_TICKERS, help='ticker list')
    ap.add_argument('--out', default=str(DEFAULT_OUT), help='output parquet path')
    ap.add_argument('--refresh', action='store_true', help='ignore raw JSON cache, re-download')
    ap.add_argument('--no-resume', action='store_true', help='do not skip already-processed tickers')
    ap.add_argument('--universe', default=None, help='parquet with ticker column (overrides --tickers)')
    ap.add_argument('--report', action='store_true', help='print coverage matrix after build')
    args = ap.parse_args()

    if args.universe:
        import pandas as _pd
        uni = _pd.read_parquet(args.universe)
        tickers_to_run = list(uni['ticker'].unique())
        logger.info('Universe loaded: %d tickers from %s', len(tickers_to_run), args.universe)
    else:
        tickers_to_run = args.tickers

    out_path = Path(args.out)
    panel = build_panel(tickers_to_run, out_path=out_path, refresh_cache=args.refresh,
                        resume=(not args.no_resume))

    if args.report and not panel.empty:
        print('\n=== Coverage Matrix (quarters per field per ticker) ===')
        pivot = panel.groupby(['ticker', 'line_item'])['date'].count().unstack(fill_value=0)
        print(pivot.to_string())

        print('\n=== Earliest date per ticker ===')
        earliest = panel.groupby('ticker')['date'].min().sort_values()
        print(earliest.to_string())

        print('\n=== Total rows by line_item ===')
        print(panel['line_item'].value_counts().to_string())
