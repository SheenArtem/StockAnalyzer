"""
fetch_institutional_total.py -- 大盤三大法人總計歷史 (FinMind)

抓 FinMind TaiwanStockTotalInstitutionalInvestors，得到全市場日頻三大法人
buy/sell/net OI。寫 data/macro/institutional_total.parquet。

依 AI 報告 2026-05-09 Section 5 P0-1 建議：投信 / 外資 / 自營商總計，
補既有 panel「外資 vs 投信 vs 散戶背離」訊號軸。

每個日期會有 6 個 rows (各 name)：
  - Foreign_Investor       外資
  - Foreign_Dealer_Self    外資自營商
  - Investment_Trust       投信
  - Dealer_self            自營商自營
  - Dealer_Hedging         自營商避險
  - total                  三大法人合計

執行：python tools/fetch_institutional_total.py [--from-year YYYY]
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "data" / "macro" / "institutional_total.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

API = "https://api.finmindtrade.com/api/v4/data"


def get_token() -> str:
    tok = os.environ.get('FINMIND_TOKEN', '')
    if not tok:
        try:
            with open(REPO / 'local' / '.env') as f:
                for line in f:
                    if 'FINMIND' in line and '=' in line:
                        tok = line.split('=', 1)[1].strip().strip('"').strip("'")
                        break
        except Exception:
            pass
    return tok


def fetch(start_date: str, end_date: str | None = None) -> pd.DataFrame:
    """Single FinMind call for full date range."""
    params = {
        'dataset': 'TaiwanStockTotalInstitutionalInvestors',
        'start_date': start_date,
        'token': get_token(),
    }
    if end_date:
        params['end_date'] = end_date

    logger.info("FinMind fetch %s ~ %s", start_date, end_date or 'today')
    r = requests.get(API, params=params, timeout=120, verify=False)
    r.raise_for_status()
    js = r.json()
    if 'data' not in js or not js['data']:
        logger.warning("Empty response: %s", js)
        return pd.DataFrame()
    df = pd.DataFrame(js['data'])
    df['date'] = pd.to_datetime(df['date'])
    return df


def aggregate_panel(raw: pd.DataFrame) -> pd.DataFrame:
    """從長表 (date × name) 轉成寬表 (date × foreign/trust/dealer net)。"""
    # net = buy - sell
    raw['net'] = raw['buy'] - raw['sell']

    # pivot
    panel = raw.pivot(index='date', columns='name', values='net').reset_index()
    panel = panel.sort_values('date').reset_index(drop=True)

    # rename to friendly
    rename = {
        'Foreign_Investor': 'foreign_investor_net',
        'Foreign_Dealer_Self': 'foreign_dealer_net',
        'Investment_Trust': 'trust_net',
        'Dealer_self': 'dealer_self_net',
        'Dealer_Hedging': 'dealer_hedging_net',
        'total': 'three_majors_total_net',
    }
    panel = panel.rename(columns=rename)

    # combine 外資 + 外資自營
    if 'foreign_investor_net' in panel.columns and 'foreign_dealer_net' in panel.columns:
        panel['foreign_total_net'] = (
            panel['foreign_investor_net'].fillna(0) + panel['foreign_dealer_net'].fillna(0)
        )
    # combine 自營
    if 'dealer_self_net' in panel.columns and 'dealer_hedging_net' in panel.columns:
        panel['dealer_total_net'] = (
            panel['dealer_self_net'].fillna(0) + panel['dealer_hedging_net'].fillna(0)
        )

    # ============================================================
    # Derived: streak / cumulative / divergence
    # ============================================================

    # 連續天數 (>0 buy / <0 sell)
    def _streak(s: pd.Series, sign: int) -> pd.Series:
        """Compute consecutive days with same sign."""
        if sign > 0:
            mask = (s > 0).astype(int)
        else:
            mask = (s < 0).astype(int)
        groups = (mask != mask.shift()).cumsum()
        return mask.groupby(groups).cumsum()

    if 'foreign_total_net' in panel.columns:
        panel['foreign_buy_streak'] = _streak(panel['foreign_total_net'], 1)
        panel['foreign_sell_streak'] = _streak(panel['foreign_total_net'], -1)
        panel['foreign_cum_5d'] = panel['foreign_total_net'].rolling(5).sum()
        panel['foreign_cum_20d'] = panel['foreign_total_net'].rolling(20).sum()

    if 'trust_net' in panel.columns:
        panel['trust_buy_streak'] = _streak(panel['trust_net'], 1)
        panel['trust_sell_streak'] = _streak(panel['trust_net'], -1)
        panel['trust_cum_5d'] = panel['trust_net'].rolling(5).sum()
        panel['trust_cum_20d'] = panel['trust_net'].rolling(20).sum()

    # 外資 vs 投信 背離（外資賣超 + 投信買超 = 散戶 vs 機構切換訊號）
    if 'foreign_total_net' in panel.columns and 'trust_net' in panel.columns:
        panel['foreign_trust_divergence'] = panel['foreign_total_net'] - panel['trust_net']

    return panel


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--from-year', type=int, default=2014)
    args = parser.parse_args()
    start_date = f"{args.from_year}-01-01"

    raw = fetch(start_date)
    if raw.empty:
        logger.error("No data, abort")
        return

    logger.info("Raw rows: %d (date range %s ~ %s, names: %s)",
                len(raw), raw['date'].min(), raw['date'].max(),
                sorted(raw['name'].unique()))

    panel = aggregate_panel(raw)
    logger.info("Wide panel: %d rows × %d cols", len(panel), len(panel.columns))
    last = panel.iloc[-1]
    for col in ['foreign_total_net', 'trust_net', 'dealer_total_net',
                'three_majors_total_net', 'foreign_buy_streak', 'foreign_sell_streak',
                'trust_buy_streak', 'foreign_trust_divergence']:
        if col in panel.columns:
            v = last.get(col)
            if pd.notna(v):
                logger.info("  %s = %s", col, f"{v:,.0f}" if abs(v) > 100 else f"{v}")

    panel.to_parquet(OUT, index=False)
    logger.info("Saved -> %s", OUT)


if __name__ == '__main__':
    main()
