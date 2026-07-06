"""
fetch_cbc_time_deposits.py -- 台灣總定存餘額抓取 + 歷史 parquet

資料源:
  CBC EF15M01.csv (中央銀行 OpenData, 1987M05 起)
  https://www.cbc.gov.tw/public/data/OpenData/經研處/EF15M01.csv

欄位 (col index 0-based, 2026-05-25 驗證):
  [0]  期間 "YYYYMmm"
  [11] 準貨幣-計-原始值
  [13] 準貨幣-定期及定期儲蓄存款-原始值  <-- 主目標「定存」
  [15] 準貨幣-外匯存款-原始值
  [17] 準貨幣-郵政儲金-原始值
  [25] M1A-原始值
  [27] M1B-原始值
  [29] M2-原始值
  原始值單位: 百萬 TWD, 日平均餘額 (CBC 慣例)

訊號:
  - 定存 MoM 連續為負 → 錢搬離銀行體系，risk-on 增強
  - M1B/定存 比上升 → 活期化, 流動性偏多

Output:
  data/sentiment/time_deposits_history.parquet (月頻歷史, full series)
  stdout: 最新月 + MoM/YoY + 近 12 月趨勢

CLI:
  python tools/fetch_cbc_time_deposits.py            # fetch + save + print
  python tools/fetch_cbc_time_deposits.py --no-save  # 只 print 不寫檔
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

OUT = REPO / "data" / "sentiment" / "time_deposits_history.parquet"

COL_PERIOD = 0
COL_QUASI_MONEY = 11
COL_TIME_DEPOSITS = 13
COL_FX_DEPOSITS = 15
COL_POSTAL = 17
COL_M1A = 25
COL_M1B = 27
COL_M2 = 29


def load_cbc_series(force_refresh: bool = False) -> pd.DataFrame:
    """走 money_supply.py 的下載 + 7 天快取機制，回傳 full CSV DataFrame。"""
    from money_supply import _M1B_CACHE_FILE, _cache_is_fresh, _M1B_CACHE_TTL, _download_cbc_m1b

    if force_refresh or not _cache_is_fresh(_M1B_CACHE_FILE, _M1B_CACHE_TTL):
        try:
            _download_cbc_m1b()
        except Exception as e:
            logger.warning("CBC download failed, fallback to stale cache: %s", e)
            if not _M1B_CACHE_FILE.exists():
                raise
    return pd.read_csv(_M1B_CACHE_FILE, encoding='utf-8-sig')


def build_time_deposits_panel(raw: pd.DataFrame) -> pd.DataFrame:
    period = raw.iloc[:, COL_PERIOD].astype(str).str.replace('M', '', regex=False)

    def num(col):
        return pd.to_numeric(raw.iloc[:, col], errors='coerce')

    df = pd.DataFrame({
        'period': period,
        'time_deposits_mil_twd': num(COL_TIME_DEPOSITS),
        'quasi_money_mil_twd': num(COL_QUASI_MONEY),
        'fx_deposits_mil_twd': num(COL_FX_DEPOSITS),
        'postal_savings_mil_twd': num(COL_POSTAL),
        'm1a_mil_twd': num(COL_M1A),
        'm1b_mil_twd': num(COL_M1B),
        'm2_mil_twd': num(COL_M2),
    }).dropna(subset=['time_deposits_mil_twd']).reset_index(drop=True)

    df['time_deposits_mom_pct'] = df['time_deposits_mil_twd'].pct_change() * 100
    df['time_deposits_yoy_pct'] = df['time_deposits_mil_twd'].pct_change(12) * 100
    df['m1b_mom_pct'] = df['m1b_mil_twd'].pct_change() * 100
    df['m1b_to_time_deposits_ratio'] = df['m1b_mil_twd'] / df['time_deposits_mil_twd']
    return df


def print_summary(df: pd.DataFrame):
    latest = df.iloc[-1]
    period = latest['period']

    print()
    print(f"=== 台灣總定存餘額 (CBC EF15M01, period={period}) ===")
    print(f"定存 (定期+定期儲蓄):       {latest['time_deposits_mil_twd']/1e6:7.2f} 兆 TWD")
    print(f"  MoM:                      {latest['time_deposits_mom_pct']:+6.2f} %")
    print(f"  YoY:                      {latest['time_deposits_yoy_pct']:+6.2f} %")
    print()
    print(f"準貨幣計:                   {latest['quasi_money_mil_twd']/1e6:7.2f} 兆 TWD")
    print(f"  外匯存款:                 {latest['fx_deposits_mil_twd']/1e6:7.2f} 兆")
    print(f"  郵政儲金:                 {latest['postal_savings_mil_twd']/1e6:7.2f} 兆")
    print()
    print(f"M1A:                        {latest['m1a_mil_twd']/1e6:7.2f} 兆")
    print(f"M1B:                        {latest['m1b_mil_twd']/1e6:7.2f} 兆  (MoM {latest['m1b_mom_pct']:+.2f}%)")
    print(f"M2:                         {latest['m2_mil_twd']/1e6:7.2f} 兆")
    print(f"M1B / 定存 比:              {latest['m1b_to_time_deposits_ratio']:6.3f}  (>1 偏活期化)")
    print()
    print("近 12 月定存趨勢 (MoM%):")
    tail = df.tail(12)[['period', 'time_deposits_mil_twd', 'time_deposits_mom_pct']]
    for _, r in tail.iterrows():
        arrow = '↑' if r['time_deposits_mom_pct'] > 0 else ('↓' if r['time_deposits_mom_pct'] < 0 else '-')
        print(f"  {r['period']}  {r['time_deposits_mil_twd']/1e6:6.2f} 兆  {arrow} {r['time_deposits_mom_pct']:+.2f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--no-save', action='store_true', help='只 print 不寫 parquet')
    ap.add_argument('--force-refresh', action='store_true', help='忽略 7 天快取強制重抓 CSV')
    args = ap.parse_args()

    raw = load_cbc_series(force_refresh=args.force_refresh)
    df = build_time_deposits_panel(raw)

    if not args.no_save:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(OUT, index=False)
        logger.info("Saved -> %s (%d rows, %s ~ %s)",
                    OUT, len(df), df.iloc[0]['period'], df.iloc[-1]['period'])

    print_summary(df)

    return 0


if __name__ == '__main__':
    sys.exit(main())
