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
import json
import logging
import os
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

OUT = REPO / "data" / "sentiment" / "time_deposits_history.parquet"
NOTIFY_STATE = REPO / "data" / "sentiment" / "_time_deposits_last_notified.json"

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


def build_discord_message(df: pd.DataFrame) -> str:
    """產生月變動 Discord 訊息（純文字 + bullet，沒用 MD 表格）。"""
    latest = df.iloc[-1]
    p = latest['period']
    bal = latest['time_deposits_mil_twd'] / 1e6
    mom = latest['time_deposits_mom_pct']
    yoy = latest['time_deposits_yoy_pct']
    qm = latest['quasi_money_mil_twd'] / 1e6
    ratio = latest['m1b_to_time_deposits_ratio']

    last3 = df.tail(3)['time_deposits_mom_pct'].tolist()
    if len(last3) == 3 and all(v < 0 for v in last3):
        signal = f":warning: **連 3 月 MoM 為負** ({last3[0]:+.2f} / {last3[1]:+.2f} / {last3[2]:+.2f}%) — 資金離開存款體系 risk-on 強訊號"
    elif len(last3) >= 2 and all(v < 0 for v in last3[-2:]):
        signal = f":bulb: 連 2 月 MoM 為負 ({last3[-2]:+.2f} / {last3[-1]:+.2f}%) — 觀察第 3 月"
    elif mom < 0:
        signal = f"單月 MoM 為負 ({mom:+.2f}%) — 留意是否續跌"
    else:
        signal = f"定存仍在增長 (MoM {mom:+.2f}%) — 沒有資金外流訊號"

    lines = [
        f"**🏦 台灣總定存餘額更新 — {p}**",
        f"• 定存餘額：**{bal:.2f} 兆 TWD** (MoM {mom:+.2f}% / YoY {yoy:+.2f}%)",
        f"• 準貨幣計：{qm:.2f} 兆",
        f"• M1B/定存 比：{ratio:.3f}",
        f"• 訊號：{signal}",
        f"_資料源：CBC EF15M01 月頻 (1.5-2 月 lag) / informational tier_",
    ]
    return "\n".join(lines)


def push_discord(text: str) -> bool:
    """月變動推 Discord；無 webhook env 直接 skip 不算錯。"""
    import requests
    url = os.environ.get('DISCORD_WEBHOOK_MACRO') or os.environ.get('DISCORD_WEBHOOK')
    if not url:
        logger.info("No DISCORD_WEBHOOK[_MACRO] env set, skip push")
        return False
    try:
        r = requests.post(url, json={'content': text}, timeout=20)
        r.raise_for_status()
        logger.info("Discord push OK")
        return True
    except Exception as e:
        logger.error("Discord push failed: %s", e)
        return False


def _load_last_notified() -> str | None:
    if not NOTIFY_STATE.exists():
        return None
    try:
        return json.loads(NOTIFY_STATE.read_text(encoding='utf-8')).get('period')
    except Exception:
        return None


def _save_last_notified(period: str):
    NOTIFY_STATE.parent.mkdir(parents=True, exist_ok=True)
    NOTIFY_STATE.write_text(json.dumps({'period': period}, ensure_ascii=False), encoding='utf-8')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--no-save', action='store_true', help='只 print 不寫 parquet')
    ap.add_argument('--force-refresh', action='store_true', help='忽略 7 天快取強制重抓 CSV')
    ap.add_argument('--notify', action='store_true',
                    help='偵測到新月資料時推 Discord (state file 去重，同月只推一次)')
    ap.add_argument('--force-notify', action='store_true',
                    help='強制推送 (忽略 state file)，debug 用')
    args = ap.parse_args()

    raw = load_cbc_series(force_refresh=args.force_refresh)
    df = build_time_deposits_panel(raw)

    if not args.no_save:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(OUT, index=False)
        logger.info("Saved -> %s (%d rows, %s ~ %s)",
                    OUT, len(df), df.iloc[0]['period'], df.iloc[-1]['period'])

    print_summary(df)

    if args.notify or args.force_notify:
        latest_period = str(df.iloc[-1]['period'])
        last_notified = _load_last_notified()
        if not args.force_notify and last_notified == latest_period:
            logger.info("Already notified for period %s, skip", latest_period)
        else:
            msg = build_discord_message(df)
            if push_discord(msg):
                _save_last_notified(latest_period)

    return 0


if __name__ == '__main__':
    sys.exit(main())
