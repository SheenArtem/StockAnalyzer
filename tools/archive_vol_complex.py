"""
archive_vol_complex.py -- Vol Complex 5 訊號共振 archiver (informational tier)

5 訊號（用戶 2026-05-25 提的 framework）:
  1. VIX/VIX3M term structure ratio       <-- > 1.00 backwardation = 急性恐慌
  2. VVIX (VIX 的 VIX)                     <-- > 110 yellow / > 130 red, 尾端對沖
  3. CBOE SKEW (左尾溢價)                  <-- > 145 yellow / > 155 red
  4. OVX (原油波動率, 地緣事件領先)        <-- > 50 yellow / > 80 red
  5. (保留) MOVE 已在 system3_move_check 處理，不重複

設計:
  - 各訊號獨立分級 green/yellow/orange/red
  - lit_count = yellow+orange+red 數量
  - regime: 0=green / 1=monitor / 2=warning / 3=high_alert / 4=defensive

⚠️ SOP-14 informational tier 規則:
  - 閾值來自美股經驗值（用戶分享框架），**未在台股 IC 驗證**
  - 不接 portfolio gating / 不上 composite risk_score
  - 等 validate_vol_complex_ic.py 出 verdict 再決定是否 promote

Output:
  data/sentiment/vol_complex_history.parquet (daily, full history since 2007-05-10)

Schema (每日一 row):
  date, vix, vix3m, vvix, skew, ovx, vix_vix3m_ratio,
  vix_vix3m_light, vvix_light, skew_light, ovx_light,
  lit_count, regime

CLI:
  python tools/archive_vol_complex.py            # rebuild full + save
  (--notify Discord push removed 2026-07-06; regime 狀態每次 run 都印在 log)
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

CACHE_DIR = REPO / "data_cache" / "fred"
OUT = REPO / "data" / "sentiment" / "vol_complex_history.parquet"

# 閾值（用戶 framework，未經 IC 驗證 - informational tier）
THRESHOLDS = {
    'vix_vix3m_ratio': {'yellow': 0.95, 'orange': 1.00, 'red': 1.05},
    'vvix':            {'yellow': 100,  'orange': 110,  'red': 130},
    'skew':            {'yellow': 140,  'orange': 145,  'red': 155},
    'ovx':             {'yellow': 40,   'orange': 50,   'red': 80},
}
LIGHTS = ['green', 'yellow', 'orange', 'red']
LIGHT_RANK = {'green': 0, 'yellow': 1, 'orange': 2, 'red': 3}
REGIME_LABELS = {0: 'green', 1: 'monitor', 2: 'warning', 3: 'high_alert', 4: 'defensive'}


def _load(name: str) -> pd.Series:
    df = pd.read_parquet(CACHE_DIR / f"{name}.parquet")
    df.index = pd.to_datetime(df.index)
    return df.iloc[:, 0].sort_index().astype(float)


def classify(value: float, thresh: dict) -> str:
    if pd.isna(value):
        return 'green'
    if value >= thresh['red']:
        return 'red'
    if value >= thresh['orange']:
        return 'orange'
    if value >= thresh['yellow']:
        return 'yellow'
    return 'green'


def build_panel() -> pd.DataFrame:
    vix = _load('vix')
    vix3m = _load('vix3m')
    vvix = _load('vvix')
    skew = _load('skew')
    ovx = _load('ovx')

    df = pd.concat({
        'vix': vix, 'vix3m': vix3m, 'vvix': vvix, 'skew': skew, 'ovx': ovx
    }, axis=1).sort_index()
    df = df.dropna(subset=['vix', 'vix3m'], how='any')

    df['vix_vix3m_ratio'] = df['vix'] / df['vix3m']

    for col, key in [
        ('vix_vix3m_ratio', 'vix_vix3m_ratio'),
        ('vvix', 'vvix'),
        ('skew', 'skew'),
        ('ovx', 'ovx'),
    ]:
        df[f'{col}_light'] = df[col].apply(lambda v: classify(v, THRESHOLDS[key]))

    light_cols = ['vix_vix3m_ratio_light', 'vvix_light', 'skew_light', 'ovx_light']
    df['lit_count'] = df[light_cols].apply(
        lambda row: sum(1 for x in row if x in ('yellow', 'orange', 'red')), axis=1
    )
    df['regime'] = df['lit_count'].map(REGIME_LABELS).fillna('defensive')

    df = df.reset_index().rename(columns={'index': 'date', 'Date': 'date'})
    if 'date' not in df.columns:
        df = df.rename(columns={df.columns[0]: 'date'})
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--no-save', action='store_true', help='只算不寫 parquet')
    args = ap.parse_args()

    df = build_panel()
    logger.info("Built panel: %d rows, %s ~ %s",
                len(df), df.iloc[0]['date'], df.iloc[-1]['date'])

    latest = df.iloc[-1]
    logger.info("Latest %s: VIX/VIX3M=%.3f (%s) / VVIX=%.1f (%s) / SKEW=%.1f (%s) / OVX=%.1f (%s)"
                " / lit=%d regime=%s",
                latest['date'], latest['vix_vix3m_ratio'], latest['vix_vix3m_ratio_light'],
                latest['vvix'], latest['vvix_light'],
                latest['skew'], latest['skew_light'],
                latest['ovx'], latest['ovx_light'],
                int(latest['lit_count']), latest['regime'])

    if not args.no_save:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(OUT, index=False)
        logger.info("Saved -> %s", OUT)

    return 0


if __name__ == '__main__':
    sys.exit(main())
