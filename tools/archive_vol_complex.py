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
  python tools/archive_vol_complex.py --notify   # 偵測 regime 升級時推 Discord (state file dedupe)
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

CACHE_DIR = REPO / "data_cache" / "fred"
OUT = REPO / "data" / "sentiment" / "vol_complex_history.parquet"
NOTIFY_STATE = REPO / "data" / "sentiment" / "_vol_complex_last_alert.json"

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


def _format_alert(latest_row, prev_row=None) -> str:
    lights = {
        'VIX/VIX3M': (latest_row['vix_vix3m_ratio'], latest_row['vix_vix3m_ratio_light']),
        'VVIX':      (latest_row['vvix'], latest_row['vvix_light']),
        'SKEW':      (latest_row['skew'], latest_row['skew_light']),
        'OVX':       (latest_row['ovx'], latest_row['ovx_light']),
    }
    emoji = {'green': ':green_circle:', 'yellow': ':yellow_circle:',
             'orange': ':orange_circle:', 'red': ':red_circle:'}

    lines = [
        f"**:warning: Vol Complex regime → {latest_row['regime'].upper()}** ({latest_row['date'].strftime('%Y-%m-%d')})",
        f"亮燈數: {int(latest_row['lit_count'])} / 4",
    ]
    for name, (val, light) in lights.items():
        if isinstance(val, float):
            lines.append(f"• {emoji[light]} {name}: {val:.2f}")
    if prev_row is not None:
        lines.append(f"\n_前次 regime: {prev_row['regime']} (lit {int(prev_row['lit_count'])})_")
    lines.append("_informational tier / 美股經驗閾值未經台股 IC 驗證 / 不接 portfolio gating_")
    return "\n".join(lines)


def push_discord(text: str) -> bool:
    import requests
    url = os.environ.get('DISCORD_WEBHOOK_MACRO') or os.environ.get('DISCORD_WEBHOOK')
    if not url:
        logger.info("No DISCORD_WEBHOOK[_MACRO] env, skip push")
        return False
    try:
        r = requests.post(url, json={'content': text}, timeout=20)
        r.raise_for_status()
        logger.info("Discord push OK")
        return True
    except Exception as e:
        logger.error("Discord push failed: %s", e)
        return False


def _load_last_alert() -> dict | None:
    if not NOTIFY_STATE.exists():
        return None
    try:
        return json.loads(NOTIFY_STATE.read_text(encoding='utf-8'))
    except Exception:
        return None


def _save_last_alert(state: dict):
    NOTIFY_STATE.parent.mkdir(parents=True, exist_ok=True)
    NOTIFY_STATE.write_text(json.dumps(state, default=str, ensure_ascii=False), encoding='utf-8')


def maybe_notify(df: pd.DataFrame, force: bool = False):
    """regime 從低升高 (monitor→warning / warning→high_alert / ...→defensive) 才推。
    同日不重複。回到 green / 持平 / 下降都不推。"""
    if df.empty:
        return
    latest = df.iloc[-1]
    today_str = pd.Timestamp(latest['date']).strftime('%Y-%m-%d')
    latest_regime = latest['regime']
    latest_lit = int(latest['lit_count'])

    last = _load_last_alert() or {}
    if not force and last.get('date') == today_str:
        logger.info("Already alerted today (%s), skip", today_str)
        return

    prev_lit = last.get('lit_count', -1)
    if not force and latest_lit <= prev_lit:
        logger.info("Lit count %d <= last alerted %d, skip (only escalation triggers)",
                    latest_lit, prev_lit)
        # 但 state 仍要更新 date 避免今日重判
        _save_last_alert({'date': today_str, 'lit_count': latest_lit, 'regime': latest_regime})
        return

    if not force and latest_regime == 'green':
        logger.info("Regime green, skip")
        _save_last_alert({'date': today_str, 'lit_count': 0, 'regime': 'green'})
        return

    prev_row = df.iloc[-2] if len(df) >= 2 else None
    msg = _format_alert(latest, prev_row)
    if push_discord(msg):
        _save_last_alert({'date': today_str, 'lit_count': latest_lit, 'regime': latest_regime})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--notify', action='store_true', help='regime 升級時推 Discord')
    ap.add_argument('--force-notify', action='store_true', help='強制推 (忽略 state)')
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

    if args.notify or args.force_notify:
        maybe_notify(df, force=args.force_notify)

    return 0


if __name__ == '__main__':
    sys.exit(main())
