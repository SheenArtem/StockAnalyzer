"""
shadow_regime_analysis.py
=========================
VF-G4 shadow analysis：比對 baseline vs volatile-only filter 的實際績效。

資料來源：
  1. data/tracking/tracking_data.json — live scanner picks + 5/10/20/40/60d realized returns
  2. data/tracking/regime_log.jsonl   — 每日 market regime

Logic：
  - 對每筆 tracked pick，查 scan_date 的 market regime
  - 分桶：baseline (all) / volatile-only (regime=volatile) / trending (regime=trending)
  - 計算 mean, win_rate, std, Sharpe per bucket per horizon

用法：
  python tools/shadow_regime_analysis.py
  python tools/shadow_regime_analysis.py --scan-type qm --market tw
  python tools/shadow_regime_analysis.py --horizon 20  # 只看 fwd_20d

Output:
  - stdout 對照表
  - reports/shadow_regime_analysis.csv
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("shadow")

TRACKING_PATH = ROOT / "data" / "tracking" / "tracking_data.json"
REGIME_LOG = ROOT / "data" / "tracking" / "regime_log.jsonl"
OUT_PATH = ROOT / "reports" / "shadow_regime_analysis.csv"


def load_regime_log() -> dict:
    """Load regime log as {date_str: regime}."""
    if not REGIME_LOG.exists():
        raise RuntimeError(f"Regime log 不存在：{REGIME_LOG}  (先跑 market_regime_logger.py)")
    m = {}
    for line in REGIME_LOG.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        m[rec['date']] = rec['regime']
    return m


def lookup_regime(regime_map: dict, date_str: str) -> str:
    """Lookup regime for a date；若當天沒有則用最近的前一天。"""
    if date_str in regime_map:
        return regime_map[date_str]
    # Find nearest earlier date
    dts = sorted([d for d in regime_map.keys() if d <= date_str])
    return regime_map[dts[-1]] if dts else 'unknown'


def flatten_picks(tracking_data: dict, scan_type: str = None, market: str = None) -> pd.DataFrame:
    """Flatten scans[].picks[] to rows。"""
    rows = []
    for scan in tracking_data['scans']:
        if scan_type and scan.get('scan_type') != scan_type:
            continue
        if market and scan.get('market') != market:
            continue
        for pick in scan.get('picks', []):
            rows.append({
                'scan_date': pick.get('scan_date'),
                'scan_type': pick.get('scan_type'),
                'market': pick.get('market'),
                'stock_id': pick.get('stock_id'),
                'name': pick.get('name'),
                'ret_5d': pick.get('return_5d'),
                'ret_10d': pick.get('return_10d'),
                'ret_20d': pick.get('return_20d'),
                'ret_40d': pick.get('return_40d'),
                'ret_60d': pick.get('return_60d'),
            })
    return pd.DataFrame(rows)


def compute_bucket(df: pd.DataFrame, horizon: str) -> dict:
    """Compute metrics for a bucket."""
    ret_col = f'ret_{horizon}d'
    d = df.dropna(subset=[ret_col])
    if d.empty:
        return {'n': 0}
    ret = d[ret_col].values
    return {
        'n': len(d),
        'mean': ret.mean(),
        'median': np.median(ret),
        'std': ret.std(),
        'sharpe': ret.mean() / ret.std() if ret.std() > 0 else 0,
        'win_rate': (ret > 0).mean() * 100,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scan-type", default=None, help="qm / momentum / value / convergence")
    ap.add_argument("--market", default='tw', help="tw / us")
    args = ap.parse_args()

    logger.info("Loading tracking: %s", TRACKING_PATH)
    tracking = json.loads(TRACKING_PATH.read_text(encoding='utf-8'))
    logger.info("Loading regime log: %s", REGIME_LOG)
    regime_map = load_regime_log()
    logger.info("Regime log: %d days", len(regime_map))

    # Flatten
    df = flatten_picks(tracking, scan_type=args.scan_type, market=args.market)
    logger.info("Picks: %d rows", len(df))

    if df.empty:
        logger.error("No picks — check --scan-type / --market")
        return

    # Attach regime
    df['regime'] = df['scan_date'].apply(lambda d: lookup_regime(regime_map, d))

    # Stats per regime
    print("\n" + "=" * 80)
    print(f"Shadow Regime Analysis — scan_type={args.scan_type or 'ALL'} market={args.market}")
    print("=" * 80)

    # Regime 分布
    print("\n[Pick distribution by scan_date regime]")
    print(df['regime'].value_counts().to_string())

    # Per horizon comparison
    results = []
    for horizon in ['5', '10', '20', '40', '60']:
        # Baseline (all)
        base = compute_bucket(df, horizon)
        vol = compute_bucket(df[df['regime'] == 'volatile'], horizon)
        trend = compute_bucket(df[df['regime'] == 'trending'], horizon)
        excl_t = compute_bucket(df[df['regime'] != 'trending'], horizon)

        for bucket, m in [('baseline', base), ('volatile_only', vol),
                          ('trending_only', trend), ('excl_trending', excl_t)]:
            if m.get('n', 0) > 0:
                results.append({
                    'horizon': f'{horizon}d',
                    'bucket': bucket,
                    'n': m['n'],
                    'mean_%': m.get('mean', 0),
                    'median_%': m.get('median', 0),
                    'sharpe': m.get('sharpe', 0),
                    'win_%': m.get('win_rate', 0),
                })

    res_df = pd.DataFrame(results)
    if res_df.empty:
        print("\n[!] 所有 horizon 都無 realized return — picks 太新尚未到期")
    else:
        print("\n[Per-horizon comparison]")
        print(res_df.round(3).to_string(index=False))

        # Key numbers
        print("\n[Key comparison: fwd_20d]")
        h20 = res_df[res_df['horizon'] == '20d']
        if len(h20) >= 2 and h20[h20['bucket'] == 'baseline']['sharpe'].iloc[0] != 0:
            base_s = h20[h20['bucket'] == 'baseline']['sharpe'].iloc[0]
            vol_row = h20[h20['bucket'] == 'volatile_only']
            if len(vol_row) > 0:
                vol_s = vol_row['sharpe'].iloc[0]
                delta = vol_s - base_s
                print(f"  Baseline Sharpe:       {base_s:+.3f}")
                print(f"  Volatile-only Sharpe:  {vol_s:+.3f}")
                print(f"  Delta:                 {delta:+.3f}  ({delta/abs(base_s)*100:+.0f}% relative)")
        else:
            print("  fwd_20d 尚無足夠 realized data（picks 多數未到 20 天）")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    res_df.to_csv(OUT_PATH, index=False, encoding='utf-8-sig')
    logger.info("Saved: %s", OUT_PATH)

    # 加註：live data 夠不夠
    unique_dates = df['scan_date'].nunique()
    print(f"\n[Live data scope]")
    print(f"  Unique scan dates: {unique_dates}")
    print(f"  Total picks:       {len(df)}")
    if unique_dates < 30:
        print(f"  [WARN] Data 太少（< 30 scan dates）— shadow 結論僅參考，需累積至少 1-3 個月")


if __name__ == "__main__":
    main()
