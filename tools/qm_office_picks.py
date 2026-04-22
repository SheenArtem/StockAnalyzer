"""
QM 精選 3 檔 — 上班族不看盤版本

Filter logic:
  Hard: TV >= 10億 (大流動), F-Score >= 8 (體質強), composite >= 75 (QM 前段)
  Sort by: composite + ETF_buy*5 - |trigger|*1.5 + min(TV億/20, 5)*3

Philosophy: 大象穩走 vs 追熱門。篩掉小型高波動 / F<8 雷股 /
           過度熱門（trig 絕對值大）。上班族看盤時間少，要的是進場後能放。

Usage:
    from tools.qm_office_picks import select_office_picks
    picks = select_office_picks(qm_result_dict, n=3)

    # CLI quick check:
    python tools/qm_office_picks.py
"""

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RESULT = ROOT / 'data' / 'latest' / 'qm_result.json'

# Filter thresholds — tuned for TW market scale (2026-04)
TV_MIN_NTD = 1_000_000_000   # 10 億 NTD 日均成交額
FSCORE_MIN = 8                # Piotroski 9-point >= 8 (top 15-20%)
COMPOSITE_MIN = 75            # QM composite >= 75


def _office_score(r):
    tv_m = r.get('avg_trading_value_5d', 0) / 1e6
    comp = r.get('composite_score', 0)
    trig = r.get('trigger_score', 0)
    etf = r.get('etf_buy_count', 0)
    return comp + etf * 5 - abs(trig) * 1.5 + min(tv_m / 2000, 5) * 3


def select_office_picks(qm_result, n=3):
    """Select top-N "office worker" picks from full QM results.

    Args:
        qm_result: dict loaded from qm_result.json (has 'results' list)
        n: int, number of picks to return (default 3)

    Returns:
        list of pick dicts (subset of qm_result['results']), with 'office_score'
        key added. Empty list if nothing passes filter.
    """
    results = qm_result.get('results', [])
    candidates = []
    for r in results:
        tv = r.get('avg_trading_value_5d', 0)
        fs = r.get('qm_f_score', 0)
        comp = r.get('composite_score', 0)
        if tv < TV_MIN_NTD or fs < FSCORE_MIN or comp < COMPOSITE_MIN:
            continue
        r2 = dict(r)  # shallow copy
        r2['office_score'] = _office_score(r)
        candidates.append(r2)
    candidates.sort(key=lambda x: x['office_score'], reverse=True)
    return candidates[:n]


def main():
    path = DEFAULT_RESULT
    if not path.exists():
        print(f'Not found: {path}')
        sys.exit(1)
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    picks = select_office_picks(data, n=3)
    if not picks:
        print('No stocks pass the office-worker filter (TV>=10e8, F>=8, comp>=75).')
        sys.exit(0)
    print(f'=== QM Office Picks (top {len(picks)} / {len(data.get("results", []))} total) ===')
    print(f'Generated: {data.get("scan_date", "?")} {data.get("scan_time", "")}')
    print()
    for i, p in enumerate(picks, 1):
        tv_m = p.get('avg_trading_value_5d', 0) / 1e6
        name = p.get('name', '')
        print(f'[{i}] {p["stock_id"]} {name}  price={p["price"]:.1f}  TV={tv_m/100:.0f}億')
        print(f'    F-Score={p.get("qm_f_score",0)}  Composite={p.get("composite_score",0):.1f}  '
              f'Trigger={p.get("trigger_score",0):+.1f}  ETF_buy={p.get("etf_buy_count",0)}  '
              f'office_score={p.get("office_score",0):.1f}')


if __name__ == '__main__':
    main()
