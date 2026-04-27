"""
promote_to_frozen.py — 把 fundamental_cache live 推升到 fundamental_frozen (Layer 0)

Cache 三層架構鐵則 #2: backfill 工具只能寫 live；frozen 必須由本工具手動推升。

用途:
- live 已驗證歷史穩定 (例如 1-3 個月不變的舊期)，推到 frozen 唯讀層
- 之後 backfill 工具不會再覆寫 frozen 內容，避免污染
- 遇 schema/data drift 時可從 frozen 還原 live

操作:
  python tools/promote_to_frozen.py --category month_revenue --cutoff 2025-12-31
  python tools/promote_to_frozen.py --category all --cutoff 2025-12-31  # 4 類一起
  python tools/promote_to_frozen.py --dry-run --cutoff 2025-12-31

Schema 安全:
- 只搬 cutoff 之前的 row 到 frozen (live 保留全期歷史不動)
- frozen 既存就 merge (按 date 去重，不覆寫)
- 寫完印對比 stats，user 可手動 audit
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

LIVE_DIR = ROOT / "data_cache" / "fundamental_cache"
FROZEN_DIR = ROOT / "data_cache" / "fundamental_frozen"

CATEGORIES_ALL = ['month_revenue', 'income', 'balance_sheet', 'cash_flow']

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("promote")


def promote_one(live_path: Path, frozen_path: Path, cutoff: pd.Timestamp,
                  dry_run: bool = False) -> dict:
    """搬 live 中 date <= cutoff 的 row 到 frozen (frozen 既存就 merge)."""
    if not live_path.exists() or live_path.stat().st_size == 0:
        return {'skipped': 'live_empty'}
    try:
        live_df = pd.read_parquet(live_path)
    except Exception as e:
        return {'error': f'live read failed: {e}'}
    if live_df.empty or 'date' not in live_df.columns:
        return {'skipped': 'no_date_column'}

    live_df['date'] = pd.to_datetime(live_df['date'])
    to_promote = live_df[live_df['date'] <= cutoff].copy()
    if to_promote.empty:
        return {'skipped': 'no_data_before_cutoff'}

    # Merge with existing frozen (date 去重，frozen 已存在就保留)
    if frozen_path.exists():
        try:
            frozen_existing = pd.read_parquet(frozen_path)
            frozen_existing['date'] = pd.to_datetime(frozen_existing['date'])
            merged = pd.concat([frozen_existing, to_promote], ignore_index=True)
            if 'stock_id' in merged.columns:
                merged = merged.drop_duplicates(subset=['stock_id', 'date'], keep='first')
            else:
                merged = merged.drop_duplicates(subset=['date'], keep='first')
            merged = merged.sort_values('date').reset_index(drop=True)
        except Exception as e:
            return {'error': f'frozen merge failed: {e}'}
    else:
        merged = to_promote.sort_values('date').reset_index(drop=True)

    n_promoted = len(to_promote)
    if dry_run:
        return {'would_promote': n_promoted, 'final_frozen_rows': len(merged)}
    try:
        frozen_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(frozen_path)
        return {'promoted': n_promoted, 'final_frozen_rows': len(merged)}
    except Exception as e:
        return {'error': f'frozen write failed: {e}'}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--category', default='all',
                    help=f'類別: {CATEGORIES_ALL} or all')
    ap.add_argument('--cutoff', required=True,
                    help='只推 date <= cutoff 的 row (YYYY-MM-DD)')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--limit', type=int, default=None,
                    help='POC: 只跑前 N 個 ticker')
    args = ap.parse_args()

    categories = CATEGORIES_ALL if args.category == 'all' else [args.category]
    cutoff = pd.Timestamp(args.cutoff)
    logger.info("Promote categories=%s cutoff=%s dry_run=%s", categories, cutoff.date(), args.dry_run)

    if not LIVE_DIR.exists():
        logger.error("Live dir not found: %s", LIVE_DIR)
        sys.exit(1)

    grand_total = {'promoted': 0, 'skipped': 0, 'errors': 0}
    for cat in categories:
        files = sorted(LIVE_DIR.glob(f'{cat}_*.parquet'))
        if args.limit:
            files = files[:args.limit]
        logger.info("[%s] %d live files", cat, len(files))
        cat_stats = {'promoted': 0, 'skipped': 0, 'errors': 0, 'rows': 0}
        for fp in files:
            sid = fp.stem.replace(f'{cat}_', '')
            frozen_fp = FROZEN_DIR / fp.name
            res = promote_one(fp, frozen_fp, cutoff, dry_run=args.dry_run)
            if 'error' in res:
                cat_stats['errors'] += 1
                logger.warning("  %s ERR: %s", sid, res['error'])
            elif 'skipped' in res:
                cat_stats['skipped'] += 1
            else:
                cat_stats['promoted'] += 1
                cat_stats['rows'] += res.get('promoted', res.get('would_promote', 0))
        logger.info("[%s] promoted=%d skipped=%d errors=%d rows=%d",
                     cat, cat_stats['promoted'], cat_stats['skipped'],
                     cat_stats['errors'], cat_stats['rows'])
        grand_total['promoted'] += cat_stats['promoted']
        grand_total['skipped'] += cat_stats['skipped']
        grand_total['errors'] += cat_stats['errors']

    logger.info("=== TOTAL: promoted=%d skipped=%d errors=%d ===",
                 grand_total['promoted'], grand_total['skipped'], grand_total['errors'])


if __name__ == "__main__":
    main()
