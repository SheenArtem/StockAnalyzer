"""
Auto-generate AI reports for QM office picks (top 3 ticker list).

Called at end of scanner_job.py. Runs Claude CLI per ticker (30-90s each),
saves HTML dashboards to reports library for quick review next morning.

Usage (after QM scan):
    python tools/auto_ai_reports.py               # default top 3 office picks (md)
    python tools/auto_ai_reports.py --tickers 2330,2454,2345
    python tools/auto_ai_reports.py --format html # html dashboard instead of md
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)


def _smoke_imports():
    """Pre-flight import check: verify every lazily-imported symbol exists BEFORE the scheduler
    commits to a long Claude CLI run. Catches module rename / API drift (2026-04-22 事件的原型）。
    Fails LOUDLY with the missing symbol so run_scanner.bat can alert Discord.
    """
    required = [
        ('ai_report_pipeline', 'generate_one_report'),
        ('technical_analysis', 'plot_dual_timeframe'),
        ('analysis_engine', 'TechnicalAnalyzer'),
        ('chip_analysis', 'ChipAnalyzer'),
        ('fundamental_analysis', 'get_fundamentals'),
        ('us_stock_chip', 'USStockChipAnalyzer'),
        ('ai_report', 'generate_report'),
        ('ai_report', 'save_report'),
        ('ai_report', 'generate_report_html'),
        ('ai_report', 'save_report_html'),
        ('tools.qm_office_picks', 'select_office_picks'),
    ]
    failed = []
    for mod, attr in required:
        try:
            m = __import__(mod, fromlist=[attr])
            if not hasattr(m, attr):
                failed.append(f'{mod}.{attr} (module loaded but attribute missing)')
        except Exception as e:
            failed.append(f'{mod}.{attr} ({type(e).__name__}: {e})')
    if failed:
        logger.critical('=== SMOKE IMPORT CHECK FAILED ===')
        for f in failed:
            logger.critical('  missing: %s', f)
        logger.critical('Aborting before Claude CLI call to avoid silent scheduler failure.')
        sys.exit(2)
    logger.info('✓ smoke import check passed (%d symbols verified)', len(required))


def _load_office_picks(n=3):
    qm_path = ROOT / 'data' / 'latest' / 'qm_result.json'
    if not qm_path.exists():
        logger.error('qm_result.json not found')
        return []
    with open(qm_path, 'r', encoding='utf-8') as f:
        qm = json.load(f)
    from tools.qm_office_picks import select_office_picks
    return [p['stock_id'] for p in select_office_picks(qm, n=n)]


def _run_one(ticker, fmt='html'):
    """Run full AI report pipeline for one ticker via shared ai_report_pipeline.

    Delegates to `generate_one_report()` (also used by app.py:_ai_report_worker)
    so CLI and UI走同一條 code path — eliminates the drift 風險 that caused
    2026-04-22 三連 bug.

    Returns:
        (ok: bool, rid_or_err: str)
    """
    from ai_report_pipeline import generate_one_report

    def _progress(msg):
        logger.info('[%s] %s', ticker, msg)

    result = generate_one_report(ticker, fmt=fmt, progress_cb=_progress)
    if result['ok']:
        return True, result['rid']
    err = result.get('error') or 'unknown error'
    return False, (err[:200] if len(err) > 200 else err)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--tickers', help='comma-separated ticker list; default = top 3 office picks')
    ap.add_argument('--n', type=int, default=3, help='number of office picks (if --tickers not given)')
    ap.add_argument('--format', choices=['html', 'md'], default='md')
    ap.add_argument('--smoke', action='store_true', help='pre-flight import check only, then exit')
    args = ap.parse_args()

    # Pre-flight import check — fail fast before spending 5min on Claude CLI
    _smoke_imports()
    if args.smoke:
        logger.info('Smoke mode: import check passed, exiting without running Claude CLI.')
        sys.exit(0)

    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(',') if t.strip()]
    else:
        tickers = _load_office_picks(n=args.n)

    if not tickers:
        logger.error('No tickers to process')
        sys.exit(1)

    logger.info('Processing %d tickers: %s', len(tickers), tickers)
    t0 = time.time()
    ok_count, fail_count = 0, 0
    for ticker in tickers:
        t_start = time.time()
        ok, rid_or_err = _run_one(ticker, fmt=args.format)
        elapsed = time.time() - t_start
        if ok:
            logger.info('[%s] OK rid=%s (%.0fs)', ticker, rid_or_err, elapsed)
            ok_count += 1
        else:
            logger.error('[%s] FAIL %s (%.0fs)', ticker, rid_or_err, elapsed)
            fail_count += 1

    total = time.time() - t0
    logger.info('Done: %d OK / %d FAIL (total %.0fs)', ok_count, fail_count, total)
    sys.exit(0 if fail_count == 0 else 1)


if __name__ == '__main__':
    main()
