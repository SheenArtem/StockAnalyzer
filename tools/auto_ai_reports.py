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
    """Run full AI report pipeline for one ticker. Returns (ok, rid_or_err)."""
    from technical_analysis import plot_dual_timeframe as run_analysis
    from analysis_engine import TechnicalAnalyzer
    from chip_analysis import ChipAnalyzer
    from fundamental_analysis import get_fundamentals

    logger.info('[%s] loading data...', ticker)
    try:
        figures, errors, df_week, df_day, meta = run_analysis(ticker, force_update=False)
    except Exception as e:
        return False, f'price load failed: {e}'
    if df_day is None or df_day.empty:
        return False, 'no price data'

    chip_data, us_chip_data = None, None
    if ticker.isdigit() or ticker.endswith('.TW'):
        try:
            chip_data, _ = ChipAnalyzer().get_chip_data(ticker, scan_mode=False)
        except Exception as e:
            logger.warning('[%s] chip load failed: %s', ticker, e)
    else:
        try:
            from us_stock_chip import USStockChipAnalyzer
            us_chip_data, _ = USStockChipAnalyzer().get_chip_data(ticker)
        except Exception as e:
            logger.warning('[%s] US chip load failed: %s', ticker, e)

    try:
        fund_data = get_fundamentals(ticker)
    except Exception as e:
        logger.warning('[%s] fundamental load failed: %s', ticker, e)
        fund_data = None

    logger.info('[%s] running analyzer...', ticker)
    try:
        analyzer = TechnicalAnalyzer(ticker, df_week, df_day, chip_data=chip_data, us_chip_data=us_chip_data)
        report = analyzer.run_analysis()
    except Exception as e:
        return False, f'analyzer failed: {e}'

    logger.info('[%s] calling Claude CLI (%s)...', ticker, fmt)
    try:
        if fmt == 'html':
            from ai_report import generate_report_html, save_report_html
            ok, content_or_err, data = generate_report_html(
                ticker, report, chip_data, us_chip_data, fund_data, df_day, timeout=None,
            )
            if not ok:
                return False, f'CLI failed: {content_or_err[:200]}'
            rid = save_report_html(
                ticker, content_or_err,
                trigger_score=report.get('trigger_score'),
                trend_score=report.get('trend_score'),
                json_data=data,
            )
        else:
            from ai_report import generate_report, save_report
            ok, content = generate_report(
                ticker, report, chip_data, us_chip_data, fund_data, df_day, timeout=None,
            )
            if not ok:
                return False, f'CLI failed: {content[:200]}'
            rid = save_report(
                ticker, content,
                trigger_score=report.get('trigger_score'),
                trend_score=report.get('trend_score'),
            )
        return True, rid
    except Exception as e:
        return False, f'report generation failed: {e}'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--tickers', help='comma-separated ticker list; default = top 3 office picks')
    ap.add_argument('--n', type=int, default=3, help='number of office picks (if --tickers not given)')
    ap.add_argument('--format', choices=['html', 'md'], default='md')
    args = ap.parse_args()

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
