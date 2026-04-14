"""
scanner_job.py — 自動選股統一入口

本機執行掃描 → 結果存入 data/ → 可選 git push 到 repo

Usage:
    python scanner_job.py                    # 右側動能掃描（預設）
    python scanner_job.py --no-chip          # 跳過籌碼（加速）
    python scanner_job.py --push             # 掃描完自動 git push
    python scanner_job.py --top 30           # 輸出前 30 名

排程（Windows Task Scheduler）:
    每日 19:00 執行: python scanner_job.py --push --no-chip
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


# ============================================================
# Level 1 Health Check — detect silent failures (e.g. yfinance 429)
# ============================================================
_MIN_SCAN_SIZE = {      # 正常 total_scanned 下限，低於此代表資料源掛了
    'us': 400,
    'tw': 1500,
}
_MIN_RESULTS = {        # 正常 results 數量下限
    ('momentum', 'us'): 10,
    ('momentum', 'tw'): 30,
    ('value', 'us'): 10,
    ('value', 'tw'): 30,
    ('swing', 'us'): 5,
    ('swing', 'tw'): 10,
}


def check_scan_health(result, market, scan_type):
    """
    檢查 scan 結果是否健康。偵測靜默失敗（yfinance 429 / FinMind 爆額度等）。
    回傳 (is_healthy, issues_list)。
    """
    results_count = len(result.get('results', []))
    total_scanned = result.get('total_scanned', 0)
    failures = result.get('failures', [])

    issues = []
    min_scan = _MIN_SCAN_SIZE.get(market, 100)
    threshold = _MIN_RESULTS.get((scan_type, market), 10)

    if total_scanned < min_scan:
        issues.append(f"total_scanned={total_scanned} (expected >={min_scan}) -- data source likely failed")
    if results_count < threshold:
        issues.append(f"results={results_count} (expected >={threshold}) -- suspiciously low hit count")
    if len(failures) > total_scanned * 0.2 and total_scanned > 0:
        issues.append(f"failures={len(failures)}/{total_scanned} ({100*len(failures)/total_scanned:.0f}%) -- high failure rate")

    if issues:
        bar = "!" * 70
        print(f"\n{bar}")
        print(f"  [ALERT] {scan_type.upper()} [{market.upper()}] SCAN HEALTH CHECK FAILED")
        for i in issues:
            print(f"  !! {i}")
        print(f"{bar}\n")
        logger.error(f"Scan alert [{scan_type}/{market}]: {' | '.join(issues)}")
        return False, issues
    return True, []


def send_alert_notification(scan_type, market, issues, webhook_url=None):
    """把健康檢查 alert 也送到 Discord（若有設 webhook）。"""
    if not webhook_url:
        env_path = Path('local/.env')
        if env_path.exists():
            with open(env_path, 'r') as f:
                for line in f:
                    if line.startswith('DISCORD_WEBHOOK_URL='):
                        webhook_url = line.strip().split('=', 1)[1].strip()
                        break
    if not webhook_url:
        return False
    content = (f"🚨 **SCAN ALERT** — {scan_type.upper()} [{market.upper()}] "
               f"{datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
               + "\n".join(f"• {i}" for i in issues))
    try:
        import requests
        resp = requests.post(webhook_url, json={'content': content}, timeout=10)
        return resp.status_code == 204
    except Exception as e:
        logger.error("Alert Discord notification failed: %s", e)
        return False


def send_discord_notification(result, webhook_url=None):
    """Send scan summary to Discord via webhook."""
    if not webhook_url:
        # Try to read from local/.env
        env_path = Path('local/.env')
        if env_path.exists():
            with open(env_path, 'r') as f:
                for line in f:
                    if line.startswith('DISCORD_WEBHOOK_URL='):
                        webhook_url = line.strip().split('=', 1)[1].strip()
                        break
    if not webhook_url:
        return False

    scan_type = result.get('scan_type', 'momentum')
    market = result.get('market', 'tw')
    label = {'momentum': 'Momentum', 'value': 'Value'}.get(scan_type, scan_type)
    mkt_label = {'tw': 'Taiwan', 'us': 'US'}.get(market, market)

    results = result.get('results', [])
    top5 = results[:5]

    lines = [f"**{label} Screener [{mkt_label}]** — {result.get('scan_date', '?')} {result.get('scan_time', '')}",
             f"Scanned {result.get('total_scanned', 0)} → Passed {result.get('passed_initial', 0)} → Scored {result.get('scored_count', 0)}",
             ""]

    if scan_type == 'value':
        for i, r in enumerate(top5, 1):
            lines.append(f"{i}. **{r['stock_id']}** {r.get('name', '')[:6]} "
                         f"PE={r.get('PE', 0):.1f} Score={r.get('value_score', 0):.1f}")
    else:
        for i, r in enumerate(top5, 1):
            sigs = ', '.join(r.get('signals', [])[:2])
            lines.append(f"{i}. **{r['stock_id']}** {r.get('name', '')[:6]} "
                         f"Score={r.get('trigger_score', 0):+.1f} [{sigs}]")

    content = '\n'.join(lines)
    try:
        import requests
        resp = requests.post(webhook_url, json={'content': content}, timeout=10)
        return resp.status_code == 204
    except Exception as e:
        logger.error("Discord notification failed: %s", e)
        return False


def git_push_results(data_dir='data'):
    """Stage and push scan results to remote."""
    try:
        data_path = Path(data_dir)
        if not data_path.exists():
            logger.warning("Data dir %s not found, skipping push", data_dir)
            return False

        # Stage data files (data/history is gitignored, only stage data/latest)
        subprocess.run(
            ['git', 'add', str(data_path / 'latest')],
            check=True, capture_output=True, text=True,
        )

        # Check if there are staged changes
        status = subprocess.run(
            ['git', 'diff', '--cached', '--quiet'],
            capture_output=True,
        )
        if status.returncode == 0:
            print("No changes to push.")
            return True

        # Commit
        now = datetime.now()
        msg = f"scan: {now.strftime('%Y-%m-%d %H:%M')} momentum results"
        subprocess.run(
            ['git', 'commit', '-m', msg],
            check=True, capture_output=True, text=True,
        )

        # Push
        result = subprocess.run(
            ['git', 'push'],
            check=True, capture_output=True, text=True,
        )
        print(f"Pushed: {msg}")
        return True

    except subprocess.CalledProcessError as e:
        logger.error("Git push failed: %s\n%s", e, e.stderr)
        print(f"Git push failed: {e.stderr}")
        return False


def print_summary(result):
    """Print a human-readable summary of scan results."""
    print("\n" + "=" * 65)
    print(f"  Momentum Screener Results — {result['scan_date']} {result['scan_time']}")
    print("=" * 65)
    print(f"  Total scanned:  {result['total_scanned']}")
    print(f"  Stage 1 passed: {result['passed_initial']}")
    print(f"  Stage 2 scored: {result['scored_count']}")
    print(f"  Time elapsed:   {result['elapsed_seconds']}s")

    if result.get('failures'):
        print(f"  Failures:       {len(result['failures'])} "
              f"({', '.join(result['failures'][:5])}...)")

    results = result.get('results', [])
    if not results:
        print("\n  No results.")
        return

    print(f"\n  Top {min(30, len(results))}:")
    print(f"  {'#':>3} {'ID':>6} {'Name':<8} {'Price':>8} {'Chg%':>6} "
          f"{'Score':>6} {'Trend':>6} {'Signals'}")
    print("  " + "-" * 63)

    for i, r in enumerate(results[:30], 1):
        sigs = ', '.join(r['signals'][:4])
        name = r['name'][:8]
        print(f"  {i:3d} {r['stock_id']:>6} {name:<8} "
              f"${r['price']:>7.1f} {r['change_pct']:+5.1f}% "
              f"{r['trigger_score']:+5.1f} {r['trend_score']:+5.1f}  {sigs}")

    print("=" * 65)


def print_value_summary(result):
    """Print value screener summary."""
    print("\n" + "=" * 70)
    print(f"  Value Screener Results — {result.get('scan_date', '?')} {result.get('scan_time', '')}")
    print("=" * 70)
    print(f"  Total scanned:  {result['total_scanned']}")
    print(f"  Stage 1 passed: {result['passed_initial']}")
    print(f"  Stage 2 scored: {result['scored_count']}")
    print(f"  Time elapsed:   {result['elapsed_seconds']}s")

    results = result.get('results', [])
    if not results:
        print("\n  No results.")
        return

    print(f"\n  Top {min(30, len(results))}:")
    print(f"  {'#':>3} {'ID':>6} {'Name':<8} {'Price':>8} {'PE':>6} {'PB':>5} "
          f"{'DY%':>5} {'Score':>6}  {'V':>3} {'Q':>3} {'R':>3} {'T':>3} {'S':>3}")
    print("  " + "-" * 68)

    for i, r in enumerate(results[:30], 1):
        s = r.get('scores', {})
        name = r.get('name', '')[:8]
        print(f"  {i:3d} {r['stock_id']:>6} {name:<8} "
              f"${r['price']:>7.1f} {r['PE']:>5.1f} {r['PB']:>5.2f} "
              f"{r['dividend_yield']:>4.1f}% {r['value_score']:>5.1f}  "
              f"{s.get('valuation', 0):>3.0f} {s.get('quality', 0):>3.0f} "
              f"{s.get('revenue', 0):>3.0f} {s.get('technical', 0):>3.0f} "
              f"{s.get('smart_money', 0):>3.0f}")

    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='StockAnalyzer Momentum Screener Job',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('--mode', choices=['momentum', 'value', 'swing', 'both', 'all'],
                        default='both',
                        help='Scan mode: momentum, value, swing, both (mom+val), all (default: both)')
    parser.add_argument('--market', choices=['tw', 'us', 'all'],
                        default='all',
                        help='Market: tw (Taiwan), us (S&P 500), all (default: all)')
    parser.add_argument('--no-chip', action='store_true',
                        help='Skip chip data (faster, ~4min vs ~8min)')
    parser.add_argument('--top', type=int, default=50,
                        help='Number of results (default: 50)')
    parser.add_argument('--push', action='store_true',
                        help='Git push results after scan')
    parser.add_argument('--notify', action='store_true',
                        help='Send results to Discord webhook (needs DISCORD_WEBHOOK_URL in local/.env)')
    parser.add_argument('--output-dir', default='data',
                        help='Output directory (default: data)')
    parser.add_argument('--stage1-only', action='store_true',
                        help='Only run Stage 1 (quick preview)')
    parser.add_argument('--quiet', action='store_true',
                        help='Minimal output')
    parser.add_argument('--twse-pct', type=float, default=0.0002,
                        help='TWSE value pct threshold (default: 0.0002)')
    parser.add_argument('--tpex-pct', type=float, default=0.0005,
                        help='TPEX value pct threshold (default: 0.0005)')
    parser.add_argument('--max-pe', type=float, default=30,
                        help='Max PE for value screener (default: 30)')

    args = parser.parse_args()

    # Setup logging
    log_level = logging.WARNING if args.quiet else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    )

    # Build config
    config = {
        'top_n': args.top,
        'include_chip': not args.no_chip,
        'twse_value_pct': args.twse_pct,
        'tpex_value_pct': args.tpex_pct,
    }

    # Progress callback
    def progress(msg):
        if not args.quiet:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    run_momentum = args.mode in ('momentum', 'both', 'all')
    run_value = args.mode in ('value', 'both', 'all')
    run_swing = args.mode in ('swing', 'all')
    markets = ['tw', 'us'] if args.market == 'all' else [args.market]

    # Pre-warm TWSE/TPEX cache if running both screeners (avoid duplicate API calls)
    if run_momentum and run_value and 'tw' in markets:
        from twse_api import TWSEOpenData
        _api = TWSEOpenData()
        progress("Pre-fetching market data (shared by both screeners)...")
        _api.get_market_daily_all()
        _api.get_pe_dividend_all_combined()

    # --- Momentum Screener ---
    if run_momentum:
        from momentum_screener import MomentumScreener
        for mkt in markets:
            mkt_label = 'Taiwan' if mkt == 'tw' else 'US (S&P 500)'
            progress(f"=== Momentum Screener [{mkt_label}] ===")
            m_screener = MomentumScreener(config=config, progress_callback=progress)

            if args.stage1_only:
                df = m_screener.run_stage1_only(market=mkt)
                print(f"\nMomentum Stage 1 [{mkt}]: {len(df)} candidates")
                if not df.empty:
                    cols = ['stock_id', 'stock_name', 'market', 'close',
                            'change_pct', 'trading_value']
                    show_cols = [c for c in cols if c in df.columns]
                    print(df[show_cols].head(30).to_string(index=False))
            else:
                m_result = m_screener.run(market=mkt)
                MomentumScreener.save_results(m_result, args.output_dir)
                progress(f"Momentum [{mkt}] results saved")
                # Level 1 health check
                healthy, issues = check_scan_health(m_result, mkt, 'momentum')
                if not healthy and args.notify:
                    send_alert_notification('momentum', mkt, issues)
                if args.notify:
                    send_discord_notification(m_result)
                if not args.quiet:
                    print_summary(m_result)

    # --- Value Screener ---
    if run_value:
        from value_screener import ValueScreener
        for mkt in markets:
            mkt_label = 'Taiwan' if mkt == 'tw' else 'US (S&P 500)'
            progress(f"=== Value Screener [{mkt_label}] ===")
            v_config = {
                'top_n': args.top,
                'include_chip': not args.no_chip,
                'max_pe': args.max_pe,
            }
            v_screener = ValueScreener(config=v_config, progress_callback=progress)

            if args.stage1_only:
                df = v_screener.run_stage1_only(market=mkt)
                print(f"\nValue Stage 1 [{mkt}]: {len(df)} candidates")
                if not df.empty:
                    cols = ['stock_id', 'stock_name', 'market', 'close',
                            'PE', 'PB', 'dividend_yield', 'trading_value']
                    show_cols = [c for c in cols if c in df.columns]
                    print(df[show_cols].head(30).to_string(index=False))
            else:
                v_result = v_screener.run(market=mkt)
                ValueScreener.save_results(v_result, args.output_dir)
                progress(f"Value [{mkt}] results saved")
                # Level 1 health check
                healthy, issues = check_scan_health(v_result, mkt, 'value')
                if not healthy and args.notify:
                    send_alert_notification('value', mkt, issues)
                if args.notify:
                    send_discord_notification(v_result)
            if not args.quiet:
                print_value_summary(v_result)

    # --- Swing Screener (reuses MomentumScreener with mode='swing') ---
    if run_swing:
        from momentum_screener import MomentumScreener
        for mkt in markets:
            mkt_label = 'Taiwan' if mkt == 'tw' else 'US'
            progress(f"=== Swing Screener [{mkt_label}] ===")
            s_screener = MomentumScreener(config=config, progress_callback=progress)
            s_result = s_screener.run(market=mkt, mode='swing')
            MomentumScreener.save_results(s_result, args.output_dir)
            progress(f"Swing [{mkt}] results saved")
            healthy, issues = check_scan_health(s_result, mkt, 'swing')
            if not healthy and args.notify:
                send_alert_notification('swing', mkt, issues)
            if not args.quiet:
                print_summary(s_result)

    # Print FinMind API usage stats
    if not args.quiet:
        from cache_manager import get_finmind_stats
        stats = get_finmind_stats()
        if stats:
            print(f"\n[FinMind API] Requests: {stats['request_count']}/{_FINMIND_RATE_LIMIT} "
                  f"| Rate: {stats['rate_per_hour']:.0f}/hr "
                  f"| Remaining: {stats['remaining']} "
                  f"| Token: {'Yes' if stats['has_token'] else 'NO!'}")

    # Performance tracking: update historical picks with latest prices
    if not args.stage1_only:
        try:
            from scan_tracker import ScanTracker
            progress("=== Performance Tracking ===")
            tracker = ScanTracker(progress_callback=progress)
            track_result = tracker.run()
            summary = track_result.get('summary', {})
            for key, s in summary.items():
                for d in [5, 10, 20, 40, 60]:
                    tracked = s.get(f'tracked_{d}d', 0)
                    if tracked > 0:
                        wr = s.get(f'win_rate_{d}d', 0)
                        avg = s.get(f'avg_return_{d}d', 0)
                        progress(f"  {key} {d}d: {tracked} tracked, Win {wr:.1f}%, Avg {avg:+.2f}%")
                # Benchmark IR (BM-b): 超額報酬 + Information Ratio
                bm_stats = s.get('benchmarks', {})
                from scan_tracker import _bm_display_name
                for bm, horizons in bm_stats.items():
                    bm_label = _bm_display_name(bm)
                    for d in [5, 10, 20, 40, 60]:
                        h = horizons.get(f'{d}d')
                        if h:
                            progress(f"    vs {bm_label} {d}d: n={h['n']}, "
                                     f"excess {h['avg_excess']:+.2f}%, "
                                     f"TE {h['tracking_error']:.2f}%, "
                                     f"IR {h['ir']:+.3f}, "
                                     f"Win {h['win_rate_vs_bm']:.1f}%")
        except Exception as e:
            progress(f"Tracking update failed: {e}")

    # Git push
    if args.push and not args.stage1_only:
        progress("Pushing results to remote...")
        if not git_push_results(args.output_dir):
            sys.exit(1)  # propagate failure so Task Scheduler / run_scanner.bat see it


_FINMIND_RATE_LIMIT = 600  # For display only


if __name__ == '__main__':
    main()
