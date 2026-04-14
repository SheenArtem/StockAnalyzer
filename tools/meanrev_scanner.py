"""
meanrev_scanner.py -- 短線均值回歸掃描器 (P3)

IC v2: 1d horizon IC=+0.060, Win 75.5%, 10d 後衰退。
適合 1-3 天短線操作，獨立於 scanner 持倉型策略。

用法:
  python tools/meanrev_scanner.py                  # 掃描最近 scanner picks (快速)
  python tools/meanrev_scanner.py --all             # 掃描所有有快取的股票
  python tools/meanrev_scanner.py --stocks 2330 2317 AAPL
  python tools/meanrev_scanner.py --top 10          # 顯示前 10
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from technical_analysis import calculate_all_indicators


CACHE_DIR = Path('data_cache')
LATEST_DIR = Path('data/latest')


def load_cached_price(stock_id):
    """Read price CSV from cache. Returns DataFrame or None."""
    path = CACHE_DIR / f'{stock_id}_price.csv'
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if len(df) < 60:
            return None
        return df
    except Exception:
        return None


def get_stock_ids(args):
    """Resolve stock list from CLI args."""
    if args.stocks:
        return args.stocks
    if args.all:
        return sorted({p.stem.replace('_price', '')
                       for p in CACHE_DIR.glob('*_price.csv')})
    # Default: latest scanner picks
    ids = set()
    for f in LATEST_DIR.glob('*.json'):
        try:
            data = json.loads(f.read_text(encoding='utf-8'))
            for r in data.get('results', []):
                ids.add(r['stock_id'])
        except Exception:
            pass
    return sorted(ids) if ids else []


def scan(stock_ids, top_n=20):
    """Compute MeanRev_Composite for each stock, return sorted list."""
    results = []
    for sid in stock_ids:
        df = load_cached_price(sid)
        if df is None:
            continue
        try:
            df = calculate_all_indicators(df)
            mr = df['MeanRev_Composite'].iloc[-1]
            if pd.isna(mr):
                continue
            close = df['Close'].iloc[-1]
            rsi = df['RSI'].iloc[-1] if 'RSI' in df.columns else None
            bias = df['BIAS'].iloc[-1] if 'BIAS' in df.columns else None
            results.append({
                'stock_id': sid,
                'close': round(close, 2),
                'meanrev': round(mr, 4),
                'rsi': round(rsi, 1) if rsi and not pd.isna(rsi) else None,
                'bias': round(bias, 2) if bias and not pd.isna(bias) else None,
            })
        except Exception:
            continue
    results.sort(key=lambda x: x['meanrev'])
    return results


def main():
    parser = argparse.ArgumentParser(description='Short-term mean reversion scanner')
    parser.add_argument('--stocks', nargs='+', help='Specific stock IDs')
    parser.add_argument('--all', action='store_true', help='Scan all cached stocks')
    parser.add_argument('--top', type=int, default=20, help='Show top N each side')
    args = parser.parse_args()

    ids = get_stock_ids(args)
    if not ids:
        print('No stocks to scan. Use --stocks or --all.')
        return

    print(f'Scanning {len(ids)} stocks...')
    results = scan(ids, args.top)
    print(f'Computed: {len(results)} stocks\n')

    n = args.top

    # Oversold (buy candidates) — most negative MeanRev
    print(f'=== OVERSOLD (Buy 1-3d) Top {n} ===')
    print(f'{"ID":>8} {"Close":>8} {"MeanRev":>8} {"RSI":>6} {"BIAS%":>7}')
    print('-' * 42)
    for r in results[:n]:
        rsi = f'{r["rsi"]:.0f}' if r['rsi'] else '  -'
        bias = f'{r["bias"]:+.1f}' if r['bias'] else '   -'
        print(f'{r["stock_id"]:>8} {r["close"]:>8.2f} {r["meanrev"]:>+8.4f} {rsi:>6} {bias:>7}')

    print()

    # Overbought (avoid/short candidates) — most positive MeanRev
    print(f'=== OVERBOUGHT (Avoid/Short 1-3d) Top {n} ===')
    print(f'{"ID":>8} {"Close":>8} {"MeanRev":>8} {"RSI":>6} {"BIAS%":>7}')
    print('-' * 42)
    for r in reversed(results[-n:]):
        rsi = f'{r["rsi"]:.0f}' if r['rsi'] else '  -'
        bias = f'{r["bias"]:+.1f}' if r['bias'] else '   -'
        print(f'{r["stock_id"]:>8} {r["close"]:>8.2f} {r["meanrev"]:>+8.4f} {rsi:>6} {bias:>7}')


if __name__ == '__main__':
    main()
