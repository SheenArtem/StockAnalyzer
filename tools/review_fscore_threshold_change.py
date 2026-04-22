"""
Review VF-Value-ex2 proposed F-Score threshold change for US.

Current scoring (both TW + US):
  F >= 7: +25
  F >= 5: +10
  F <= 3: -20
  else : 0

Proposed (US only):
  F >= 8: +25
  F >= 7: +10
  F >= 5: +3
  F <= 3: -20
  else : 0

TW keeps current (already validated A-grade IR 0.892).

Output: side-by-side comparison of scoring impact on S&P 500 current universe.
"""

import pandas as pd
from pathlib import Path


def score_current(fs):
    """Live value_screener.py logic (both markets)."""
    if fs >= 7: return 25
    if fs >= 5: return 10
    if fs <= 3: return -20
    return 0


def score_proposed_us(fs):
    """Proposed US logic (stricter +25 threshold)."""
    if fs >= 8: return 25
    if fs >= 7: return 10
    if fs >= 5: return 3
    if fs <= 3: return -20
    return 0


def main():
    q = pd.read_parquet(Path('data_cache/backtest/quality_scores_us.parquet'))
    q['score_current'] = q['f_score'].map(score_current)
    q['score_proposed'] = q['f_score'].map(score_proposed_us)
    q['delta'] = q['score_proposed'] - q['score_current']

    n = len(q)
    print(f'=== VF-Value-ex2 Review: US F-Score threshold change ===')
    print(f'Universe: S&P 500 ({n} stocks)\n')

    print('[Distribution of delta per stock]')
    print(q['delta'].value_counts().sort_index().to_string())
    print()

    print('[Per F-Score bucket]')
    by_fs = q.groupby('f_score').agg(
        n=('ticker', 'count'),
        pct=('ticker', lambda x: f'{len(x)/n*100:.1f}%'),
        current=('score_current', 'first'),
        proposed=('score_proposed', 'first'),
        delta=('delta', 'first'),
    )
    print(by_fs.to_string())
    print()

    print('[Aggregate impact]')
    print(f'  Mean current score   : {q["score_current"].mean():+.2f}')
    print(f'  Mean proposed score  : {q["score_proposed"].mean():+.2f}')
    print(f'  Mean delta           : {q["delta"].mean():+.2f}')
    print(f'  Stocks losing >= 15pt: {(q["delta"] <= -15).sum()} ({(q["delta"]<=-15).mean()*100:.1f}%)')
    print(f'  Stocks gaining  >= 5pt: {(q["delta"] >=  5).sum()} ({(q["delta"]>= 5).mean()*100:.1f}%)')
    print(f'  Stocks unchanged      : {(q["delta"] ==  0).sum()} ({(q["delta"]== 0).mean()*100:.1f}%)')
    print()

    print('[Score stratification after change]')
    buckets = {
        'Elite (+25)': (q['score_proposed'] == 25).sum(),
        'Strong (+10)': (q['score_proposed'] == 10).sum(),
        'OK (+3)': (q['score_proposed'] == 3).sum(),
        'Neutral (0)': (q['score_proposed'] == 0).sum(),
        'Trap (-20)': (q['score_proposed'] == -20).sum(),
    }
    for k, v in buckets.items():
        print(f'  {k:15} : {v:3} ({v/n*100:.1f}%)')
    print()

    print('[Sample stocks per bucket after change]')
    for bucket_score, label in [(25, 'Elite (F>=8)'), (10, 'Strong (F==7)'), (3, 'OK (F=5-6)'), (-20, 'Trap (F<=3)')]:
        sub = q[q['score_proposed'] == bucket_score].head(5)
        if not sub.empty:
            tickers = ', '.join(sub['ticker'] + f"(F={sub['f_score']})".values) if False else ', '.join(f"{r.ticker}(F={r.f_score})" for r in sub.itertuples())
            print(f'  [{label}] {tickers}')


if __name__ == '__main__':
    main()
