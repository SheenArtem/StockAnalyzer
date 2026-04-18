"""
tw_calendar.py - Taiwan stock market trading calendar helpers.

Lightweight utilities to answer "which date should data be available for, given
the current time?" without pulling a full holiday calendar. Weekday-only check
covers ~99% of cases; rare government holidays are handled implicitly because
TWSE/FinMind return empty data on those days, which the caller interprets as
"no new data".

Why this exists:
  cache_manager.load_cache used to compare `last_date.date() < now.date()` to
  decide whether the cache was stale. On Saturday morning (after a Friday-night
  scan), this triggered an incremental FinMind fetch for "today's data" that
  could never exist (Saturday is not a trading day). Multiplied by hundreds of
  tracked positions in scan_tracker, this wasted FinMind quota and time.
"""

from datetime import date, datetime, time, timedelta


def is_tw_trading_day(d: date) -> bool:
    """Coarse check: weekday Mon-Fri. Government holidays not enumerated.

    For our caching use case this is sufficient; on a Taiwan holiday weekday
    FinMind returns empty data, which the caller already handles as "no new
    data, trust cache".
    """
    return d.weekday() < 5


def last_tw_trading_day(d: date) -> date:
    """Most recent trading day on or before d."""
    while not is_tw_trading_day(d):
        d -= timedelta(days=1)
    return d


def expected_tw_data_date(cutoff: time, now: datetime | None = None) -> date:
    """Date for which trading-session data should already be available.

    Rules:
      - now's date is a trading day AND now's time >= cutoff -> today
      - else -> previous trading day (walk back through weekends)

    Typical cutoffs:
      - 13:30 -- daily kline finalized after market close
      - 17:00 -- TWSE/TPEX institutional buy/sell published
      - 21:00 -- margin / day-trading / shareholding published
    """
    if now is None:
        now = datetime.now()
    today = now.date()
    if is_tw_trading_day(today) and now.time() >= cutoff:
        return today
    return last_tw_trading_day(today - timedelta(days=1))
