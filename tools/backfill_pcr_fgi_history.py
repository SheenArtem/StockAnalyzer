"""
Backfill scripts for:
  1. TAIFEX PCR (Put/Call Ratio) daily history -> data/sentiment/pcr_history.parquet
  2. TW FGI daily panel -> data/sentiment/fgi_history.parquet

Run:
    python tools/backfill_pcr_fgi_history.py [--pcr-only | --fgi-only]

Notes:
  - PCR source: TAIFEX /cht/3/pcRatio, HTML table, batch by year
  - FGI components:
      market_momentum  : ^TWII close vs 52w high/low (yfinance, 1999+)
      market_breadth   : N/A - no daily advance/decline history; proxy = ^TWII 5d/20d momentum
      put_call_ratio   : pcr_history.parquet (built first)
      volatility       : rv30 column from crash_predictor_tw_panel.parquet (pre-built)
      margin_balance   : chip_history/margin.parquet (2021-04-16+)
"""

import argparse
import logging
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
SENTIMENT_DIR = BASE_DIR / "data" / "sentiment"
PANEL_PATH = BASE_DIR / "reports" / "_history" / "2026_05_crash_predictor_closed" / "crash_predictor_tw_panel.parquet"
MARGIN_PATH = BASE_DIR / "data_cache" / "chip_history" / "margin.parquet"
PCR_OUT = SENTIMENT_DIR / "pcr_history.parquet"
FGI_OUT = SENTIMENT_DIR / "fgi_history.parquet"

TAIFEX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.taifex.com.tw/",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8",
}

# ============================================================
# Part 1: PCR Backfill
# ============================================================

def _fetch_pcr_one_month(session: requests.Session, year: int, month: int) -> pd.DataFrame:
    """
    Fetch PCR data for one calendar month from TAIFEX /cht/3/pcRatio.
    TAIFEX limits the query window to ~1 month; larger ranges return empty tables.

    Returns DataFrame with index=date, columns=[pc_ratio_volume, pc_ratio_oi].
    Empty DataFrame on failure or no data.
    """
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    start = f"{year}/{month:02d}/01"
    end = f"{year}/{month:02d}/{last_day:02d}"
    url = "https://www.taifex.com.tw/cht/3/pcRatio"
    params = {"queryStartDate": start, "queryEndDate": end}

    try:
        resp = session.get(url, params=params, timeout=20, verify=False)
        resp.encoding = "big5"
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("PCR fetch %d/%02d failed: %s", year, month, exc)
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table")
    if not tables or len(tables) < 4:
        # Full result has 4 tables; <4 means no data (404-like page has 2 tables)
        return pd.DataFrame()

    rows = tables[0].find_all("tr")
    if len(rows) < 2:
        return pd.DataFrame()

    records = []
    for row in rows[1:]:
        cells = row.find_all(["th", "td"])
        if len(cells) < 7:
            continue
        raw_date = cells[0].get_text(strip=True).strip()
        # col[3] = volume PCR (call/put ratio expressed as %, i.e. call_vol/put_vol*100)
        # col[6] = OI PCR (call_oi/put_oi*100)
        # NOTE: TAIFEX expresses as CALL/PUT * 100, so high value = more calls = greed
        # We store as put/call ratio (standard convention) so we invert:
        # pc_ratio = put/call = 100 / pcr_pct
        try:
            vol_pct = float(cells[3].get_text(strip=True).replace(",", ""))
            oi_pct = float(cells[6].get_text(strip=True).replace(",", ""))
        except ValueError:
            continue
        if vol_pct <= 0 or oi_pct <= 0:
            continue
        # Convert from call/put% back to put/call ratio (standard)
        pc_vol = 100.0 / vol_pct
        pc_oi = 100.0 / oi_pct
        try:
            dt = pd.to_datetime(raw_date, format="%Y/%m/%d")
        except Exception:
            continue
        records.append({"date": dt, "pc_ratio_volume": round(pc_vol, 4), "pc_ratio_oi": round(pc_oi, 4)})

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records).set_index("date").sort_index()
    return df


def build_pcr_history(start_year: int = 2010) -> pd.DataFrame:
    """
    Fetch PCR history from start_year/01 to current month, batching by month.
    Returns merged DataFrame saved to PCR_OUT.
    """
    session = requests.Session()
    session.headers.update(TAIFEX_HEADERS)

    today = date.today()
    all_frames = []
    total_months = (today.year - start_year) * 12 + today.month

    processed = 0
    year = start_year
    month = 1
    while (year, month) <= (today.year, today.month):
        df_m = _fetch_pcr_one_month(session, year, month)
        if not df_m.empty:
            all_frames.append(df_m)
        processed += 1
        if processed % 12 == 0:
            logger.info("PCR backfill: %d/%d months done (current: %d-%02d)",
                        processed, total_months, year, month)
        # Move to next month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        # Polite delay: 1s between requests
        time.sleep(1.0)

    if not all_frames:
        logger.error("PCR backfill: no data retrieved at all")
        return pd.DataFrame()

    df_all = pd.concat(all_frames).sort_index()
    df_all = df_all[~df_all.index.duplicated(keep="last")]
    df_all.index.name = "date"

    SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)
    df_all.to_parquet(PCR_OUT)
    logger.info("PCR history saved: %d rows %s .. %s -> %s",
                len(df_all), df_all.index.min().date(), df_all.index.max().date(), PCR_OUT)
    return df_all


# ============================================================
# Part 2: FGI History
# ============================================================

# --- FGI weight spec (from taifex_data.TaiwanFearGreedIndex.WEIGHTS) ---
WEIGHTS = {
    "market_momentum": 0.20,
    "market_breadth": 0.20,    # N/A -> proxy or NaN
    "put_call_ratio": 0.20,
    "volatility": 0.20,
    "margin_balance": 0.20,
}

# Scoring parameters (matched to TaiwanFearGreedIndex._calc_* methods)
MOMENTUM_LOW = 0.0      # 52w low = score 0
MOMENTUM_HIGH = 100.0   # 52w high = score 100
PCR_SCORE_HIGH = 1.5    # pc_ratio >= 1.5 -> score 0 (fear)
PCR_SCORE_LOW = 0.5     # pc_ratio <= 0.5 -> score 100 (greed)
VOL_SCORE_HIGH = 30.0   # annual vol >= 30% -> score 0
VOL_SCORE_LOW = 10.0    # annual vol <= 10% -> score 100
MARGIN_SCORE_HIGH = 5.0   # change_rate >= +5% -> score 100
MARGIN_SCORE_LOW = -5.0   # change_rate <= -5% -> score 0
# Breadth proxy: 5d/20d momentum ratio for ^TWII
BREADTH_PROXY_HIGH = 1.05  # 5d/20d return ratio >= 1.05 -> score 100
BREADTH_PROXY_LOW = 0.95   # ratio <= 0.95 -> score 0


def _score_clip(value: float, low: float, high: float, invert: bool = False) -> float:
    """Linear interpolation from [low, high] -> [0, 100], optionally inverted."""
    if high == low:
        return 50.0
    raw = (value - low) / (high - low) * 100.0
    if invert:
        raw = 100.0 - raw
    return float(np.clip(raw, 0.0, 100.0))


def build_fgi_history(pcr_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build daily FGI history panel.

    Args:
        pcr_df: DataFrame with index=date, columns=[pc_ratio_volume, pc_ratio_oi]
                (from build_pcr_history)

    Returns:
        DataFrame with columns: score, market_momentum_score, breadth_score,
        pcr_score, volatility_score, margin_score, breadth_is_proxy
    Saved to FGI_OUT.
    """
    import yfinance as yf

    # --- Load ^TWII full history ---
    logger.info("Fetching ^TWII full history via yfinance ...")
    twii = yf.Ticker("^TWII")
    hist = twii.history(start="1999-01-01", end=str(date.today() + timedelta(days=1)))
    if hist.empty:
        raise RuntimeError("^TWII history empty from yfinance")
    hist.index = pd.DatetimeIndex(hist.index).tz_localize(None)
    twii_close = hist["Close"].rename("twii_close").sort_index()
    logger.info("^TWII history: %d rows (%s .. %s)",
                len(twii_close), twii_close.index.min().date(), twii_close.index.max().date())

    # --- Load rv30 from crash predictor panel (reuse, don't recompute) ---
    rv30_series = None
    if PANEL_PATH.exists():
        panel = pd.read_parquet(PANEL_PATH, columns=["close", "rv10", "rv30"])
        panel.index = pd.DatetimeIndex(panel.index).tz_localize(None)
        rv30_series = panel["rv30"].dropna()
        logger.info("rv30 loaded from panel: %d rows", len(rv30_series))
    else:
        logger.warning("Panel not found at %s; will compute rv30 from ^TWII", PANEL_PATH)

    # --- Load margin balance (chip_history) ---
    margin_series = None
    if MARGIN_PATH.exists():
        margin_df = pd.read_parquet(MARGIN_PATH, columns=["date", "stock_id", "margin_balance"])
        # Aggregate all stocks daily total margin balance
        margin_df["date"] = pd.to_datetime(margin_df["date"])
        daily_margin = margin_df.groupby("date")["margin_balance"].sum()
        daily_margin.index = pd.DatetimeIndex(daily_margin.index).tz_localize(None)
        margin_series = daily_margin.sort_index()
        logger.info("Margin balance loaded: %d days (%s .. %s)",
                    len(margin_series),
                    margin_series.index.min().date(),
                    margin_series.index.max().date())
    else:
        logger.warning("Margin parquet not found at %s", MARGIN_PATH)

    # --- Build date index (intersection of TWII trading days) ---
    all_dates = twii_close.index

    records = []
    twii_arr = twii_close.values
    twii_dates = twii_close.index

    # Pre-build numpy arrays for rolling calcs
    logger.info("Computing component scores for %d days ...", len(all_dates))

    for i, dt in enumerate(all_dates):
        row = {"date": dt}

        # == 1. Market Momentum: close vs rolling 252-day (1yr) high/low ==
        window_start = max(0, i - 252)
        window = twii_arr[window_start: i + 1]
        current = float(twii_arr[i])
        high_52w = float(window.max())
        low_52w = float(window.min())
        if high_52w == low_52w or len(window) < 10:
            row["market_momentum_score"] = np.nan
        else:
            row["market_momentum_score"] = round(
                _score_clip(current, low_52w, high_52w), 1
            )

        # == 2. Market Breadth (proxy): 5d/20d ^TWII return ratio ==
        if i >= 20:
            ret_5d = (twii_arr[i] / twii_arr[i - 5]) - 1.0 if twii_arr[i - 5] > 0 else 0.0
            ret_20d = (twii_arr[i] / twii_arr[i - 20]) - 1.0 if twii_arr[i - 20] > 0 else 0.0
            # Normalize both into a single breadth-like score
            # positive 5d momentum + relative to 20d
            breadth_raw = ret_5d - ret_20d  # outperformance of short-term vs medium-term
            # mapping: breadth_raw +3% -> score 100, -3% -> score 0
            row["breadth_score"] = round(_score_clip(breadth_raw * 100, -3.0, 3.0), 1)
        else:
            row["breadth_score"] = np.nan
        row["breadth_is_proxy"] = True  # always proxy for historical

        # == 3. Put/Call Ratio ==
        if pcr_df is not None and not pcr_df.empty and dt in pcr_df.index:
            pc = float(pcr_df.loc[dt, "pc_ratio_oi"])
            row["pcr_score"] = round(_score_clip(pc, PCR_SCORE_LOW, PCR_SCORE_HIGH, invert=True), 1)
            row["pcr_value"] = pc
        else:
            row["pcr_score"] = np.nan
            row["pcr_value"] = np.nan

        # == 4. Volatility: use rv30 from panel if available, else compute ==
        # Panel rv30 is stored as annualized vol fraction (e.g. 0.22 = 22%).
        # Fallback rv30 is computed as daily std then annualized.
        if rv30_series is not None and dt in rv30_series.index:
            rv30_val = float(rv30_series.loc[dt])
            # rv30 in panel is already annualized fraction: convert to % for scoring
            annual_vol = rv30_val * 100.0
            row["volatility_score"] = round(_score_clip(annual_vol, VOL_SCORE_LOW, VOL_SCORE_HIGH, invert=True), 1)
            row["rv30"] = rv30_val
        elif i >= 30:
            # Fallback: compute 30-day realized vol from ^TWII log returns
            rets = np.diff(np.log(twii_arr[max(0, i - 30): i + 1]))
            if len(rets) >= 20:
                rv = float(np.std(rets))
                annual_vol = rv * np.sqrt(252) * 100.0
                row["volatility_score"] = round(_score_clip(annual_vol, VOL_SCORE_LOW, VOL_SCORE_HIGH, invert=True), 1)
                row["rv30"] = rv * np.sqrt(252)  # store as annualized fraction
            else:
                row["volatility_score"] = np.nan
                row["rv30"] = np.nan
        else:
            row["volatility_score"] = np.nan
            row["rv30"] = np.nan

        # == 5. Margin Balance: 20-day change rate ==
        if margin_series is not None and dt in margin_series.index:
            margin_idx = margin_series.index.get_loc(dt)
            if margin_idx >= 20:
                mb_now = float(margin_series.iloc[margin_idx])
                mb_prev20 = float(margin_series.iloc[margin_idx - 20])
                if mb_prev20 > 0:
                    chg = (mb_now / mb_prev20 - 1.0) * 100.0
                    row["margin_score"] = round(_score_clip(chg, MARGIN_SCORE_LOW, MARGIN_SCORE_HIGH), 1)
                    row["margin_chg_pct"] = chg
                else:
                    row["margin_score"] = np.nan
                    row["margin_chg_pct"] = np.nan
            else:
                row["margin_score"] = np.nan
                row["margin_chg_pct"] = np.nan
        else:
            row["margin_score"] = np.nan
            row["margin_chg_pct"] = np.nan

        records.append(row)

    df = pd.DataFrame(records).set_index("date")
    df.index = pd.DatetimeIndex(df.index).tz_localize(None)
    df.index.name = "date"

    # == Composite FGI score (weight-normalized over available components) ==
    component_cols = {
        "market_momentum": "market_momentum_score",
        "market_breadth": "breadth_score",
        "put_call_ratio": "pcr_score",
        "volatility": "volatility_score",
        "margin_balance": "margin_score",
    }

    scores_df = df[[v for v in component_cols.values()]].copy()
    # For each row, compute weighted average over non-NaN components
    composite_scores = []
    for _, row_s in scores_df.iterrows():
        valid = {k: row_s[v] for k, v in component_cols.items() if not np.isnan(row_s[v])}
        if not valid:
            composite_scores.append(np.nan)
            continue
        total_w = sum(WEIGHTS[k] for k in valid)
        if total_w <= 0:
            composite_scores.append(np.nan)
            continue
        weighted = sum(valid[k] * WEIGHTS[k] / total_w for k in valid)
        composite_scores.append(round(float(np.clip(weighted, 0, 100)), 1))

    df["score"] = composite_scores

    # FGI label
    def label(s):
        if np.isnan(s):
            return "N/A"
        if s < 20:
            return "Extreme Fear"
        if s < 40:
            return "Fear"
        if s < 60:
            return "Neutral"
        if s < 80:
            return "Greed"
        return "Extreme Greed"

    df["label"] = df["score"].map(label)
    df["twii_close"] = twii_close.reindex(df.index)

    SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(FGI_OUT)
    logger.info("FGI history saved: %d rows %s .. %s -> %s",
                len(df), df.index.min().date(), df.index.max().date(), FGI_OUT)
    return df


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Backfill PCR + FGI history panels")
    parser.add_argument("--pcr-only", action="store_true", help="Only run PCR backfill")
    parser.add_argument("--fgi-only", action="store_true", help="Only run FGI backfill (PCR must exist)")
    parser.add_argument("--pcr-start-year", type=int, default=2010,
                        help="PCR start year (default 2010)")
    args = parser.parse_args()

    if args.fgi_only:
        if not PCR_OUT.exists():
            logger.error("PCR history not found at %s; run without --fgi-only first", PCR_OUT)
            return
        pcr_df = pd.read_parquet(PCR_OUT)
        build_fgi_history(pcr_df)
        return

    # Build PCR
    pcr_df = build_pcr_history(start_year=args.pcr_start_year)

    if args.pcr_only:
        return

    # Build FGI
    if pcr_df.empty:
        logger.warning("PCR build returned empty; FGI will use NaN for pcr component")
        pcr_df = pd.DataFrame(columns=["pc_ratio_volume", "pc_ratio_oi"])

    build_fgi_history(pcr_df)


if __name__ == "__main__":
    main()
