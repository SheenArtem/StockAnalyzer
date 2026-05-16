"""
Whale Picks Screener — Phase 1 production selector.

Run weekly/monthly to produce top-20 candidate list using the 8-factor composite_parsi
locked in docs/whale_picks_spec.md v0.4.

Config (LOCKED per v11 backtest):
  - 8 factors pre-registered with sign weights
  - Industry-neutral standardization (by date × industry_category)
  - Monthly rebalance
  - K=20 top picks

Output:
  - data/whale_picks/latest.parquet — full universe scored
  - data/whale_picks/{YYYY-MM-DD}.parquet — dated snapshot
  - data/latest/whale_picks_top20.json — Discord/UI payload
  - Optional Discord push (if --push)

Usage:
    python tools/whale_picks_screener.py                  # latest as-of today
    python tools/whale_picks_screener.py --asof 2025-12-31
    python tools/whale_picks_screener.py --push           # send Discord
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("whale_picks_screener")

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

CACHE = REPO / "data_cache" / "backtest"
OUT_DIR = REPO / "data" / "whale_picks"
OUT_DIR.mkdir(parents=True, exist_ok=True)
LATEST_DIR = REPO / "data" / "latest"
LATEST_DIR.mkdir(parents=True, exist_ok=True)

# Re-use feature engineering from phase2 tool
from tools.whale_picks_phase2 import (
    load_indicators, load_smart_money, load_quality, load_revenue,
    load_financials_panel, load_universe_industry, build_feature_panel,
    winsorize_standardize,
)

# Production config locked per docs/whale_picks_spec.md v0.4
COMPOSITE_PARSI = {
    'f_score':                +1.0,
    'f_score_4q_delta':       +1.0,
    'eps_yoy':                +1.0,
    'revenue_score_6m_delta': +1.0,
    'turnover_log':           -1.0,
    'dist_52w_high':          -1.0,
    'stealth_volume_20d':     +1.0,
    'capex_intensity':        -1.0,
}
K_DEFAULT = 20
LOOKBACK_DAYS = 400  # need 252d for 52w + 60d MA buffer


def score_universe(asof: date, lookback_days: int = LOOKBACK_DAYS) -> pd.DataFrame:
    """Compute composite_parsi score for full universe at asof date."""
    start = (pd.Timestamp(asof) - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    end = pd.Timestamp(asof).strftime('%Y-%m-%d')

    log.info("Loading panels: %s ~ %s", start, end)
    indicators = load_indicators(start, end)
    smart_money = load_smart_money(start, end)
    quality = load_quality(start, end)
    revenue = load_revenue(start, end)
    financials = load_financials_panel(start, end)
    universe_industry = load_universe_industry()

    # Use empty fwd_returns (not needed for selector)
    fwd_returns = pd.DataFrame(columns=['stock_id', 'date', 'fwd_20d', 'fwd_60d',
                                         'fwd_120d', 'fwd_60d_max', 'fwd_60d_min'])

    log.info("Building feature panel...")
    feat = build_feature_panel(indicators, smart_money, fwd_returns, quality,
                                revenue, financials, universe_industry)

    # Keep only rows on/before asof; pick latest per stock
    feat = feat[feat['date'] <= pd.Timestamp(asof)].copy()
    feat = feat.sort_values(['stock_id', 'date']).groupby('stock_id').tail(1).reset_index(drop=True)
    log.info("Universe at asof %s: %d stocks", asof, len(feat))

    # Industry-neutral standardize ONLY the composite features
    feat = winsorize_standardize(feat, list(COMPOSITE_PARSI.keys()), industry_neutral=True)

    # Compute composite_parsi
    feat['composite_parsi'] = 0.0
    n_valid = pd.Series(0, index=feat.index)
    for f, w in COMPOSITE_PARSI.items():
        if f not in feat.columns:
            log.warning("  feature missing: %s", f)
            continue
        v = feat[f].fillna(0.0)
        feat['composite_parsi'] = feat['composite_parsi'] + w * v
        n_valid = n_valid + feat[f].notna().astype(int)
    # Require at least 5/8 features non-null
    feat.loc[n_valid < 5, 'composite_parsi'] = np.nan

    log.info("Scored %d stocks (%d with valid composite)",
             len(feat), feat['composite_parsi'].notna().sum())
    return feat


def attach_metadata(scored: pd.DataFrame) -> pd.DataFrame:
    """Add stock_name + industry for output."""
    u = pd.read_parquet(CACHE / "universe_tw.parquet")
    u_keep = u[['stock_id', 'stock_name', 'industry_category']].drop_duplicates('stock_id')
    return scored.merge(u_keep, on='stock_id', how='left', suffixes=('', '_u'))


def apply_hard_exclusions(scored: pd.DataFrame,
                            min_avg_volume_lots: int = 300,
                            min_avg_turnover_twd: float = 1e7) -> pd.DataFrame:
    """Filter out hard exclusions per SPEC §3:
      - KY 股 (海外子公司)
      - ETF / 特別股 / DR
      - 流動性過低（避免下單困難 / manipulation risk）
      - 上市未滿 1 年 (skip — would need IPO date data)
    """
    before = len(scored)
    if 'stock_name' in scored.columns:
        is_ky = scored['stock_name'].fillna('').str.contains('KY', na=False)
        scored = scored[~is_ky].copy()
    # ETF / 00xxx pattern (4-digit starting 00 typically ETF)
    scored = scored[~scored['stock_id'].str.match(r'^00\d{2,}.*$')].copy()
    # 特別股 (suffix A/B for preferred)
    scored = scored[~scored['stock_id'].str.contains(r'[A-Z]$', na=False, regex=True)].copy()
    after_id = len(scored)
    log.info("Hard exclusions (ID/KY/ETF): %d -> %d (removed %d)", before, after_id, before - after_id)

    # Liquidity filter (SPEC §3 C5)
    # avg_tv_60d in TWD; need volume too
    if 'avg_tv_60d' in scored.columns:
        # avg_tv_60d 是 60d 平均成交值（元）
        liquid_ok = scored['avg_tv_60d'] >= min_avg_turnover_twd
        scored = scored[liquid_ok].copy()
        after_liq = len(scored)
        log.info("Liquidity filter (avg_tv_60d >= %.0fM TWD): %d -> %d (removed %d)",
                 min_avg_turnover_twd / 1e6, after_id, after_liq, after_id - after_liq)
    if 'Volume' in scored.columns:
        # Latest day volume (already last-row); a noisy proxy but ok
        vol_ok = scored['Volume'] >= min_avg_volume_lots * 1000  # convert 張 (lots) to shares
        scored = scored[vol_ok].copy()
        after_vol = len(scored)
        log.info("Volume filter (latest >= %d lots): %d -> %d (removed %d)",
                 min_avg_volume_lots, after_liq if 'avg_tv_60d' in scored.columns else after_id,
                 after_vol, (after_liq if 'avg_tv_60d' in scored.columns else after_id) - after_vol)

    return scored


def render_top_k(scored: pd.DataFrame, K: int = K_DEFAULT) -> pd.DataFrame:
    """Sort by composite_parsi desc, take top K with key columns."""
    cols = ['stock_id', 'stock_name', 'industry_category', 'composite_parsi',
            'f_score', 'eps_yoy', 'dist_52w_high', 'turnover_log',
            'stealth_volume_20d', 'revenue_score_6m_delta',
            'f_score_4q_delta', 'capex_intensity', 'Close']
    valid = scored.dropna(subset=['composite_parsi']).copy()
    top = valid.nlargest(K, 'composite_parsi')
    return top[[c for c in cols if c in top.columns]].reset_index(drop=True)


def save_outputs(top: pd.DataFrame, full: pd.DataFrame, asof: date) -> Dict[str, str]:
    """Save parquet snapshots + JSON for Discord/UI."""
    asof_str = asof.isoformat()
    paths = {}

    # Full universe scored
    fp = OUT_DIR / f"{asof_str}.parquet"
    full.to_parquet(fp, index=False)
    paths['full'] = str(fp)

    # Latest convenience
    lp = OUT_DIR / "latest.parquet"
    full.to_parquet(lp, index=False)
    paths['latest'] = str(lp)

    # Top-K JSON for UI / Discord
    json_obj = {
        'asof': asof_str,
        'universe_size': int(len(full)),
        'valid_scored': int(full['composite_parsi'].notna().sum()),
        'top': top.to_dict(orient='records'),
        'config': {
            'composite': COMPOSITE_PARSI,
            'K': K_DEFAULT,
            'standardization': 'industry-neutral',
            'spec_version': '0.4',
            'informational_tier': True,
        },
    }
    jp = LATEST_DIR / "whale_picks_top20.json"
    jp.write_text(json.dumps(json_obj, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    paths['json'] = str(jp)

    log.info("Saved outputs: %s", paths)
    return paths


def format_discord_message(top: pd.DataFrame, asof: date) -> str:
    """Format top-K as Discord-friendly bullet list (no MD tables per feedback)."""
    lines = []
    lines.append(f"🐋 **Whale Picks Top-{len(top)} ({asof.isoformat()})**")
    lines.append(f"_8-factor composite_parsi / industry-neutral / monthly / informational tier_")
    lines.append("")
    for i, r in top.iterrows():
        sname = str(r.get('stock_name') or '')
        score = float(r['composite_parsi'])
        fs = r.get('f_score')
        eps = r.get('eps_yoy')
        close = r.get('Close')
        fs_s = f"{fs:.1f}" if pd.notna(fs) else "n/a"
        eps_s = f"{eps*100:+.1f}%" if pd.notna(eps) else "n/a"
        close_s = f"{close:.1f}" if pd.notna(close) else "n/a"
        lines.append(f"{i+1:>2}. **{r['stock_id']}** {sname}  score={score:+.2f}  F={fs_s}  EPS%={eps_s}  close={close_s}")
    lines.append("")
    lines.append("_Per docs/whale_picks_spec.md §13: 永遠 informational tier, 不接 portfolio gating, live winrate 預期低於 backtest_")
    return "\n".join(lines)


def push_discord(text: str) -> bool:
    """Send to Discord via webhook from env DISCORD_WEBHOOK_WHALE_PICKS or DISCORD_WEBHOOK."""
    import requests
    url = os.environ.get('DISCORD_WEBHOOK_WHALE_PICKS') or os.environ.get('DISCORD_WEBHOOK')
    if not url:
        log.warning("No Discord webhook env set (DISCORD_WEBHOOK_WHALE_PICKS / DISCORD_WEBHOOK)")
        return False
    # Discord limits 2000 chars per message
    chunks = [text[i:i+1900] for i in range(0, len(text), 1900)]
    for c in chunks:
        try:
            r = requests.post(url, json={'content': c}, timeout=20)
            r.raise_for_status()
        except Exception as e:
            log.error("Discord push failed: %s", e)
            return False
    log.info("Discord push OK (%d chunks)", len(chunks))
    return True


def main():
    parser = argparse.ArgumentParser(description="Whale Picks production selector")
    parser.add_argument('--asof', default=date.today().isoformat(),
                        help='Snapshot date YYYY-MM-DD (default today)')
    parser.add_argument('--k', type=int, default=K_DEFAULT, help='Top-K (default 20)')
    parser.add_argument('--push', action='store_true', help='Push top-K to Discord')
    args = parser.parse_args()

    asof = date.fromisoformat(args.asof)
    log.info("Whale Picks screener — asof %s, K=%d", asof, args.k)

    scored = score_universe(asof)
    scored = attach_metadata(scored)
    scored = apply_hard_exclusions(scored)
    top = render_top_k(scored, K=args.k)

    paths = save_outputs(top, scored, asof)

    log.info("Top-%d picks:", len(top))
    print(top.to_string(index=False))

    if args.push:
        msg = format_discord_message(top, asof)
        push_discord(msg)


if __name__ == "__main__":
    main()
