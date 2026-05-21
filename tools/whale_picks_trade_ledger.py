"""Whale Picks Trade Ledger - backtest position-level entry/exit records.

Re-runs v13 production strategy (monthly K=20, industry-neutral, liquidity filter
avg_tv_60d >= 10M TWD) and outputs a position-level trade ledger with entry/exit
dates + prices + 8-factor snapshots + (optional LLM) Chinese narrative reasons.

連續持有合併為單筆 position：
  - 月底某檔進入 top-20 -> 進場
  - 連續月底仍在 top-20 -> 維持持有
  - 某月底掉出 top-20 -> 用該月底收盤價出場
  - 若回測末月仍在 top-20 -> still_holding=True，exit_price 留空

Output:
  - data/whale_picks/trade_ledger.parquet  (主檔)
  - data/whale_picks/trade_ledger_meta.json (metadata + config)
  - data/whale_picks/trade_reasons_cache.json (LLM reason cache)

Usage:
  python tools/whale_picks_trade_ledger.py
  python tools/whale_picks_trade_ledger.py --with-reasons
  python tools/whale_picks_trade_ledger.py --start 2021-01-01 --end 2025-12-31 --k 20

LLM 規範 (CLAUDE.md): reason 生成走 Sonnet (`--model sonnet`) + 600s timeout，純文字摘要任務。
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("whale_picks_trade_ledger")

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from tools.whale_picks_phase2 import (  # noqa: E402
    load_indicators, load_smart_money, load_quality, load_revenue,
    load_financials_panel, load_universe_industry, build_feature_panel,
    winsorize_standardize,
)

# Production config locked per docs/whale_picks_spec.md v0.5
COMPOSITE_PARSI: Dict[str, float] = {
    'f_score':                +1.0,
    'f_score_4q_delta':       +1.0,
    'eps_yoy':                +1.0,
    'revenue_score_6m_delta': +1.0,
    'turnover_log':           -1.0,
    'dist_52w_high':          -1.0,
    'stealth_volume_20d':     +1.0,
    'capex_intensity':        -1.0,
}
FACTOR_LABEL_ZH: Dict[str, str] = {
    'f_score':                'Piotroski F-Score (財務體質)',
    'f_score_4q_delta':       'F-Score 年增 (體質改善)',
    'eps_yoy':                'EPS 年增率',
    'revenue_score_6m_delta': '營收 6 月改善',
    'turnover_log':           '小型優勢 (成交值反向加權)',
    'dist_52w_high':          '距 52 週高點 (近高扣分)',
    'stealth_volume_20d':     '量縮中爆量 (主力吸籌)',
    'capex_intensity':        '資本支出強度 (反向)',
}
FACTOR_RAW_HINT: Dict[str, str] = {
    'f_score':                'F-Score (0-9 越高越好)',
    'f_score_4q_delta':       'F-Score 較 4 季前差 (>0 改善)',
    'eps_yoy':                'EPS 年增率 (小數，0.5=+50%)',
    'revenue_score_6m_delta': '營收分數 6 月差 (>0 改善)',
    'turnover_log':           'log(平均成交值)',
    'dist_52w_high':          '與 52 週高點距離 (0.0=同高)',
    'stealth_volume_20d':     '近 20 日量縮爆量分',
    'capex_intensity':        '資本支出 / 總資產',
}
K_DEFAULT = 20
MIN_AVG_TV = 1e7  # 10M TWD liquidity filter

OUT_DIR = REPO / "data" / "whale_picks"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE = REPO / "data_cache" / "backtest"
LEDGER_PATH = OUT_DIR / "trade_ledger.parquet"
META_PATH = OUT_DIR / "trade_ledger_meta.json"
REASON_CACHE_PATH = OUT_DIR / "trade_reasons_cache.json"

_CLAUDE_CLI = shutil.which("claude") or "claude"
CLAUDE_TIMEOUT = 600


# =============================================================================
# Stage A — Build v13 feature panel (re-uses phase2 loaders)
# =============================================================================

def build_v13_feat(start: str, end: str) -> pd.DataFrame:
    """Build feature panel: monthly + liquidity-filtered + composite_parsi computed.

    Returns one row per (stock_id, month_end_date) with:
      - composite_parsi (standardized)
      - Close, stock_name, industry_category
      - f_<factor>: standardized factor values (for top_drivers)
      - r_<factor>: raw factor values (for LLM narrative)
    """
    log.info("Loading panels: %s ~ %s", start, end)
    indicators = load_indicators(start, end)
    fwd_returns = pd.DataFrame(columns=['stock_id', 'date', 'fwd_5d', 'fwd_10d',
                                         'fwd_20d', 'fwd_60d', 'fwd_120d',
                                         'fwd_60d_max', 'fwd_60d_min'])
    smart_money = load_smart_money(start, end)
    quality = load_quality(start, end)
    revenue = load_revenue(start, end)
    financials = load_financials_panel(start, end)
    universe_industry = load_universe_industry()

    log.info("Building feature panel (1-3 min)...")
    feat = build_feature_panel(indicators, smart_money, fwd_returns, quality,
                                revenue, financials, universe_industry)
    feat['date'] = pd.to_datetime(feat['date'])
    feat = feat[(feat['date'] >= start) & (feat['date'] <= end)].copy()

    # Monthly rebalance: last available date per (stock, month)
    feat['_period'] = feat['date'].dt.to_period('M')
    feat = feat.sort_values(['stock_id', 'date'])
    feat = feat.groupby(['stock_id', '_period']).tail(1).drop(columns=['_period']).reset_index(drop=True)
    log.info("After monthly filter: %d rows", len(feat))

    # Liquidity filter (production-equivalent)
    if 'avg_tv_60d' in feat.columns:
        before = len(feat)
        feat = feat[feat['avg_tv_60d'] >= MIN_AVG_TV].copy()
        log.info("Liquidity filter (avg_tv_60d>=%.0fM TWD): %d -> %d (-%.1f%%)",
                 MIN_AVG_TV / 1e6, before, len(feat),
                 100 * (before - len(feat)) / before)

    # Snapshot raw factor values BEFORE standardization (for LLM narrative)
    raw_cols = [c for c in COMPOSITE_PARSI.keys() if c in feat.columns]
    feat_raw = feat[['stock_id', 'date'] + raw_cols].copy()
    feat_raw = feat_raw.rename(columns={c: f"r_{c}" for c in raw_cols})

    # Industry-neutral standardize the 8 composite features
    feat = winsorize_standardize(feat, list(COMPOSITE_PARSI.keys()), industry_neutral=True)

    # Build composite_parsi (same logic as screener)
    feat['composite_parsi'] = 0.0
    n_valid = pd.Series(0, index=feat.index)
    for f, w in COMPOSITE_PARSI.items():
        if f not in feat.columns:
            log.warning("  feature missing: %s", f)
            continue
        v = feat[f].fillna(0.0)
        feat['composite_parsi'] = feat['composite_parsi'] + w * v
        n_valid = n_valid + feat[f].notna().astype(int)
    feat.loc[n_valid < 5, 'composite_parsi'] = np.nan

    # Rename standardized factor cols -> f_<name>
    feat = feat.rename(columns={c: f"f_{c}" for c in COMPOSITE_PARSI.keys() if c in feat.columns})

    # Merge raw values back
    feat = feat.merge(feat_raw, on=['stock_id', 'date'], how='left')

    # Attach stock_name (industry_category already attached by add_sector_features)
    u = pd.read_parquet(CACHE / "universe_tw.parquet")
    u_name = u[['stock_id', 'stock_name']].drop_duplicates('stock_id')
    feat = feat.merge(u_name, on='stock_id', how='left')

    log.info("Final feat: %d rows, %d sids, %d valid composite",
             len(feat), feat['stock_id'].nunique(),
             feat['composite_parsi'].notna().sum())
    return feat


# =============================================================================
# Stage B — Position aggregation
# =============================================================================

def build_positions(feat: pd.DataFrame, K: int = K_DEFAULT) -> pd.DataFrame:
    """For each month-end, pick top-K by composite_parsi. For each stock, aggregate
    continuous runs into positions.

    Continuous = appears in top-K on consecutive month-ends for the same stock.
    Note: a stock that drops then re-enters spawns a NEW position.
    """
    feat = feat.copy()
    feat['_rank'] = feat.groupby('date')['composite_parsi'].rank(ascending=False, method='first')
    feat['in_topk'] = (feat['_rank'] <= K) & feat['composite_parsi'].notna()

    rebal_dates = sorted(feat['date'].dropna().unique())
    log.info("Rebalance dates: %d (range %s ~ %s)",
             len(rebal_dates), pd.Timestamp(rebal_dates[0]).date(), pd.Timestamp(rebal_dates[-1]).date())

    factor_cols = [c for c in COMPOSITE_PARSI.keys()]
    last_rebal_date = pd.Timestamp(rebal_dates[-1])

    positions: List[Dict] = []
    for sid, grp in feat.sort_values(['stock_id', 'date']).groupby('stock_id', sort=False):
        in_topk = grp['in_topk'].values
        n = len(in_topk)
        i = 0
        while i < n:
            if not in_topk[i]:
                i += 1
                continue
            j = i
            while j < n and in_topk[j]:
                j += 1
            entry_row = grp.iloc[i]
            if j < n:
                # Sold: at next month-end where stock dropped out, sell at that month-end close
                exit_row = grp.iloc[j]
                still_holding = False
            else:
                # Still in top-K on last available rebalance for this stock
                exit_row = grp.iloc[j - 1]
                still_holding = pd.Timestamp(exit_row['date']) >= (last_rebal_date - pd.Timedelta(days=10))

            entry_dt = pd.Timestamp(entry_row['date'])
            exit_dt = pd.Timestamp(exit_row['date'])
            entry_close = entry_row.get('Close')
            exit_close = exit_row.get('Close')

            if pd.notna(entry_close) and pd.notna(exit_close) and entry_close > 0:
                pnl_pct = float(exit_close) / float(entry_close) - 1.0
            else:
                pnl_pct = np.nan

            holding_months = max(1, (exit_dt.year - entry_dt.year) * 12 + (exit_dt.month - entry_dt.month))
            if still_holding:
                holding_months = max(1, (exit_dt.year - entry_dt.year) * 12 + (exit_dt.month - entry_dt.month) + 1)

            pos = {
                'stock_id': sid,
                'stock_name': entry_row.get('stock_name'),
                'industry': entry_row.get('industry_category'),
                'entry_date': entry_dt,
                'entry_price': float(entry_close) if pd.notna(entry_close) else np.nan,
                'exit_date': exit_dt if not still_holding else pd.NaT,
                'exit_price': float(exit_close) if (pd.notna(exit_close) and not still_holding) else np.nan,
                'still_holding': bool(still_holding),
                'holding_months': int(holding_months),
                'pnl_pct': pnl_pct,
                'composite_at_entry': float(entry_row['composite_parsi']) if pd.notna(entry_row['composite_parsi']) else np.nan,
                'composite_at_exit': float(exit_row['composite_parsi']) if pd.notna(exit_row['composite_parsi']) else np.nan,
                'rank_at_entry': int(entry_row['_rank']) if pd.notna(entry_row.get('_rank')) else None,
            }
            # Standardized factor scores (for top-driver calc)
            for c in factor_cols:
                pos[f'f_{c}_entry'] = entry_row.get(f'f_{c}')
                pos[f'f_{c}_exit'] = exit_row.get(f'f_{c}')
            # Raw factor values (for LLM narrative)
            for c in factor_cols:
                pos[f'r_{c}_entry'] = entry_row.get(f'r_{c}')
                pos[f'r_{c}_exit'] = exit_row.get(f'r_{c}')
            positions.append(pos)
            i = j

    df = pd.DataFrame(positions)
    if len(df) == 0:
        log.warning("No positions built — check feat / top-K filter")
        return df
    log.info("Built %d positions across %d stocks (%d still holding)",
             len(df), df['stock_id'].nunique(),
             int(df['still_holding'].sum()))
    return df


def attach_top_drivers(df: pd.DataFrame) -> pd.DataFrame:
    """Attach entry_top_drivers + exit_top_drivers (top 3 contributing factors)."""
    factor_cols = list(COMPOSITE_PARSI.keys())

    def _entry_drivers(row) -> str:
        contribs = []
        for c in factor_cols:
            v = row.get(f"f_{c}_entry")
            if pd.isna(v):
                continue
            contribs.append((c, float(v) * COMPOSITE_PARSI[c]))
        contribs.sort(key=lambda x: x[1], reverse=True)
        top = [c for c, v in contribs[:3] if v > 0]
        return " / ".join(FACTOR_LABEL_ZH.get(c, c) for c in top) if top else "n/a"

    def _exit_drivers(row) -> str:
        if row.get('still_holding'):
            return "(尚未出場)"
        deltas = []
        for c in factor_cols:
            ve = row.get(f"f_{c}_entry")
            vx = row.get(f"f_{c}_exit")
            if pd.isna(ve) or pd.isna(vx):
                continue
            # contribution drop = (entry - exit) * weight
            drop = (float(ve) - float(vx)) * COMPOSITE_PARSI[c]
            deltas.append((c, drop))
        deltas.sort(key=lambda x: x[1], reverse=True)
        top = [c for c, v in deltas[:3] if v > 0]
        return " / ".join(FACTOR_LABEL_ZH.get(c, c) for c in top) if top else "排名滑落"

    df['entry_top_drivers'] = df.apply(_entry_drivers, axis=1)
    df['exit_top_drivers'] = df.apply(_exit_drivers, axis=1)
    return df


# =============================================================================
# Stage C — LLM reason generation (Sonnet, batched + cached)
# =============================================================================

def _load_reason_cache() -> Dict[str, Dict[str, str]]:
    if not REASON_CACHE_PATH.exists():
        return {}
    try:
        return json.loads(REASON_CACHE_PATH.read_text(encoding='utf-8'))
    except Exception as e:
        log.warning("reason cache parse failed: %s — starting fresh", e)
        return {}


def _save_reason_cache(cache: Dict[str, Dict[str, str]]) -> None:
    REASON_CACHE_PATH.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2), encoding='utf-8'
    )


def _format_factor_block(row: pd.Series, suffix: str) -> str:
    """Format raw factor values for LLM prompt."""
    lines = []
    for c in COMPOSITE_PARSI.keys():
        v_raw = row.get(f"r_{c}_{suffix}")
        v_std = row.get(f"f_{c}_{suffix}")
        hint = FACTOR_RAW_HINT.get(c, c)
        if pd.notna(v_raw):
            if c == 'eps_yoy':
                raw_s = f"{float(v_raw) * 100:+.1f}%"
            elif c == 'capex_intensity':
                raw_s = f"{float(v_raw) * 100:.1f}%"
            elif c == 'dist_52w_high':
                raw_s = f"{float(v_raw) * 100:.1f}%"
            else:
                raw_s = f"{float(v_raw):+.2f}"
        else:
            raw_s = "n/a"
        std_s = f"{float(v_std):+.2f}" if pd.notna(v_std) else "n/a"
        lines.append(f"  - {hint}: raw={raw_s}, 標準化={std_s}")
    return "\n".join(lines)


def _build_prompt(batch: List[pd.Series]) -> str:
    """Build batched prompt for Sonnet — 5-10 positions per call."""
    intro = (
        "你是台股量化策略分析師。以下是「主力選股」回測選出的多筆 position，"
        "每筆都列了進場/出場時的 8 個因子原始值 + 標準化 z-score。\n\n"
        "請為每筆 position 寫：\n"
        "  - entry_reason: 1-2 句中文，解釋為什麼這檔在進場月底被選上 (top-20 of composite_parsi)。\n"
        "  - exit_reason: 1-2 句中文，解釋為什麼這檔在出場月底掉出 top-20。\n\n"
        "重點：聚焦在 z-score 最高的進場 driver 和 entry-exit 跌幅最大的出場 driver；用具體數字。\n"
        "若 still_holding=true，exit_reason 寫「目前仍在 top-20 持有中」。\n\n"
        "回傳 JSON array，每筆: {\"key\": \"<stock_id>_<entry_date>\", \"entry_reason\": \"...\", \"exit_reason\": \"...\"}\n"
        "不要 markdown fence，直接 JSON。\n\n"
    )
    items = []
    for row in batch:
        key = f"{row['stock_id']}_{pd.Timestamp(row['entry_date']).date().isoformat()}"
        entry_dt = pd.Timestamp(row['entry_date']).date().isoformat()
        exit_dt = pd.Timestamp(row['exit_date']).date().isoformat() if pd.notna(row['exit_date']) else "still_holding"
        pnl_s = f"{row['pnl_pct'] * 100:+.1f}%" if pd.notna(row['pnl_pct']) else "n/a"
        items.append(
            f"---\nkey: {key}\n"
            f"股票: {row['stock_id']} {row['stock_name']} ({row['industry']})\n"
            f"進場 {entry_dt} @ {row['entry_price']:.2f}, 出場 {exit_dt} "
            f"@ {row['exit_price'] if pd.notna(row['exit_price']) else 'n/a'}\n"
            f"持有 {row['holding_months']} 個月, P&L {pnl_s}, still_holding={bool(row['still_holding'])}\n"
            f"composite: entry={row['composite_at_entry']:+.2f}, exit={row['composite_at_exit']:+.2f}\n"
            f"進場因子：\n{_format_factor_block(row, 'entry')}\n"
            f"出場因子：\n{_format_factor_block(row, 'exit')}\n"
        )
    return intro + "\n".join(items)


def _call_sonnet(prompt: str) -> Optional[str]:
    """Per CLAUDE.md LLM rules: Sonnet + 600s timeout for short narrative tasks."""
    # 2026-05-21: --effort xhigh (claude -p 不繼承 settings.json effortLevel)
    cmd = [_CLAUDE_CLI, '-p', '--model', 'sonnet', '--effort', 'xhigh', '--output-format', 'json']
    try:
        result = subprocess.run(
            cmd, input=prompt, capture_output=True, text=True,
            timeout=CLAUDE_TIMEOUT, encoding='utf-8', errors='replace', shell=False,
        )
    except subprocess.TimeoutExpired:
        log.error("Sonnet call timeout (%ds)", CLAUDE_TIMEOUT)
        return None
    except FileNotFoundError:
        log.error("claude CLI not found")
        return None
    if result.returncode != 0:
        log.error("Sonnet exit %d: %s", result.returncode, result.stderr[:300])
        return None
    raw = result.stdout
    try:
        envelope = json.loads(raw)
        if envelope.get('is_error'):
            log.error("Sonnet is_error=true")
            return None
        return envelope.get('result', '')
    except json.JSONDecodeError:
        return raw


def _parse_reason_json(text: str) -> List[Dict[str, str]]:
    """Parse JSON array out of Sonnet response (tolerate markdown fences)."""
    s = text.strip()
    if s.startswith('```'):
        lines = s.split('\n')
        s = '\n'.join(lines[1:-1] if len(lines) >= 3 else lines)
        if s.startswith('json'):
            s = s[4:].lstrip()
    # Find first [ and matching ]
    i = s.find('[')
    if i < 0:
        return []
    depth = 0
    end = -1
    in_str = False
    esc = False
    for k in range(i, len(s)):
        c = s[k]
        if esc:
            esc = False
            continue
        if c == '\\':
            esc = True
            continue
        if c == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if c == '[':
            depth += 1
        elif c == ']':
            depth -= 1
            if depth == 0:
                end = k
                break
    if end < 0:
        return []
    try:
        return json.loads(s[i:end + 1])
    except json.JSONDecodeError as e:
        log.warning("reason JSON parse failed: %s", e)
        return []


def generate_reasons(df: pd.DataFrame, batch_size: int = 8) -> pd.DataFrame:
    """Fill entry_reason_zh + exit_reason_zh via batched Sonnet calls.

    Cached by (stock_id, entry_date) — re-runs skip cached rows.
    """
    cache = _load_reason_cache()
    log.info("Reason cache: %d entries loaded", len(cache))

    df = df.copy()
    if 'entry_reason_zh' not in df.columns:
        df['entry_reason_zh'] = ""
    if 'exit_reason_zh' not in df.columns:
        df['exit_reason_zh'] = ""

    # Hydrate from cache first
    for idx, row in df.iterrows():
        key = f"{row['stock_id']}_{pd.Timestamp(row['entry_date']).date().isoformat()}"
        if key in cache:
            df.at[idx, 'entry_reason_zh'] = cache[key].get('entry_reason', '')
            df.at[idx, 'exit_reason_zh'] = cache[key].get('exit_reason', '')

    # Identify pending rows
    pending = df[(df['entry_reason_zh'] == '') | df['entry_reason_zh'].isna()].copy()
    log.info("Pending LLM rows: %d / %d", len(pending), len(df))

    if len(pending) == 0:
        return df

    total_batches = (len(pending) + batch_size - 1) // batch_size
    for bi in range(total_batches):
        chunk = pending.iloc[bi * batch_size:(bi + 1) * batch_size]
        batch_rows = [chunk.iloc[k] for k in range(len(chunk))]
        log.info("Batch %d/%d (%d positions) -> Sonnet", bi + 1, total_batches, len(chunk))
        prompt = _build_prompt(batch_rows)
        resp = _call_sonnet(prompt)
        if not resp:
            log.warning("  batch %d failed — skipping", bi + 1)
            continue
        parsed = _parse_reason_json(resp)
        if not parsed:
            log.warning("  batch %d JSON empty", bi + 1)
            continue
        # Update cache + df
        for item in parsed:
            key = item.get('key')
            if not key:
                continue
            cache[key] = {
                'entry_reason': item.get('entry_reason', ''),
                'exit_reason': item.get('exit_reason', ''),
            }
        _save_reason_cache(cache)
        # Apply back to df
        for idx, row in chunk.iterrows():
            key = f"{row['stock_id']}_{pd.Timestamp(row['entry_date']).date().isoformat()}"
            if key in cache:
                df.at[idx, 'entry_reason_zh'] = cache[key].get('entry_reason', '')
                df.at[idx, 'exit_reason_zh'] = cache[key].get('exit_reason', '')

    log.info("LLM reasons filled: %d / %d",
             (df['entry_reason_zh'].astype(str).str.len() > 0).sum(), len(df))
    return df


# =============================================================================
# Stage D — Save outputs
# =============================================================================

def save_ledger(df: pd.DataFrame, start: str, end: str, K: int, with_reasons: bool) -> None:
    df.to_parquet(LEDGER_PATH, index=False)
    meta = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'start': start,
        'end': end,
        'K': K,
        'min_avg_tv_twd': MIN_AVG_TV,
        'composite': COMPOSITE_PARSI,
        'standardization': 'industry-neutral',
        'with_llm_reasons': with_reasons,
        'n_positions': int(len(df)),
        'n_stocks': int(df['stock_id'].nunique()) if len(df) else 0,
        'n_still_holding': int(df['still_holding'].sum()) if len(df) else 0,
        'win_rate': float((df['pnl_pct'] > 0).mean()) if len(df) else None,
        'avg_pnl_pct': float(df['pnl_pct'].mean()) if len(df) else None,
        'median_pnl_pct': float(df['pnl_pct'].median()) if len(df) else None,
        'best_pnl_pct': float(df['pnl_pct'].max()) if len(df) else None,
        'worst_pnl_pct': float(df['pnl_pct'].min()) if len(df) else None,
        'spec_note': '永遠 informational tier per docs/whale_picks_spec.md §13',
    }
    META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    log.info("Saved ledger: %s (%d rows)", LEDGER_PATH, len(df))
    log.info("Saved meta:   %s", META_PATH)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Whale Picks trade ledger generator")
    parser.add_argument('--start', default='2021-01-01')
    parser.add_argument('--end', default='2025-12-31')
    parser.add_argument('--k', type=int, default=K_DEFAULT)
    parser.add_argument('--with-reasons', action='store_true',
                        help='Generate LLM (Sonnet) entry/exit narratives. Slow (~30 min).')
    parser.add_argument('--reasons-batch-size', type=int, default=8)
    parser.add_argument('--reasons-only', action='store_true',
                        help='Skip rebuilding ledger; fill LLM reasons on existing ledger.')
    args = parser.parse_args()

    if args.reasons_only:
        if not LEDGER_PATH.exists():
            log.error("No existing ledger at %s — run without --reasons-only first", LEDGER_PATH)
            return
        df = pd.read_parquet(LEDGER_PATH)
        log.info("Loaded existing ledger: %d positions", len(df))
        df = generate_reasons(df, batch_size=args.reasons_batch_size)
        save_ledger(df, args.start, args.end, args.k, with_reasons=True)
        return

    feat = build_v13_feat(args.start, args.end)
    df = build_positions(feat, K=args.k)
    if len(df) == 0:
        log.error("No positions generated — exiting")
        return
    df = attach_top_drivers(df)
    df['entry_reason_zh'] = ''
    df['exit_reason_zh'] = ''

    if args.with_reasons:
        df = generate_reasons(df, batch_size=args.reasons_batch_size)

    save_ledger(df, args.start, args.end, args.k, with_reasons=args.with_reasons)


if __name__ == "__main__":
    main()
