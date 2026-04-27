"""
週榜資料載入器 (BL-4 整合用)

從 data/weekly_chip_latest.parquet 載入 long-format 三大法人週榜，
提供 UI / Scanner / AI 報告共用的查詢 API。

Schema: week_end | dim | dim_name_zh | rank_type | rank | stock_id |
        stock_name | consec_days | weekly_amount_k | weekly_shares

dim ∈ {total, foreign, trust, dealer}
rank_type ∈ {consec_buy, consec_sell, week_buy, week_sell}
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent
LATEST_PARQUET = REPO / "data" / "weekly_chip_latest.parquet"

_CACHE: dict[str, object] = {'df': None, 'mtime': 0}

DIM_LABELS_ZH = {
    'total': '三大法人合計',
    'foreign': '外資',
    'trust': '投信',
    'dealer': '自營商',
}
DIM_LABELS_SHORT = {
    'total': '三大',
    'foreign': '外資',
    'trust': '投信',
    'dealer': '自營',
}
RANK_TYPE_LABELS_ZH = {
    'consec_buy': '連續買超天數',
    'consec_sell': '連續賣超天數',
    'week_buy': '當週買超金額',
    'week_sell': '當週賣超金額',
}


def load_latest() -> Optional[pd.DataFrame]:
    """載入最新一份週榜 long-format DataFrame，無檔回 None。Cache by mtime。"""
    if not LATEST_PARQUET.exists():
        logger.warning("weekly_chip_latest.parquet not found at %s", LATEST_PARQUET)
        return None
    mtime = LATEST_PARQUET.stat().st_mtime
    if _CACHE['df'] is not None and _CACHE['mtime'] == mtime:
        return _CACHE['df']
    try:
        df = pd.read_parquet(LATEST_PARQUET)
        df['stock_id'] = df['stock_id'].astype(str)
        _CACHE['df'] = df
        _CACHE['mtime'] = mtime
        return df
    except Exception as e:
        logger.warning("Failed to load weekly_chip_latest.parquet: %s", e)
        return None


def get_metadata() -> Optional[dict]:
    """回傳 week_end / window dates 等 metadata。"""
    df = load_latest()
    if df is None or df.empty:
        return None
    return {
        'week_end': pd.Timestamp(df['week_end'].iloc[0]),
        'total_rows': len(df),
        'unique_stocks': df['stock_id'].nunique(),
    }


def get_rankings(dim: str = 'total', rank_type: str = 'consec_buy',
                  top_n: int = 10) -> pd.DataFrame:
    """取單一 (dim, rank_type) 的 Top N 排行。dim ∈ {total/foreign/trust/dealer}."""
    df = load_latest()
    if df is None:
        return pd.DataFrame()
    sub = df[(df['dim'] == dim) & (df['rank_type'] == rank_type)].head(top_n)
    return sub.reset_index(drop=True)


def get_stock_tags(stock_id: str) -> list[str]:
    """個股有哪些「上榜標記」for picks 表 column 用。

    Returns list of short tags like ['📊三大連買5d', '外資週買#3']。
    Empty list if 該股本週無上榜。
    """
    df = load_latest()
    if df is None or df.empty:
        return []
    sid = str(stock_id).replace('.TW', '').replace('.TWO', '').strip()
    sub = df[df['stock_id'] == sid]
    if sub.empty:
        return []
    tags = []
    for _, r in sub.iterrows():
        dim_short = DIM_LABELS_SHORT.get(r['dim'], r['dim'])
        rt = r['rank_type']
        if rt == 'consec_buy':
            tags.append(f"📊{dim_short}連買{int(r['consec_days'])}d")
        elif rt == 'consec_sell':
            tags.append(f"📊{dim_short}連賣{int(r['consec_days'])}d")
        elif rt == 'week_buy':
            tags.append(f"📊{dim_short}買#{int(r['rank'])}")
        elif rt == 'week_sell':
            tags.append(f"📊{dim_short}賣#{int(r['rank'])}")
    return tags


def get_stock_summary(stock_id: str) -> Optional[dict]:
    """個股本週 4 維度動向總覽 (個股分析籌碼面 mini-section 用)。

    Returns dict {dim: {ranks: [(rank_type, rank, consec_days, amount_k)]}, ...}
    Empty dim 不會出現。None if 全無上榜。
    """
    df = load_latest()
    if df is None or df.empty:
        return None
    sid = str(stock_id).replace('.TW', '').replace('.TWO', '').strip()
    sub = df[df['stock_id'] == sid]
    if sub.empty:
        return None
    out = {}
    for dim, gdim in sub.groupby('dim'):
        out[dim] = {
            'dim_name_zh': DIM_LABELS_ZH.get(dim, dim),
            'ranks': [
                {
                    'rank_type': r['rank_type'],
                    'rank_type_zh': RANK_TYPE_LABELS_ZH.get(r['rank_type'], r['rank_type']),
                    'rank': int(r['rank']),
                    'consec_days': int(r['consec_days']),
                    'amount_k': float(r['weekly_amount_k']),
                }
                for _, r in gdim.iterrows()
            ],
        }
    return out


def format_summary_for_ai(stock_id: str) -> str:
    """For AI report prompt: short string like
    '本週三大法人合計買超#3 (+25 億, 連 5 日); 外資連買 5 日'。
    Empty if 無上榜。"""
    summary = get_stock_summary(stock_id)
    if not summary:
        return ''
    parts = []
    for dim_key, info in summary.items():
        dim_short = DIM_LABELS_SHORT.get(dim_key, dim_key)
        for r in info['ranks']:
            rt = r['rank_type']
            amt_b = r['amount_k'] / 1e5  # 千元 -> 億元
            amt_str = f"{amt_b:+.1f}億"
            if rt == 'consec_buy':
                parts.append(f"{dim_short}連買{r['consec_days']}日({amt_str})")
            elif rt == 'consec_sell':
                parts.append(f"{dim_short}連賣{r['consec_days']}日({amt_str})")
            elif rt == 'week_buy':
                parts.append(f"{dim_short}買超#{r['rank']}({amt_str})")
            elif rt == 'week_sell':
                parts.append(f"{dim_short}賣超#{r['rank']}({amt_str})")
    return '; '.join(parts)
