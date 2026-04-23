"""
Chip fetch helpers for scanner/screener batch context.

H7 (2026-04-23, Robustness First Phase 3):
  value_screener.py 和 momentum_screener.py 之前各寫一份「先試 batch cache →
  fallback FinMind」的邏輯，兩份幾乎一樣。code duplication drift 風險（若
  ChipAnalyzer API 再變，兩個 screener 都要同步改）。

  本模組抽出單一 function `fetch_institutional_for_scan`，兩個 screener 都
  呼叫它 → 不可能再 drift。

注意：
  - AI 報告 / UI 個股分析 pipeline **不用**此 helper（他們要完整 chip 資料，
    不是 scan_mode=True 的 institutional-only）。那條路徑走 ChipAnalyzer.fetch_chip 直接呼叫。
  - Batch cache 是 scanner 層級的優化（pre-fetched TWSE/TPEX batch API），
    個股分析路徑無此優化。
"""

import logging
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def fetch_institutional_for_scan(
    stock_id: str,
    batch_cache: Optional[Dict[str, pd.DataFrame]] = None,
) -> Optional[pd.DataFrame]:
    """Fetch institutional chip data for scanner batch context.

    Priority:
      1. batch_cache (pre-fetched TWSE/TPEX batch API) — no FinMind cost
      2. ChipAnalyzer.fetch_chip(scan_mode=True) — FinMind fallback,
         只抓 institutional，不抓 margin/day_trading/shareholding/sbl（節省 4 個 call）

    Args:
        stock_id: '2330' / '8086' etc (digit string)
        batch_cache: dict {stock_id: institutional DataFrame}, from
          TWSEOpenData.get_all_stocks_institutional_batch()

    Returns:
        institutional DataFrame (columns: 外資 / 投信 / 自營商 / 合計 / 三大法人合計),
        or None if both cache miss and FinMind fail.
    """
    # 1st: pre-fetched batch cache
    if batch_cache and stock_id in batch_cache:
        return batch_cache[stock_id]

    # 2nd: FinMind fallback
    try:
        from chip_analysis import ChipAnalyzer, ChipFetchError
        chip_data = ChipAnalyzer().fetch_chip(stock_id, scan_mode=True)
        return chip_data.get('institutional')
    except Exception as e:
        # ChipFetchError 和其他未預期 exception 都吞，單檔失敗不 block batch scan
        # 但要留 DEBUG log 方便診斷（Robustness First 原則 #4）
        logger.debug("fetch_institutional_for_scan failed for %s: %s: %s",
                     stock_id, type(e).__name__, e)
        return None
