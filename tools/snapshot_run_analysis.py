"""
M2 analysis_engine.py 拆分 — regression fixture。

拆分 TechnicalAnalyzer 之類 god class 時用來 byte-for-byte 驗證 run_analysis() 無
行為變化。所有輸入（df_week / df_day / chip_data）pickle 凍結，HMM regime cache
預先注入固定值，確保兩次執行完全 deterministic。

Workflow:
  1. python tools/snapshot_run_analysis.py --prepare    一次性下載並 pickle 原始輸入
  2. python tools/snapshot_run_analysis.py --baseline   拆分前建 baseline JSON
  3. 做拆分
  4. python tools/snapshot_run_analysis.py --verify     拆分後 byte-for-byte 比對

固定 tickers: 2330.TW (TW 權值)、AAPL (US)。刻意不挑 TPEx / 基本面特殊 case，
純驗證程式邏輯重構。
"""
from __future__ import annotations

import argparse
import json
import pickle
import sys
import time
from pathlib import Path

# Make repo root importable
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

FIXTURE_DIR = REPO_ROOT / 'tools' / 'fixtures' / 'm2_snapshot'
TICKERS = ['2330.TW', 'AAPL']

# 預設 HMM regime（避開 yfinance 網路不確定性）
FROZEN_REGIMES = {
    '2330.TW': {'regime': 'neutral', 'confidence': 0.0, 'details': 'frozen for regression'},
    'AAPL':    {'regime': 'neutral', 'confidence': 0.0, 'details': 'frozen for regression'},
}


def _prepare_inputs(ticker: str) -> dict:
    """下載原始輸入（df_week / df_day / chip_data）。只有 --prepare 時呼叫。"""
    from technical_analysis import plot_dual_timeframe
    _figs, _errs, df_week, df_day, _meta = plot_dual_timeframe(ticker, force_update=False)
    if df_day is None or df_day.empty:
        raise RuntimeError(f"[{ticker}] no price data")

    chip_data, us_chip_data = None, None
    if ticker.endswith('.TW') or ticker.endswith('.TWO'):
        try:
            from chip_analysis import ChipAnalyzer, ChipFetchError
            chip_data = ChipAnalyzer().fetch_chip(ticker, scan_mode=False)
        except ChipFetchError as e:
            print(f"[{ticker}] TW chip fetch failed: {e}")
        except Exception as e:
            print(f"[{ticker}] TW chip unexpected error: {type(e).__name__}: {e}")
    else:
        try:
            from us_stock_chip import USStockChipAnalyzer
            us_chip_data, _err = USStockChipAnalyzer().get_chip_data(ticker)
        except Exception as e:
            print(f"[{ticker}] US chip load failed: {type(e).__name__}: {e}")

    return {
        'ticker': ticker,
        'df_week': df_week,
        'df_day': df_day,
        'chip_data': chip_data,
        'us_chip_data': us_chip_data,
    }


def _freeze_hmm_cache():
    """
    預先注入 HMM regime cache，避開 yfinance 網路呼叫。
    TechnicalAnalyzer._detect_regime 仍會跑 per-stock ADX / squeeze 邏輯（deterministic）。
    """
    import analysis_engine
    now = time.time()
    for market in ('tw', 'us'):
        analysis_engine._hmm_cache[market] = {
            'regime': 'neutral',
            'confidence': 0.0,
            'details': 'frozen for regression',
            'ts': now,
        }


def _run_analysis(inputs: dict) -> dict:
    """從 pickle 輸入跑 run_analysis()。scan_mode=True 避開 FinMind/yfinance 外呼。"""
    _freeze_hmm_cache()
    from analysis_engine import TechnicalAnalyzer
    analyzer = TechnicalAnalyzer(
        inputs['ticker'],
        inputs['df_week'],
        inputs['df_day'],
        chip_data=inputs['chip_data'],
        us_chip_data=inputs['us_chip_data'],
        scan_mode=True,  # 跳過 _fetch_fundamental_snapshot 的外網呼叫
    )
    return analyzer.run_analysis()


def _sanitize(obj):
    """把 DataFrame / Series / numpy scalar 轉 JSON-able 型別。"""
    import numpy as np
    import pandas as pd

    if isinstance(obj, dict):
        return {str(k): _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, (pd.DataFrame, pd.Series)):
        return obj.to_dict()
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, float):
        # NaN → null；JSON 比對需要穩定
        if obj != obj:
            return None
        return round(obj, 10)  # 避免浮點 rounding noise
    return obj


def _dump_json(data, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as f:
        json.dump(_sanitize(data), f, ensure_ascii=False, indent=2, sort_keys=True)


def cmd_prepare():
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    for ticker in TICKERS:
        print(f"\n=== PREPARE {ticker} ===")
        inputs = _prepare_inputs(ticker)
        pkl_path = FIXTURE_DIR / f"{ticker}.pkl"
        with pkl_path.open('wb') as f:
            pickle.dump(inputs, f)
        print(f"  saved {pkl_path} ({pkl_path.stat().st_size / 1024:.1f} KB)")
        print(f"  df_day rows={len(inputs['df_day'])}, df_week rows={len(inputs['df_week'])}")
        print(f"  chip_data={'set' if inputs['chip_data'] else 'None'}, us_chip_data={'set' if inputs['us_chip_data'] else 'None'}")


def cmd_baseline():
    for ticker in TICKERS:
        print(f"\n=== BASELINE {ticker} ===")
        pkl_path = FIXTURE_DIR / f"{ticker}.pkl"
        if not pkl_path.exists():
            raise FileNotFoundError(f"{pkl_path} missing. Run --prepare first.")
        with pkl_path.open('rb') as f:
            inputs = pickle.load(f)
        result = _run_analysis(inputs)
        json_path = FIXTURE_DIR / f"{ticker}.baseline.json"
        _dump_json(result, json_path)
        print(f"  saved {json_path}")
        print(f"  trend={result['trend_score']} trigger={result['trigger_score']:.2f} regime={result['regime']}")


def cmd_verify():
    rc = 0
    for ticker in TICKERS:
        print(f"\n=== VERIFY {ticker} ===")
        pkl_path = FIXTURE_DIR / f"{ticker}.pkl"
        baseline_path = FIXTURE_DIR / f"{ticker}.baseline.json"
        if not pkl_path.exists() or not baseline_path.exists():
            print(f"  SKIP: fixture missing ({pkl_path.exists()=}, {baseline_path.exists()=})")
            rc = 2
            continue

        with pkl_path.open('rb') as f:
            inputs = pickle.load(f)
        result = _run_analysis(inputs)
        current_path = FIXTURE_DIR / f"{ticker}.current.json"
        _dump_json(result, current_path)

        baseline_text = baseline_path.read_text(encoding='utf-8')
        current_text = current_path.read_text(encoding='utf-8')
        if baseline_text == current_text:
            print(f"  [OK] byte-for-byte match")
        else:
            print(f"  [FAIL] diff detected; compare {baseline_path} vs {current_path}")
            # 列出 top-level key 差異
            bj = json.loads(baseline_text)
            cj = json.loads(current_text)
            for k in sorted(set(bj) | set(cj)):
                bv, cv = bj.get(k, '__MISSING__'), cj.get(k, '__MISSING__')
                if bv != cv:
                    print(f"    key={k!r} baseline={str(bv)[:120]!r} current={str(cv)[:120]!r}")
            rc = 1
    return rc


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--prepare', action='store_true', help='下載並 pickle 原始輸入')
    g.add_argument('--baseline', action='store_true', help='從 pickle 產生 baseline JSON')
    g.add_argument('--verify', action='store_true', help='驗證 run_analysis 結果 byte-for-byte 一致')
    args = ap.parse_args()

    if args.prepare:
        cmd_prepare()
    elif args.baseline:
        cmd_baseline()
    elif args.verify:
        sys.exit(cmd_verify())


if __name__ == '__main__':
    main()
