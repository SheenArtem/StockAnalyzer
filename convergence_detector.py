"""
convergence_detector.py -- 多策略共振偵測

所有 scanner 模式跑完後，交叉比對結果，找出同時出現在多個模式的股票。
Tier 1: 動能類 + 價值 = 最高信號
Tier 2: 純動能交叉 (momentum+swing, momentum+qm 等)
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# 動能類模式（trigger_score 系）
_MOMENTUM_MODES = {'momentum', 'swing', 'qm'}

# 結果檔對應
_MODE_FILES = {
    'momentum': 'momentum',
    'swing': 'swing',
    'qm': 'qm',
    'value': 'value',
}


class ConvergenceDetector:

    def __init__(self, data_dir='data'):
        self.latest_dir = Path(data_dir) / 'latest'

    def detect(self, market='tw'):
        """
        讀取 data/latest/ 所有模式結果，找出 2+ 模式重疊的股票。

        Returns:
            dict: 標準 scan result 格式 (scan_type='convergence')
        """
        suffix = '_us' if market == 'us' else ''

        # 1. 載入所有存在的結果
        mode_data = {}  # mode -> {scan_date, results_by_id}
        for mode, prefix in _MODE_FILES.items():
            fpath = self.latest_dir / f'{prefix}{suffix}_result.json'
            if not fpath.exists():
                continue
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                scan_date = data.get('scan_date', '')
                results = data.get('results', [])
                if not results:
                    continue
                by_id = {}
                for i, r in enumerate(results):
                    by_id[r['stock_id']] = {'rank': i + 1, **r}
                mode_data[mode] = {'scan_date': scan_date, 'by_id': by_id}
            except Exception as e:
                logger.warning("Failed to load %s: %s", fpath, e)

        if len(mode_data) < 2:
            return self._make_result([], market)

        # 2. 驗證 scan_date 一致（不同天的結果不交叉）
        dates = {m: d['scan_date'] for m, d in mode_data.items()}
        most_recent = max(dates.values())
        valid_modes = {m for m, d in dates.items() if d == most_recent}
        if len(valid_modes) < 2:
            logger.info("Convergence: only 1 mode on %s, skip", most_recent)
            return self._make_result([], market)

        # 3. 建 stock_id -> 出現的模式集合
        stock_modes = {}  # stock_id -> set of modes
        for mode in valid_modes:
            for sid in mode_data[mode]['by_id']:
                stock_modes.setdefault(sid, set()).add(mode)

        # 4. 過濾出現在 2+ 模式的
        overlaps = {sid: modes for sid, modes in stock_modes.items() if len(modes) >= 2}

        if not overlaps:
            return self._make_result([], market)

        # 5. 組裝結果
        results = []
        for sid, modes in overlaps.items():
            has_value = 'value' in modes
            has_momentum_type = bool(modes & _MOMENTUM_MODES)
            tier = 1 if (has_value and has_momentum_type) else 2

            # 從各模式取資料
            entry = {
                'stock_id': sid,
                'convergence_tier': tier,
                'modes': sorted(modes),
                'mode_count': len(modes),
            }

            # 取基本資訊（從第一個有資料的模式）
            for mode in modes:
                r = mode_data[mode]['by_id'][sid]
                if 'name' not in entry or not entry.get('name'):
                    entry['name'] = r.get('name', '')
                    entry['price'] = r.get('price', 0)
                    entry['change_pct'] = r.get('change_pct', 0)
                    entry['market'] = r.get('market', '')
                break

            # 動能類分數
            for m in ('momentum', 'qm', 'swing'):
                if m in modes and m in mode_data:
                    r = mode_data[m]['by_id'][sid]
                    entry['trigger_score'] = r.get('trigger_score')
                    entry['trend_score'] = r.get('trend_score')
                    entry['signals'] = r.get('signals', [])
                    entry['scenario'] = r.get('scenario', {})
                    entry['action_plan'] = r.get('action_plan', {})
                    entry.setdefault('mode_ranks', {})[m] = mode_data[m]['by_id'][sid]['rank']
                    break  # 取第一個動能模式的分數

            # 價值類分數
            if 'value' in modes and 'value' in mode_data:
                r = mode_data['value']['by_id'][sid]
                entry['value_score'] = r.get('value_score')
                entry['value_scores'] = r.get('scores', {})
                entry['PE'] = r.get('PE')
                entry['PB'] = r.get('PB')
                entry['dividend_yield'] = r.get('dividend_yield')
                entry.setdefault('mode_ranks', {})['value'] = mode_data['value']['by_id'][sid]['rank']

            # 補齊其他模式的 rank
            for m in modes:
                if m in mode_data:
                    entry.setdefault('mode_ranks', {})[m] = mode_data[m]['by_id'][sid]['rank']

            results.append(entry)

        # 6. 排序: tier ASC, mode_count DESC, trigger_score DESC
        results.sort(key=lambda x: (
            x['convergence_tier'],
            -x['mode_count'],
            -(x.get('trigger_score') or 0),
        ))

        return self._make_result(results, market)

    def _make_result(self, results, market):
        now = datetime.now()
        return {
            'scan_type': 'convergence',
            'scan_date': now.strftime('%Y-%m-%d'),
            'scan_time': now.strftime('%H:%M'),
            'market': market,
            'total_found': len(results),
            'results': results,
        }

    @staticmethod
    def save_results(result, output_dir='data'):
        base = Path(output_dir)
        latest_dir = base / 'latest'
        history_dir = base / 'history'
        latest_dir.mkdir(parents=True, exist_ok=True)
        history_dir.mkdir(parents=True, exist_ok=True)

        market = result.get('market', 'tw')
        suffix = '_us' if market == 'us' else ''

        latest_file = latest_dir / f'convergence{suffix}_result.json'
        with open(latest_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        date_str = result.get('scan_date', datetime.now().strftime('%Y-%m-%d'))
        history_file = history_dir / f'{date_str}_convergence{suffix}.json'
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        return str(latest_file), str(history_file)
