"""QM 每日決策引擎 — 套用 5 步 SOP 自動產出今日操作建議。

Step 1: gate filter (qm_entry_gate.level=='green' AND ready)
Step 2: 對 entry zone (rec_entry_low/high, rec_sl_price)
Step 3: sympathy filter (sector peer 當日 avg change ≤ -5% → 折扣半倉)
Step 4: 排序 Top 1-2 (composite × recommended_pct)
Step 5: 輸出掛單建議 (entry zone / SL / TP1 / size_pct)

純 deterministic rule + yfinance 即時報價，no LLM。
"""
from __future__ import annotations
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import yfinance as yf

logger = logging.getLogger(__name__)

QM_RESULT_PATH = Path(__file__).resolve().parent.parent / 'data' / 'latest' / 'qm_result.json'

# Thresholds（可調，目前對齊 SOP expander 文字）
CHASE_PCT = 1.05           # 現價 > rec_entry_high × 1.05 → 追高放棄
BROKEN_PCT = 1.02          # 現價 < rec_sl × 1.02 → 跌穿停損附近放棄
LIMIT_THRESHOLD = 0.095    # |change_pct| ≥ 9.5% 視為近 limit 鎖死
SYMPATHY_PCT = -0.05       # 同類股 avg change ≤ -5% → 折扣半倉
MAX_PICKS = 2              # 取 Top 1-2 進場
SYMPATHY_DISCOUNT = 0.5    # sympathy 觸發時倉位再砍半
FIRST_BATCH = 0.5          # 第 1 批 50%


def load_qm_results(path: Path = QM_RESULT_PATH) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        with path.open(encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.warning("load qm_result.json failed: %s", e)
        return None


def _yf_symbols(stock_id: str) -> List[str]:
    """TW: 4-digit -> try .TW then .TWO (TPEx). 已含 .TW 直接用。"""
    sid = str(stock_id).strip()
    if '.TW' in sid:
        return [sid]
    return [f"{sid}.TW", f"{sid}.TWO"]


def fetch_intraday_quotes(tickers: List[str]) -> Dict[str, dict]:
    """Batch-fetch yfinance 2-day daily bars for TW tickers.

    Returns: {stock_id: {current, prev_close, open, high, low, change_pct, limit_down, limit_up, quote_date}}
    若市場未開（quote_date != today）僅回 prev_close + 標記 stale。
    """
    if not tickers:
        return {}

    yf_syms = []
    sym_to_sid = {}
    for sid in tickers:
        for sym in _yf_symbols(sid):
            yf_syms.append(sym)
            sym_to_sid[sym] = sid

    quotes: Dict[str, dict] = {}
    try:
        data = yf.download(
            ' '.join(yf_syms), period='2d', interval='1d',
            group_by='ticker', progress=False, auto_adjust=False,
            threads=True, timeout=20,
        )
    except Exception as e:
        logger.warning("yf batch fetch failed: %s", e)
        return {}

    if data is None or data.empty:
        return {}

    today_str = date.today().isoformat()
    multi = (getattr(data.columns, 'nlevels', 1) == 2)

    for sym, sid in sym_to_sid.items():
        if sid in quotes:
            continue
        try:
            if multi:
                if sym not in data.columns.get_level_values(0):
                    continue
                sub = data[sym].dropna(how='all')
            else:
                sub = data.dropna(how='all')
            if sub.empty:
                continue
            last_idx = sub.index[-1]
            quote_date = last_idx.date().isoformat() if hasattr(last_idx, 'date') else str(last_idx)
            today_close = float(sub['Close'].iloc[-1])
            today_open = float(sub['Open'].iloc[-1]) if 'Open' in sub else today_close
            today_low = float(sub['Low'].iloc[-1]) if 'Low' in sub else today_close
            today_high = float(sub['High'].iloc[-1]) if 'High' in sub else today_close
            if len(sub) >= 2:
                prev_close = float(sub['Close'].iloc[-2])
            else:
                prev_close = today_close
            chg = (today_close - prev_close) / prev_close if prev_close else 0.0
            quotes[sid] = {
                'current': today_close,
                'prev_close': prev_close,
                'open': today_open,
                'low': today_low,
                'high': today_high,
                'change_pct': chg,
                'limit_down': chg <= -LIMIT_THRESHOLD,
                'limit_up': chg >= LIMIT_THRESHOLD,
                'quote_date': quote_date,
                'stale': quote_date != today_str,
            }
        except Exception as e:
            logger.debug("parse %s failed: %s", sym, e)
            continue

    return quotes


def _get_peers_for(stock_id: str) -> List[str]:
    """從 sector_tags_manual.json 取同題材 peer ticker (排除 self)。"""
    try:
        from peer_comparison import get_ticker_themes, get_theme_peers
    except ImportError:
        return []
    themes = get_ticker_themes(stock_id)
    peers = set()
    for t in themes:
        peers.update(get_theme_peers(t['id'], exclude_ticker=stock_id))
    return sorted(peers)


def evaluate_one(stock: dict, quote: Optional[dict], sector_avg_change: Optional[float]) -> dict:
    """套 5 步 SOP 對單檔評估。回傳 decision dict.

    decision 值:
      - 'enter' / 'enter_discounted' (sympathy 折半)
      - 'limit_locked' / 'chase' / 'broken' / 'wait_pullback'
      - 'gate_yellow' / 'gate_red' / 'gate_unknown'
      - 'no_quote' / 'no_plan' / 'market_closed'
    """
    sid = stock['stock_id']
    name = stock.get('name', '')
    ap = stock.get('action_plan', {}) or {}
    gate = ap.get('qm_entry_gate', {}) or {}

    # Step 1
    level = gate.get('level')
    if level != 'green' or not gate.get('ready'):
        return {
            'stock_id': sid, 'name': name,
            'decision': f'gate_{level}' if level else 'gate_unknown',
            'reason': gate.get('text', '未達 green gate'),
            'enter': False,
        }

    elo_raw = ap.get('rec_entry_low')
    ehi_raw = ap.get('rec_entry_high')
    rec_sl = ap.get('rec_sl_price')
    rec_tp = ap.get('rec_tp_price')
    if elo_raw is None or ehi_raw is None:
        return {'stock_id': sid, 'name': name, 'decision': 'no_plan',
                'reason': '無 entry plan', 'enter': False}
    elo, ehi = sorted([elo_raw, ehi_raw])

    if not quote:
        return {'stock_id': sid, 'name': name, 'decision': 'no_quote',
                'reason': '無即時報價（盤前？資料源異常）', 'enter': False,
                'entry_low': elo, 'entry_high': ehi, 'sl': rec_sl, 'tp1': rec_tp}

    if quote.get('stale'):
        return {'stock_id': sid, 'name': name, 'decision': 'market_closed',
                'reason': f'市場未開，最後報價 {quote.get("quote_date")} = {quote["current"]:.2f}',
                'enter': False, 'current': quote['current'],
                'entry_low': elo, 'entry_high': ehi, 'sl': rec_sl, 'tp1': rec_tp}

    cur = quote['current']

    if quote.get('limit_down'):
        return {'stock_id': sid, 'name': name, 'decision': 'limit_locked',
                'reason': f'跌停鎖死（{cur:.1f}, {quote["change_pct"]*100:+.1f}%）',
                'enter': False, 'current': cur,
                'entry_low': elo, 'entry_high': ehi, 'sl': rec_sl, 'tp1': rec_tp}
    if quote.get('limit_up'):
        return {'stock_id': sid, 'name': name, 'decision': 'chase',
                'reason': f'漲停（{cur:.1f}, {quote["change_pct"]*100:+.1f}%），追高放棄',
                'enter': False, 'current': cur,
                'entry_low': elo, 'entry_high': ehi, 'sl': rec_sl, 'tp1': rec_tp}
    if rec_sl and cur < rec_sl * BROKEN_PCT:
        return {'stock_id': sid, 'name': name, 'decision': 'broken',
                'reason': f'現價 {cur:.1f} < SL {rec_sl:.1f} × 1.02（已跌穿停損附近）',
                'enter': False, 'current': cur,
                'entry_low': elo, 'entry_high': ehi, 'sl': rec_sl, 'tp1': rec_tp}
    if cur > ehi * CHASE_PCT:
        return {'stock_id': sid, 'name': name, 'decision': 'chase',
                'reason': f'現價 {cur:.1f} > entry 上緣 {ehi:.1f} × 1.05（已追高）',
                'enter': False, 'current': cur,
                'entry_low': elo, 'entry_high': ehi, 'sl': rec_sl, 'tp1': rec_tp}

    # Step 3 sympathy
    sympathy = sector_avg_change is not None and sector_avg_change <= SYMPATHY_PCT

    # Step 4 size
    base_pct = (stock.get('qm_position_size') or {}).get('recommended_pct') or 5.0
    size_pct = base_pct * FIRST_BATCH
    if sympathy:
        size_pct *= SYMPATHY_DISCOUNT

    composite = stock.get('composite_score') or 0
    return {
        'stock_id': sid, 'name': name,
        'decision': 'enter_discounted' if sympathy else 'enter',
        'reason': (f'落在 entry zone（{elo:.1f}~{ehi:.1f}）內'
                   + (f'，但同類股普跌 {sector_avg_change*100:+.1f}% → 折扣半倉'
                      if sympathy else '')),
        'enter': True,
        'current': cur,
        'change_pct': quote['change_pct'],
        'entry_low': elo,
        'entry_high': ehi,
        'sl': rec_sl,
        'tp1': rec_tp,
        'size_pct': round(size_pct, 1),
        'composite': composite,
        'sector_avg_change': sector_avg_change,
        'sympathy': sympathy,
        'priority': composite * size_pct,
    }


def daily_decision(qm_data: Optional[dict] = None) -> dict:
    """主入口：套 5 步 SOP，回傳 {enter: [...], stand_down: [...], 摘要欄位...}。"""
    if qm_data is None:
        qm_data = load_qm_results()
    if not qm_data or not qm_data.get('results'):
        return {'error': 'no_qm_result'}

    results = qm_data['results']

    # Step 1 gate filter
    green = [r for r in results
             if (r.get('action_plan', {}).get('qm_entry_gate', {}).get('level') == 'green'
                 and r.get('action_plan', {}).get('qm_entry_gate', {}).get('ready'))]
    yellow_red = [r for r in results if r not in green]

    if not green:
        return {
            'scan_date': qm_data.get('scan_date'),
            'scan_time': qm_data.get('scan_time'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'green_count': 0,
            'yellow_red_count': len(yellow_red),
            'enter': [],
            'stand_down': [],
            'message': f'Top {len(results)} 沒有任何 green/ready，今日不進場',
        }

    # 收集所有要報價的 ticker（candidate + peer）
    all_tickers = set(r['stock_id'] for r in green)
    peer_map: Dict[str, List[str]] = {}
    for r in green:
        peers = _get_peers_for(r['stock_id'])
        peer_map[r['stock_id']] = peers
        all_tickers.update(peers)

    quotes = fetch_intraday_quotes(sorted(all_tickers))

    # Step 3 sector avg per candidate
    sector_changes: Dict[str, Optional[float]] = {}
    for sid, peers in peer_map.items():
        peer_changes = [quotes[p]['change_pct'] for p in peers
                        if p in quotes and not quotes[p].get('stale')]
        sector_changes[sid] = (sum(peer_changes) / len(peer_changes)) if peer_changes else None

    # Step 5 evaluate
    decisions = [evaluate_one(s, quotes.get(s['stock_id']), sector_changes.get(s['stock_id']))
                 for s in green]

    enter_all = sorted([d for d in decisions if d.get('enter')],
                      key=lambda d: -(d.get('priority') or 0))
    enter = enter_all[:MAX_PICKS]
    stand_down = [d for d in decisions if not d.get('enter')]
    extra_enter = enter_all[MAX_PICKS:]
    for d in extra_enter:
        d['decision'] = 'over_max_picks'
        d['reason'] = f'符合進場條件但超過 Top {MAX_PICKS} 名額'
        d['enter'] = False
    stand_down.extend(extra_enter)

    return {
        'scan_date': qm_data.get('scan_date'),
        'scan_time': qm_data.get('scan_time'),
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'green_count': len(green),
        'yellow_red_count': len(yellow_red),
        'enter': enter,
        'stand_down': stand_down,
    }


if __name__ == '__main__':
    import pprint
    out = daily_decision()
    pprint.pprint(out, sort_dicts=False, width=120)
