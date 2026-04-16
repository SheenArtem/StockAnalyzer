"""
position_monitor.py — 持股每日監控 + 出場警報

每日檢查使用者持股是否觸發出場條件（硬警報 = 立即全出 / 軟警報 = 考慮減碼）。

出場條件：
  - hard: 價格跌破動態停損（ATR% 調整，預設 -8%；見 exit_manager.py）
  - hard: 週 Supertrend 翻空
  - hard: 週 MA20 跌破 3%
  - hard: 月營收 YoY 連 2 月轉負（台股，每月 10 日後更新）
  - soft: trend_score < 1（趨勢論據弱化）
  - soft: trigger_score 峰值 ≥ +5 掉至 ≤ -2（動能急轉，峰值取最近 20 日）
  - soft: trigger_score 連續 5 個交易日 < 0（持續弱化）

trigger_score 歷史累積在 data/latest/position_history.json，每日 scanner 執行後自動寫入。

資料來源：
  - 價格/技術：load_and_resample + calculate_all_indicators（既有）
  - trend_score：TechnicalAnalyzer.run_analysis（既有）
  - 月營收：DividendRevenueAnalyzer.get_monthly_revenue（既有）

Usage:
    python position_monitor.py              # 跑一次監控
    （app.py QM tab 內建「立即檢查警報」按鈕）

排程：每日 scanner 結束後由 scanner_job.py 自動呼叫。
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

POSITIONS_FILE = Path('data/positions.json')
ALERTS_FILE = Path('data/latest/position_alerts.json')
HISTORY_FILE = Path('data/latest/position_history.json')

# trigger_score 歷史保留天數（用於軟警報判斷）
_HISTORY_MAX_DAYS = 20


# ============================================================
#  持股 CRUD
# ============================================================

def load_positions(path=POSITIONS_FILE):
    """讀取持股清單。檔案不存在回傳空 list。"""
    path = Path(path)
    if not path.exists():
        return []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('positions', [])
    except Exception as e:
        logger.warning("Failed to load positions from %s: %s", path, e)
        return []


def save_positions(positions, path=POSITIONS_FILE):
    """寫入持股清單。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({'positions': positions}, f, ensure_ascii=False, indent=2)


def load_alerts(path=ALERTS_FILE):
    """讀取最新警報結果。檔案不存在回傳 None。"""
    path = Path(path)
    if not path.exists():
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Failed to load alerts from %s: %s", path, e)
        return None


# ============================================================
#  trigger_score 歷史（軟警報用）
# ============================================================

def _history_key(stock_id, buy_date):
    """以 stock_id|buy_date 為 key，避免同一檔多次買進混淆。"""
    return f"{stock_id}|{buy_date or '-'}"


def load_history(path=HISTORY_FILE):
    """讀取 trigger_score 歷史 dict（key=stock_id|buy_date）。"""
    path = Path(path)
    if not path.exists():
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f) or {}
    except Exception as e:
        logger.warning("Failed to load history from %s: %s", path, e)
        return {}


def save_history(history, path=HISTORY_FILE):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def _append_history(history, key, trigger_score, today_str):
    """附加一筆 {date, trigger_score} 到歷史，同日覆寫，保留最近 N 日。"""
    series = history.get(key, [])
    # 移除今日舊紀錄（同日重跑以最新為準）
    series = [e for e in series if e.get('date') != today_str]
    series.append({'date': today_str, 'trigger_score': round(float(trigger_score), 2)})
    # 依日期排序（字串 ISO 排序即可）
    series.sort(key=lambda e: e.get('date', ''))
    # 截尾
    if len(series) > _HISTORY_MAX_DAYS:
        series = series[-_HISTORY_MAX_DAYS:]
    history[key] = series
    return series


# ============================================================
#  單檔出場條件檢查
# ============================================================

def _check_hard_stop(current_price, buy_price, atr_pct=None):
    """動態硬停損（exit_manager 依 ATR% 計算，預設 -8%）。"""
    from exit_manager import compute_exit_plan
    plan = compute_exit_plan(buy_price, atr_pct=atr_pct)
    threshold = plan['hard_stop']
    stop_pct = plan['hard_stop_pct']
    if current_price < threshold:
        return {
            'type': 'hard_stop',
            'severity': 'hard',
            'desc': f'{stop_pct*100:+.1f}% 硬停損觸發',
            'value': f'現價 {current_price:.2f} < 停損 {threshold:.2f} (method={plan["method"]})',
        }
    return None


def _check_supertrend_bear(df_week):
    """週 Supertrend 翻空。"""
    if 'Supertrend_Dir' not in df_week.columns or df_week.empty:
        return None
    st_dir = df_week['Supertrend_Dir'].iloc[-1]
    if pd.notna(st_dir) and int(st_dir) == -1:
        return {
            'type': 'supertrend_bear',
            'severity': 'hard',
            'desc': '週 Supertrend 翻空',
            'value': '方向 -1（空方）',
        }
    return None


def _check_weekly_ma20_break(df_week, current_price, atr_pct=None):
    """週 MA20 跌破閾值（exit_manager 依 ATR% 調整，預設 -3%）。"""
    if df_week.empty or len(df_week) < 20 or 'Close' not in df_week.columns:
        return None
    ma20 = df_week['Close'].rolling(20).mean().iloc[-1]
    if pd.isna(ma20):
        return None
    from exit_manager import compute_ma20_break_threshold
    threshold = compute_ma20_break_threshold(ma20, atr_pct=atr_pct)
    if threshold > 0 and current_price < threshold:
        break_pct = (current_price / ma20 - 1) * 100
        return {
            'type': 'ma20_break',
            'severity': 'hard',
            'desc': f'週 MA20 跌破 {abs(break_pct):.1f}%',
            'value': f'現價 {current_price:.2f} / 週 MA20 {ma20:.2f} ({break_pct:+.1f}%)',
        }
    return None


def _check_trend_weak(trend_score):
    """trend_score < 1 軟警報。"""
    if trend_score is None:
        return None
    if trend_score < 1:
        return {
            'type': 'trend_weak',
            'severity': 'soft',
            'desc': '趨勢分數 < 1（趨勢論據弱化）',
            'value': f'trend_score = {trend_score:.1f}',
        }
    return None


def _check_trigger_peak_drop(series):
    """trigger_score 峰值 ≥ +5 掉至 ≤ -2（動能急轉）。需至少 3 筆歷史。"""
    if not series or len(series) < 3:
        return None
    today = series[-1].get('trigger_score')
    if today is None or today > -2:
        return None
    peak = max(e.get('trigger_score', 0) for e in series)
    if peak >= 5:
        return {
            'type': 'trigger_peak_drop',
            'severity': 'soft',
            'desc': 'trigger_score 峰值 ≥ +5 掉至 ≤ -2（動能急轉）',
            'value': f'近 {len(series)} 日峰值 {peak:+.1f} → 今日 {today:+.1f}',
        }
    return None


def _check_trigger_neg_streak(series, streak=5):
    """trigger_score 連續 N 個交易日 < 0（持續弱化）。"""
    if not series or len(series) < streak:
        return None
    last_n = series[-streak:]
    if all((e.get('trigger_score', 0) or 0) < 0 for e in last_n):
        values = ' / '.join(f"{e['trigger_score']:+.1f}" for e in last_n)
        return {
            'type': 'trigger_neg_streak',
            'severity': 'soft',
            'desc': f'trigger_score 連 {streak} 日 < 0（持續弱化）',
            'value': values,
        }
    return None


def _check_revenue_yoy_neg2(stock_id):
    """月營收 YoY 連續 2 月轉負（台股）。"""
    try:
        from dividend_revenue import DividendRevenueAnalyzer
        dra = DividendRevenueAnalyzer()
        df = dra.get_monthly_revenue(stock_id, months=3)
    except Exception as e:
        logger.debug("Revenue fetch failed for %s: %s", stock_id, e)
        return None
    if df is None or df.empty or 'yoy_pct' not in df.columns or len(df) < 2:
        return None
    last2 = df['yoy_pct'].head(2).tolist()
    try:
        last2 = [float(x) for x in last2]
    except Exception:
        return None
    if all(y < 0 for y in last2):
        return {
            'type': 'revenue_yoy_neg2',
            'severity': 'hard',
            'desc': '月營收 YoY 連 2 月轉負',
            'value': f'{last2[0]:+.1f}% / {last2[1]:+.1f}%',
        }
    return None


def check_single_position(pos, history=None, today_str=None):
    """檢查單一持股。回傳 alert dict（若無觸發）或 None。

    Parameters
    ----------
    pos : dict  持股資料（stock_id/buy_price/buy_date/...）
    history : dict or None  trigger_score 歷史 dict（會就地更新）
    today_str : str or None  今日 ISO 日期，供 history 附加用；預設為 date.today()
    """
    stock_id = str(pos.get('stock_id', '')).strip()
    if not stock_id:
        return None
    try:
        buy_price = float(pos.get('buy_price', 0))
    except Exception:
        return None
    if buy_price <= 0:
        return None

    if today_str is None:
        today_str = date.today().isoformat()

    # 1. Load price data (reuse existing loader)
    from technical_analysis import load_and_resample, calculate_all_indicators
    try:
        _, df_day, df_week, _ = load_and_resample(stock_id)
    except Exception as e:
        logger.warning("load_and_resample failed for %s: %s", stock_id, e)
        return None
    if df_day.empty or df_week.empty:
        return None

    # 2. Indicators
    try:
        df_day = calculate_all_indicators(df_day)
        df_week = calculate_all_indicators(df_week)
    except Exception as e:
        logger.warning("Indicator calc failed for %s: %s", stock_id, e)
        return None

    try:
        current_price = float(df_day['Close'].iloc[-1])
    except Exception:
        return None

    # 3. trend_score + trigger_score from TechnicalAnalyzer
    trend_score = None
    trigger_score = None
    try:
        from analysis_engine import TechnicalAnalyzer
        analyzer = TechnicalAnalyzer(stock_id, df_week, df_day, scan_mode=True)
        report = analyzer.run_analysis()
        trend_score = report.get('trend_score')
        trigger_score = report.get('trigger_score')
    except Exception as e:
        logger.debug("analyzer failed for %s: %s", stock_id, e)

    # 3b. 更新 trigger_score 歷史（若有傳入 history dict）
    buy_date_str = pos.get('buy_date', '')
    series = []
    if history is not None and trigger_score is not None:
        key = _history_key(stock_id, buy_date_str)
        series = _append_history(history, key, trigger_score, today_str)

    # 3c. ATR%（Phase 2 動態停損停利）
    atr_pct = None
    if 'ATR_pct' in df_day.columns:
        last_atr = df_day['ATR_pct'].iloc[-1]
        if pd.notna(last_atr):
            atr_pct = float(last_atr)

    # 4. Run checks
    triggers = []
    for t in [
        _check_hard_stop(current_price, buy_price, atr_pct=atr_pct),
        _check_supertrend_bear(df_week),
        _check_weekly_ma20_break(df_week, current_price, atr_pct=atr_pct),
        _check_trend_weak(trend_score),
        _check_trigger_peak_drop(series),
        _check_trigger_neg_streak(series),
    ]:
        if t is not None:
            triggers.append(t)

    # 5. 月營收（台股：純數字 stock_id）
    if stock_id.isdigit():
        t = _check_revenue_yoy_neg2(stock_id)
        if t is not None:
            triggers.append(t)

    if not triggers:
        return None

    # 6. 組合警報
    hold_days = 0
    if buy_date_str:
        try:
            bd = date.fromisoformat(buy_date_str)
            hold_days = (date.today() - bd).days
        except Exception:
            pass

    pnl_pct = (current_price / buy_price - 1) * 100
    severity = 'hard' if any(t['severity'] == 'hard' for t in triggers) else 'soft'

    return {
        'stock_id': stock_id,
        'name': pos.get('name', ''),
        'buy_date': buy_date_str,
        'buy_price': round(buy_price, 2),
        'current_price': round(current_price, 2),
        'pnl_pct': round(pnl_pct, 2),
        'hold_days': hold_days,
        'shares': pos.get('shares', 0),
        'trigger_score': round(float(trigger_score), 2) if trigger_score is not None else None,
        'severity': severity,
        'triggers': triggers,
    }


# ============================================================
#  主入口
# ============================================================

def run_monitor(positions=None, output_path=ALERTS_FILE, progress=None):
    """對所有持股跑一次監控，寫入 alerts + trigger_score 歷史 JSON。"""
    if positions is None:
        positions = load_positions()

    # 載入 trigger_score 歷史（軟警報用）
    history = load_history()
    # 清理：移除已不在持股中的 key（stock_id|buy_date）
    active_keys = {_history_key(p.get('stock_id', ''), p.get('buy_date', ''))
                   for p in positions}
    history = {k: v for k, v in history.items() if k in active_keys}

    today_iso = date.today().isoformat()
    alerts = []
    for i, pos in enumerate(positions):
        sid = pos.get('stock_id', '?')
        if progress:
            progress(f"[{i+1}/{len(positions)}] checking {sid}")
        try:
            a = check_single_position(pos, history=history, today_str=today_iso)
            if a is not None:
                alerts.append(a)
        except Exception as e:
            logger.warning("Monitor failed for %s: %s", sid, e)

    # 寫入 trigger_score 歷史（無論是否有警報）
    save_history(history)

    # 硬警報排前面
    alerts.sort(key=lambda a: (0 if a['severity'] == 'hard' else 1, -abs(a.get('pnl_pct', 0))))

    now = datetime.now()
    output = {
        'scan_date': now.strftime('%Y-%m-%d'),
        'scan_time': now.strftime('%H:%M'),
        'position_count': len(positions),
        'alert_count': len(alerts),
        'hard_count': sum(1 for a in alerts if a['severity'] == 'hard'),
        'soft_count': sum(1 for a in alerts if a['severity'] == 'soft'),
        'alerts': alerts,
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    return output


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s')

    def _p(msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    result = run_monitor(progress=_p)
    print()
    print("=" * 65)
    print(f"  Position Monitor — {result['scan_date']} {result['scan_time']}")
    print("=" * 65)
    print(f"  Positions: {result['position_count']}")
    print(f"  Alerts:    {result['alert_count']} "
          f"(hard={result['hard_count']}, soft={result['soft_count']})")
    for a in result['alerts']:
        print(f"\n  [{a['severity'].upper()}] {a['stock_id']} {a['name']} "
              f"PnL {a['pnl_pct']:+.1f}% / {a['hold_days']}d held")
        for t in a['triggers']:
            print(f"    - {t['desc']}: {t['value']}")
    print("=" * 65)
