"""
交易紀錄 / 投資組合資料模型 — 💼 投資組合 tab (2026-07-01)

逐筆交易 (buy/sell) -> 推導當前持倉 (移動平均成本法) + 已實現損益。
儲存：data/manual_trades/transactions.json（git 追蹤，累積型 state，
同 whale_picks/trade_ledger 政策；見 memory project_daily_outputs_untracked）。

純 Python，無 streamlit 依賴，可單元測試（見 tests/test_portfolio_store.py）。

成本/損益慣例（移動平均成本法，台券商慣例）：
  - 買入：cost_basis += 股數*價格 + 手續費；shares += 股數；avg = cost_basis / shares
  - 賣出：realized += 股數*價格 - 手續費 - 稅 - avg*股數；cost_basis -= avg*股數；
          shares -= 股數（avg 不變）；全平倉時歸零重置（再買入重新計均價）
  - 賣超（賣出 > 當時持股）-> raise ValueError（fail loud，不靜默；Robustness First）
  - 交易依 (date, created_at, id) 時序處理，輸入順序不限
"""
import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd

# 儲存位置（測試用 monkeypatch 覆寫 MANUAL_TRADES_DIR 即可，_store_file 動態組路徑）
MANUAL_TRADES_DIR = Path(__file__).resolve().parent / 'data' / 'manual_trades'
STORE_FILENAME = 'transactions.json'
SCHEMA_VERSION = 1

_VALID_ACTIONS = ('buy', 'sell')
_EPS = 1e-6  # 股數浮點容差（判斷平倉/賣超）

# 台股費用估算率（券商手續費未計低消 20 元 / 折讓；使用者可自行覆寫）
_TW_FEE_RATE = 0.001425   # 手續費 0.1425%（買賣皆收）
_TW_TAX_RATE = 0.003      # 證交稅 0.3%（僅賣出，當沖減半此處不處理）


def estimate_tw_costs(shares, price, action) -> tuple:
    """台股費用估算，回 (fee, tax)（四捨五入到整數元）。
    手續費 0.1425%（買賣皆收），證交稅 0.3%（僅賣出）。不含券商低消/折讓/當沖減半。"""
    amount = float(shares) * float(price)
    fee = float(round(amount * _TW_FEE_RATE))
    tax = float(round(amount * _TW_TAX_RATE)) if action == 'sell' else 0.0
    return fee, tax


def _store_file() -> Path:
    return MANUAL_TRADES_DIR / STORE_FILENAME


# ====================================================================
#  代號 / 市場判別（沿用 ai_report.py 慣例：純數字=台股，含字母=美股）
# ====================================================================

def detect_market(ticker: str) -> str:
    """純數字（含 .TW/.TWO 後綴）-> 'tw'；含字母 -> 'us'。"""
    core = str(ticker).upper().replace('.TWO', '').replace('.TW', '').strip()
    return 'tw' if core.isdigit() else 'us'


def normalize_ticker(ticker: str) -> str:
    """正規化代號：台股去 .TW/.TWO 存純代號；美股大寫。"""
    t = str(ticker).strip().upper()
    if detect_market(t) == 'tw':
        return t.replace('.TWO', '').replace('.TW', '')
    return t


# ====================================================================
#  日期
# ====================================================================

def _parse_date(value) -> date:
    """接受 date / datetime / 'YYYY-MM-DD' 字串 -> date。"""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value).strip(), '%Y-%m-%d').date()


# ====================================================================
#  儲存層（load/save）
# ====================================================================

def load_transactions() -> list:
    """讀取所有交易紀錄。

    檔案不存在 -> [] （全新開始）。
    檔案存在但無法解析 -> raise ValueError（fail loud，避免後續 save 覆蓋掉毀損但可能可救的資料）。
    """
    path = _store_file()
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except Exception as e:
        raise ValueError(f"交易紀錄檔毀損無法解析：{path}（{e}）") from e
    if isinstance(data, dict):
        return list(data.get('transactions', []))
    if isinstance(data, list):  # 容忍舊格式（純 list）
        return data
    raise ValueError(f"交易紀錄檔格式非預期：{path}")


def save_transactions(transactions: list) -> None:
    MANUAL_TRADES_DIR.mkdir(parents=True, exist_ok=True)
    payload = {'schema_version': SCHEMA_VERSION, 'transactions': list(transactions)}
    _store_file().write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


# ====================================================================
#  驗證 + CRUD
# ====================================================================

def validate_transaction(ticker, action, txn_date, shares, price,
                         fee=0.0, tax=0.0) -> tuple:
    """回傳 (ok: bool, err_msg: str)。UI 先呼叫顯示錯誤；add/update 內也會擋。"""
    if not str(ticker or '').strip():
        return False, "代號不可空白"
    if action not in _VALID_ACTIONS:
        return False, f"action 必須是 buy/sell，收到 {action!r}"
    try:
        if float(shares) <= 0:
            return False, "股數必須 > 0"
        if float(price) <= 0:
            return False, "價格必須 > 0"
        if float(fee) < 0 or float(tax) < 0:
            return False, "手續費 / 稅不可為負"
    except (TypeError, ValueError):
        return False, "股數 / 價格 / 費用必須是數字"
    try:
        _parse_date(txn_date)
    except Exception:
        return False, f"日期格式錯誤（需 YYYY-MM-DD）：{txn_date!r}"
    return True, ""


def _gen_id(transactions: list, d: date) -> str:
    """MT-YYYYMMDD-NNNN；NNNN 為當日最大流水號 +1（刪除後不重用、不碰撞）。"""
    prefix = f"MT-{d.strftime('%Y%m%d')}-"
    mx = 0
    for t in transactions:
        tid = str(t.get('id', ''))
        if tid.startswith(prefix):
            try:
                mx = max(mx, int(tid[len(prefix):]))
            except ValueError:
                pass
    return f"{prefix}{mx + 1:04d}"


def _build_record(txns, ticker, action, txn_date, shares, price,
                  fee, tax, note) -> dict:
    d = _parse_date(txn_date)
    return {
        'id': _gen_id(txns, d),
        'ticker': normalize_ticker(ticker),
        'market': detect_market(ticker),
        'action': action,
        'date': d.strftime('%Y-%m-%d'),
        'shares': float(shares),
        'price': float(price),
        'fee': float(fee or 0),
        'tax': float(tax or 0),
        'note': str(note or ''),
        'created_at': datetime.now().isoformat(timespec='seconds'),
    }


def add_transaction(ticker, action, txn_date, shares, price,
                    fee=0.0, tax=0.0, note='') -> dict:
    ok, err = validate_transaction(ticker, action, txn_date, shares, price, fee, tax)
    if not ok:
        raise ValueError(err)
    txns = load_transactions()
    rec = _build_record(txns, ticker, action, txn_date, shares, price, fee, tax, note)
    txns.append(rec)
    save_transactions(txns)
    return rec


def update_transaction(txn_id: str, **fields) -> dict:
    """更新既有交易的可編輯欄位（ticker/action/date/shares/price/fee/tax/note）。
    重新驗證 + 重新正規化 ticker/market/date。找不到 id -> KeyError。
    """
    txns = load_transactions()
    for t in txns:
        if t.get('id') != txn_id:
            continue
        merged = {
            'ticker': fields.get('ticker', t.get('ticker')),
            'action': fields.get('action', t.get('action')),
            'txn_date': fields.get('date', t.get('date')),
            'shares': fields.get('shares', t.get('shares')),
            'price': fields.get('price', t.get('price')),
            'fee': fields.get('fee', t.get('fee', 0)),
            'tax': fields.get('tax', t.get('tax', 0)),
        }
        ok, err = validate_transaction(**merged)
        if not ok:
            raise ValueError(err)
        d = _parse_date(merged['txn_date'])
        t['ticker'] = normalize_ticker(merged['ticker'])
        t['market'] = detect_market(merged['ticker'])
        t['action'] = merged['action']
        t['date'] = d.strftime('%Y-%m-%d')
        t['shares'] = float(merged['shares'])
        t['price'] = float(merged['price'])
        t['fee'] = float(merged['fee'] or 0)
        t['tax'] = float(merged['tax'] or 0)
        if 'note' in fields:
            t['note'] = str(fields['note'] or '')
        save_transactions(txns)
        return t
    raise KeyError(txn_id)


def delete_transaction(txn_id: str) -> bool:
    """刪除指定 id。有刪到回 True，找不到回 False。"""
    txns = load_transactions()
    kept = [t for t in txns if t.get('id') != txn_id]
    if len(kept) == len(txns):
        return False
    save_transactions(kept)
    return True


# ====================================================================
#  推導：交易 -> 持倉 + 已實現損益
# ====================================================================

def _sorted_txns(transactions: list) -> list:
    return sorted(transactions,
                  key=lambda t: (t.get('date', ''), t.get('created_at', ''),
                                 t.get('id', '')))


def derive_holdings(transactions: list) -> dict:
    """折疊交易 -> 每檔持倉狀態（移動平均成本法）。

    回傳 dict：ticker -> {
        ticker, market, shares, avg_cost, cost_basis, realized_pnl,
        buy_shares, sell_shares
    }
    已平倉（shares≈0）者仍保留於結果（realized_pnl 供顯示已實現），shares=0。
    賣超 -> raise ValueError。
    """
    state = {}
    for t in _sorted_txns(transactions):
        tk = t['ticker']
        rec = state.setdefault(tk, {
            'ticker': tk,
            'market': t.get('market') or detect_market(tk),
            'shares': 0.0, 'cost_basis': 0.0, 'avg_cost': 0.0,
            'realized_pnl': 0.0, 'buy_shares': 0.0, 'sell_shares': 0.0,
            'entry_date': None,
        })
        s = float(t['shares'])
        px = float(t['price'])
        fee = float(t.get('fee', 0) or 0)
        tax = float(t.get('tax', 0) or 0)
        action = t['action']

        if action == 'buy':
            if rec['shares'] <= _EPS:          # 開新一輪持倉：記錄這輪建倉日
                rec['entry_date'] = t['date']
            rec['cost_basis'] += s * px + fee
            rec['shares'] += s
            rec['buy_shares'] += s
            rec['avg_cost'] = rec['cost_basis'] / rec['shares'] if rec['shares'] > _EPS else 0.0
        elif action == 'sell':
            if s > rec['shares'] + _EPS:
                raise ValueError(
                    f"{tk} 於 {t.get('date')} 賣出 {s:g} 股，"
                    f"超過當時持股 {rec['shares']:g} 股（賣超）")
            cost_of_sold = rec['avg_cost'] * s
            rec['realized_pnl'] += s * px - fee - tax - cost_of_sold
            rec['cost_basis'] -= cost_of_sold
            rec['shares'] -= s
            rec['sell_shares'] += s
            if rec['shares'] <= _EPS:  # 全平倉：歸零重置，再買入重新計均價
                rec['shares'] = 0.0
                rec['cost_basis'] = 0.0
                rec['avg_cost'] = 0.0
                rec['entry_date'] = None
        else:
            raise ValueError(f"未知 action: {action!r}（ticker {tk}）")
    return state


def open_positions(holdings: dict) -> list:
    """僅回傳仍持有（shares>0）的部位 list。"""
    return [rec for rec in holdings.values() if rec['shares'] > _EPS]


def closed_positions(transactions: list) -> list:
    """所有『已完全平倉』的 round-trip（含後來又重新建倉的先前回合）。

    每筆 dict：ticker, market, entry_date, exit_date, holding_days,
        shares（該回合買進總股數）, avg_buy（含手續費）, avg_sell（扣手續費+稅後淨額）,
        cost（總投入含費）, proceeds（總取回淨額）, realized_pnl, return_pct。
    仍持有中的部位不列入（partial 賣出的已實現損益留在 derive_holdings 的 realized_pnl）。
    移動平均成本法；賣超 -> raise ValueError（與 derive_holdings 一致）。
    """
    result = []
    runs = {}  # ticker -> 當前回合狀態
    for t in _sorted_txns(transactions):
        tk = t['ticker']
        s = float(t['shares'])
        px = float(t['price'])
        fee = float(t.get('fee', 0) or 0)
        tax = float(t.get('tax', 0) or 0)
        run = runs.get(tk)

        if t['action'] == 'buy':
            if run is None:
                run = runs[tk] = {
                    'market': t.get('market') or detect_market(tk),
                    'entry_date': t['date'], 'shares': 0.0, 'cost_basis': 0.0,
                    'avg_cost': 0.0, 'buy_shares': 0.0, 'buy_amount': 0.0,
                    'sell_shares': 0.0, 'proceeds': 0.0, 'realized': 0.0,
                }
            run['cost_basis'] += s * px + fee
            run['shares'] += s
            run['buy_shares'] += s
            run['buy_amount'] += s * px + fee
            run['avg_cost'] = run['cost_basis'] / run['shares']
        elif t['action'] == 'sell':
            if run is None or s > run['shares'] + _EPS:
                raise ValueError(
                    f"{tk} 於 {t.get('date')} 賣出 {s:g} 股，超過當時持股（賣超）")
            cost_of_sold = run['avg_cost'] * s
            run['realized'] += s * px - fee - tax - cost_of_sold
            run['proceeds'] += s * px - fee - tax
            run['sell_shares'] += s
            run['cost_basis'] -= cost_of_sold
            run['shares'] -= s
            if run['shares'] <= _EPS:   # 完全平倉 -> 收一筆 round-trip
                entry = _parse_date(run['entry_date'])
                exit_d = _parse_date(t['date'])
                result.append({
                    'ticker': tk, 'market': run['market'],
                    'entry_date': run['entry_date'], 'exit_date': t['date'],
                    'holding_days': (exit_d - entry).days,
                    'shares': run['buy_shares'],
                    'avg_buy': run['buy_amount'] / run['buy_shares'] if run['buy_shares'] else 0.0,
                    'avg_sell': run['proceeds'] / run['sell_shares'] if run['sell_shares'] else 0.0,
                    'cost': run['buy_amount'], 'proceeds': run['proceeds'],
                    'realized_pnl': run['realized'],
                    'return_pct': run['realized'] / run['buy_amount'] if run['buy_amount'] else None,
                })
                runs.pop(tk)   # 回合結束，下次 buy 開新回合
        else:
            raise ValueError(f"未知 action: {t['action']!r}（ticker {tk}）")
    return result


# ====================================================================
#  估值 + 彙總（prices 由 portfolio_pricing 提供；此層純計算可測）
# ====================================================================

def value_positions(holdings: dict, prices: dict) -> list:
    """holdings: derive_holdings 結果；prices: {ticker: 現價}（原幣別）。
    回傳僅未平倉部位 list，每筆加上 current_price / market_value /
    unrealized_pnl / return_pct。價格缺失 -> 該三值為 None（UI 顯示 N/A）。
    """
    out = []
    for rec in open_positions(holdings):
        px = prices.get(rec['ticker']) if prices else None
        row = dict(rec)
        if px is None:
            row.update(current_price=None, market_value=None,
                       unrealized_pnl=None, return_pct=None)
        else:
            mv = rec['shares'] * float(px)
            row['current_price'] = float(px)
            row['market_value'] = mv
            row['unrealized_pnl'] = mv - rec['cost_basis']
            row['return_pct'] = ((mv - rec['cost_basis']) / rec['cost_basis']
                                 if rec['cost_basis'] > _EPS else None)
        out.append(row)
    return out


def summarize(holdings: dict, prices: dict) -> tuple:
    """分市場彙總。回傳 (by_market: dict, valued_positions: list)。

    by_market[market] = {market, cost, market_value, unrealized_pnl,
                         realized_pnl, total_pnl, return_pct, n_open,
                         has_missing_price}
    realized_pnl 涵蓋所有部位（含已平倉）；market_value / unrealized 只算未平倉。
    """
    valued = value_positions(holdings, prices)
    by_market = {}

    def _slot(m):
        return by_market.setdefault(m, dict(
            market=m, cost=0.0, market_value=0.0, unrealized_pnl=0.0,
            realized_pnl=0.0, total_pnl=0.0, return_pct=None,
            n_open=0, has_missing_price=False))

    # realized 涵蓋全部（含已平倉）
    for rec in holdings.values():
        _slot(rec['market'])['realized_pnl'] += rec['realized_pnl']
    # 未平倉部位算成本 / 市值 / 未實現
    for row in valued:
        s = _slot(row['market'])
        s['n_open'] += 1
        s['cost'] += row['cost_basis']
        if row['market_value'] is None:
            s['has_missing_price'] = True
        else:
            s['market_value'] += row['market_value']
            s['unrealized_pnl'] += row['unrealized_pnl']
    for s in by_market.values():
        s['total_pnl'] = s['unrealized_pnl'] + s['realized_pnl']
        s['return_pct'] = (s['unrealized_pnl'] / s['cost']) if s['cost'] > _EPS else None
    return by_market, valued


# ====================================================================
#  淨值曲線（時間加權報酬 TWR，中和買賣現金流）
# ====================================================================

def build_nav_series(transactions: list, price_history: dict, market: str):
    """逐日 TWR 淨值曲線（單一市場；不同幣別不混算）。

    transactions: 全部交易；price_history: {ticker: pandas.Series(Close, DatetimeIndex)}；
    market: 'tw' | 'us'。

    回傳 DataFrame(index=交易日)，欄：
      mv   當日收盤持股市值
      flow 當日淨現金流（買入 +現金投入、賣出 -現金取回，含 fee/tax）
      ret  當日 TWR 報酬 = (mv - flow - prev_mv) / prev_mv（現金流視為當日末發生，不賺當日報酬）
      nav  淨值指數（起始 1.0）；prev_mv=0（尚未建倉/全數出場）當日 ret=0
    無足夠資料 -> 空 DataFrame。

    以 TWR 中和加減碼時點，讓 nav / Sharpe 反映「策略」表現而非資金進出時機。
    """
    empty = pd.DataFrame(columns=['mv', 'flow', 'ret', 'nav'])
    txns = [t for t in _sorted_txns(transactions)
            if (t.get('market') or detect_market(t['ticker'])) == market]
    if not txns:
        return empty

    tickers = sorted({t['ticker'] for t in txns})
    ph = {tk: price_history[tk].sort_index() for tk in tickers
          if tk in price_history and len(price_history.get(tk, [])) > 0}
    if not ph:
        return empty

    idx = None
    for s in ph.values():
        idx = s.index if idx is None else idx.union(s.index)
    first = pd.Timestamp(min(t['date'] for t in txns))
    idx = idx[idx >= first].sort_values()
    if len(idx) == 0:
        return empty

    def _eff(dstr):
        later = idx[idx >= pd.Timestamp(dstr)]
        return later[0] if len(later) else None

    price_df = pd.DataFrame(index=idx)
    for tk, s in ph.items():
        price_df[tk] = s.reindex(idx, method='ffill')

    shares = pd.DataFrame(0.0, index=idx, columns=list(ph.keys()))
    flow = pd.Series(0.0, index=idx)
    for t in txns:
        tk = t['ticker']
        if tk not in shares.columns:
            continue
        ed = _eff(t['date'])
        if ed is None:
            continue
        s_qty = float(t['shares'])
        px = float(t['price'])
        fee = float(t.get('fee', 0) or 0)
        tax = float(t.get('tax', 0) or 0)
        if t['action'] == 'buy':
            shares.loc[shares.index >= ed, tk] += s_qty
            flow.loc[ed] += s_qty * px + fee
        else:
            shares.loc[shares.index >= ed, tk] -= s_qty
            flow.loc[ed] -= (s_qty * px - fee - tax)

    mv = (shares * price_df).fillna(0.0).sum(axis=1)

    ret = pd.Series(0.0, index=idx)
    nav = pd.Series(1.0, index=idx)
    prev_mv, cur_nav = 0.0, 1.0
    for d in idx:
        mv_d = float(mv.loc[d])
        r = (mv_d - float(flow.loc[d]) - prev_mv) / prev_mv if prev_mv > _EPS else 0.0
        ret.loc[d] = r
        cur_nav *= (1.0 + r)
        nav.loc[d] = cur_nav
        prev_mv = mv_d

    return pd.DataFrame({'mv': mv, 'flow': flow, 'ret': ret, 'nav': nav})


def ytd_baseline(series, year: int):
    """YTD 基準值：前一年最後交易日收盤；若無前一年資料則取當年第一個交易日收盤。
    series: pandas.Series（Close 或 nav，DatetimeIndex）。空/None 回 None。"""
    if series is None or len(series) == 0:
        return None
    cutoff = pd.Timestamp(year, 1, 1)
    prev = series[series.index < cutoff]
    if len(prev):
        return float(prev.iloc[-1])
    cur = series[series.index >= cutoff]
    return float(cur.iloc[0]) if len(cur) else None


def ytd_return(series, current_value, year: int):
    """YTD 報酬率 = current_value / baseline - 1（baseline 取前一年末收盤）。
    無基準 / 基準為 0 / 無現值 -> None。"""
    base = ytd_baseline(series, year)
    if not base or current_value is None:
        return None
    return current_value / base - 1.0


def position_ytd(entry_date, avg_cost, current_price, price_series, year: int):
    """個人部位 YTD（「我的今年報酬」）：
      - 今年建倉（entry_date 落在 year）：以建倉均價為基準 -> current/avg_cost - 1
        （年初尚未持有，YTD 只能從建倉起算；此時等同該部位總報酬率）
      - 跨年持有（entry_date 在 year 之前）：以該股去年末收盤為基準 -> 今年這段股價表現
        （price_series 為該股 Close 歷史，僅跨年持有時需要）
    無法計算 -> None。
    """
    if current_price is None:
        return None
    ed = None
    if entry_date:
        try:
            ed = _parse_date(entry_date)
        except Exception:
            ed = None
    if ed is not None and ed.year >= year:
        return (current_price / avg_cost - 1.0) if avg_cost else None
    base = ytd_baseline(price_series, year)
    return (current_price / base - 1.0) if base else None
