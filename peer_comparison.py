"""
同業比較模組 — 找同產業公司，比較估值/獲利指標

台股（2026-04-21 起）優先順序：
  1. MANUAL_PEER_OVERRIDE — 手動白名單覆蓋 TV 已知誤分類
  2. TradingView sector + industry — 112 細分類，精準度優於 FinMind 大類
  3. FinMind industry_category — fallback（~30 大類，粒度最粗）

美股: Finviz sector/industry
"""

import logging
import time

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_CACHE = {}
_CACHE_TTL = 3600  # 1 hour
_TV_MAP_CACHE = {'data': None, 'ts': 0}
_TV_MAP_TTL = 7 * 24 * 3600  # 7 days — industry classification rarely changes


# ============================================================
# MANUAL_PEER_OVERRIDE — 修正 TradingView 已知誤分類
# ============================================================
# Key: target stock_id, Value: list of peer stock_ids (不含 target 自己)
# 觸發條件：TV industry 誤分類（probe 結果 2026-04-21 確認）
# 未列出的股票走 TV industry → FinMind 的優先順序
def _expand_groups(groups):
    """Expand {label: [sid1, sid2, ...]} into {sid: [other_sids]} symmetric override dict."""
    out = {}
    for sids in groups.values():
        for i, sid in enumerate(sids):
            peers = [s for s in sids if s != sid]
            # Merge if sid appears in multiple groups
            if sid in out:
                out[sid] = list(dict.fromkeys(out[sid] + peers))
            else:
                out[sid] = peers
    return out


# 按族群定義（auto-expand 保證 symmetric）
_PEER_GROUPS = {
    '重電': ['1519', '1503', '1513', '1504', '1514'],
    '工具機/線性傳動': ['2049', '4583', '7750', '4538', '8255', '1597'],
    '晶圓代工': ['2330', '2303', '6770', '5347'],
    'IC 封測 OSAT': ['3711', '6239', '2449', '2441', '3374'],
    '面板': ['3481', '2409', '6116'],
    '光學鏡頭': ['3008', '3406', '3019'],
    '散熱模組': ['3017', '4540', '2421', '3483'],
    'NB/AI 伺服器 ODM': ['2382', '3231', '2324', '2356', '6669', '3706'],
    'PCB 載板/CCL': ['3037', '8046', '3189', '3044', '6213', '2383'],
    '海運貨櫃': ['2603', '2609', '2615'],
    '航空': ['2610', '2618'],
    '金控': ['2881', '2882', '2891', '2886', '2880', '2892', '2887', '2883'],
    '水泥': ['1101', '1102', '1103', '1104'],
    '塑化三寶': ['1301', '1303', '1326'],
    '食品': ['1216', '1201', '1210', '1227'],
    '電信': ['2412', '3045', '4904'],
    '記憶體 flash/DRAM': ['2337', '2344', '2408'],
}

MANUAL_PEER_OVERRIDE = _expand_groups(_PEER_GROUPS)


def _cache_get(key):
    if key in _CACHE:
        data, ts = _CACHE[key]
        if time.time() - ts < _CACHE_TTL:
            return data
    return None


def _cache_set(key, data):
    _CACHE[key] = (data, time.time())


def _fetch_tv_industry_map():
    """Bulk fetch TradingView sector/industry for all TW stocks.

    Returns DataFrame with columns [stock_id, sector, industry], indexed by stock_id.
    Cached 7 days (industry classification changes rarely).
    """
    now = time.time()
    if _TV_MAP_CACHE['data'] is not None and now - _TV_MAP_CACHE['ts'] < _TV_MAP_TTL:
        return _TV_MAP_CACHE['data']
    try:
        from tradingview_screener import Query
        result = (Query()
            .select('name', 'sector', 'industry')
            .set_markets('taiwan')
            .limit(5000)
            .get_scanner_data())
        df = result[1]
        if df is None or df.empty:
            return None
        df = df.rename(columns={'name': 'stock_id'})
        df['stock_id'] = df['stock_id'].astype(str)
        df = df.dropna(subset=['sector', 'industry']).set_index('stock_id')
        _TV_MAP_CACHE['data'] = df
        _TV_MAP_CACHE['ts'] = now
        logger.info("Fetched TV industry map for %d TW stocks", len(df))
        return df
    except Exception as e:
        logger.warning("TV industry map fetch failed: %s", e)
        return None


def _get_peer_ids_and_label(stock_id, info):
    """Resolve peer ids + industry label for a target stock.

    Priority:
      1. MANUAL_PEER_OVERRIDE → explicit peer list
      2. TradingView sector + industry → all stocks in same group
      3. FinMind industry_category → fallback

    Returns (peer_ids: list[str], industry_label: str, source: str).
    """
    # 1. Manual override
    if stock_id in MANUAL_PEER_OVERRIDE:
        peer_ids = list(MANUAL_PEER_OVERRIDE[stock_id])
        if stock_id not in peer_ids:
            peer_ids = [stock_id] + peer_ids
        return peer_ids, 'Manual override', 'manual'

    # 2. TradingView industry
    tv_map = _fetch_tv_industry_map()
    if tv_map is not None and stock_id in tv_map.index:
        target = tv_map.loc[stock_id]
        t_sector = target['sector']
        t_industry = target['industry']
        mask = (tv_map['sector'] == t_sector) & (tv_map['industry'] == t_industry)
        peer_ids = tv_map[mask].index.tolist()
        label = f"{t_sector} / {t_industry}"
        return peer_ids, label, 'tv'

    # 3. FinMind fallback
    target_row = info[info['stock_id'] == stock_id]
    if target_row.empty:
        return [], '', 'none'
    industry = target_row.iloc[0].get('industry_category', '')
    peer_ids = info[info['industry_category'] == industry]['stock_id'].tolist()
    return peer_ids, industry, 'finmind'


def get_tw_peer_comparison(stock_id, max_peers=10):
    """
    Get peer comparison for a Taiwan stock.

    Returns:
        dict: {
            'industry': str,
            'industry_source': 'manual' | 'tv' | 'finmind',
            'target': dict (PE/PB/DY for target stock),
            'peers': DataFrame (peer stocks with PE/PB/DY),
            'rank': dict (target's rank within peers),
        }
    """
    stock_id = str(stock_id).replace('.TW', '').replace('.TWO', '').strip()
    cache_key = f"peer_tw_{stock_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # 1. Load FinMind info (for stock names + fallback)
    try:
        from cache_manager import get_finmind_loader
        dl = get_finmind_loader()
        info = dl.taiwan_stock_info()
    except Exception as e:
        logger.warning("FinMind stock info failed: %s", e)
        return None

    # 2. Resolve peer ids via priority chain (manual → TV → FinMind)
    peer_ids, industry, source = _get_peer_ids_and_label(stock_id, info)
    if not peer_ids:
        logger.warning("No peer group found for %s", stock_id)
        return None

    peer_names = dict(zip(info['stock_id'], info['stock_name']))

    # 3. Get PER data for all TWSE + TPEX stocks
    per_data = _fetch_all_per_data()
    if per_data is None or per_data.empty:
        logger.warning("Failed to fetch PER data")
        return None

    # 4. Filter to peers only
    peer_per = per_data[per_data['stock_id'].isin(peer_ids)].copy()
    if peer_per.empty:
        return None

    peer_per['name'] = peer_per['stock_id'].map(peer_names)

    # Filter out invalid PE (0 or negative usually means loss-making)
    peer_per = peer_per[peer_per['PE'] > 0].copy()

    # Sort by PE for ranking
    peer_per.sort_values('PE', inplace=True)
    peer_per.reset_index(drop=True, inplace=True)

    # 5. Find target stock data
    target = peer_per[peer_per['stock_id'] == stock_id]
    target_data = target.iloc[0].to_dict() if not target.empty else None

    # 6. Compute ranks
    rank = {}
    if target_data:
        pe_rank = peer_per[peer_per['PE'] <= target_data['PE']].shape[0]
        _sorted_pb = peer_per.sort_values('PB').reset_index(drop=True)
        pb_rank = _sorted_pb[_sorted_pb['PB'] <= target_data['PB']].shape[0]
        rank = {
            'pe_rank': pe_rank,
            'pb_rank': pb_rank,
            'total_peers': len(peer_per),
            'pe_percentile': round(pe_rank / len(peer_per) * 100, 1),
        }

    # 7. Select representative peers: top/bottom PE + similar PE
    selected = _select_representative_peers(peer_per, stock_id, max_peers)

    result = {
        'industry': industry,
        'industry_source': source,
        'target': target_data,
        'peers': selected,
        'rank': rank,
        'total_in_industry': len(peer_per),
    }

    _cache_set(cache_key, result)
    return result


def get_us_peer_comparison(ticker, max_peers=8):
    """
    Get peer comparison for a US stock using Finviz sector data.

    Returns:
        dict similar to tw version
    """
    cache_key = f"peer_us_{ticker}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        from finviz_data import FinvizAnalyzer
        fv = FinvizAnalyzer()
        target_data, _ = fv.get_stock_data(ticker)
        if not target_data:
            return None

        overview = target_data.get('overview', {})
        industry = overview.get('industry', '')
        sector = overview.get('sector', '')
        valuation = target_data.get('valuation', {})

        if not industry:
            return None

        # Finviz doesn't have a bulk peer API, so we use yfinance
        import yfinance as yf
        info = yf.Ticker(ticker).info
        peers_list = info.get('comprisonPeers', []) or info.get('recommendationKey', [])

        # Simple approach: use known sector ETF constituents or just report the target
        result = {
            'industry': f"{sector} / {industry}",
            'target': {
                'stock_id': ticker,
                'PE': valuation.get('pe', 0),
                'PB': valuation.get('pb', 0),
                'DY': valuation.get('dividend_yield', 0),
                'forward_pe': valuation.get('forward_pe', 0),
                'peg': valuation.get('peg', 0),
            },
            'peers': pd.DataFrame(),  # TODO: bulk peer data
            'rank': {},
            'total_in_industry': 0,
        }

        _cache_set(cache_key, result)
        return result

    except Exception as e:
        logger.warning("US peer comparison failed: %s", e)
        return None


def _fetch_all_per_data():
    """Fetch PE/PB/DY for all TWSE + TPEX stocks."""
    cache_key = "all_per_data"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    import requests

    all_rows = []
    session = requests.Session()
    session.verify = False
    session.headers.update({'User-Agent': 'Mozilla/5.0'})

    # TWSE
    try:
        from twse_api import TWSEOpenData
        api = TWSEOpenData()
        dates = api._get_recent_trading_dates(days=3)
        for dt in dates:
            date_str = api._to_twse_date(dt)
            url = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_ALL"
            params = {'date': date_str, 'response': 'json'}
            api._throttle()
            resp = session.get(url, params=params, timeout=15)
            data = resp.json()
            if data.get('stat') == 'OK' and data.get('data'):
                for r in data['data']:
                    try:
                        sid = str(r[0]).strip()
                        if not sid.isdigit():
                            continue
                        pe = float(str(r[2]).replace(',', '')) if r[2] and r[2] != '-' else 0
                        dy = float(str(r[3]).replace(',', '')) if r[3] and r[3] != '-' else 0
                        pb = float(str(r[4]).replace(',', '')) if r[4] and r[4] != '-' else 0
                        all_rows.append({'stock_id': sid, 'PE': pe, 'PB': pb, 'DY': dy})
                    except (ValueError, IndexError):
                        continue
                break  # Got data for one date
    except Exception as e:
        logger.warning("TWSE PER fetch failed: %s", e)

    # TPEX
    try:
        from twse_api import TWSEOpenData
        api = TWSEOpenData()
        dates = api._get_recent_trading_dates(days=3)
        for dt in dates:
            roc_date = api._to_tpex_date(dt)
            url = "https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php"
            params = {'l': 'zh-tw', 'o': 'json', 'd': roc_date}
            api._throttle()
            resp = session.get(url, params=params, timeout=15)
            data = resp.json()
            tables = data.get('tables', [])
            if tables and isinstance(tables[0], dict):
                rows = tables[0].get('data', [])
                if rows:
                    for r in rows:
                        try:
                            sid = str(r[0]).strip()
                            if not sid.isdigit():
                                continue
                            pe = float(str(r[2]).replace(',', '')) if r[2] and r[2] != '-' else 0
                            dy = float(str(r[5]).replace(',', '')) if len(r) > 5 and r[5] and r[5] != '-' else 0
                            pb = float(str(r[6]).replace(',', '')) if len(r) > 6 and r[6] and r[6] != '-' else 0
                            all_rows.append({'stock_id': sid, 'PE': pe, 'PB': pb, 'DY': dy})
                        except (ValueError, IndexError):
                            continue
                    break
    except Exception as e:
        logger.warning("TPEX PER fetch failed: %s", e)

    if not all_rows:
        return None

    df = pd.DataFrame(all_rows)
    _cache_set(cache_key, df)
    logger.info("Fetched PER data for %d stocks", len(df))
    return df


def _get_tw_market_caps():
    """Get {stock_id: market_cap} from MomentumScreener TV cache (1h cache)."""
    try:
        from momentum_screener import MomentumScreener
        tv_data = MomentumScreener._fetch_tv_marketcap_volume() or {}
        return {sid: d.get('market_cap', 0) or 0 for sid, d in tv_data.items()}
    except Exception as e:
        logger.warning("TW market cap lookup failed: %s", e)
        return {}


def _select_representative_peers(peer_per, stock_id, max_peers, market_caps=None):
    """Phase 1c (2026-04-22): Market-cap aware peer selection.

    當 industry 大 (>max_peers) 時，舊邏輯只挑 PE 極端 + 鄰近，會漏掉「核心同業標竿」
    （例：2454 聯發科在半導體 140 檔裡，舊邏輯可能挑到無名 small-cap 而非 2330/3034）。

    新邏輯按優先級填入 max_peers 個 slot：
      P1: target stock 自己（必含）
      P2: 市值 top 3 (核心同業標竿)
      P3: PE 鄰近 ±2 (直接估值對比)
      P4: PE 最低 + 最高 (估值區間)
      P5: 均勻採樣填補剩餘空位
    """
    if len(peer_per) <= max_peers:
        selected = peer_per.copy()
        selected['is_target'] = selected['stock_id'] == stock_id
        return selected

    target_idx = peer_per[peer_per['stock_id'] == stock_id].index
    if target_idx.empty:
        return peer_per.head(max_peers)

    tidx = target_idx[0]
    n = len(peer_per)

    if market_caps is None:
        market_caps = _get_tw_market_caps()

    indices_priority = [tidx]  # P1: target

    # P2: Top 3 by market cap (excluding target)
    if market_caps:
        peer_mcap = peer_per.assign(
            _mcap=peer_per['stock_id'].map(market_caps).fillna(0)
        )
        top_mcap = peer_mcap.nlargest(4, '_mcap')  # nlargest 4 in case target is #1
        for idx in top_mcap.index:
            if idx != tidx and idx not in indices_priority:
                indices_priority.append(idx)
                if len(indices_priority) >= 4:  # target + 3 mcap
                    break

    # P3: PE neighbors (±2)
    for offset in [-2, -1, 1, 2]:
        idx = tidx + offset
        if 0 <= idx < n and idx not in indices_priority:
            indices_priority.append(idx)

    # P4: PE extremes (lowest + highest)
    for idx in [0, n - 1]:
        if idx not in indices_priority:
            indices_priority.append(idx)

    # P5: evenly spaced fill
    if len(indices_priority) < max_peers:
        for step in np.linspace(0, n - 1, max_peers * 2, dtype=int):
            if int(step) not in indices_priority:
                indices_priority.append(int(step))
            if len(indices_priority) >= max_peers:
                break

    selected_idx = sorted(indices_priority[:max_peers])
    selected = peer_per.iloc[selected_idx].copy()
    selected['is_target'] = selected['stock_id'] == stock_id
    return selected


def format_peer_comparison(result):
    """Format peer comparison for AI report prompt."""
    if not result:
        return "N/A (peer data unavailable)"

    lines = []
    src = result.get('industry_source', '')
    src_tag = f" [{src}]" if src else ""
    lines.append(f"Industry: {result['industry']}{src_tag}")
    lines.append(f"Total peers: {result['total_in_industry']}")

    target = result.get('target')
    if target:
        lines.append(f"\nTarget: {target.get('stock_id', '')} "
                      f"PE={target.get('PE', 0):.1f} PB={target.get('PB', 0):.2f} DY={target.get('DY', 0):.1f}%")

    rank = result.get('rank', {})
    if rank:
        lines.append(f"PE rank: {rank.get('pe_rank', '?')}/{rank.get('total_peers', '?')} "
                      f"(percentile: {rank.get('pe_percentile', '?')}%)")

    peers = result.get('peers')
    if peers is not None and not peers.empty:
        lines.append(f"\nPeer Comparison (sorted by PE):")
        lines.append(f"{'ID':>6} {'Name':<8} {'PE':>8} {'PB':>8} {'DY%':>8} {'Note'}")
        for _, row in peers.iterrows():
            mark = ' <<<' if row.get('is_target', False) else ''
            name = str(row.get('name', ''))[:8]
            lines.append(f"{row['stock_id']:>6} {name:<8} {row['PE']:>8.1f} {row['PB']:>8.2f} {row['DY']:>8.1f}{mark}")

    return "\n".join(lines)
