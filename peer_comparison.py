"""
同業比較模組 — 找同產業公司，比較估值/獲利指標

台股: FinMind 產業分類 + TWSE/TPEX PER 數據
美股: Finviz sector/industry
"""

import logging
import time

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_CACHE = {}
_CACHE_TTL = 3600  # 1 hour


def _cache_get(key):
    if key in _CACHE:
        data, ts = _CACHE[key]
        if time.time() - ts < _CACHE_TTL:
            return data
    return None


def _cache_set(key, data):
    _CACHE[key] = (data, time.time())


def get_tw_peer_comparison(stock_id, max_peers=10):
    """
    Get peer comparison for a Taiwan stock.

    Returns:
        dict: {
            'industry': str,
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

    # 1. Get industry classification from FinMind
    try:
        from cache_manager import get_finmind_loader
        dl = get_finmind_loader()
        info = dl.taiwan_stock_info()
    except Exception as e:
        logger.warning("FinMind stock info failed: %s", e)
        return None

    target_row = info[info['stock_id'] == stock_id]
    if target_row.empty:
        logger.warning("Stock %s not found in FinMind info", stock_id)
        return None

    industry = target_row.iloc[0].get('industry_category', '')
    market_type = target_row.iloc[0].get('type', '')  # 'twse' or 'tpex'

    # 2. Get all stocks in same industry
    peer_ids = info[info['industry_category'] == industry]['stock_id'].tolist()
    peer_names = dict(zip(
        info[info['industry_category'] == industry]['stock_id'],
        info[info['industry_category'] == industry]['stock_name'],
    ))

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


def _select_representative_peers(peer_per, stock_id, max_peers):
    """Select representative peers: lowest PE, highest PE, and around target."""
    if len(peer_per) <= max_peers:
        return peer_per

    target_idx = peer_per[peer_per['stock_id'] == stock_id].index
    if target_idx.empty:
        return peer_per.head(max_peers)

    tidx = target_idx[0]
    n = len(peer_per)

    # Always include: top 2 lowest PE, top 2 highest PE, target, and neighbors
    indices = set()
    indices.update(range(min(2, n)))  # lowest PE
    indices.update(range(max(0, n - 2), n))  # highest PE
    indices.add(tidx)  # target itself

    # Add neighbors around target
    for offset in [-2, -1, 1, 2]:
        idx = tidx + offset
        if 0 <= idx < n:
            indices.add(idx)

    # Fill remaining slots with evenly spaced stocks
    while len(indices) < max_peers and len(indices) < n:
        for step in np.linspace(0, n - 1, max_peers, dtype=int):
            indices.add(step)
            if len(indices) >= max_peers:
                break

    selected = peer_per.iloc[sorted(indices)].copy()
    # Mark target stock
    selected['is_target'] = selected['stock_id'] == stock_id
    return selected


def format_peer_comparison(result):
    """Format peer comparison for AI report prompt."""
    if not result:
        return "N/A (peer data unavailable)"

    lines = []
    lines.append(f"Industry: {result['industry']}")
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
