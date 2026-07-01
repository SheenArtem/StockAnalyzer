"""
投資組合 view — 💼 投資組合 tab (2026-07-01)

仿 TradingView 投組：手動輸入逐筆交易（st.dialog 彈窗，日期預填今天）->
推導持股 + 即時損益 + 分市場彙總。純本地檔案（data/manual_trades/transactions.json）。

資料/計算層在 portfolio_store（可測）、報價層在 portfolio_pricing（複用 mis.twse /
Yahoo v8 / load_and_resample）。本檔只負責 Streamlit UI 與 session_state 快取。

配色依台股慣例（memory feedback_report_color_convention）：紅=漲/正、綠=跌/負。
"""
import logging
from datetime import date, datetime

import pandas as pd
import streamlit as st

import mis_twse_client
import portfolio_pricing as pp
import portfolio_store as ps

logger = logging.getLogger(__name__)

# 紅漲綠跌
_UP = '#ff4b4b'
_DOWN = '#26a69a'
_MARKET_LABEL = {'tw': '台股 (TWD)', 'us': '美股 (USD)'}
# 各市場的自然對標指數（NAV 疊圖比較用）
_BENCHMARK = {'us': ('^GSPC', 'S&P 500'), 'tw': ('^TWII', '加權指數')}


def _color_signed(v):
    """Styler：正紅、負綠、其餘無色。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ''
    if v > 0:
        return f'color: {_UP}'
    if v < 0:
        return f'color: {_DOWN}'
    return ''


def _is_na(v):
    return v is None or (isinstance(v, float) and pd.isna(v))


def _fmt_money(v):
    """金額：千分位 + 2 位小數（保留分/角，勿四捨五入成整數）。"""
    return '—' if _is_na(v) else f'{v:,.2f}'


def _fmt_money_signed(v):
    return '—' if _is_na(v) else f'{v:+,.2f}'


def _fmt_shares(v):
    """股數：整數不顯示小數；有零股才顯示（最多 4 位）。"""
    if _is_na(v):
        return '—'
    return f'{v:,.0f}' if float(v).is_integer() else f'{v:,.4f}'


def _fmt_price_raw(v):
    """原始成交價：保留輸入精度（最多 4 位、去尾零），勿損失低價股小數。"""
    if _is_na(v):
        return '—'
    return f'{v:,.4f}'.rstrip('0').rstrip('.')


# ====================================================================
#  價格快取（避免每次 rerun 重打 API；live 尤其重要）
# ====================================================================

def _load_prices(tickers, live, force):
    key = (tuple(sorted(set(tickers))), bool(live))
    cache = st.session_state.get('_pf_price_cache')
    if not force and cache and cache.get('key') == key:
        return cache['prices'], cache['ts']
    prices = pp.get_current_prices(list(tickers), live=live) if tickers else {}
    ts = datetime.now()
    st.session_state['_pf_price_cache'] = {'key': key, 'prices': prices, 'ts': ts}
    return prices, ts


def _load_history(tickers):
    """歷史收盤 Series，per-ticker session 快取（只抓沒抓過的，抓不到記 None 不重抓）。
    回 {ticker: pandas.Series}（只含抓到的）。YTD 與 NAV 共用，避免不同 ticker set 互相洗快取。"""
    want = list(dict.fromkeys(t for t in tickers if t))
    cache = st.session_state.setdefault('_pf_history', {})
    missing = [t for t in want if t not in cache]
    if missing:
        with st.spinner(f"載入 {len(missing)} 檔歷史價（算 YTD / 淨值曲線）…"):
            fetched = pp.get_price_history(missing)
        for t in missing:
            cache[t] = fetched.get(t)   # None = 抓不到，記錄避免每次 rerun 重抓
    return {t: cache[t] for t in want if cache.get(t) is not None}


def _portfolio_metrics(ret_series):
    """複用 whale_picks_phase2.portfolio_metrics（tools/，日頻 freq=252）。"""
    import sys
    from pathlib import Path
    tools = str(Path(__file__).resolve().parent / 'tools')
    if tools not in sys.path:
        sys.path.insert(0, tools)
    try:
        from whale_picks_phase2 import portfolio_metrics
        return portfolio_metrics(ret_series, freq=252)
    except Exception as e:  # 指標算不出不擋 NAV 曲線
        logger.warning("portfolio_metrics failed: %s", e)
        return {}


# ====================================================================
#  交易輸入彈窗（st.dialog）
# ====================================================================

def _txn_form(edit_txn=None):
    """交易輸入表單（新增 / 編輯共用）。edit_txn=None 為新增。"""
    is_edit = edit_txn is not None
    st.caption("代號：台股純數字（2330）/ 美股英文（AAPL）。日期預設今天。")
    ticker = st.text_input("代號", value=(edit_txn['ticker'] if is_edit else ''),
                           key='pf_dlg_ticker', placeholder="2330 或 AAPL")
    c1, c2 = st.columns(2)
    action = c1.radio("動作", options=['buy', 'sell'], horizontal=True,
                      index=(1 if is_edit and edit_txn['action'] == 'sell' else 0),
                      format_func=lambda a: '買進' if a == 'buy' else '賣出',
                      key='pf_dlg_action')
    _def_date = date.fromisoformat(edit_txn['date']) if is_edit else date.today()
    txn_date = c2.date_input("日期", value=_def_date, key='pf_dlg_date')
    c3, c4 = st.columns(2)
    shares = c3.number_input("股數", min_value=0.0, step=1000.0,
                             value=float(edit_txn['shares']) if is_edit else 1000.0,
                             key='pf_dlg_shares', help="含零股；台股 1 張 = 1000 股")
    price = c4.number_input("成交價", min_value=0.0, step=0.5,
                            value=float(edit_txn['price']) if is_edit else 0.0,
                            format="%.4f", key='pf_dlg_price')
    auto = st.checkbox("自動估算台股費用（手續費 0.1425% + 賣出證交稅 0.3%）",
                       value=False, key='pf_dlg_auto',
                       help="勾選則忽略下方手動費用、依成交金額估算（美股不適用）")
    c5, c6 = st.columns(2)
    fee = c5.number_input("手續費", min_value=0.0, step=1.0,
                          value=float(edit_txn.get('fee', 0)) if is_edit else 0.0,
                          key='pf_dlg_fee')
    tax = c6.number_input("交易稅", min_value=0.0, step=1.0,
                          value=float(edit_txn.get('tax', 0)) if is_edit else 0.0,
                          key='pf_dlg_tax', help="台股賣出證交稅 0.3%（買進填 0）")
    note = st.text_input("備註", value=(edit_txn.get('note', '') if is_edit else ''),
                         key='pf_dlg_note', placeholder="（選填）進場理由 / 標記")

    if st.button("💾 更新" if is_edit else "💾 儲存", type='primary', key='pf_dlg_save'):
        if auto and ps.detect_market(ticker) == 'tw':
            fee, tax = ps.estimate_tw_costs(shares, price, action)
        ok, err = ps.validate_transaction(ticker, action, txn_date, shares, price, fee, tax)
        if not ok:
            st.error(err)
            return
        if is_edit:
            ps.update_transaction(edit_txn['id'], ticker=ticker, action=action,
                                  date=txn_date.isoformat(), shares=shares, price=price,
                                  fee=fee, tax=tax, note=note)
        else:
            ps.add_transaction(ticker, action, txn_date, shares, price,
                               fee=fee, tax=tax, note=note)
        st.session_state.pop('_pf_price_cache', None)   # 持股變了，清快取
        st.session_state.pop('_pf_history', None)
        st.rerun()


@st.dialog("➕ 新增交易")
def _add_txn_dialog():
    _txn_form(None)


@st.dialog("✏️ 編輯交易")
def _edit_txn_dialog(txn):
    _txn_form(txn)


# ====================================================================
#  彙總 metrics + 持股表
# ====================================================================

def _render_summary(by_market, day_pnl_by_market):
    cols = st.columns(len(by_market)) if by_market else []
    for col, (mkt, s) in zip(cols, by_market.items()):
        with col:
            st.markdown(f"##### {_MARKET_LABEL.get(mkt, mkt)}")
            mv = s['market_value']
            unreal = s['unrealized_pnl']
            ret = s['return_pct']
            m1, m2 = st.columns(2)
            m1.metric("總市值", f"{mv:,.2f}")
            m2.metric("未實現損益", f"{unreal:+,.2f}",
                      delta=(f"{ret:+.2%}" if ret is not None else None),
                      delta_color='inverse')  # 台股慣例紅漲
            m3, m4 = st.columns(2)
            day = day_pnl_by_market.get(mkt)
            m3.metric("當日損益", f"{day:+,.2f}" if day is not None else "—")
            m4.metric("已實現損益", f"{s['realized_pnl']:+,.2f}")
            if s['has_missing_price']:
                st.caption("⚠️ 部分持股缺現價，市值/損益未計入該檔")


def _holdings_table(valued, quotes, market, ytd_map):
    rows = []
    total_mv = sum(r['market_value'] for r in valued
                   if r['market'] == market and r['market_value'] is not None) or 0.0
    for r in valued:
        if r['market'] != market:
            continue
        q = quotes.get(r['ticker'], {})
        mv = r['market_value']
        rows.append({
            '代號': r['ticker'],
            '名稱': q.get('name') or '',
            '股數': r['shares'],
            '均價': r['avg_cost'],
            '現價': r['current_price'],
            '當日%': q.get('change_pct'),
            '市值': mv,
            '未實現損益': r['unrealized_pnl'],
            'YTD%': ytd_map.get(r['ticker']),         # 今年以來（基準=去年末收盤）
            '總報酬率%': r['return_pct'],               # 相對持有均價的累積報酬
            '權重%': (mv / total_mv) if (mv is not None and total_mv > 0) else None,
        })
    if not rows:
        return
    df = pd.DataFrame(rows)
    sty = (df.style
           .format({'股數': _fmt_shares, '均價': '{:,.2f}', '現價': '{:,.2f}',
                    '當日%': '{:+.2%}', '市值': _fmt_money, '未實現損益': _fmt_money_signed,
                    'YTD%': '{:+.2%}', '總報酬率%': '{:+.2%}', '權重%': '{:.1%}'}, na_rep='—')
           .map(_color_signed, subset=['當日%', '未實現損益', 'YTD%', '總報酬率%']))
    st.dataframe(sty, hide_index=True, use_container_width=True)


def _closed_table(closed_rows):
    df = pd.DataFrame([{
        '代號': c['ticker'],
        '建倉日': c['entry_date'],
        '出場日': c['exit_date'],
        '持有天數': c['holding_days'],
        '股數': c['shares'],
        '買均價': c['avg_buy'],
        '賣均價': c['avg_sell'],
        '已實現損益': c['realized_pnl'],
        '報酬率%': c['return_pct'],
    } for c in sorted(closed_rows, key=lambda x: x['exit_date'], reverse=True)])
    sty = (df.style
           .format({'股數': _fmt_shares, '買均價': '{:,.2f}', '賣均價': '{:,.2f}',
                    '已實現損益': _fmt_money_signed, '報酬率%': '{:+.2%}'}, na_rep='—')
           .map(_color_signed, subset=['已實現損益', '報酬率%']))
    st.dataframe(sty, hide_index=True, use_container_width=True)


# ====================================================================
#  交易紀錄清單（含刪除）
# ====================================================================

def _render_history(txns):
    st.markdown("#### 📜 交易紀錄")
    if not txns:
        st.caption("（還沒有交易紀錄，點上方「新增交易」開始）")
        return
    ordered = sorted(txns, key=lambda t: (t.get('date', ''), t.get('created_at', '')),
                     reverse=True)
    disp = pd.DataFrame([{
        '日期': t['date'],
        '代號': t['ticker'],
        '動作': '買進' if t['action'] == 'buy' else '賣出',
        '股數': t['shares'],
        '成交價': t['price'],
        '手續費': t.get('fee', 0),
        '交易稅': t.get('tax', 0),
        '備註': t.get('note', ''),
    } for t in ordered])
    st.dataframe(
        disp.style.format({'股數': _fmt_shares, '成交價': _fmt_price_raw,
                           '手續費': _fmt_money, '交易稅': _fmt_money}),
        hide_index=True, use_container_width=True)

    # 刪除單筆（明確選取，低風險）
    labels = {t['id']: f"{t['date']}  {t['ticker']}  "
                       f"{'買' if t['action'] == 'buy' else '賣'} "
                       f"{t['shares']:g} @ {t['price']:g}"
              for t in ordered}
    c1, c2, c3 = st.columns([3, 1, 1])
    sel = c1.selectbox("選擇交易", options=list(labels), index=None,
                       format_func=lambda i: labels[i],
                       placeholder="選一筆交易編輯 / 刪除…", label_visibility='collapsed')
    if c2.button("✏️ 編輯", disabled=(sel is None), use_container_width=True):
        _edit_txn_dialog(next(t for t in ordered if t['id'] == sel))
    if c3.button("🗑 刪除", disabled=(sel is None), use_container_width=True):
        ps.delete_transaction(sel)
        st.session_state.pop('_pf_price_cache', None)
        st.session_state.pop('_pf_history', None)
        st.rerun()


# ====================================================================
#  績效 / 淨值曲線（TWR）
# ====================================================================

def _render_performance(txns):
    all_tickers = sorted({t['ticker'] for t in txns})
    markets_present = [m for m in ('tw', 'us')
                       if any((t.get('market') or ps.detect_market(t['ticker'])) == m for t in txns)]
    bench_syms = [_BENCHMARK[m][0] for m in markets_present]
    history = _load_history(all_tickers + bench_syms)   # 併抓 benchmark；內含 spinner
    year = date.today().year
    shown = False
    for mkt in markets_present:
        nav_df = ps.build_nav_series(txns, history, mkt)
        if nav_df.empty or len(nav_df) < 2:
            continue
        shown = True
        st.markdown(f"##### {_MARKET_LABEL[mkt]}")
        base = ps.ytd_baseline(nav_df['nav'], year)
        ytd = (nav_df['nav'].iloc[-1] / base - 1.0) if base else None
        m = _portfolio_metrics(nav_df['ret'])
        if m:
            cc = st.columns(6)
            cc[0].metric("總報酬", f"{m['total_return']:+.2%}")
            cc[1].metric("YTD", f"{ytd:+.2%}" if ytd is not None else "—")
            cc[2].metric("年化 CAGR", f"{m['cagr']:+.2%}")
            cc[3].metric("Sharpe", f"{m['sharpe']:.2f}")
            cc[4].metric("最大回撤", f"{m['mdd']:.2%}")
            cc[5].metric("日勝率", f"{m['win_rate']:.1%}")
        else:
            _ytd_txt = f" · YTD {ytd:+.2%}" if ytd is not None else ""
            st.caption(f"總報酬 {(nav_df['nav'].iloc[-1] - 1):+.2%}{_ytd_txt}"
                       "（交易日 < 6，年化/Sharpe 從略）")

        # 投組 NAV vs benchmark（皆正規化到起點 1.0）
        bench_sym, bench_name = _BENCHMARK[mkt]
        chart_df = pd.DataFrame({'投組': nav_df['nav']})
        b_total = None
        bser = history.get(bench_sym)
        if bser is not None and len(bser):
            b = bser.reindex(nav_df.index, method='ffill').bfill()
            if b.notna().any() and b.iloc[0]:
                chart_df[bench_name] = b / b.iloc[0]
                b_total = float(b.iloc[-1] / b.iloc[0] - 1.0)
        st.line_chart(chart_df, height=240)
        if b_total is not None:
            p_total = float(nav_df['nav'].iloc[-1] - 1.0)
            st.caption(f"同期 {bench_name}：{b_total:+.2%} · 投組超額報酬 (alpha)：{p_total - b_total:+.2%}")
        else:
            st.caption(f"（{bench_name} 同期資料抓取失敗，僅顯示投組曲線）")

    if not shown:
        st.caption("尚無足夠歷史可繪製淨值曲線（需至少 2 個交易日）。")


# ====================================================================
#  主 render
# ====================================================================

def render_portfolio():
    st.subheader("💼 投資組合")

    # ---- 頂部：新增交易 / 更新即時價 ----
    c1, c2, c3 = st.columns([1.2, 1.2, 4])
    if c1.button("➕ 新增交易", use_container_width=True):
        _add_txn_dialog()
    tw_hours = mis_twse_client.is_tw_trading_hours()
    if c2.button("🔄 更新即時價", use_container_width=True,
                 help="台股盤中抓 mis.twse 即時、美股抓 Yahoo（~15min 延遲）；"
                      "非交易時段台股用最近收盤"):
        st.session_state['_pf_live'] = True
        st.session_state['_pf_force_price'] = True
        st.rerun()

    # ---- 讀交易（毀損 fail-loud）----
    try:
        txns = ps.load_transactions()
    except ValueError as e:
        st.error(f"交易紀錄檔讀取失敗：{e}")
        return

    if not txns:
        st.info("點「➕ 新增交易」建立第一筆交易紀錄。系統會自動推導持股、"
                "以即時/收盤價顯示損益與績效。")
        return

    # ---- 推導持股（賣超 fail-loud，仍顯示交易讓使用者修正）----
    try:
        holdings = ps.derive_holdings(txns)
    except ValueError as e:
        st.error(f"持股推導失敗：{e}")
        st.caption("請到下方交易紀錄刪除或修正有問題的交易。")
        _render_history(txns)
        return

    open_pos = ps.open_positions(holdings)
    open_tickers = [r['ticker'] for r in open_pos]

    # ---- 報價（session 快取）----
    live = st.session_state.get('_pf_live', False)
    force = st.session_state.pop('_pf_force_price', False)
    quotes, price_ts = _load_prices(open_tickers, live=live, force=force)
    price_map = {t: q['price'] for t, q in quotes.items() if q.get('price') is not None}

    # 報價來源說明
    srcs = {q.get('source') for q in quotes.values()}
    mode_txt = ("即時（台股 mis.twse / 美股 Yahoo 延遲）" if live else "最近收盤（EOD）")
    live_note = "" if tw_hours or not live else "（台股非交易時段，顯示最近收盤）"
    st.caption(f"報價：{mode_txt}{live_note} · 來源 {', '.join(sorted(s for s in srcs if s))} "
               f"· 更新 {price_ts.strftime('%H:%M:%S')}")

    # ---- 彙總 + 當日損益 ----
    by_market, valued = ps.summarize(holdings, price_map)
    day_pnl_by_market = {}
    for r in valued:
        q = quotes.get(r['ticker'], {})
        prev = q.get('prev_close')
        if r['current_price'] is not None and prev not in (None, 0):
            day_pnl_by_market[r['market']] = day_pnl_by_market.get(r['market'], 0.0) \
                + r['shares'] * (r['current_price'] - prev)

    # ---- YTD%（個人今年報酬：今年建倉從建倉均價起算、跨年持有從今年 1/1 起算）----
    # 只有跨年持股才需個股歷史 -> 全今年建倉的投組主頁不必載歷史（維持秒開）。
    _yr = date.today().year
    ytd_map = {}
    if open_pos:
        _cross_year = [p['ticker'] for p in open_pos
                       if not p.get('entry_date') or p['entry_date'][:4] < str(_yr)]
        _hist = _load_history(_cross_year) if _cross_year else {}
        for p in open_pos:
            ytd_map[p['ticker']] = ps.position_ytd(
                p.get('entry_date'), p['avg_cost'], price_map.get(p['ticker']),
                _hist.get(p['ticker']), _yr)

    if open_pos:
        _render_summary(by_market, day_pnl_by_market)
        st.markdown("---")
        # ---- 分市場持股表 ----
        for mkt in ('tw', 'us'):
            if any(r['market'] == mkt for r in valued):
                st.markdown(f"#### {_MARKET_LABEL[mkt]}")
                _holdings_table(valued, quotes, mkt, ytd_map)
    else:
        st.info("目前無未平倉持股（已全數出場）。下方為交易紀錄與已實現損益。")
        # 已實現損益彙總
        rl = {m: s['realized_pnl'] for m, s in by_market.items()}
        if rl:
            cols = st.columns(len(rl))
            for col, (m, v) in zip(cols, rl.items()):
                col.metric(f"{_MARKET_LABEL.get(m, m)} 已實現損益", f"{v:+,.2f}")

    # ---- 已清倉損益（已實現 round-trip 明細）----
    closed = ps.closed_positions(txns)
    if closed:
        st.markdown("---")
        st.markdown(f"#### 📕 已清倉損益（已實現 · {len(closed)} 筆）")
        for mkt in ('tw', 'us'):
            mkt_closed = [c for c in closed if c['market'] == mkt]
            if mkt_closed:
                st.markdown(f"##### {_MARKET_LABEL[mkt]}")
                _closed_table(mkt_closed)

    # ---- 績效 / 淨值曲線（按鈕觸發，避免歷史回補拖慢主頁）----
    st.markdown("---")
    if st.button("📈 載入 / 更新績效曲線 (TWR)"):
        st.session_state['_pf_show_perf'] = True
        st.session_state.pop('_pf_history', None)
        st.rerun()
    if st.session_state.get('_pf_show_perf'):
        _render_performance(txns)

    st.markdown("---")
    _render_history(txns)
