"""Whale Picks tab — 今日 BUY/SELL 訊號 + 歷史回測表格 + Top-K 持倉清單。

訊號定義 (per docs/whale_picks_spec.md v0.5)：
  - BUY: 月底 rebalance 新進 top-10 / alerts 模組偵測 7d 急升 rank
  - SELL: 月底 rebalance 掉出 top-10 / alerts 偵測持倉 ≥ -15% drawdown

⚠️ SPEC §13 紅線：永遠不接自動下單；訊號展示用，下單由人類判斷。
"""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

REPO = Path(__file__).resolve().parent
JSON_PATH = REPO / "data" / "latest" / "whale_picks_top20.json"
SNAPSHOT_DIR = REPO / "data" / "whale_picks"
HOLDINGS_PATH = SNAPSHOT_DIR / "_active_holdings.json"
LEDGER_PATH = SNAPSHOT_DIR / "trade_ledger.parquet"
LEDGER_META_PATH = SNAPSHOT_DIR / "trade_ledger_meta.json"


def _load_latest_json() -> dict | None:
    if not JSON_PATH.exists():
        return None
    try:
        return json.loads(JSON_PATH.read_text(encoding='utf-8'))
    except Exception as e:
        st.error(f"無法讀取 {JSON_PATH}: {e}")
        return None


def _load_active_holdings() -> dict | None:
    if not HOLDINGS_PATH.exists():
        return None
    try:
        return json.loads(HOLDINGS_PATH.read_text(encoding='utf-8'))
    except Exception:
        return None


def _list_snapshots() -> list[str]:
    if not SNAPSHOT_DIR.exists():
        return []
    return sorted([f.stem for f in SNAPSHOT_DIR.glob('20*.parquet')], reverse=True)


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_live_quote_cached(stock_id: str) -> dict | None:
    """mis.twse 即時報價 cache 5min (盤中即時 / 盤後 prev_close)。
    回 dict {'price', 'date', 'price_source', ...} 或 None。"""
    try:
        from mis_twse_client import get_quote
        return get_quote(stock_id)
    except Exception:
        return None


# =============================================================================
# Section 1 — 今日訊號 (BUY/SELL)
# =============================================================================

def _render_today_signals(obj: dict) -> None:
    """月底 rebalance 訊號 + 與上次 snapshot diff 的 BUY/SELL 列表。"""
    st.subheader("📡 今日訊號 (BUY / SELL)")
    st.caption(
        "BUY = 月底 rebalance 新進 top-10，或 alerts 模組 7d 急升 rank。"
        "SELL = 月底 rebalance 掉出 top-10，或 alerts 偵測持倉 -15% drawdown。"
        "**SPEC §13: 訊號展示用，永不自動下單。**"
    )

    holdings = _load_active_holdings()
    if holdings:
        rebal_date = holdings.get('rebalance_date', '?')
        reason = holdings.get('reason', '?')
        tickers = holdings.get('tickers', [])
        cA, cB, cC = st.columns(3)
        cA.metric("最後 rebalance", rebal_date)
        cB.metric("rebalance 觸發", reason)
        cC.metric("持倉檔數", len(tickers))

    snaps = _list_snapshots()
    if len(snaps) >= 2:
        latest_d, prev_d = snaps[0], snaps[1]
        try:
            snap_now = pd.read_parquet(SNAPSHOT_DIR / f"{latest_d}.parquet")
            snap_prev = pd.read_parquet(SNAPSHOT_DIR / f"{prev_d}.parquet")
            # 2026-05-16: composite_score 切為 default，但舊 snapshot (5e10f6e 前)
            # 只有 composite_parsi。各 snapshot 各自選可用的 score column 排序。
            def _pick_score_col(snap):
                return 'composite_score' if 'composite_score' in snap.columns else 'composite_parsi'
            score_now = _pick_score_col(snap_now)
            score_prev = _pick_score_col(snap_prev)
            top_now = snap_now.dropna(subset=[score_now]).nlargest(10, score_now)
            top_prev = snap_prev.dropna(subset=[score_prev]).nlargest(10, score_prev)
            ids_now = set(top_now['stock_id'])
            ids_prev = set(top_prev['stock_id'])
            buys = ids_now - ids_prev
            sells = ids_prev - ids_now
            holds = ids_now & ids_prev

            cBuy, cSell, cHold = st.columns(3)
            cBuy.metric("🟢 BUY 訊號", len(buys), help=f"從 {prev_d} → {latest_d} 新進 top-10")
            cSell.metric("🔴 SELL 訊號", len(sells), help=f"從 {prev_d} → {latest_d} 掉出 top-10")
            cHold.metric("⚪ 維持", len(holds))

            if buys:
                buy_rows = top_now[top_now['stock_id'].isin(buys)].copy()
                buy_rows['信號類型'] = '🟢 BUY (新進 top-10)'
                buy_rows = buy_rows[['信號類型', 'stock_id', 'stock_name',
                                      'industry_category', score_now, 'Close']].rename(
                    columns={'industry_category': '產業', score_now: '分數', 'Close': '當前收盤'}
                )
                buy_rows['分數'] = buy_rows['分數'].apply(lambda v: f"{v:+.2f}" if pd.notna(v) else "n/a")
                buy_rows['當前收盤'] = buy_rows['當前收盤'].apply(lambda v: f"{v:.2f}" if pd.notna(v) else "n/a")
                st.markdown("**🟢 今日 BUY 候選**")
                st.dataframe(buy_rows.reset_index(drop=True), use_container_width=True, hide_index=True)

            if sells:
                prev_lookup = top_prev.set_index('stock_id')
                sell_data = []
                for sid in sorted(sells):
                    if sid in prev_lookup.index:
                        r = prev_lookup.loc[sid]
                        sell_data.append({
                            '信號類型': '🔴 SELL (掉出 top-10)',
                            'stock_id': sid,
                            'stock_name': r.get('stock_name', '?'),
                            '產業': r.get('industry_category', ''),
                            '前次分數': f"{r[score_prev]:+.2f}" if pd.notna(r.get(score_prev)) else "n/a",
                            '前次收盤': f"{r['Close']:.2f}" if pd.notna(r.get('Close')) else "n/a",
                        })
                if sell_data:
                    st.markdown("**🔴 今日 SELL 候選**")
                    st.dataframe(pd.DataFrame(sell_data), use_container_width=True, hide_index=True)

            if not buys and not sells:
                st.info(f"與上次 snapshot ({prev_d}) 相比沒有新的 BUY/SELL 訊號。")
        except Exception as e:
            st.warning(f"BUY/SELL diff 計算失敗: {e}")
    else:
        st.info("尚未累積 2 份以上的歷史 snapshot — 等 daily scan 跑幾天後就有 BUY/SELL diff 訊號。")


# =============================================================================
# Section 2 — 今日 Top-10 持倉清單
# =============================================================================

def _render_current_holdings(obj: dict) -> None:
    """目前 Top-10 名單 — 視同 active BUY list。"""
    st.subheader("📋 今日 Top-10 持倉清單 (BUY List)")
    st.caption(f"月底 rebalance 之後 → 持有至下次月底 rebalance；資料生成於 {obj.get('asof', '?')}")

    top = obj.get('top', [])
    if not top:
        st.info("Top list 為空。")
        return

    df = pd.DataFrame(top)
    show_cols = ['stock_id', 'stock_name', 'industry_category', 'composite_score',
                 'f_score', 'eps_yoy', 'dist_52w_high', 'turnover_log',
                 'stealth_volume_20d', 'revenue_score_6m_delta',
                 'f_score_4q_delta', 'capex_intensity', 'Close']
    df_show = df[[c for c in show_cols if c in df.columns]].copy()
    df_show.index = range(1, len(df_show) + 1)
    df_show.index.name = '排名'

    if 'eps_yoy' in df_show.columns:
        df_show['eps_yoy'] = df_show['eps_yoy'].apply(lambda v: f"{v*100:+.1f}%" if pd.notna(v) else "n/a")
    for col in ['composite_score', 'f_score', 'dist_52w_high', 'turnover_log',
                'stealth_volume_20d', 'revenue_score_6m_delta',
                'f_score_4q_delta', 'capex_intensity']:
        if col in df_show.columns:
            df_show[col] = df_show[col].apply(lambda v: f"{v:+.2f}" if pd.notna(v) else "n/a")
    if 'Close' in df_show.columns:
        df_show['Close'] = df_show['Close'].apply(lambda v: f"{v:.2f}" if pd.notna(v) else "n/a")

    # 中文欄位 + 把標準化分數正負方向加進標題（負號因子加「(反向)」）
    df_show = df_show.rename(columns={
        'stock_id':               '股票代號',
        'stock_name':              '股票名稱',
        'industry_category':        '產業',
        'composite_score':          '綜合分數',
        'f_score':                  'F-Score (體質)',
        'eps_yoy':                  'EPS 年增率',
        'dist_52w_high':            '距 52 週高 (反向)',
        'turnover_log':             '成交值 log (反向)',
        'stealth_volume_20d':       '量縮爆量 (主力吸籌)',
        'revenue_score_6m_delta':   '營收 6 月改善',
        'f_score_4q_delta':         'F-Score 年增',
        'capex_intensity':          'Capex 強度 (反向)',
        'Close':                    '當前收盤',
    })

    st.dataframe(df_show, use_container_width=True)
    st.caption(
        "**(反向)** 標記的因子：分數越**高**反而**扣分**（例：成交值大 = 大型股不利、"
        "距 52 週高近 = 動能已盡、Capex 重 = 資本黑洞）。綜合分數已經是 7 因子加權後的結果，"
        "正分數 = 整體 favorable。"
    )


# =============================================================================
# Section 3a — 當前持倉即時損益 (Live Holdings PnL)
# =============================================================================

def _render_current_holdings_pnl() -> None:
    """當前 _active_holdings.json 對照即時報價算 PnL。"""
    holdings = _load_active_holdings()
    if not holdings or not holdings.get('tickers'):
        st.info("📭 尚未產生當前持倉清單 (`_active_holdings.json`)。等下次 M15 rebalance (每月 15 號或之前最後交易日) 或手動跑 `python tools/whale_picks_alerts.py --update-holdings`。")
        return

    rebalance_date = holdings.get('rebalance_date', '?')
    reason = holdings.get('reason', '?')
    tickers = holdings['tickers']

    st.subheader("💼 當前持倉即時損益")
    st.caption(
        f"上次 rebalance: **{rebalance_date}** ({reason}) / 持有 **{len(tickers)}** 檔。"
        f" 即時價走 mis.twse (盤中即時 / 盤後 prev_close)，cache 5 分鐘 TTL。"
    )

    rows = []
    for t in tickers:
        sid = str(t.get('stock_id', ''))
        entry = t.get('entry_close')
        quote = _fetch_live_quote_cached(sid) if sid else None
        latest = quote.get('price') if quote else None
        if entry and latest and entry > 0:
            pnl_pct = (latest - entry) / entry
        else:
            pnl_pct = None
        rows.append({
            'stock_id': sid,
            'stock_name': t.get('stock_name', ''),
            'industry': t.get('industry', ''),
            'entry_close': entry,
            'latest': latest,
            'source': quote.get('price_source') if quote else None,
            'pnl_pct': pnl_pct,
        })

    df = pd.DataFrame(rows)

    # 加總指標
    closed = df.dropna(subset=['pnl_pct'])
    cM1, cM2, cM3, cM4 = st.columns(4)
    cM1.metric("持倉檔數", f"{len(df)}", help=f"成功抓到報價: {len(closed)} 檔")
    if len(closed):
        avg_pnl = closed['pnl_pct'].mean() * 100
        cM2.metric("平均報酬", f"{avg_pnl:+.2f}%")
        cM3.metric("勝率", f"{(closed['pnl_pct'] > 0).mean() * 100:.0f}%",
                   help=f"{int((closed['pnl_pct'] > 0).sum())}/{len(closed)} 檔正報酬")
        cM4.metric("最佳/最差", f"{closed['pnl_pct'].max()*100:+.1f}% / {closed['pnl_pct'].min()*100:+.1f}%")
    else:
        cM2.metric("平均報酬", "n/a")
        cM3.metric("勝率", "n/a")
        cM4.metric("最佳/最差", "n/a")

    # 顯示表格
    display = pd.DataFrame({
        '股票': df['stock_id'] + ' ' + df['stock_name'].fillna(''),
        '產業': df['industry'].fillna(''),
        '進場價': df['entry_close'].apply(lambda v: f"{v:.2f}" if pd.notna(v) else "—"),
        '即時價': df['latest'].apply(lambda v: f"{v:.2f}" if pd.notna(v) else "—"),
        '價源': df['source'].fillna('—'),
        '損益%': df['pnl_pct'].apply(
            lambda v: ("🟢 +{:.1f}%".format(v*100) if pd.notna(v) and v > 0
                       else ("🔴 {:.1f}%".format(v*100) if pd.notna(v) and v < 0
                             else ("⚪ {:.1f}%".format(v*100) if pd.notna(v) else "—")))
        ),
    })
    display = display.sort_values('損益%', ascending=False).reset_index(drop=True)
    st.dataframe(display, use_container_width=True, hide_index=True)

    with st.expander("ℹ️ 價源說明", expanded=False):
        st.markdown(
            "- **z** = 該秒成交價 (盤中即時)\n"
            "- **pz** = 上次成交價 (盤中無撮合 fallback)\n"
            "- **mid** = 五檔買賣 1 中點 (停撮 fallback)\n"
            "- **prev_close** = 昨收 (盤後 / 停牌)\n"
            "- 報價來源 mis.twse 5 sec/3 req 限制 → 用 streamlit cache 5 min TTL 包"
        )


# =============================================================================
# Section 3 — 歷史回測訊號 (Trade Ledger)
# =============================================================================

def _render_trade_ledger() -> None:
    """2021-2026 歷史回測：每筆 BUY/SELL position 的進出場記錄。"""
    # 上方先顯示「當前持倉即時損益」(每天最有用的 panel)
    _render_current_holdings_pnl()
    st.markdown("---")

    st.subheader("📊 歷史回測訊號 (Trade Ledger)")
    st.caption(
        "依 v13.4 production 策略 (monthly K=10 / industry-neutral / liquidity ≥ 10M TWD) "
        "在歷史每月底實際會發出的 BUY/SELL 訊號 — 連續持有合併為單筆 position，純價格報酬。 "
        "⚠️ 此 ledger 為 5/16 一次性歷史 snapshot (K=20 / composite_parsi)，跟現在 production "
        "(K=10 / composite_score) 參數不同；**當前實際持倉看上方「💼 當前持倉即時損益」**。"
    )

    if not LEDGER_PATH.exists():
        st.warning(
            "尚未產生 trade ledger。請先跑：\n"
            "```\npython tools/whale_picks_trade_ledger.py --start 2021-01-01 --end 2026-04-30\n```\n"
            "加 `--with-reasons` 可生成 LLM 中文進出場理由 (Sonnet, ~30min)。"
        )
        return

    try:
        df = pd.read_parquet(LEDGER_PATH)
    except Exception as e:
        st.error(f"無法讀取 ledger: {e}")
        return

    meta = {}
    if LEDGER_META_PATH.exists():
        try:
            meta = json.loads(LEDGER_META_PATH.read_text(encoding='utf-8'))
        except Exception:
            pass

    # Header metrics
    closed = df[~df['still_holding']].copy()
    cM1, cM2, cM3, cM4, cM5 = st.columns(5)
    cM1.metric("總筆數", f"{len(df):,}", help=f"持有中 {int(df['still_holding'].sum())} 筆未計入勝率")
    cM2.metric("勝率", f"{(closed['pnl_pct'] > 0).mean() * 100:.1f}%" if len(closed) else "n/a")
    cM3.metric("平均報酬", f"{closed['pnl_pct'].mean() * 100:+.2f}%" if len(closed) else "n/a")
    cM4.metric("最佳", f"{closed['pnl_pct'].max() * 100:+.1f}%" if len(closed) else "n/a")
    cM5.metric("最差", f"{closed['pnl_pct'].min() * 100:+.1f}%" if len(closed) else "n/a")

    # Filters
    with st.expander("🔍 篩選", expanded=False):
        years = sorted(pd.to_datetime(df['entry_date']).dt.year.unique())
        sel_years = st.multiselect("年份 (進場年)", years, default=years, key='wp_ledger_years')
        industries = sorted([i for i in df['industry'].dropna().unique()])
        sel_inds = st.multiselect("產業", industries, default=industries, key='wp_ledger_inds')
        pnl_filter = st.radio("損益", ["全部", "🟢 獲利", "🔴 虧損", "🟡 持有中"],
                               horizontal=True, key='wp_ledger_pnl')
        max_hold = int(df['holding_months'].max()) if len(df) else 12
        min_hold = st.slider("最少持有月數", 1, max(max_hold, 2), 1, key='wp_ledger_minhold')

    mask = (
        pd.to_datetime(df['entry_date']).dt.year.isin(sel_years)
        & df['industry'].fillna('').isin(sel_inds + [''])
        & (df['holding_months'] >= min_hold)
    )
    if pnl_filter == "🟢 獲利":
        mask = mask & (df['pnl_pct'] > 0) & (~df['still_holding'])
    elif pnl_filter == "🔴 虧損":
        mask = mask & (df['pnl_pct'] < 0) & (~df['still_holding'])
    elif pnl_filter == "🟡 持有中":
        mask = mask & df['still_holding']
    fdf = df[mask].copy().sort_values('entry_date', ascending=False)

    if len(fdf) == 0:
        st.info("篩選後無 position。")
        return

    # Display table
    display = pd.DataFrame({
        '股票': fdf['stock_id'] + ' ' + fdf['stock_name'].fillna(''),
        '產業': fdf['industry'].fillna(''),
        '🟢 進場日': pd.to_datetime(fdf['entry_date']).dt.strftime('%Y-%m-%d'),
        '進場價': fdf['entry_price'].apply(lambda v: f"{v:.2f}" if pd.notna(v) else "—"),
        '🔴 出場日': fdf['exit_date'].apply(
            lambda v: pd.Timestamp(v).strftime('%Y-%m-%d') if pd.notna(v) else "持有中"
        ),
        '出場價': fdf['exit_price'].apply(lambda v: f"{v:.2f}" if pd.notna(v) else "—"),
        '持有月': fdf['holding_months'],
        '損益%': fdf.apply(
            lambda r: ("持有中" if r['still_holding']
                       else (f"🟢 +{r['pnl_pct'] * 100:.1f}%" if r['pnl_pct'] > 0
                             else f"🔴 {r['pnl_pct'] * 100:.1f}%")),
            axis=1
        ),
        '進場理由 (top driver)': fdf['entry_top_drivers'].fillna(''),
        '出場理由 (top driver)': fdf['exit_top_drivers'].fillna(''),
    })
    if 'entry_reason_zh' in fdf.columns and fdf['entry_reason_zh'].astype(str).str.len().max() > 0:
        display['🤖 LLM 進場理由'] = fdf['entry_reason_zh'].fillna('')
        display['🤖 LLM 出場理由'] = fdf['exit_reason_zh'].fillna('')

    st.dataframe(display.reset_index(drop=True), use_container_width=True, hide_index=True, height=500)

    with st.expander("ℹ️ 回測 metadata + 方法論", expanded=False):
        if meta:
            st.markdown(f"**回測期間**: {meta.get('start', '?')} ~ {meta.get('end', '?')}")
            st.markdown(f"**重新平衡**: 月底 / K={meta.get('K', '?')} / 流動性門檻 ≥ NT$ {meta.get('min_avg_tv_twd', 0) / 1e6:.0f}M")
            st.markdown(f"**生成時間**: {meta.get('generated_at', '?')}")
            st.markdown(f"**LLM 理由**: {'✅ 已生成' if meta.get('with_llm_reasons') else '❌ 未生成 (`--with-reasons` flag)'}")
            st.markdown(f"**Win rate** (含持有中): {meta.get('win_rate', 0) * 100:.1f}% / "
                        f"**平均報酬**: {meta.get('avg_pnl_pct', 0) * 100:+.2f}% / "
                        f"**中位數**: {meta.get('median_pnl_pct', 0) * 100:+.2f}%")
        st.markdown("---")
        st.markdown(
            "**回測說明**:\n"
            "- 每月底依 composite_score 排名取 top-10，連續入榜合併成 1 個 position\n"
            "- 進場價 = 進場月底收盤；出場價 = 掉出 top-10 那個月底收盤\n"
            "- P&L = 純價格報酬 (exit/entry - 1)，**未扣手續費 + 證交稅**\n"
            "- 仍在 top-10 的最新 position 標「持有中」，不計入勝率\n"
            "- 此為 **歷史模擬**，live 績效預期受 survivorship + regime drift 拖累"
        )


# =============================================================================
# Section 4 — 任意兩日 snapshot diff (進階)
# =============================================================================

def _render_history_diff() -> None:
    snapshots = _list_snapshots()
    if len(snapshots) <= 1:
        return
    with st.expander("📅 任意兩日 snapshot diff (進階)", expanded=False):
        colA, colB = st.columns(2)
        with colA:
            pick_a = st.selectbox("基準日 (A)", snapshots, index=min(1, len(snapshots) - 1), key='whale_diff_a')
        with colB:
            pick_b = st.selectbox("對照日 (B)", snapshots, index=0, key='whale_diff_b')

        try:
            snap_a = pd.read_parquet(SNAPSHOT_DIR / f"{pick_a}.parquet")
            snap_b = pd.read_parquet(SNAPSHOT_DIR / f"{pick_b}.parquet")
            # 舊 snapshot fallback：composite_score 不存在則用 composite_parsi
            col_a = 'composite_score' if 'composite_score' in snap_a.columns else 'composite_parsi'
            col_b = 'composite_score' if 'composite_score' in snap_b.columns else 'composite_parsi'
            top_a = set(snap_a.dropna(subset=[col_a]).nlargest(10, col_a)['stock_id'].tolist())
            top_b = set(snap_b.dropna(subset=[col_b]).nlargest(10, col_b)['stock_id'].tolist())
            entered = top_b - top_a
            exited = top_a - top_b
            kept = top_a & top_b

            col_in, col_out, col_keep = st.columns(3)
            col_in.metric("📈 新進", len(entered))
            col_out.metric("📉 掉出", len(exited))
            col_keep.metric("➖ 維持", len(kept))

            if entered or exited:
                name_lookup = snap_b.drop_duplicates('stock_id').set_index('stock_id')['stock_name'].to_dict()
                name_lookup_a = snap_a.drop_duplicates('stock_id').set_index('stock_id')['stock_name'].to_dict()

                if entered:
                    st.markdown(f"**📈 新進 (A→B, {len(entered)})**")
                    rows_in = [{'stock_id': s, 'stock_name': name_lookup.get(s, '?')} for s in sorted(entered)]
                    st.dataframe(pd.DataFrame(rows_in), use_container_width=True, hide_index=True)
                if exited:
                    st.markdown(f"**📉 掉出 (A→B, {len(exited)})**")
                    rows_out = [{'stock_id': s, 'stock_name': name_lookup_a.get(s, '?')} for s in sorted(exited)]
                    st.dataframe(pd.DataFrame(rows_out), use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"diff 計算失敗: {e}")


# =============================================================================
# Main entry
# =============================================================================

def render_whale_picks() -> None:
    st.title("🐋 主力選股 (Whale Picks) — BUY/SELL 訊號")
    st.caption(
        "7-feature composite_score (誠實 Sharpe 1.52 K=10) / industry-neutral / monthly rebalance K=10 / "
        "每日 alerts (急升 BUY + drawdown SELL)。"
    )

    obj = _load_latest_json()
    if obj is None:
        st.warning(
            "尚未產生 Whale Picks 名單。請先跑：\n"
            "```\npython tools/whale_picks_screener.py\n```\n"
            "或等 daily scan (`run_scanner.bat`) 自動跑 — 已整合進每日 stage"
        )
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Asof", obj.get('asof', '?'))
    col2.metric("Universe", f"{obj.get('universe_size', 0):,}")
    col3.metric("Valid scored", f"{obj.get('valid_scored', 0):,}")
    col4.metric("Top-K", len(obj.get('top', [])))

    st.warning(
        "⚠️ **SPEC §13 紅線**: 訊號為「下單參考」，**永不接自動下單**。"
        "live 績效預期低於 backtest 誠實 Sharpe 1.52 (K=10 composite_score, production default) / 1.46 (K=20 對照)，"
        "建議當 Mode D 觀察候選池。"
    )

    _render_today_signals(obj)
    st.divider()
    _render_current_holdings(obj)
    st.divider()
    _render_trade_ledger()
    st.divider()
    _render_history_diff()

    with st.expander("ℹ️ 配置 / 方法論 (composite_score 7 因子)"):
        cfg = obj.get('config', {})
        st.markdown(f"**SPEC version**: v{cfg.get('spec_version', '?')}")
        st.markdown(f"**Standardization**: {cfg.get('standardization', '?')}")
        st.markdown(f"**K**: {cfg.get('K', '?')}")
        st.markdown("**Composite 7-feature weights**:")
        # config 從 v0.7 後分 weights_score / weights_parsi（向後相容 'composite'）
        comp = cfg.get('weights_score') or cfg.get('composite', {})
        for f, w in comp.items():
            sign = "+" if w >= 0 else ""
            st.markdown(f"- `{f}`: {sign}{w}")
        st.markdown("---")
        st.markdown(
            "**Backtest (2021-2025 OOS walk-forward, v13.2 4-blocker fix 誠實 baseline)**:\n"
            "- Sharpe **1.49** (composite_score, vs B&H TWII 0.73, composite_parsi 1.01)\n"
            "- CAGR **19.5%** (B&H 11.5%)\n"
            "- MDD **-12.3%** (B&H -28.9%)\n"
            "- ⚠️ 之前宣稱 Sharpe 1.52/1.70 含 look-ahead+survivor leak (aa045f6 揭露)\n"
            "- 詳見 `project_audit_4_blocker_fix` memory"
        )
        st.caption("詳見 `docs/whale_picks_spec.md` v0.8 + `reports/whale_picks_phase2_v13_blocker_fix/report_v2.md`")

    st.caption(
        f"資料生成於 {obj.get('asof', '?')}。**每日自動更新** (run_scanner.bat)，"
        f"月底自動 Discord push (`DISCORD_WEBHOOK_WHALE_PICKS`)。"
    )
