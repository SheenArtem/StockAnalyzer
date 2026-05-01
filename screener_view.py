"""自動選股 view (Phase B 從 app.py 抽出)

對應 app_mode == 'screener' 的整段邏輯，包含 6 個 tab:
- 品質選股 (QM)
- 價值池 (Value)
- 均值回歸 (MeanRev)
- 績效追蹤 (Track)
- Mode D (thesis-driven)
- 4 hidden tabs (Swing / Convergence / US Momentum / US Value) 保留 if False: 不渲染

設計原則: 整 block 內容 wrap 進單一 render_screener() 函式，
跨 tab 共享 state (_qm_value_resonance_tw / _conv_map_tw 等) 維持 function-scope closure 語義。
"""

import logging

import pandas as pd
import streamlit as st

from ui_helpers import _convergence_label, _theme_tags_short, _wc_tags_short

logger = logging.getLogger(__name__)


def render_screener():
    """渲染自動選股模式整個畫面 (含 5 visible + 4 hidden tabs)。"""
    # ====================================================================
    #  自動選股模式 — 右側動能 + 左側價值
    # ====================================================================
    import json as _json
    from pathlib import Path as _Path

    # 2026-04-21: Value TW tab 恢復顯示 (VF-VC P3-b 落地，權重 30/25/30/15/0)
    # 2026-04-22: Value US tab 再度隱藏 — VF-Value-ex2 EDGAR walk-forward D 級反向
    # 且 US 側動能/估值/營收/技術全部 signal 未經 IC 驗證。picks 無實證基礎，避免誤導。
    # 恢復條件：US QM/Value 跑完同級 VF 驗證（類 TW 25+45 項）且有 A/B 級訊號。
    screener_tab_qm, screener_tab2, screener_tab_meanrev, screener_tab_track, screener_tab_mode_d = st.tabs(
        ["🛡️ 品質選股", "💎 價值池 (搭 regime filter)", "🔄 均值回歸", "📊 績效追蹤", "🎯 Mode D"]
    )
    # Hidden tabs (code preserved, just not displayed)
    screener_tab1 = screener_tab_us = screener_tab_swing = screener_tab_conv = screener_tab_us_val = None

    # ====================================================================
    # Pre-load convergence data for badges on all tabs
    # ====================================================================
    _conv_map_tw = {}   # stock_id -> {'tier': int, 'modes': [...]}
    _conv_map_us = {}
    for _conv_suffix, _conv_target in [('', _conv_map_tw), ('_us', _conv_map_us)]:
        _conv_file = _Path(f'data/latest/convergence{_conv_suffix}_result.json')
        if _conv_file.exists():
            try:
                with open(_conv_file, 'r', encoding='utf-8') as _f:
                    _conv_data = _json.load(_f)
                for _cr in _conv_data.get('results', []):
                    _conv_target[_cr['stock_id']] = {
                        'tier': _cr.get('convergence_tier', 0),
                        'modes': _cr.get('modes', []),
                    }
            except Exception:
                pass

    # ====================================================================
    # BL-1 (2026-04-22): QM + Value 共振標記
    # 同時出現在動能 + 價值選股 = 便宜 + 轉強組合，值得優先關注
    # ====================================================================
    _qm_value_resonance_tw = set()
    try:
        _qm_pre_file = _Path('data/latest/qm_result.json')
        _val_pre_file = _Path('data/latest/value_result.json')
        if _qm_pre_file.exists() and _val_pre_file.exists():
            with open(_qm_pre_file, 'r', encoding='utf-8') as _f:
                _qm_pre = _json.load(_f)
            with open(_val_pre_file, 'r', encoding='utf-8') as _f:
                _val_pre = _json.load(_f)
            _qm_ids_pre = {r['stock_id'] for r in _qm_pre.get('results', [])}
            _val_ids_pre = {r['stock_id'] for r in _val_pre.get('results', [])}
            _qm_value_resonance_tw = _qm_ids_pre & _val_ids_pre
    except Exception:
        pass

    # _wc_tags_short / _theme_tags_short / _convergence_label moved to ui_helpers.py

    # ====================================================================
    # Removed 2026-04-17: 右側動能選股 (TW+US) 隱藏 tab
    # VF-6 A 級驗證：QM pure_right 改版後與舊動能選股重疊；移除舊 code 減少維護
    # 若需復活，見 git history commit 前版本或 data/latest/momentum_result.json
    # ====================================================================

    # ====================================================================
    # Tab Swing: 波段選股 (hidden)
    # ====================================================================
    if False:  # hidden tab
        st.markdown("### 🔄 波段選股 (台股)")
        st.markdown("""
**持倉期 2 週 ~ 3 個月**，結合動能評分與週線趨勢，以 低波放量 排序。

**選股邏輯**：觸發分數 Top 候選 → 趨勢分數 >= 1（週線上升趨勢）→ 低波放量 排序（放量+低波動優先）

| 依據 | 回測績效（60 日 horizon） |
|------|------------------------|
| 低波放量 Top-20 | Sharpe **9.50**, 勝率 **76%**, 平均報酬 +3.2% |
| Scanner Top-20 | Sharpe 6.50, 勝率 66%, 平均報酬 +5.5% |

**低波放量計算方式**

```
RVOL     = 今日成交量 / 20日均量        （相對成交量）
ATR_pct  = ATR(14) / 收盤價 x 100      （波動率佔比）
低波放量 = RVOL 的 Z-Score - ATR_pct 的 Z-Score
         （Z-Score = 252 日滾動標準化）
```

越高 = 成交量異常放大 + 波動率異常收斂 = 有人安靜吃貨。
""")

        swing_file = _Path('data/latest/swing_result.json')
        swing_result = None
        if swing_file.exists():
            try:
                with open(swing_file, 'r', encoding='utf-8') as _f:
                    swing_result = _json.load(_f)
            except Exception:
                swing_result = None

        if swing_result and swing_result.get('results'):
            sw_results = swing_result['results']
            st.caption(
                f"掃描日期: {swing_result.get('scan_date', '?')} {swing_result.get('scan_time', '')} | "
                f"全市場 {swing_result.get('total_scanned', 0)} 檔 → "
                f"評分 {swing_result.get('scored_count', 0)} 檔 | "
                f"耗時 {swing_result.get('elapsed_seconds', 0):.0f}s"
            )

            _scenario_map_sw = {'A': 'A 強攻', 'B': 'B 拉回', 'C': 'C 搶短', 'D': 'D 空手', 'N': 'N 觀望'}
            _sw_rows = []
            for r in sw_results:
                _rl = r.get('rvol_lowatr')
                _sc = r.get('scenario', {}).get('code', '')
                _sw_rows.append({
                    '代號': r['stock_id'],
                    '名稱': r.get('name', ''),
                    '收盤': r.get('price', 0),
                    '漲跌%': r.get('change_pct', 0),
                    '均量(億)': round(r.get('avg_trading_value_5d', 0) / 1e8, 2),
                    '趨勢分數': r.get('trend_score', 0),
                    '觸發分數': r.get('trigger_score', 0),
                    '劇本': _scenario_map_sw.get(_sc, _sc),
                    '低波放量': round(_rl, 2) if _rl is not None else None,
                    'ETF買超': r.get('etf_buy_count', 0),
                    '共振': _convergence_label(r['stock_id'], _conv_map_tw),
                    '關鍵訊號': ', '.join(r.get('signals', [])[:3]),
                })
            _df_swing = pd.DataFrame(_sw_rows)

            _sort_opts_sw = {
                '低波放量 (高→低)': ('低波放量', False),
                '趨勢分數 (高→低)': ('趨勢分數', False),
                '觸發分數 (高→低)': ('觸發分數', False),
                '均量(億) (高→低)': ('均量(億)', False),
            }
            _sw_sort = st.selectbox("排序方式", list(_sort_opts_sw.keys()), key='swing_tw_sort')
            _sw_col, _sw_asc = _sort_opts_sw[_sw_sort]
            _df_swing = _df_swing.sort_values(_sw_col, ascending=_sw_asc).reset_index(drop=True)
            _df_swing.index = range(1, len(_df_swing) + 1)

            st.dataframe(
                _df_swing,
                width='stretch',
                height=600,
                column_config={
                    '觸發分數': st.column_config.NumberColumn(format="%.1f"),
                    '趨勢分數': st.column_config.NumberColumn(format="%.1f"),
                    '漲跌%': st.column_config.NumberColumn(format="%.1f%%"),
                    '收盤': st.column_config.NumberColumn(format="%.1f"),
                    '均量(億)': st.column_config.NumberColumn(format="%.2f"),
                },
            )

            st.caption("趨勢分數 >= 1（週線上升趨勢）/ 低波放量 越高=低波放量 / 建議持倉 2w-3m")

            # 操作建議
            with st.expander("個股操作建議"):
                _sw_selected = st.selectbox(
                    "選擇股票",
                    options=[f"{r['stock_id']} {r.get('name', '')}" for r in sw_results],
                    key='swing_detail_select',
                )
                if _sw_selected:
                    _sw_sid = _sw_selected.split()[0]
                    _sw_match = next((r for r in sw_results if r['stock_id'] == _sw_sid), None)
                    if _sw_match:
                        _sc = _sw_match.get('scenario', {})
                        _ap = _sw_match.get('action_plan', {})

                        st.markdown(f"### {_sw_sid} {_sw_match.get('name', '')}")
                        st.markdown(f"**{_sc.get('title', '')}** -- {_sc.get('desc', '')}")
                        st.markdown(f"趨勢分數: **{_sw_match['trend_score']:+.1f}** / "
                                    f"觸發分數: **{_sw_match['trigger_score']:+.1f}** / "
                                    f"低波放量: **{_sw_match.get('rvol_lowatr', 'N/A')}**")

                        if _ap.get('strategy'):
                            st.markdown(f"\n{_ap['strategy']}")

                        st.markdown("**波段操作要點**")
                        st.markdown("- 停損參考: 週線 Supertrend 或 MA60 跌破")
                        st.markdown("- 停利方式: 趨勢跟蹤，週線翻空才出場")
                        st.markdown("- 加碼條件: 拉回週線 MA20 不破 + 量縮後放量")

                        _el = _ap.get('rec_entry_low')
                        _eh = _ap.get('rec_entry_high')
                        if _el and _eh:
                            st.markdown(f"- 進場區間: **{_el:.1f} ~ {_eh:.1f}** ({_ap.get('rec_entry_desc', '')})")

                        if _ap.get('sl_list'):
                            st.markdown("**停損參考價位**")
                            for _sl in _ap['sl_list']:
                                st.markdown(f"- {_sl['method']}: {_sl['price']:.1f}")

                        with st.expander("評分明細", expanded=False):
                            for d in _sw_match.get('trigger_details', []):
                                st.markdown(f"- {d}")

        else:
            st.info("尚無波段掃描結果。\n\n"
                    "在命令列執行 `python scanner_job.py --mode swing` 進行波段掃描\n"
                    "（使用與動能相同的分析引擎，但以趨勢分數+低波放量排序）")

        st.caption("💡 Full scan: `python scanner_job.py --mode swing`")

    # ====================================================================
    # Tab QM: 品質選股選股
    # ====================================================================
    with screener_tab_qm:
        st.markdown("### 🛡️ 品質選股")

        # ================================================================
        # 持股監控 + 每日警報（B 任務）
        # ================================================================
        _POS_FILE = _Path('data/positions.json')
        _ALERT_FILE = _Path('data/latest/position_alerts.json')

        _alert_data = None
        if _ALERT_FILE.exists():
            try:
                with open(_ALERT_FILE, 'r', encoding='utf-8') as _f:
                    _alert_data = _json.load(_f)
            except Exception:
                pass

        _n_hard = (_alert_data or {}).get('hard_count', 0)
        _n_soft = (_alert_data or {}).get('soft_count', 0)
        _n_pos_a = (_alert_data or {}).get('position_count', 0)

        if _n_hard > 0:
            _pm_title = f"🚨 持股警報 — 硬警報 {_n_hard} 筆（立即處理）"
            _pm_expanded = True
        elif _n_soft > 0:
            _pm_title = f"⚠️ 持股警報 — 軟警報 {_n_soft} 筆（考慮減碼）"
            _pm_expanded = True
        elif _n_pos_a > 0:
            _pm_title = f"✅ 持股監控 — {_n_pos_a} 檔持股全部正常"
            _pm_expanded = False
        else:
            _pm_title = "📦 我的持股 + 每日警報"
            _pm_expanded = False

        if False:  # 持股警報暫時隱藏 (user req 2026-04-30)
            from position_monitor import (
                load_positions as _pm_load,
                save_positions as _pm_save,
            )

            # A. 警報區
            if _alert_data and _alert_data.get('alerts'):
                st.markdown("#### 🚨 今日警報")
                for _a in _alert_data['alerts']:
                    _sev = _a['severity']
                    _ic = '🔴' if _sev == 'hard' else '🟡'
                    _ts = _a.get('trigger_score')
                    _ts_txt = f" · trigger {_ts:+.1f}" if _ts is not None else ""
                    st.markdown(
                        f"**{_ic} {_a['stock_id']} {_a.get('name','')}** · "
                        f"PnL {_a['pnl_pct']:+.1f}% · 持有 {_a.get('hold_days',0)} 天 · "
                        f"現價 {_a.get('current_price',0):.2f} / 進場 {_a.get('buy_price',0):.2f}"
                        f"{_ts_txt}"
                    )
                    for _t in _a.get('triggers', []):
                        _sub = '❌' if _t.get('severity') == 'hard' else '⚠️'
                        st.markdown(f"  - {_sub} {_t.get('desc','')}：{_t.get('value','')}")
                st.caption(
                    f"警報產生時間：{_alert_data.get('scan_date','?')} "
                    f"{_alert_data.get('scan_time','')}"
                )
                st.markdown("---")
            elif _alert_data and _alert_data.get('position_count', 0) > 0:
                st.caption(
                    f"✅ 最後檢查：{_alert_data.get('scan_date','?')} "
                    f"{_alert_data.get('scan_time','')} — 所有持股正常"
                )
                st.markdown("---")

            # B. 持股清單 + 最近 trigger_score（軟警報資料）
            _pos_list = _pm_load()
            st.markdown(f"#### 📋 持股清單（{len(_pos_list)} 檔）")
            if _pos_list:
                # 載入 trigger_score 歷史（軟警報累積資料）
                from position_monitor import (
                    load_history as _pm_load_hist,
                    _history_key as _pm_hkey,
                )
                _pm_hist = _pm_load_hist()

                # 台股市值排名（1 = 台股市值最大）— 與 QM 表格共用 1h cache
                try:
                    from momentum_screener import MomentumScreener as _MS
                    _tv_all = _MS._fetch_tv_marketcap_volume() or {}
                    _tv_filtered = {
                        sid: d for sid, d in _tv_all.items()
                        if sid.isdigit() and len(sid) == 4 and not sid.startswith('0')
                    }
                    _pm_mc_rank = {
                        sid: i + 1
                        for i, (sid, _) in enumerate(
                            sorted(
                                _tv_filtered.items(),
                                key=lambda x: x[1].get('market_cap', 0) or 0,
                                reverse=True,
                            )
                        )
                    }
                except Exception:
                    _pm_mc_rank = {}

                _pos_rows = []
                for p in _pos_list:
                    _hk = _pm_hkey(p.get('stock_id', ''), p.get('buy_date', ''))
                    _series = _pm_hist.get(_hk, [])
                    _last_ts = _series[-1]['trigger_score'] if _series else None
                    _peak_ts = max((e['trigger_score'] for e in _series), default=None)
                    _pos_rows.append({
                        '代號': p.get('stock_id', ''),
                        '名稱': p.get('name', ''),
                        '市值排名': _pm_mc_rank.get(p.get('stock_id', '')),
                        '進場日': p.get('buy_date', ''),
                        '進場價': p.get('buy_price', 0),
                        '股數': p.get('shares', 0),
                        '近峰值': _peak_ts,
                        '最新': _last_ts,
                        '歷史天數': len(_series),
                        '備註': p.get('notes', ''),
                    })
                st.dataframe(
                    pd.DataFrame(_pos_rows),
                    width='stretch',
                    hide_index=True,
                    column_config={
                        '市值排名': st.column_config.NumberColumn(format="%d", help="1 = 台股市值最大（僅普通股）"),
                        '進場價': st.column_config.NumberColumn(format="%.2f"),
                        '股數': st.column_config.NumberColumn(format="%d"),
                        '近峰值': st.column_config.NumberColumn(format="%+.1f", help="trigger_score 近 20 日峰值"),
                        '最新': st.column_config.NumberColumn(format="%+.1f", help="trigger_score 最近一次值"),
                        '歷史天數': st.column_config.NumberColumn(format="%d", help="已累積幾天 trigger_score"),
                    },
                )
                st.caption("軟警報觸發條件：近峰值 ≥ +5 且最新 ≤ -2（動能急轉）/ 連續 5 日 < 0（持續弱化）")
            else:
                st.caption("尚未新增持股。填下方表單新增第一筆。")

            # C. 新增持股
            st.markdown("#### ➕ 新增持股")
            with st.form('pm_add_form', clear_on_submit=True):
                _pm_c1, _pm_c2, _pm_c3 = st.columns(3)
                _pm_sid = _pm_c1.text_input("代號", key='pm_sid')
                _pm_nm = _pm_c2.text_input("名稱（選填）", key='pm_name')
                _pm_dt = _pm_c3.date_input("進場日期", value=None, key='pm_date')
                _pm_c4, _pm_c5, _pm_c6 = st.columns(3)
                _pm_pr = _pm_c4.number_input(
                    "進場價", min_value=0.0, step=0.01, format="%.2f", key='pm_price')
                _pm_sh = _pm_c5.number_input(
                    "股數", min_value=0, step=100, key='pm_shares')
                _pm_nt = _pm_c6.text_input("備註（選填）", key='pm_notes')
                _pm_ok = st.form_submit_button("新增持股")
                if _pm_ok:
                    if not _pm_sid.strip():
                        st.error("必填：股票代號")
                    elif _pm_pr <= 0:
                        st.error("必填：進場價 > 0")
                    else:
                        _pos_list.append({
                            'stock_id': _pm_sid.strip(),
                            'name': _pm_nm.strip(),
                            'buy_date': _pm_dt.isoformat() if _pm_dt else '',
                            'buy_price': float(_pm_pr),
                            'shares': int(_pm_sh),
                            'notes': _pm_nt.strip(),
                        })
                        _pm_save(_pos_list)
                        st.success(f"已新增 {_pm_sid}")
                        st.rerun()

            # D. 刪除持股
            if _pos_list:
                st.markdown("#### 🗑️ 刪除持股")
                _pm_del_opts = [
                    f"{p['stock_id']} {p.get('name','')} @ {p.get('buy_date','-')}"
                    for p in _pos_list
                ]
                _pm_del_sel = st.selectbox(
                    "選擇要刪除", options=_pm_del_opts, key='pm_del_sel')
                if st.button("確認刪除", key='pm_del_btn'):
                    _pm_tgt_sid = _pm_del_sel.split()[0]
                    _pm_tgt_dt = _pm_del_sel.split('@')[-1].strip()
                    _pos_list = [
                        p for p in _pos_list
                        if not (p.get('stock_id') == _pm_tgt_sid
                                and p.get('buy_date', '-') == _pm_tgt_dt)
                    ]
                    _pm_save(_pos_list)
                    st.success(f"已刪除 {_pm_tgt_sid}")
                    st.rerun()

            # E. 手動執行監控
            st.markdown("---")
            _pm_run_col, _pm_cap_col = st.columns([1, 4])
            if _pm_run_col.button("🔄 立即檢查", key='pm_run_btn',
                                  disabled=not _pos_list):
                with st.spinner("檢查中..."):
                    from position_monitor import run_monitor as _pm_run
                    _pm_result = _pm_run(positions=_pos_list)
                    st.success(
                        f"完成：{_pm_result['position_count']} 檔 / "
                        f"硬警報 {_pm_result['hard_count']} · "
                        f"軟警報 {_pm_result['soft_count']}"
                    )
                    st.rerun()
            _pm_cap_col.caption(
                "出場條件：動態停損(ATR%調整, -5%~-14%) / 週 Supertrend 翻空 / 週 MA20 動態跌破 / "
                "月營收 YoY 連 2 月負 / trend_score < 1。"
                "TUE-SAT 00:00 scanner 自動跑，此按鈕可立即檢查。"
            )

        # 篩選條件說明 + 操作 SOP 已挪到頁面最下方 (user req 2026-04-30)

        qm_file = _Path('data/latest/qm_result.json')
        qm_result = None
        if qm_file.exists():
            try:
                with open(qm_file, 'r', encoding='utf-8') as _f:
                    qm_result = _json.load(_f)
            except Exception:
                qm_result = None

        if qm_result and qm_result.get('results'):
            qm_results = qm_result['results']
            st.caption(
                f"掃描日期: {qm_result.get('scan_date', '?')} {qm_result.get('scan_time', '')} | "
                f"全市場 {qm_result.get('total_scanned', 0)} 檔 → "
                f"品質篩 {qm_result.get('passed_initial', 0)} 檔 → "
                f"評分 {qm_result.get('scored_count', 0)} 檔 | "
                f"耗時 {qm_result.get('elapsed_seconds', 0):.0f}s"
            )
            if _qm_value_resonance_tw:
                _res_in_qm = [r['stock_id'] for r in qm_results if r['stock_id'] in _qm_value_resonance_tw]
                if _res_in_qm:
                    st.success(f"✨ **動能+價值共振** ({len(_res_in_qm)} 檔): {', '.join(_res_in_qm)} — 同時通過兩個 screener 的稀有組合")

            # 🎯 精選 3 檔（上班族）— TV>=10億 + F>=8 + Comp>=75 + weighted rank
            # 2026-04-22: set-and-forget 用，篩掉小型高波動 / F<8 雷股 / 過熱 FOMO
            from tools.qm_office_picks import select_office_picks as _office_pick
            _office_picks = _office_pick(qm_result, n=3)
            if _office_picks:
                with st.expander(
                    f"🎯 精選 3 檔（上班族不看盤版）— 共 {len(_office_picks)} 檔通過硬篩",
                    expanded=True,
                ):
                    st.caption(
                        "硬篩：日均成交 ≥ 10 億 · F-Score ≥ 8 · Composite ≥ 75。"
                        "排序：Composite + ETF×5 − |Trigger|×1.5 + 流動性加分。"
                    )
                    _cols = st.columns(len(_office_picks))
                    for _i, _p in enumerate(_office_picks):
                        _tv_yi = _p.get('avg_trading_value_5d', 0) / 1e8
                        with _cols[_i]:
                            st.markdown(
                                f"**#{_i+1} {_p['stock_id']} {_p.get('name','')}**"
                            )
                            st.metric(
                                "Office Score",
                                f"{_p.get('office_score',0):.1f}",
                                delta=f"QM#{qm_results.index(next(r for r in qm_results if r['stock_id']==_p['stock_id']))+1}",
                            )
                            st.markdown(
                                f"💰 {_p['price']:.0f} · 📊 TV {_tv_yi:.0f}億  \n"
                                f"F={_p.get('qm_f_score',0)}/9 · "
                                f"Comp {_p.get('composite_score',0):.1f} · "
                                f"Trig {_p.get('trigger_score',0):+.1f} · "
                                f"ETF×{_p.get('etf_buy_count',0)}"
                            )
                    st.caption(
                        "💡 適合持倉 40-60 天的中長線。高 |Trigger| 分代表熱度高，"
                        "可分批進場避免追高；低 |Trigger| 分適合直接進場後放。"
                    )

            # 🎯 今日擇時 Top 5（依 trigger_score 由高到低）
            #    trigger_score 整合日線 MACD/KD/RSI/RVOL/籌碼/情緒/營收/ETF，
            #    用於「今天該下手哪檔」的進場時機判斷（不影響選股排名）
            def _timing_badge(ts):
                if ts is None:
                    return '⚪'
                if ts >= 3:
                    return '🟢'
                if ts >= 0:
                    return '🟡'
                return '🔴'

            _qm_by_trigger = sorted(
                qm_results,
                key=lambda r: r.get('trigger_score', 0) or 0,
                reverse=True,
            )[:5]
            if _qm_by_trigger:
                st.markdown("#### 🎯 今日擇時 Top 5")
                _top5_cols = st.columns(5)
                for _i, _r in enumerate(_qm_by_trigger):
                    _ts = _r.get('trigger_score', 0) or 0
                    _cs = _r.get('composite_score')
                    _trend = _r.get('trend_score', 0) or 0
                    _badge = _timing_badge(_ts)
                    _cs_txt = f"{_cs:.0f}" if _cs is not None else "-"
                    with _top5_cols[_i]:
                        st.metric(
                            label=f"{_badge} {_r['stock_id']} {_r.get('name', '')[:6]}",
                            value=f"{_ts:+.1f}",
                            delta=f"綜合 {_cs_txt} / 趨勢 {_trend:+.1f}",
                            delta_color="off",
                        )
                st.caption("🟢 ≥3 今日可進場 / 🟡 0-3 觀察 / 🔴 <0 等訊號轉強（trigger_score 為日線擇時指標）")

            # 台股市值排名（1 = 台股市值最大）— 復用 momentum_screener 的 1h cache
            # 過濾 ETF/特別股/權證：僅保留 1000-9999 的一般普通股
            try:
                from momentum_screener import MomentumScreener
                _tv_data_all = MomentumScreener._fetch_tv_marketcap_volume() or {}
                _tv_data = {
                    sid: d for sid, d in _tv_data_all.items()
                    if sid.isdigit() and len(sid) == 4 and not sid.startswith('0')
                }
                _mc_rank = {
                    sid: i + 1
                    for i, (sid, _) in enumerate(
                        sorted(
                            _tv_data.items(),
                            key=lambda x: x[1].get('market_cap', 0) or 0,
                            reverse=True,
                        )
                    )
                }
            except Exception:
                _mc_rank = {}

            _qm_rows = []
            for r in qm_results:
                _fs = r.get('qm_f_score')
                _bs = r.get('qm_body_score')
                _cs = r.get('composite_score')
                _ts = r.get('trigger_score', 0) or 0
                _ap = r.get('action_plan', {}) or {}
                _sl = _ap.get('rec_sl_price')
                _rr = _ap.get('rr_ratio')
                _el = _ap.get('rec_entry_low')
                _eh = _ap.get('rec_entry_high')
                _entry_str = f"{_el:.1f}~{_eh:.1f}" if (_el and _eh) else None
                _qm_rows.append({
                    '代號': r['stock_id'],
                    '名稱': r.get('name', ''),
                    '共振': '✨' if r['stock_id'] in _qm_value_resonance_tw else '',
                    '週榜': _wc_tags_short(r['stock_id']),
                    '題材': _theme_tags_short(r['stock_id']),
                    '市值排名': _mc_rank.get(r['stock_id']),
                    '綜合': _cs if _cs is not None else None,
                    'F-Score': _fs if _fs is not None else None,
                    '體質分': round(_bs, 0) if _bs is not None else None,
                    '趨勢分數': r.get('trend_score', 0),
                    '擇時': _timing_badge(_ts),
                    '觸發分數': _ts,
                    '收盤': r.get('price', 0),
                    '建議進場': _entry_str,
                    '推薦停損': _sl if _sl else None,
                    'R:R': _rr if _rr else None,
                    '漲跌%': r.get('change_pct', 0),
                })
            _df_qm = pd.DataFrame(_qm_rows)

            _sort_opts_qm = {
                '綜合 (高→低)': ('綜合', False),
                '觸發分數 (高→低)': ('觸發分數', False),
                'F-Score (高→低)': ('F-Score', False),
                '體質分 (高→低)': ('體質分', False),
                '趨勢分數 (高→低)': ('趨勢分數', False),
                'R:R (高→低)': ('R:R', False),
                '市值排名 (小→大)': ('市值排名', True),
            }
            _qm_sort = st.selectbox("排序方式", list(_sort_opts_qm.keys()), key='qm_tw_sort')
            _qm_col, _qm_asc = _sort_opts_qm[_qm_sort]
            _df_qm = _df_qm.sort_values(_qm_col, ascending=_qm_asc).reset_index(drop=True)
            _df_qm.index = range(1, len(_df_qm) + 1)

            st.dataframe(
                _df_qm,
                width='stretch',
                height=int(38 + len(_df_qm) * 35 + 3),
                column_config={
                    '共振': st.column_config.TextColumn(width='small', help="✨ = 同時出現在動能+價值選股（便宜+轉強組合）"),
                    '週榜': st.column_config.TextColumn(width='medium', help="本週三大法人榜單上的標記（連買/連賣天數 + 4 維度排名）"),
                    '題材': st.column_config.TextColumn(width='medium', help="4 層融合：sector_tags_manual.json AI era 題材 (140 ticker) → News RSS 萃取 (30d) → YT 法說提及 (180d) → TV industry 中文 fallback；最多顯示 2 個 + 餘數"),
                    '市值排名': st.column_config.NumberColumn(format="%d", help="1 = 台股市值最大"),
                    '綜合': st.column_config.NumberColumn(format="%.1f"),
                    'F-Score': st.column_config.NumberColumn(format="%d"),
                    '體質分': st.column_config.NumberColumn(format="%.0f"),
                    '趨勢分數': st.column_config.NumberColumn(format="%.1f"),
                    '觸發分數': st.column_config.NumberColumn(format="%+.1f"),
                    '漲跌%': st.column_config.NumberColumn(format="%.1f%%"),
                    '收盤': st.column_config.NumberColumn(format="%.1f"),
                    '建議進場': st.column_config.TextColumn(help="rec_entry_low ~ rec_entry_high"),
                    '推薦停損': st.column_config.NumberColumn(format="%.1f"),
                    'R:R': st.column_config.NumberColumn(format="%.2f"),
                },
            )

            st.caption("綜合 = F-Score 50% + 體質分 30% + 趨勢分數 20%（選股排名用） · "
                       "觸發分數為日線擇時指標（決定今天該下手哪檔，不影響選股）")

            # 操作建議
            with st.expander("個股操作建議"):
                _qm_selected = st.selectbox(
                    "選擇股票",
                    options=[f"{r['stock_id']} {r.get('name', '')}" for r in qm_results],
                    key='qm_detail_select',
                )
                if _qm_selected:
                    _qm_sid = _qm_selected.split()[0]
                    _qm_match = next((r for r in qm_results if r['stock_id'] == _qm_sid), None)
                    if _qm_match:
                        _ap = _qm_match.get('action_plan', {})

                        st.markdown(f"### {_qm_sid} {_qm_match.get('name', '')}")
                        _cs = _qm_match.get('composite_score')
                        _fs = _qm_match.get('qm_f_score')
                        _bs = _qm_match.get('qm_body_score')
                        _qs = _qm_match.get('qm_quality_score')
                        st.markdown(f"**綜合: {_cs}** / "
                                    f"F-Score: {_fs if _fs is not None else 'N/A'} / "
                                    f"體質: {round(_bs, 0) if _bs is not None else 'N/A'} / "
                                    f"趨勢: {_qm_match['trend_score']:+.1f} / "
                                    f"品質總分: {_qs if _qs is not None else 'N/A'}")

                        if _ap.get('strategy'):
                            st.markdown(f"\n{_ap['strategy']}")

                        _el = _ap.get('rec_entry_low')
                        _eh = _ap.get('rec_entry_high')
                        if _el and _eh:
                            st.markdown(f"- 進場區間: **{_el:.1f} ~ {_eh:.1f}** ({_ap.get('rec_entry_desc', '')})")

                        # QM 風險報酬摘要
                        _qm_sl = _ap.get('rec_sl_price')
                        _qm_tp = _ap.get('rec_tp_price')
                        _qm_rr = _ap.get('rr_ratio')
                        if _qm_sl and _qm_tp:
                            _c1, _c2, _c3 = st.columns(3)
                            _c1.metric("推薦停損", f"{_qm_sl:.2f}", _ap.get('rec_sl_method', ''))
                            _c2.metric("首要停利 (+15%)", f"{_qm_tp:.2f}", "TP1 減碼 1/3")
                            if _qm_rr:
                                _c3.metric("風報比 R:R", f"{_qm_rr:.2f}", "TP1 vs 停損")

                        # QM 分批進場（A#2：依 trigger_score 色燈顯示）
                        _qm_batches = _ap.get('qm_entry_batches')
                        _qm_gate = _ap.get('qm_entry_gate') or {}
                        _qm_gate_level = _qm_gate.get('level', 'unknown')
                        if _qm_batches:
                            if _qm_gate_level == 'green':
                                st.success(f"📥 **分批進場**: {_qm_batches}")
                            elif _qm_gate_level == 'yellow':
                                st.warning(f"📥 **分批進場**: {_qm_batches}")
                            elif _qm_gate_level == 'red':
                                st.error(f"📥 **分批進場**: {_qm_batches}")
                            else:
                                st.info(f"📥 **分批進場**: {_qm_batches}")

                        # QM 動態倉位建議（A#3：composite × trigger）
                        _qm_size = _ap.get('qm_position_size')
                        if _qm_size:
                            _qm_pct = _qm_size.get('recommended_pct', 0)
                            _qm_base = _qm_size.get('base_pct', 0)
                            _qm_mult = _qm_size.get('multiplier', 1.0)
                            _sc1, _sc2, _sc3 = st.columns(3)
                            _sc1.metric("建議倉位", f"{_qm_pct:.1f}%",
                                        f"×{_qm_mult:.2f} 擇時調整")
                            _sc2.metric("基礎倉位", f"{_qm_base:.1f}%",
                                        "依綜合評分 / 80")
                            _sc3.metric("擇時係數", f"×{_qm_mult:.2f}",
                                        "clip(trigger/5, 0.5, 1.5)")
                            st.caption(f"💰 {_qm_size.get('rationale', '')}")

                        # QM 三段停利
                        if _ap.get('tp_list'):
                            st.markdown("**停利階梯**")
                            for _tp in _ap['tp_list']:
                                _mark = " ← 推薦" if _tp.get('is_rec') else ""
                                st.markdown(f"- {_tp['method']}: {_tp['price']:.1f} ({_tp.get('desc', '')}){_mark}")

                        if _ap.get('sl_list'):
                            st.markdown("**停損參考價位**")
                            for _sl in _ap['sl_list']:
                                _p = _sl.get('price', 0)
                                _loss = _sl.get('loss')
                                _loss_txt = f" ({_loss:+.1f}%)" if _loss is not None else ""
                                st.markdown(f"- {_sl['method']}: {_p:.1f}{_loss_txt}")

                        # QM 出場訊號
                        _qm_exits = _ap.get('qm_exit_signals', [])
                        if _qm_exits:
                            st.markdown("**出場訊號 (任一觸發即全出)**")
                            for _e in _qm_exits:
                                st.markdown(f"- 🚨 {_e}")

                        _q_details = _qm_match.get('qm_quality_details', [])
                        if _q_details:
                            with st.expander("品質評分明細", expanded=False):
                                for d in _q_details:
                                    st.markdown(f"- {d}")

                        with st.expander("技術評分明細", expanded=False):
                            for d in _qm_match.get('trigger_details', []):
                                st.markdown(f"- {d}")
        else:
            st.info("尚無品質選股掃描結果。\n\n"
                    "在命令列執行 `python scanner_job.py --mode qm` 進行品質選股掃描\n"
                    "（動能選股 + 品質門檻，過濾虧損/高負債/營收崩的股票）")

        st.caption("💡 Full scan: `python scanner_job.py --mode qm`")

        # ====================================================================
        # 頁面最下方：篩選條件說明 + 操作 SOP (2026-04-30 從頁首挪到頁尾)
        # ====================================================================
        with st.expander("📋 篩選條件說明", expanded=False):
            st.markdown("""
結合**技術面動能**、**基本面品質**與**波段趨勢**，三層篩選找出體質好、趨勢向上、有人吃貨的股票。

---

#### Stage 1：初篩（市值 + 流動性）

從全市場約 1,900 檔中，用兩個條件的**聯集**快速篩出候選池：

| 條件 | 門檻 | 說明 |
|------|------|------|
| 市值前 300 大 | TradingView 即時市值 | 涵蓋大型+中型股，確保機構有在看 |
| **OR** 20 日均成交值 | > 5 億 | 高流動性的中小型股也入選，不會錯殺熱門股 |
| 當日漲跌幅 | > -1% | 允許微跌，排除當天大跌的股票 |

兩個條件取聯集：市值大的一定選，成交活躍的也選。通過約 300-400 檔。

---

#### Stage 1.5：品質門檻（基本面快篩）

用 TradingView 免費批次資料，刷掉明顯地雷：

| 條件 | 門檻 | 目的 |
|------|------|------|
| ROE | > 0% | 公司有在賺錢 |
| 淨利率 | > 0% | 本業不虧損 |
| 負債比 | < 200% | 不是高槓桿爆雷股 |
| 營收 YoY | > -20% | 營收沒有崩盤 |

門檻故意**寬鬆**，目的只是排除明顯有問題的。資料缺失的股票**不懲罰**（放行）。
通常刷掉約 80 檔，剩 250-320 檔進入 Stage 2。

---

#### Stage 2：逐檔分析

每檔股票逐一載入 1 年歷史 K 線，計算以下分數（**QM 最終排序不用觸發分數**，僅供參考顯示）：

**趨勢分數（-5 ~ +5）** — 週線趨勢方向（QM 綜合評分佔 20%）

週 K 的 MA、Supertrend、DMI 綜合判斷，正值 = 週線多頭，負值 = 空頭。

**觸發分數（-10 ~ +10）** — 日線多空信號（QM 不採用，IC=+0.010 接近無效）

| 組別 | 指標 | 說明 |
|------|------|------|
| 趨勢組 | MA 均線回歸、Supertrend、DMI | 價格相對趨勢的位置 |
| 動能組 | MACD 交叉/背離、KD 交叉、RSI 背離 | 動能轉折信號 |
| 量能組 | RVOL（相對成交量 Z-Score） | 量能確認 |
| 籌碼組 | 法人動向、融資、券資比、借券 | 籌碼面評分（見下方） |

**籌碼面評分（±2.0）** — C2-b IC 驗證修正版（2026-04-16）

方向依據 5 年截面 IC 驗證：「籌碼乾淨 = 好」（法人不追、散戶不擠）

| 因子 | 加/減分 | IC 驗證 |
|------|---------|---------|
| **外資** 5 日買賣超 | 買超 +0.3 / 賣超 -0.3 | IR +0.06（微弱正，保守給分） |
| **投信** 5 日買賣超 | **買超 -0.5** / 賣超 +0.3 | IR **-0.32**（過熱逆向指標） |
| **融資使用率** | >60% -0.4 / <20% +0.2 | IR -0.24（散戶追漲） |
| **融資增量** 5 日 | 增 >5% -0.3 / 減 >5% +0.2 | 同上 |
| **券資比** | >30% **-0.6** / >15% -0.3 / <3% +0.2 | IR **-0.57**（最強因子） |
| **借券** 5 日增減 | 大增 -0.6 / 增 -0.3 / 大減 +0.4 | IR -0.33 |

**低波放量** — 僅供參考（QM 品質池內 IC=-0.037 負向，不列入評分）

```
低波放量 = RVOL 的 Z-Score - ATR_pct 的 Z-Score
```
- 越高 = 成交量異常放大 + 波動率異常收斂
- 注意：全市場 Sharpe 9.50，但品質池（移除爛股後）IC 反轉為負

Stage 2 完成後，過濾**趨勢分數 >= 1**，通常剩 50-100 檔。

趨勢分數由週 K 線的 6 個因子加總（-5 ~ +5）：均線架構(±2)、DMI 趨勢(±1)、OBV 能量潮(±1)、EFI 資金流(±1)、K 線形態(±2)、量價配合(±1)。
>= 1 表示至少有一個多方因子成立（例如站上週 MA20），週線偏多。

---

#### Stage 3：品質評分

對所有趨勢 >= 1 的股票，逐檔計算精細品質分（FinMind 財報 + 月營收）。

**品質分（0-100）= 體質分 x 60% + 營收分 x 40%**

**體質分（基準 50，加減分制）**

| 項目 | 來源 | 加/減分規則 |
|------|------|-----------|
| **F-Score** (0-9) | FinMind 財報三表 | >= 7: +25（強）/ <= 3: -20（價值陷阱） |
| **Z-Score** | FinMind | 安全區: +8 / 危險區: -20（破產風險） |
| ROIC | FinMind | > 15%: +8 / < 0: -5 |
| FCF Yield | FinMind | > 8%: +8 / < -5%: -5 |
| ROE | FinMind / TradingView | > 15%: +5 / < 0: -10 |
| 連續獲利 | FinMind EPS | 連續 4 季: +5 / 僅 1 季: -10 |
| 毛利率 | TradingView | > 40%: +5 / < 10%: -5 |
| 營益率 | TradingView | > 20%: +5 / < 0: -8 |
| 負債/權益 | TradingView | > 200%: -5 |
| 流動比率 | FinMind | > 2.0: +5 / < 1.0: -8 |

**營收分（基準 50，加減分制）**

| 項目 | 來源 | 加/減分規則 |
|------|------|-----------|
| 營收 YoY 已轉正 | 月營收 | +10 |
| 營收衰退收斂 | 月營收趨勢 | 最高 +20（收斂幅度越大越多） |
| 營收加速衰退 | 月營收趨勢 | 最高 -20 |
| 營收正驚喜 | 月營收 | +12 |
| 營收負驚喜 | 月營收 | -8 |

---

#### Stage 4：綜合評分 → Top 20

三個維度加權計算**綜合評分**（組內百分位加權），取 Top 20 輸出：

| 維度 | 權重 | 60d IC | 60d 勝率 | 來源 |
|------|------|--------|---------|------|
| **F-Score** (0-9) | **50%** | **+0.113** | **81%** | Piotroski 9 項財報指標 |
| **體質分** (0-100) | **30%** | +0.073 | 76% | ROE/Z-Score/ROIC/三率/流動比率 |
| **趨勢分數** (-5~+5) | **20%** | +0.043 | 52% | 週線 MA/DMI/OBV/EFI/形態 |

每個維度先算組內百分位排名（0-100），再加權得到綜合分。**最終排序和選取都以綜合評分為準。**

**權重來源：2026-04-15 IC 驗證 + NN 測試**
- 驗證期間：2022-01 ~ 2026-04（Test: 2024-07 之後）
- 回測 Sharpe：**60d 1.67**（勝率 76%, 報酬 +13.99%）
- 對比原始權重（rvol30/trig25/qual25/trend20）Sharpe 1.28，改善 **+30%**

**已移除的維度：**
- 低波放量（rvol_lowatr）60d IC=-0.037 負向，全市場有效但在品質池內反指標
- 觸發分數（trigger_score）60d IC=+0.010 幾乎無效

---

#### 訊號代碼對照

| 訊號 | 說明 |
|------|------|
| `supertrend_bull/bear` | Supertrend 多方/空方 |
| `macd_golden/dead` | MACD 黃金/死亡交叉 |
| `rsi_bull_div/bear_div` | RSI 底/頂背離 |
| `rvol_high/low` | 爆量確認/量能萎縮 |
| `inst_buy/sell` | 法人買超/賣超 |
| `etf_sync_buy/sell` | ETF 同步買超/賣超 |
| `squeeze_fire` | 布林帶壓縮釋放 |
""")

        with st.expander("📖 操作 SOP（選出來之後怎麼做）", expanded=False):
            st.markdown("""
### 一、QM 定位：**右側交易 + 基本面保險**

**不是左側抄底**。進場門檻 `trend_score >= 1` 代表週線+日線都已在趨勢中
（MA 多頭排列、Supertrend 多方、ADX 上升），在確認「股票已經開漲」後才進。

| 左側 | 右側 | **QM (右側 + 品質)** |
|------|------|---------------------|
| 逆勢抄底，等反轉 | 順勢追擊，等確認 | 趨勢確認後挑 F-Score 高、體質好的 |
| 勝率低、單次報酬大 | 勝率中、穩定 | 勝率 **76%** 高、平均 +14%/60d |
| Left tail 風險大 | Left tail 中等 | Left tail 低（品質過濾掉地雷） |

價值選股（左側抄底）請看 `💎 價值 (台股)` tab，QM 不要拿來抄底用。

---

### 二、驗證數據錨點（決定操作參數）

| Horizon | 平均報酬 | Sharpe | 勝率 |
|---------|---------|--------|------|
| **20d** | +4.4% | **1.99** | 79% |
| **40d** | +9.2% | 1.81 | 78% |
| **60d** | **+14.0%** | 1.67 | 76% |

- Sharpe 高點在 20d，絕對報酬高點在 60d
- 最佳 R:R 出現在 **40d**（兼顧兩者）
- **建議基準持倉 = 40-60 天**

---

### 三、操作 SOP

#### 進場

| 項目 | 建議 | 理由 |
|------|------|------|
| 批次 | **分 2 批（50%+50%）** | 右側怕追在短線高點 |
| 第二批加碼條件 | 日 RSI 回 45-55 或觸日 MA10 | 短線過熱釋放後 |
| 放棄進場 | 當日已漲 >5% 或跳空缺口 >3% | R:R 惡化 |
| 時段避開 | 財報前 5 個交易日 | QM 靠基本面，不在資訊盲區加倉 |
| 單檔上限 | 總資金 **8-10%** | 勝率 76% 可進取，但分散仍重要 |

#### 停損（雙保險）

```
硬停損 = max(動態停損, 週線 MA20)
動態停損 = 進場價 × (1 - clip(ATR% × 3, 5%, 14%))
```

- **動態硬停損**：依 ATR% 自動調整（低波動 -5% / 中等 -8% / 高波動 -14%）
- **週線 MA20 跌破**：趨勢結構破壞即出（容忍度依 ATR% 調整 -2%~-5%）
- **基本面急煞**：月營收 YoY **連續 2 個月轉負 → 立刻全出**
  （QM alpha 來源是基本面，基本面破 = 論據失效）

#### 停利 / 減碼

| 階段 | 動作 | 理由 |
|------|------|------|
| +8% 或持倉 20 日 | 移動停損升至成本價 | 鎖定不虧 |
| **TP1 (依 ATR% 縮放)** | **減碼 1/3** | 低波動 +10% / 中等 +15% / 高波動 +24% |
| **TP2** | **改用週 MA10 移動停利** | 已拿走超額，剩餘跟趨勢 |
| **TP3 或 60 日滿** | 清倉或換股輪動 | 驗證期外報酬衰減 |

#### 出場訊號（任一觸發即出）

1. 週線 Supertrend 翻空
2. 週 MA20 跌破 3% 以上（非插針）
3. 月營收 YoY 連 2 個月轉負
4. F-Score 季更新後掉 2 分以上

---

### 四、風報比試算

```
期望值 = 勝率 × 平均賺 − 敗率 × 平均賠
       = 0.76 × 14% − 0.24 × 8%
       = +8.7% 每筆 (60d)
```

年化：60 天轉一次，理論一年 5-6 循環 → **年化期望 ~40-50%**（未計成本）。
扣交易成本 0.5%/次 × 5 次 = 2.5% 損耗 → **淨年化約 35-45%**。

組合建議 **5-8 檔**分散單一事件風險，Sharpe 能從單檔 1.67 提升到組合 2.0+。

---

### 五、三個最容易犯的錯

1. **短抱** — 看到 +5% 就賣。QM 的 alpha 集中在 20-60 天，短抱等於丟掉 2/3 報酬
2. **當抄底用** — 跌下來加碼 QM 名單。QM 要求 `trend_score >= 1`，跌破趨勢後這檔已經不再是 QM
3. **忽略營收** — 只看技術停損。F-Score/營收是最強因子（IC +0.113），營收崩是比週 MA20 跌破**更早**的警報

---

> 選出個股後，下方「個股操作建議」區塊的 `strategy / 停損 / 三段停利 / 出場訊號`
> 已依本 SOP 自動計算並顯示（驗證錨點：Round 4 F50/Body30/Trend20 權重）。
""")

    # ====================================================================
    # Tab Convergence: 多策略共振 (hidden)
    # ====================================================================
    if False:  # hidden tab
        st.markdown("### 🔀 多策略共振")

        with st.expander("📋 共振偵測說明"):
            st.markdown("""
**同時出現在多個掃描模式的股票 = 多策略共振**

所有模式（動能/波段/品質選股/價值）各自獨立掃描後，系統自動交叉比對，找出重疊的股票。

**共振等級**

| Tier | 條件 | 意義 |
|------|------|------|
| **T1** | 動能類 + 價值 | 技術面強勢 + 基本面便宜 = 最高信號 |
| **T2** | 純動能交叉 | 多個技術模式認同，但缺基本面驗證 |

**為什麼共振重要？**
- 單一模式可能有偏差（動能追高、價值陷阱）
- 多策略同時選中 = 不同角度的共識
- 共振本身是稀缺事件（通常 0~5 支），每一支都值得關注
""")

        # TW convergence
        _conv_tw_file = _Path('data/latest/convergence_result.json')
        _conv_tw = None
        if _conv_tw_file.exists():
            try:
                with open(_conv_tw_file, 'r', encoding='utf-8') as _f:
                    _conv_tw = _json.load(_f)
            except Exception:
                _conv_tw = None

        # US convergence
        _conv_us_file = _Path('data/latest/convergence_us_result.json')
        _conv_us = None
        if _conv_us_file.exists():
            try:
                with open(_conv_us_file, 'r', encoding='utf-8') as _f:
                    _conv_us = _json.load(_f)
            except Exception:
                _conv_us = None

        _has_any = ((_conv_tw and _conv_tw.get('results'))
                    or (_conv_us and _conv_us.get('results')))

        if _has_any:
            for _conv_label, _conv_data in [('台股', _conv_tw), ('美股', _conv_us)]:
                if not _conv_data or not _conv_data.get('results'):
                    continue
                _cr = _conv_data['results']
                st.markdown(f"#### {_conv_label} ({len(_cr)} 支共振)")
                st.caption(f"偵測日期: {_conv_data.get('scan_date', '?')} {_conv_data.get('scan_time', '')}")

                _conv_rows = []
                for r in _cr:
                    _modes_str = ' + '.join(r.get('modes', []))
                    _conv_rows.append({
                        '代號': r['stock_id'],
                        '名稱': r.get('name', ''),
                        '收盤': r.get('price', 0),
                        '漲跌%': r.get('change_pct', 0),
                        'Tier': f"T{r.get('convergence_tier', '?')}",
                        '模式': _modes_str,
                        '模式數': r.get('mode_count', 0),
                        '觸發分數': r.get('trigger_score'),
                        '趨勢分數': r.get('trend_score'),
                        '價值分數': r.get('value_score'),
                        'PE': r.get('PE'),
                        '殖利率%': r.get('dividend_yield'),
                        '訊號': ', '.join(r.get('signals', [])[:3]),
                    })
                _df_conv = pd.DataFrame(_conv_rows)
                _df_conv.index = range(1, len(_df_conv) + 1)

                st.dataframe(
                    _df_conv,
                    width='stretch',
                    column_config={
                        '觸發分數': st.column_config.NumberColumn(format="%.1f"),
                        '趨勢分數': st.column_config.NumberColumn(format="%.1f"),
                        '價值分數': st.column_config.NumberColumn(format="%.1f"),
                        '漲跌%': st.column_config.NumberColumn(format="%.1f%%"),
                        '收盤': st.column_config.NumberColumn(format="%.1f"),
                        'PE': st.column_config.NumberColumn(format="%.1f"),
                        '殖利率%': st.column_config.NumberColumn(format="%.1f%%"),
                    },
                )

                # 個股細節
                for r in _cr:
                    ranks = r.get('mode_ranks', {})
                    ranks_str = ', '.join(f"{m} #{rk}" for m, rk in ranks.items())
                    vs = r.get('value_scores', {})
                    vs_str = ' / '.join(f"{k}={v:.0f}" for k, v in vs.items()) if vs else ''
                    with st.expander(f"{r['stock_id']} {r.get('name', '')} — T{r.get('convergence_tier', '?')} [{' + '.join(r.get('modes', []))}]"):
                        st.markdown(f"**模式排名**: {ranks_str}")
                        if r.get('trigger_score') is not None:
                            st.markdown(f"**觸發分數**: {r['trigger_score']:+.1f} / 趨勢: {r.get('trend_score', 0):+.1f}")
                        if r.get('value_score') is not None:
                            st.markdown(f"**價值分數**: {r['value_score']:.1f} ({vs_str})")
                        if r.get('signals'):
                            st.markdown(f"**訊號**: {', '.join(r['signals'])}")
        else:
            st.info("尚無共振結果。\n\n"
                    "共振偵測在所有 scanner 模式跑完後自動執行。\n"
                    "執行 `python scanner_job.py --mode all` 後會自動產出共振結果。\n\n"
                    "共振本身是稀缺事件，結果為 0 是正常的。")

        st.caption("💡 共振偵測自動執行於 `--mode all` / `--mode both` 掃描後")

    # ====================================================================
    # Tab 2: 左側價值選股 (VF-VC P3-b 2026-04-21 恢復, 權重 30/25/30/15/0)
    # ====================================================================
    with screener_tab2:

        # ----------------------------------------------------------------
        # Regime Badge：告訴使用者今天該不該啟用 Value 池
        # (2026-04-23 Value Portfolio 回測發現：純 Value 單獨用不如大盤，
        #  Value+only_volatile 才是 Sharpe 0.932 最佳 — 見
        #  project_value_portfolio_backtest.md)
        # ----------------------------------------------------------------
        _regime_entry = None
        try:
            _regime_log = _Path('data/tracking/regime_log.jsonl')
            if _regime_log.exists():
                _lines = _regime_log.read_text(encoding='utf-8').strip().split('\n')
                if _lines:
                    _regime_entry = _json.loads(_lines[-1])
        except Exception:
            _regime_entry = None

        if _regime_entry:
            _r = _regime_entry.get('regime', 'unknown')
            _rdate = _regime_entry.get('date', '?')
            _range20 = _regime_entry.get('range_20d')
            _ret20 = _regime_entry.get('ret_20d')
            _range_str = f"{_range20*100:.1f}%" if _range20 is not None else "N/A"
            _ret_str = f"{_ret20*100:+.1f}%" if _ret20 is not None else "N/A"
            if _r == 'volatile':
                st.success(
                    f"✅ **Regime = volatile** ({_rdate}): range_20d={_range_str}, ret_20d={_ret_str} "
                    f"→ **建議啟用 Value 池**（回測 Sharpe 0.932 / MDD -12.79%）"
                )
            else:
                st.warning(
                    f"⚠️ **Regime = {_r}** ({_rdate}): range_20d={_range_str}, ret_20d={_ret_str} "
                    f"→ 非 volatile 期建議 **sit out**（純 Value top-20 在非 volatile 回測 CAGR 輸 TWII -2.3pp）"
                )
        else:
            st.caption("⚠️ 尚無 regime 資料（`data/tracking/regime_log.jsonl` 缺）— 建議搭 Dual 50/50 策略使用")

        st.caption(
            "📌 **為何要搭 regime filter？** 純 Value top-20 月頻 rebalance 回測 2020-2025 "
            "CAGR 12.55% 輸 TWII 14.82%、MDD -44.7%；加上 only_volatile filter 後 "
            "CAGR 15.05% / **Sharpe 0.932** / **MDD -12.79%**（2022 空頭翻正）。"
            "詳見 `reports/vf_value_portfolio_backtest_only_volatile.md`。"
        )

        # 篩選條件說明已挪到頁面最下方 (user req 2026-04-30)

        value_file = _Path('data/latest/value_result.json')
        value_result = None
        if value_file.exists():
            try:
                with open(value_file, 'r', encoding='utf-8') as _f:
                    value_result = _json.load(_f)
            except Exception:
                value_result = None

        if value_result and value_result.get('results'):
            v_results = value_result['results']
            st.caption(
                f"掃描日期: {value_result.get('scan_date', '?')} {value_result.get('scan_time', '')} | "
                f"全市場 {value_result.get('total_scanned', 0)} 檔 → "
                f"初篩 {value_result.get('passed_initial', 0)} 檔 → "
                f"評分 {value_result.get('scored_count', 0)} 檔 | "
                f"耗時 {value_result.get('elapsed_seconds', 0):.0f}s"
            )
            if _qm_value_resonance_tw:
                _res_in_val = [r['stock_id'] for r in v_results if r['stock_id'] in _qm_value_resonance_tw]
                if _res_in_val:
                    st.success(f"✨ **動能+價值共振** ({len(_res_in_val)} 檔): {', '.join(_res_in_val)} — 同時通過兩個 screener 的稀有組合")
            _bypass_picks = [r['stock_id'] for r in v_results if r.get('bypass_reason') == 'large_cap_graham_exempt']
            if _bypass_picks:
                st.info(f"🏛️ **大型股例外通道** ({len(_bypass_picks)} 檔): {', '.join(_bypass_picks)} — 市值前 50 + F-Score≥5，被 Graham PE×PB≤22.5 擋下但放行")

            _v_rows = []
            for r in v_results:
                s = r.get('scores', {})
                _v_rows.append({
                    '代號': r['stock_id'],
                    '名稱': r.get('name', ''),
                    '共振': '✨' if r['stock_id'] in _qm_value_resonance_tw else '',
                    '大型股': '🏛️' if r.get('bypass_reason') == 'large_cap_graham_exempt' else '',
                    '週榜': _wc_tags_short(r['stock_id']),
                    '題材': _theme_tags_short(r['stock_id']),
                    '綜合分數': r.get('value_score', 0),
                    '收盤': r.get('price', 0),
                    'PE': r.get('PE', 0),
                    'PB': r.get('PB', 0),
                    '殖利率%': r.get('dividend_yield', 0),
                    '均量(億)': round(r.get('avg_trading_value_5d', 0) / 1e8, 2),
                    '估值': s.get('valuation', 0),
                    '體質': s.get('quality', 0),
                    '營收': s.get('revenue', 0),
                    # 技術轉折 欄位隱藏 — VF-VD 驗證 2026-04-19 所有加分反 alpha 砍除，全為 50 baseline
                    '聰明錢': s.get('smart_money', 0),
                })
            _v_df_results = pd.DataFrame(_v_rows)

            _sort_opts_v = {
                '綜合分數 (高→低)': ('綜合分數', False),
                '均量(億) (高→低)': ('均量(億)', False),
                '殖利率% (高→低)': ('殖利率%', False),
                'PE (低→高)': ('PE', True),
            }
            _v_sort = st.selectbox("排序方式", list(_sort_opts_v.keys()), key='value_tw_sort')
            _v_sc, _v_sa = _sort_opts_v[_v_sort]
            _v_df_results = _v_df_results.sort_values(_v_sc, ascending=_v_sa).reset_index(drop=True)
            _v_df_results.index = range(1, len(_v_df_results) + 1)

            st.dataframe(
                _v_df_results,
                width='stretch',
                height=int(38 + len(_v_df_results) * 35 + 3),
                column_config={
                    '共振': st.column_config.TextColumn(width='small', help="✨ = 同時出現在動能+價值選股（便宜+轉強組合）"),
                    '大型股': st.column_config.TextColumn(width='small', help="🏛️ = 走大型股 Graham 例外通道（市值前 50 + F-Score>=5 + quality>=50），PE×PB>22.5 但被放行"),
                    '週榜': st.column_config.TextColumn(width='medium', help="本週三大法人榜單上的標記（連買/連賣天數 + 4 維度排名）"),
                    '題材': st.column_config.TextColumn(width='medium', help="4 層融合：sector_tags_manual.json AI era 題材 (140 ticker) → News RSS 萃取 (30d) → YT 法說提及 (180d) → TV industry 中文 fallback；最多顯示 2 個 + 餘數"),
                    '綜合分數': st.column_config.NumberColumn(format="%.1f"),
                    'PE': st.column_config.NumberColumn(format="%.1f"),
                    'PB': st.column_config.NumberColumn(format="%.2f"),
                    '殖利率%': st.column_config.NumberColumn(format="%.1f%%"),
                    '收盤': st.column_config.NumberColumn(format="%.1f"),
                    '均量(億)': st.column_config.NumberColumn(format="%.2f"),
                },
            )
            st.caption("綜合分數 0~100 (估值 25% + 體質 25% + 營收 25% + 技術 15% + 毛利邊際 10% + 聰明錢 0%) [VF-GM 2026-04-27]")

            # Detailed scoring
            with st.expander("個股詳細評分"):
                _v_selected = st.selectbox(
                    "選擇股票",
                    options=[f"{r['stock_id']} {r.get('name', '')}" for r in v_results],
                    key='value_detail_select',
                )
                if _v_selected:
                    _v_sid = _v_selected.split()[0]
                    _v_match = next((r for r in v_results if r['stock_id'] == _v_sid), None)
                    if _v_match:
                        _vs = _v_match.get('scores', {})
                        st.markdown(
                            f"**{_v_sid} {_v_match.get('name', '')}** — "
                            f"綜合: {_v_match['value_score']:.1f} | "
                            f"估值: {_vs.get('valuation', 0):.0f} | "
                            f"體質: {_vs.get('quality', 0):.0f} | "
                            f"營收: {_vs.get('revenue', 0):.0f} | "
                            f"技術: {_vs.get('technical', 0):.0f} | "
                            f"聰明錢: {_vs.get('smart_money', 0):.0f}"
                        )
                        for d in _v_match.get('details', []):
                            st.markdown(f"- {d}")

                        # Value-#5b 左側分批進場 SOP（2026-04-23）
                        _ap = _v_match.get('action_plan')
                        if _ap:
                            st.markdown("---")
                            st.markdown("### 📋 左側操作 SOP")
                            _col_a, _col_b = st.columns([1, 1])
                            with _col_a:
                                st.markdown(f"**進場區間**: {_ap['entry_low']} ~ {_ap['entry_high']}")
                                _batch_rows = "\n".join([
                                    f"| {b['pct']}% | {b['price']} | {b['trigger']} |"
                                    for b in _ap.get('entry_batches', [])
                                ])
                                st.markdown(
                                    "| 批次 | 價位 | 觸發 |\n"
                                    "|---|---|---|\n"
                                    f"{_batch_rows}"
                                )
                            with _col_b:
                                st.markdown(
                                    f"**停損**: {_ap['stop_loss']} ({_ap['stop_method']}, "
                                    f"{_ap['stop_loss_pct']:+.1f}%)"
                                )
                                _tp_rows = "\n".join([
                                    f"| TP{t['tier']} | {t['price']} | +{t['pct']:.1f}% | {t['method']} | {t['action']} |"
                                    for t in _ap.get('tp_list', [])
                                ])
                                st.markdown(
                                    "| 階段 | 目標 | 漲幅 | 方法 | 動作 |\n"
                                    "|---|---|---|---|---|\n"
                                    f"{_tp_rows}"
                                )
                                st.caption(f"建議持倉: {_ap['horizon_days']} 天")
                            st.info(_ap['strategy_text'])

        else:
            st.info("尚無掃描結果。\n\n"
                    "在命令列執行 `python scanner_job.py --mode value` 進行完整掃描\n"
                    "（含 5 維評分，約需 20-40 分鐘）")

        # ====================================================================
        # 頁面最下方：篩選條件說明 (2026-04-30 從頁首挪到頁尾)
        # ====================================================================
        with st.expander("📋 篩選條件說明"):
            st.markdown("""
**Stage 1 初篩**

| 條件 | 門檻 | 說明 |
|------|------|------|
| PE (本益比) | 0.1 ~ 12 | 排除虧損股和高估值股（VF-VA B 級落地） |
| PB (股價淨值比) | ≤ 3.0 | 排除資產泡沫股 |
| Graham 複合 | PE × PB ≤ 22.5 | PE 或 PB 單邊可偏高，乘積需合理 |
| 成交值 | > 3000 萬 | 機構可交易水準 |
| 🏛️ 大型股例外 | 市值前 50 + F≥5 + Q≥50 + PE≤50 | Value-#4 通道：台積/中華電類被 Graham 擋下但體質佳者放行 |

**Stage 2 綜合評分（0-100 分）** — VF-GM 落地 2026-04-27

| 面向 | 權重 | 評分項目 | 加分/扣分規則 |
|------|------|----------|---------------|
| **估值** | 25% | PE/PB 高低、歷史分位、殖利率、PEG、DDM 折價 | PE<8 +25, PB<1 +15, 殖利率>6% +10, PEG<0.5 +12 |
| **體質** | 25% | Piotroski F-Score、Altman Z-Score、ROIC、FCF Yield | F≥7/9 +25, Z-Score 安全 +8, ROIC>15% +8 |
| **營收** | 25% | 月營收 YoY 趨勢、營收驚喜 | YoY轉正 +10, 衰退收斂 +改善幅度×2, 驚喜 +12 |
| **技術轉折** | 15% | RSI 超賣、量能萎縮、BB 壓縮、距 52 週低點 | RSI<30 +20, RVOL<0.5 +15, 近低點10% +15 |
| **毛利邊際** | 10% | GM QoQ Δ（單季毛利率 vs 上一季）| Δ>+3pp +20, +1<Δ≤+3 +10, 持平 0, -3≤Δ<-1 -10, Δ<-3 -20（F2 A 級 IR=+0.872）|
| **聰明錢** | 0% | (已停用，VF-VE D 級無 alpha) | — |

**體質指標說明**

| 指標 | 說明 |
|------|------|
| **Piotroski F-Score** | 9 項財務健康指標（獲利/槓桿/效率），7 分以上為強健 |
| **Altman Z-Score** | 破產風險指標，>2.99 安全，<1.81 有風險 |
| **ROIC** | 投入資本報酬率，衡量公司用資本賺錢的效率 |
| **FCF Yield** | 自由現金流殖利率，衡量實際產生的現金回報 |
| **PEG** | PE / 盈餘成長率，<1 表示成長相對估值便宜 |
| **DDM** | 股利折現模型，估算合理股價與目前折溢價 |
""")

    # ====================================================================
    # Tab US Value: 美股價值選股
    # 2026-04-22 (再隱藏): VF-Value-ex2 EDGAR walk-forward D 級反向，
    # US 全 signal 未經 IC 驗證。保留程式碼待未來 US QM 驗證後恢復。
    # ====================================================================
    if False:  # screener_tab_us_val — 隱藏，待 US QM/Value VF 驗證完成

        with st.expander("📋 Screening Criteria"):
            st.markdown("""
**Stage 1 Initial Filter**

| Criteria | Threshold | Description |
|----------|-----------|-------------|
| Universe | S&P 500 | 掃描範圍 |
| Min Price | > $5.00 | 排除低價股 |
| Min Volume | > 500,000 | 過濾低流動性 |

**Stage 2 Scoring (0-100)**

| Dimension | Weight | Metrics | Scoring Examples |
|-----------|--------|---------|------------------|
| **Valuation** | 30% | PE/PB, Forward PE, Finviz PEG, DDM, Analyst Target | PEG<0.5 +12, Target>30% +10 |
| **Quality** | 25% | F-Score (info), Z-Score, Current Ratio | Z safe +8, F≤3 -20, ROIC/FCF info only (D noise) |
| **Revenue** | 15% | Sales Q/Q, EPS Q/Q, Revenue YoY trend | Sales Q/Q>20% +15, EPS Q/Q>25% +10 |
| **Technical** | 15% | RSI oversold, Volume dry-up, BB squeeze, 52W low | RSI<30 +20, Near 52W low +15 |
| **Smart Money** | 15% | Institutional %, Short interest, Insider activity | Inst>80% +10, Insider bullish +12, Short>10% -10 |

**Key Metrics**

| Metric | Description |
|--------|-------------|
| **F-Score** | Piotroski 9-point financial health (≥7 = strong) |
| **Z-Score** | Altman bankruptcy risk (>2.99 safe, <1.81 distress) |
| **ROIC** | Return on invested capital |
| **FCF Yield** | Free cash flow yield |
| **PEG** | PE / Earnings growth, <1 = undervalued |
| **Forward PE** | PE based on estimated future earnings |
| **Short %** | Short interest as % of float, >10% = risky |
""")

        us_val_file = _Path('data/latest/value_us_result.json')
        us_val_result = None
        if us_val_file.exists():
            try:
                with open(us_val_file, 'r', encoding='utf-8') as _f:
                    us_val_result = _json.load(_f)
            except Exception:
                us_val_result = None

        if us_val_result and us_val_result.get('results'):
            uv_results = us_val_result['results']
            st.caption(
                f"Scan: {us_val_result.get('scan_date', '?')} {us_val_result.get('scan_time', '')} | "
                f"Scored: {us_val_result.get('scored_count', 0)} | "
                f"Time: {us_val_result.get('elapsed_seconds', 0):.0f}s"
            )
            _uv_rows = []
            for r in uv_results:
                s = r.get('scores', {})
                _uv_rows.append({
                    'Ticker': r['stock_id'],
                    'Score': r.get('value_score', 0),
                    'Price': r.get('price', 0),
                    'PE': r.get('PE', 0),
                    'PB': r.get('PB', 0),
                    'DY%': r.get('dividend_yield', 0),
                    'TV(M)': round(r.get('avg_trading_value_5d', 0) / 1e6, 1),
                    'Val': s.get('valuation', 0),
                    'Qual': s.get('quality', 0),
                    'Tech': s.get('technical', 0),
                    'Smart$': s.get('smart_money', 0),
                })
            _uv_df = pd.DataFrame(_uv_rows)

            _sort_opts_uv = {
                'Score (High→Low)': ('Score', False),
                'TV(M) (High→Low)': ('TV(M)', False),
                'DY% (High→Low)': ('DY%', False),
                'PE (Low→High)': ('PE', True),
            }
            _uv_sort = st.selectbox("Sort by", list(_sort_opts_uv.keys()), key='value_us_sort')
            _uv_sc, _uv_sa = _sort_opts_uv[_uv_sort]
            _uv_df = _uv_df.sort_values(_uv_sc, ascending=_uv_sa).reset_index(drop=True)
            _uv_df.index = range(1, len(_uv_df) + 1)

            st.dataframe(
                _uv_df,
                width='stretch', height=600,
                column_config={
                    'Score': st.column_config.NumberColumn(format="%.1f"),
                    'Price': st.column_config.NumberColumn(format="$%.2f"),
                    'PE': st.column_config.NumberColumn(format="%.1f"),
                    'TV(M)': st.column_config.NumberColumn(format="%.1f"),
                },
            )
            st.caption("Score 0~100 (valuation + quality + revenue + technical + smart money)")
            with st.expander("Detailed Scores"):
                _uv_sel = st.selectbox("Select", [r['stock_id'] for r in uv_results], key='us_val_detail')
                if _uv_sel:
                    _uv_m = next((r for r in uv_results if r['stock_id'] == _uv_sel), None)
                    if _uv_m:
                        for d in _uv_m.get('details', []):
                            st.markdown(f"- {d}")
        else:
            st.info("No US value scan results yet.\n\n"
                    "Run: `python scanner_job.py --mode value --market us`")

        st.caption("💡 Full scan: `python scanner_job.py --mode value --market us`")

    # ====================================================================
    # Tab: 短線均值回歸 (P3)
    # ====================================================================
    with screener_tab_meanrev:

        st.markdown("""
**短線均值回歸掃描** — 找出超賣/超買股票，供 **1-3 天**短線操作參考。

獨立於 Scanner 動能策略，用 5 個高度相關的均值回歸指標（MA20偏離/VWAP偏離/BB%B/RSI偏離/EFI）合成單一 MeanRev Composite。
MeanRev 越負 = 越超賣（買入候選），越正 = 越超買（避開）。

| 驗證項目 | 數據 |
|----------|------|
| IC (1d) | +0.060 (75.5% 勝率) |
| IC (5d) | +0.055 (73.3% 勝率) |
| Walk-forward 1d | IS +1.67 → OOS +0.89 (**-47% 衰退**，短線最不穩定) |
| Walk-forward 5-20d | OOS > IS (穩健) |

> **注意**: 此策略 1d horizon 在 out-of-sample 有顯著衰退。建議持倉 **2-5 天**而非隔日沖，
> 並搭配 RSI < 30 + BIAS < -5% 雙重確認再進場。10 天後信號衰退，不適合長抱。
""")
        with st.expander("MeanRev Composite 用在哪些地方"):
            st.markdown("""
MeanRev Composite 是 5 個高度相關指標（corr 0.78-0.93）的 252 日 z-score 均值：

| 用途 | 模組 | 說明 |
|------|------|------|
| **動能 Scanner T1 信號** | `analysis_engine.py` | 取代原本的 binary MA20 → tanh(MeanRev) 連續值 [-1,+1]，讓趨勢組中位數更平滑 |
| **本 Tab 超賣/超買掃描** | `tools/meanrev_scanner.py` | 排序 MeanRev 最負（超賣）/ 最正（超買）的股票 |
| **個股分析技術圖表** | `technical_analysis.py` | 計算並存入 DataFrame，供 AI 報告參考 |

不直接影響：價值選股、籌碼面評分、週線趨勢分數。
""")

        _mr_top_n = st.slider("顯示前 N 檔", 5, 50, 20, key='mr_top_n')
        _mr_source = st.radio(
            "掃描範圍",
            ["最近 Scanner Picks (快速)", "所有快取股票 (完整)"],
            key='mr_source', horizontal=True
        )

        if st.button("開始掃描", key='mr_scan_btn'):
            with st.spinner("掃描中..."):
                from tools.meanrev_scanner import get_stock_ids, scan
                import types
                _mr_args = types.SimpleNamespace(
                    stocks=None,
                    all=(_mr_source != "最近 Scanner Picks (快速)"),
                )
                _mr_ids = get_stock_ids(_mr_args)
                if not _mr_ids:
                    st.warning("無可掃描股票。請先執行 Scanner 或使用「所有快取股票」。")
                else:
                    _mr_results = scan(_mr_ids, _mr_top_n)
                    _mr_tw = [r for r in _mr_results if r['market'] == 'tw']
                    _mr_us = [r for r in _mr_results if r['market'] == 'us']
                    st.success(f"掃描完成: {len(_mr_results)} 檔 (台股 {len(_mr_tw)} / 美股 {len(_mr_us)})")

                    def _mr_table(data, top_n, is_tw=True):
                        """Build DataFrame for display."""
                        if not data:
                            st.info("無資料")
                            return
                        df = pd.DataFrame(data)
                        df.index = range(1, len(df) + 1)
                        if is_tw:
                            df = df[['stock_id', 'name', 'close', 'meanrev', 'rsi', 'bias']]
                            df.columns = ['代號', '名稱', '收盤', 'MeanRev', 'RSI', 'BIAS%']
                        else:
                            df = df[['stock_id', 'close', 'meanrev', 'rsi', 'bias']]
                            df.columns = ['Ticker', 'Price', 'MeanRev', 'RSI', 'BIAS%']
                        st.dataframe(df, use_container_width=True, column_config={
                            'MeanRev': st.column_config.NumberColumn(format="%+.3f"),
                            'RSI': st.column_config.NumberColumn(format="%.0f"),
                            'BIAS%': st.column_config.NumberColumn(format="%+.1f"),
                        })

                    # === 台股 ===
                    if _mr_tw:
                        st.markdown("### 🇹🇼 台股")
                        _c1, _c2 = st.columns(2)
                        with _c1:
                            st.markdown(f"**📉 超賣 Top {_mr_top_n}**")
                            _mr_table(_mr_tw[:_mr_top_n], _mr_top_n, is_tw=True)
                        with _c2:
                            st.markdown(f"**📈 超買 Top {_mr_top_n}**")
                            _mr_table(list(reversed(_mr_tw[-_mr_top_n:])), _mr_top_n, is_tw=True)

                    # === 美股 ===
                    if _mr_us:
                        st.markdown("### 🇺🇸 美股")
                        _c3, _c4 = st.columns(2)
                        with _c3:
                            st.markdown(f"**📉 Oversold Top {_mr_top_n}**")
                            _mr_table(_mr_us[:_mr_top_n], _mr_top_n, is_tw=False)
                        with _c4:
                            st.markdown(f"**📈 Overbought Top {_mr_top_n}**")
                            _mr_table(list(reversed(_mr_us[-_mr_top_n:])), _mr_top_n, is_tw=False)

        st.caption("💡 CLI: `python tools/meanrev_scanner.py --top 20`")

    # ====================================================================
    # Tab: 績效追蹤
    # ====================================================================
    with screener_tab_track:

        st.markdown("""
**品質選股績效追蹤** — 追蹤品質選股 (QM) 選出的股票在 5 / 10 / 20 / 40 / 60 個交易日後的表現。
每次掃描後自動更新，資料越多越有參考價值。
""")

        try:
            from scan_tracker import ScanTracker
            _tracker = ScanTracker()
            _track_data = _tracker.load_latest()
            _summary = _track_data.get('summary', {})
            _updated = _track_data.get('updated_at', '')

            if _summary:
                if _updated:
                    st.caption(f"最後更新: {_updated[:19]}")

                _type_labels = {
                    'qm': '品質選股', 'momentum': '動能', 'value': '價值',
                    'swing': '波段', 'convergence': '共振',
                }
                for _tk, _ts in _summary.items():
                    # Only show QM tracks
                    if _ts.get('scan_type') != 'qm':
                        continue
                    _type_label = _type_labels.get(_ts['scan_type'], _ts['scan_type'])
                    _mkt_label = '台股' if _ts['market'] == 'tw' else '美股'
                    st.markdown(f"#### {_type_label} ({_mkt_label})")
                    st.caption(f"掃描次數: {_ts['total_scans']} | 總選股: {_ts['total_picks']}")

                    _perf_rows = []
                    for _d in [5, 10, 20, 40, 60]:
                        _tracked = _ts.get(f'tracked_{_d}d', 0)
                        if _tracked > 0:
                            _perf_rows.append({
                                '追蹤天數': f'{_d}d',
                                '追蹤檔數': _tracked,
                                '勝率': f"{_ts.get(f'win_rate_{_d}d', 0):.1f}%",
                                '平均報酬': f"{_ts.get(f'avg_return_{_d}d', 0):+.2f}%",
                                '中位數': f"{_ts.get(f'median_return_{_d}d', 0):+.2f}%",
                                '最佳': f"{_ts.get(f'best_{_d}d', 0):+.2f}%",
                                '最差': f"{_ts.get(f'worst_{_d}d', 0):+.2f}%",
                            })
                        else:
                            _perf_rows.append({
                                '追蹤天數': f'{_d}d',
                                '追蹤檔數': 0,
                                '勝率': '—',
                                '平均報酬': '—',
                                '中位數': '—',
                                '最佳': '—',
                                '最差': '—',
                            })

                    if _perf_rows:
                        st.dataframe(pd.DataFrame(_perf_rows), width='stretch', hide_index=True)

                    # Benchmark IR (BM-b)
                    _bm_data = _ts.get('benchmarks', {})
                    if _bm_data:
                        from scan_tracker import _bm_display_name
                        _ir_rows = []
                        for _bm, _horizons in _bm_data.items():
                            _bm_label = _bm_display_name(_bm)
                            for _d in [5, 10, 20, 40, 60]:
                                _h = _horizons.get(f'{_d}d')
                                if _h:
                                    _ir_rows.append({
                                        'Benchmark': _bm_label,
                                        'Horizon': f'{_d}d',
                                        'N': _h['n'],
                                        'Excess': f"{_h['avg_excess']:+.2f}%",
                                        'TE': f"{_h['tracking_error']:.2f}%",
                                        'IR': f"{_h['ir']:+.3f}",
                                        'Win vs BM': f"{_h['win_rate_vs_bm']:.1f}%",
                                    })
                        if _ir_rows:
                            st.markdown("**vs Benchmark (Information Ratio)**")
                            st.dataframe(pd.DataFrame(_ir_rows), width='stretch', hide_index=True)

                # Detailed picks table
                with st.expander("個股追蹤明細"):
                    _track_mkt = st.selectbox("市場", ['tw', 'us'], key='track_mkt_sel',
                                              format_func=lambda x: '台股' if x == 'tw' else '美股')
                    _picks_df = _tracker.get_picks_dataframe('qm', _track_mkt)
                    if not _picks_df.empty:
                        _show_cols = ['scan_date', 'stock_id', 'name', 'price_at_scan']
                        if 'trigger_score' in _picks_df.columns:
                            _show_cols.append('trigger_score')
                        for _d in [5, 10, 20, 40, 60]:
                            col = f'return_{_d}d'
                            if col in _picks_df.columns:
                                _show_cols.append(col)
                        _show_cols = [c for c in _show_cols if c in _picks_df.columns]
                        st.dataframe(_picks_df[_show_cols], width='stretch', height=400)
                    else:
                        st.info("尚無品質選股追蹤資料")

            else:
                st.info("尚無績效追蹤資料。\n\n"
                        "Scanner 每次執行後會自動追蹤歷史選股表現。\n"
                        "需要累積至少 5 個交易日的掃描歷史才會出現數據。\n\n"
                        "手動更新: `python scan_tracker.py`")

        except Exception as _track_err:
            st.warning(f"追蹤模組載入失敗: {_track_err}")

    # ====================================================================
    # Mode D tab (2026-04-25): thesis-driven discretionary 策略展示
    # QM 機械層 + C1 tilt + YT mention + scenario entry
    # ====================================================================
    with screener_tab_mode_d:
        st.markdown("### 🎯 Mode D — Hybrid Thesis-Driven Discretionary")
        st.caption("QM 機械選股 + C1 月營收拐點 tilt + YT 節目 mention + scenario 進場計畫 → 人工拍板下單（無 API 自動交易）")

        _mode_d_sub1, _mode_d_sub2, _mode_d_sub3, _mode_d_sub4 = st.tabs([
            "📋 今日 Pick", "📺 YT 熱度榜", "📈 C1 拐點清單", "🎯 Thesis Panel"
        ])

        # Lazy-load panels shared by subtabs
        import pandas as _pd_d
        _yt_panel = None
        try:
            _yt_path = _Path('data/sector_tags_dynamic.parquet')
            if _yt_path.exists():
                _yt_panel = _pd_d.read_parquet(_yt_path)
        except Exception:
            pass
        _c1_panel = None
        try:
            _c1_path = _Path('data/c1_tilt_flags.parquet')
            if _c1_path.exists():
                _c1_panel = _pd_d.read_parquet(_c1_path)
        except Exception:
            pass

        # ---- Sub 1: 今日 Pick ----
        with _mode_d_sub1:
            try:
                _qm_file = _Path('data/latest/qm_result.json')
                if not _qm_file.exists():
                    st.info("尚無 QM 選股結果，等 Scanner TUE-SAT 00:00 跑完。")
                else:
                    with open(_qm_file, 'r', encoding='utf-8') as _f:
                        _qm_data = _json.load(_f)
                    _picks = _qm_data.get('results', [])[:10]
                    _scan_dt = f"{_qm_data.get('scan_date', '?')} {_qm_data.get('scan_time', '')}"
                    st.caption(f"QM top 10 ({_scan_dt})")

                    # Build c1 tilt lookup
                    _c1_tilt_set = set()
                    _is_ai_era = False
                    if _c1_panel is not None and not _c1_panel.empty:
                        _is_ai_era = bool(_c1_panel['is_ai_era'].iloc[0])
                        _c1_tilt_set = set(
                            _c1_panel[_c1_panel['c1_tilt_on']]['stock_id'].astype(str).tolist()
                        )
                    st.caption(f"Regime: {'🟢 AI era (C1 tilt ON)' if _is_ai_era else '⚪ Pre-AI (C1 tilt OFF)'}")

                    from datetime import date as _date_d, timedelta as _td_d
                    _cutoff = _date_d.today() - _td_d(days=7)

                    _rows = []
                    for i, _r in enumerate(_picks, 1):
                        _sid = str(_r.get('stock_id', ''))
                        # YT mention
                        _yt_cnt = 0
                        _yt_sent = 0.0
                        _yt_shows = []
                        if _yt_panel is not None and not _yt_panel.empty:
                            _sub = _yt_panel[(_yt_panel['ticker'] == _sid) & (_yt_panel['date'] >= _cutoff)]
                            _yt_cnt = len(_sub)
                            if _yt_cnt > 0:
                                _yt_sent = _sub['sentiment'].mean()
                                _yt_shows = _sub['show_key'].unique().tolist()
                        _sent_icon = "🟢" if _yt_sent > 0.3 else ("🔴" if _yt_sent < -0.3 else "⚪")
                        _yt_str = f"{_sent_icon}×{_yt_cnt}" if _yt_cnt > 0 else "—"

                        # C1 tilt
                        _c1 = "✅" if _sid in _c1_tilt_set else "—"

                        # Scenario entry
                        _ap = _r.get('action_plan') or {}
                        _entry_low = _ap.get('rec_entry_low')
                        _entry_high = _ap.get('rec_entry_high')
                        _sl = _ap.get('rec_sl_price')
                        _tp = _ap.get('rec_tp_price')
                        _scenario = _ap.get('scenario_code', '-')

                        _rows.append({
                            '#': i,
                            '代號': _sid,
                            '名稱': _r.get('name', '')[:6],
                            'QM 分': round(_r.get('composite_score', 0), 1),
                            '觸發': f"{_r.get('trigger_score', 0):+.1f}",
                            '劇本': _scenario,
                            '建議進場': f"{_entry_low}-{_entry_high}" if _entry_low else '-',
                            'SL': _sl if _sl else '-',
                            'TP': _tp if _tp else '-',
                            'C1 拐點': _c1,
                            'YT 7d': _yt_str,
                            '週榜': _wc_tags_short(_sid),
                            '題材': _theme_tags_short(_sid),
                        })

                    if _rows:
                        st.dataframe(_pd_d.DataFrame(_rows), use_container_width=True, hide_index=True)
                        st.caption("劇本 A=現價可進 / B=等拉回 5-10MA / C=觀望 / D=空頭避開 | C1 ✅ = 月營收 YoY 拐點 (×1.2 加分) | YT 7d = 近 7 日節目提及次數 | 週榜 = 本週三大法人榜上標記 | 題材 = AI era sector tag (manual + News 30d + YT 180d + TV industry 四層融合)")
                    else:
                        st.info("無 pick 資料")
            except Exception as _e:
                st.warning(f"今日 Pick 載入失敗: {_e}")

        # ---- Sub 2: YT 熱度榜 ----
        with _mode_d_sub2:
            if _yt_panel is None or _yt_panel.empty:
                st.info("尚無 YT mention 資料。跑 `run_yt_sync.bat` 或 `python tools/build_yt_sector_panel.py` 產生。")
            else:
                _window = st.radio("視窗", [7, 14, 30], index=0, horizontal=True, key='_mode_d_yt_window')
                from datetime import date as _date_d2, timedelta as _td_d2
                _cutoff2 = _date_d2.today() - _td_d2(days=_window)
                _recent = _yt_panel[_yt_panel['date'] >= _cutoff2]
                if _recent.empty:
                    st.info(f"近 {_window} 日無 mention 資料")
                else:
                    _agg = _recent.groupby(['ticker', 'name']).agg(
                        mentions=('video_id', 'count'),
                        shows=('show_key', 'nunique'),
                        sentiment_avg=('sentiment', 'mean'),
                        confidence_avg=('confidence', 'mean'),
                    ).reset_index().sort_values('mentions', ascending=False).head(30)
                    _agg['sentiment'] = _agg['sentiment_avg'].apply(
                        lambda v: f"🟢 {v:+.2f}" if v > 0.3 else (f"🔴 {v:+.2f}" if v < -0.3 else f"⚪ {v:+.2f}")
                    )
                    _display = _agg[['ticker', 'name', 'mentions', 'shows', 'sentiment', 'confidence_avg']].copy()
                    _display['confidence_avg'] = _display['confidence_avg'].round(0).astype(int)
                    _display.columns = ['代號', '名稱', '提及次數', '節目數', '情感', '平均信心']
                    st.dataframe(_display, use_container_width=True, hide_index=True)
                    st.caption(f"近 {_window} 日 top 30，來源: 錢線百分百 + 鈔錢部署")

        # ---- Sub 3: C1 拐點清單 ----
        with _mode_d_sub3:
            if _c1_panel is None or _c1_panel.empty:
                st.info("尚無 C1 tilt 資料。跑 `python tools/compute_c1_tilt.py` 產生。")
            else:
                _tilt_on = _c1_panel[_c1_panel['c1_tilt_on']].copy()
                _tilt_on = _tilt_on.sort_values('yoy_m0', ascending=False)
                st.caption(f"C1 tilt ON: {len(_tilt_on)}/{len(_c1_panel)} tickers ({len(_tilt_on)/len(_c1_panel)*100:.1f}%)")
                _c1_display = _tilt_on[['stock_id', 'yoy_m2', 'yoy_m1', 'yoy_m0']].copy()
                _c1_display.columns = ['代號', 'YoY T-2 月', 'YoY T-1 月', 'YoY T 月']
                for _col in ['YoY T-2 月', 'YoY T-1 月', 'YoY T 月']:
                    _c1_display[_col] = _c1_display[_col].apply(
                        lambda v: f"{v:+.1f}%" if _pd_d.notna(v) else '—'
                    )
                st.dataframe(_c1_display.head(50), use_container_width=True, hide_index=True)
                st.caption("近 3 月月營收 YoY 從負轉正 (T-2<-2% AND T>+2% or T-1>+2%)。QM 選股在 AI era 自動 ×1.2 加分。")

        # ---- Sub 4: Thesis Panel (Wave 0 skeleton, Wave 1 填實) ----
        with _mode_d_sub4:
            st.caption("Thesis 層輔助資訊整合（Wave 1 填實，目前為 skeleton）")

            # Section 0: 大盤情緒 (Day 3, 2026-05-01)
            st.markdown("#### 🌡️ 大盤情緒")
            try:
                from market_sentiment import render_market_sentiment_block
                render_market_sentiment_block()
            except Exception as _e:
                st.caption(f"市場情緒模組載入失敗: {_e}")

            st.markdown("---")

            # Section 1: 劇本進行式 (Pair Divergence info display, Wave 1 #1)
            st.markdown("#### 📜 劇本進行式")
            st.caption("同業 pair 近 20 日表現差 — 純觀察，V12 已驗 C 級無 edge，不進 Pick 不發 Discord")

            # V12 12 pairs (from tools/v12_pair_divergence_ic.py)
            _PAIRS = [
                ('ai_server_odm',       'AI 伺服器 ODM',  '2382', '廣達',    '3231', '緯創',     True),
                ('ai_cooling',          'AI 散熱',        '3017', '奇鋐',    '3324', '雙鴻',     True),
                ('abf_substrate',       'ABF 載板',       '3037', '欣興',    '3189', '景碩',     False),
                ('abf_substrate',       'ABF 載板',       '3037', '欣興',    '8046', '南電',     False),
                ('ccl',                 'CCL 銅箔基板',    '2383', '台光電',  '6274', '台燿',     True),
                ('pcb_hard',            'PCB 硬板',       '2368', '金像電',  '3044', '健鼎',     True),
                ('advanced_test',       '先進測試',       '3711', '日月光',  '2449', '京元電',   True),
                ('semi_equipment',      '半導體設備',     '6515', '穎崴',    '6223', '旺矽',     True),
                ('semi_equipment',      '半導體設備',     '6223', '旺矽',    '6510', '中華精測', True),
                ('asic_design_service', 'ASIC 設計服務',  '3443', '創意',    '3661', '世芯',     True),
                ('silicon_wafer',       '矽晶圓',         '6488', '環球晶',  '5483', '中美晶',   True),
                ('optical_lens',        '光學元件',       '3008', '大立光',  '3406', '玉晶光',   True),
            ]

            @st.cache_data(ttl=3600)
            def _load_pair_ohlcv():
                _p = _Path('data_cache/backtest/ohlcv_tw.parquet')
                if not _p.exists():
                    return None
                try:
                    return _pd_d.read_parquet(_p, columns=['stock_id', 'date', 'Close'])
                except Exception:
                    return None

            _ohlcv = _load_pair_ohlcv()
            if _ohlcv is None or _ohlcv.empty:
                st.info("尚無 `data_cache/backtest/ohlcv_tw.parquet` 歷史資料。")
            else:
                _lookback = 20
                _pair_rows = []
                for _tid, _tzh, _a, _a_name, _b, _b_name, _suit in _PAIRS:
                    _da = _ohlcv[_ohlcv['stock_id'] == _a].sort_values('date').tail(_lookback + 1)
                    _db = _ohlcv[_ohlcv['stock_id'] == _b].sort_values('date').tail(_lookback + 1)
                    if len(_da) < _lookback + 1 or len(_db) < _lookback + 1:
                        continue
                    _ra = (_da['Close'].iloc[-1] / _da['Close'].iloc[0] - 1) * 100
                    _rb = (_db['Close'].iloc[-1] / _db['Close'].iloc[0] - 1) * 100
                    _diff = _rb - _ra
                    if _diff > 3:
                        _regime = "🟢 Convergence"
                    elif _diff < -3:
                        _regime = "🔴 Divergence"
                    else:
                        _regime = "⚪ Neutral"
                    _pair_rows.append({
                        '題材': _tzh,
                        'A': f"{_a} {_a_name}",
                        'B': f"{_b} {_b_name}",
                        'A 20d': f"{_ra:+.1f}%",
                        'B 20d': f"{_rb:+.1f}%",
                        'B-A': f"{_diff:+.1f}%",
                        'Regime': _regime,
                        '適用': '✓' if _suit else '✗',
                    })
                if _pair_rows:
                    st.dataframe(_pd_d.DataFrame(_pair_rows), use_container_width=True, hide_index=True)
                    st.caption(
                        "B-A > +3% = Convergence (B 追上 A) / < -3% = Divergence (B 落後) / 其他 = Neutral | "
                        "**適用 ✗** = V12 驗過該題材無 pair signal alpha，僅觀察不當進場依據"
                    )
                else:
                    st.info("無足夠歷史資料計算 pair divergence (需近 20 個交易日)")

            # Section 2: 題材熱度展開 (themes → ticker, Wave 1 #8)
            st.markdown("#### 🔥 題材熱度展開")
            st.caption("節目整集討論題材 + 反查同題材股 (weak signal，不進 Pick 不發 Discord)")
            _videos_path = _Path('data/yt_videos_panel.parquet')
            _sector_json = _Path('data/sector_tags_manual.json')
            from datetime import date as _date_d3, timedelta as _td_d3
            if not _videos_path.exists() or not _sector_json.exists():
                st.info("需要 YT video panel + sector_tags_manual.json")
            else:
                _videos_t = _pd_d.read_parquet(_videos_path)
                _themes_window = st.radio(
                    "視窗 ", [7, 14, 30], index=0, horizontal=True, key='_mode_d_themes_window'
                )
                _cutoff_t = _date_d3.today() - _td_d3(days=_themes_window)
                _recent_vt = _videos_t[_videos_t['date'] >= _cutoff_t]
                # explode themes_discussed
                _theme_rows = []
                for _, _vr in _recent_vt.iterrows():
                    _theme_list = _vr['themes_discussed'] if _vr['themes_discussed'] is not None else []
                    for _tname in list(_theme_list):
                        _theme_rows.append({
                            'theme': str(_tname),
                            'show_key': _vr['show_key'],
                            'video_id': _vr['video_id'],
                        })
                if not _theme_rows:
                    st.info(f"近 {_themes_window} 日無 themes_discussed 資料。")
                else:
                    _df_t = _pd_d.DataFrame(_theme_rows)
                    _agg_t = _df_t.groupby('theme').agg(
                        mention_count=('video_id', 'count'),
                        show_count=('show_key', 'nunique'),
                    ).sort_values('mention_count', ascending=False).head(15)
                    # load manual themes
                    with open(_sector_json, 'r', encoding='utf-8') as _fj:
                        _manual = _json.load(_fj)
                    _manual_themes = _manual.get('themes', [])

                    def _norm(s):
                        return str(s).lower().replace(' ', '').replace('-', '').replace('/', '').replace('、', '')

                    def _match_theme(yt_str, themes):
                        yt_n = _norm(yt_str)
                        for _t in themes:
                            tid_n = _norm(_t.get('theme_id', ''))
                            zh_n = _norm(_t.get('theme_name_zh', ''))
                            en_n = _norm(_t.get('theme_name_en', ''))
                            if tid_n and (tid_n in yt_n or yt_n in tid_n):
                                return _t
                            if zh_n and (yt_n in zh_n or zh_n in yt_n):
                                return _t
                            if en_n and (yt_n in en_n or en_n in yt_n):
                                return _t
                        return None

                    st.caption(f"近 {_themes_window} 日 top 15 熱議題材")
                    for _theme, _trow in _agg_t.iterrows():
                        _matched = _match_theme(_theme, _manual_themes)
                        _mcnt = int(_trow['mention_count'])
                        _scnt = int(_trow['show_count'])
                        if _matched:
                            _t1 = _matched.get('tier1', []) or []
                            _tickers = [f"{_x.get('ticker', '')}({str(_x.get('name', ''))[:4]})" for _x in _t1[:5]]
                            _tstr = '、'.join(_tickers) if _tickers else '(無 tier1)'
                            st.markdown(
                                f"**{_theme}** — {_mcnt} 次 / {_scnt} 節目 "
                                f"→ `{_matched.get('theme_id', '')}` tier1: {_tstr}"
                            )
                        else:
                            st.markdown(
                                f"**{_theme}** — {_mcnt} 次 / {_scnt} 節目 "
                                f"→ *(未匹配 manual sector tag)*"
                            )

            # Section 3: 大盤 Macro Views (Wave 1 #9)
            st.markdown("#### 🌏 大盤 Macro Views")
            st.caption("近 N 日財經節目對 Fed / 利率 / 大盤 / 美中政策的整體看法（非個股訊號）")
            _videos_path = _Path('data/yt_videos_panel.parquet')
            if not _videos_path.exists():
                st.info("尚無 YT video panel。跑 `run_yt_sync.bat` 或等 scanner TUE-SAT 00:00 排程產資料。")
            else:
                _videos = _pd_d.read_parquet(_videos_path)
                _macro_window = st.radio(
                    "視窗", [7, 14, 30], index=0, horizontal=True, key='_mode_d_macro_window'
                )
                from datetime import date as _date_d3, timedelta as _td_d3
                _cutoff3 = _date_d3.today() - _td_d3(days=_macro_window)
                _recent_v = _videos[_videos['date'] >= _cutoff3].sort_values('date', ascending=False)
                _has_macro = _recent_v[
                    _recent_v['macro_views'].apply(lambda v: isinstance(v, str) and len(v.strip()) > 0)
                ]
                if _has_macro.empty:
                    st.info(f"近 {_macro_window} 日無節目 macro 看法資料。")
                else:
                    st.caption(f"近 {_macro_window} 日 {len(_has_macro)} 集節目的 macro 看法")
                    for _, _row in _has_macro.iterrows():
                        _t_short = str(_row['video_title'])[:40]
                        _label = f"{_row['date']} | {_row['show_name']} — {_t_short}"
                        _guests_list = list(_row['guests']) if _row['guests'] is not None else []
                        _guests_str = '、'.join(_guests_list) if _guests_list else '(無紀錄)'
                        with st.expander(_label, expanded=False):
                            st.markdown(f"**來賓**: {_guests_str}")
                            st.markdown(f"**macro views**: {_row['macro_views']}")

            # === Section 4: 市場主流 flow (BL-4 Phase F) ===
            # 機構買賣共識 vs YT 提及對照
            st.markdown("---")
            st.subheader("📊 本週市場主流 flow")
            try:
                from weekly_chip_loader import (
                    load_latest as _wc_load_md,
                    get_metadata as _wc_md_md,
                )
                _wc_df_md = _wc_load_md()
                _wc_md_md_obj = _wc_md_md()
                if _wc_df_md is None or _wc_md_md_obj is None:
                    st.info("尚無週榜資料，等週六 08:00 batch 跑完。")
                else:
                    _we_str_md = _wc_md_md_obj['week_end'].strftime('%Y-%m-%d')
                    st.caption(f"週末 {_we_str_md} · 機構買賣 vs YT 節目提及對照（找共振 thesis）")

                    # 機構買 (三大合計 連續買 Top 5 + 當週買 Top 5 取 union 去重)
                    _buy_consec = _wc_df_md[(_wc_df_md['dim']=='total') & (_wc_df_md['rank_type']=='consec_buy')].head(5)
                    _buy_week = _wc_df_md[(_wc_df_md['dim']=='total') & (_wc_df_md['rank_type']=='week_buy')].head(5)
                    _sell_consec = _wc_df_md[(_wc_df_md['dim']=='total') & (_wc_df_md['rank_type']=='consec_sell')].head(5)
                    _sell_week = _wc_df_md[(_wc_df_md['dim']=='total') & (_wc_df_md['rank_type']=='week_sell')].head(5)

                    # 算 YT 7d mention（如果 _yt_panel 已 load 在 sub4 上方 scope）
                    def _yt_7d_str(sid):
                        if _yt_panel is None or _yt_panel.empty:
                            return '—'
                        from datetime import date as _dd, timedelta as _td_dd
                        _co = _dd.today() - _td_dd(days=7)
                        _s = _yt_panel[(_yt_panel['ticker']==str(sid)) & (_yt_panel['date']>=_co)]
                        if _s.empty:
                            return '—'
                        _avg = _s['sentiment'].mean()
                        _icon = '🟢' if _avg > 0.3 else ('🔴' if _avg < -0.3 else '⚪')
                        return f"{_icon}×{len(_s)}"

                    _col_md_buy, _col_md_sell = st.columns(2)
                    with _col_md_buy:
                        st.markdown("**🟢 機構在買**")
                        _seen_buy = set()
                        _buy_rows = []
                        for _src_df, _tag in [(_buy_consec, '連買'), (_buy_week, '週買')]:
                            for _, _r in _src_df.iterrows():
                                _sid_md = str(_r['stock_id'])
                                if _sid_md in _seen_buy:
                                    continue
                                _seen_buy.add(_sid_md)
                                _amt_b = _r['weekly_amount_k'] / 1e5
                                _yt_md = _yt_7d_str(_sid_md)
                                _disp_tag = f"連{int(_r['consec_days'])}d" if _tag == '連買' else f"#{int(_r['rank'])}"
                                _buy_rows.append({
                                    '代號': _sid_md,
                                    '名稱': _r['stock_name'],
                                    '榜': f"{_tag}{_disp_tag}",
                                    '金額': f"{_amt_b:+.1f}億",
                                    'YT 7d': _yt_md,
                                })
                        if _buy_rows:
                            st.dataframe(_pd_d.DataFrame(_buy_rows), hide_index=True, use_container_width=True)
                    with _col_md_sell:
                        st.markdown("**🔴 機構在賣**")
                        _seen_sell = set()
                        _sell_rows = []
                        for _src_df, _tag in [(_sell_consec, '連賣'), (_sell_week, '週賣')]:
                            for _, _r in _src_df.iterrows():
                                _sid_md = str(_r['stock_id'])
                                if _sid_md in _seen_sell:
                                    continue
                                _seen_sell.add(_sid_md)
                                _amt_b = _r['weekly_amount_k'] / 1e5
                                _yt_md = _yt_7d_str(_sid_md)
                                _disp_tag = f"連{int(_r['consec_days'])}d" if _tag == '連賣' else f"#{int(_r['rank'])}"
                                _sell_rows.append({
                                    '代號': _sid_md,
                                    '名稱': _r['stock_name'],
                                    '榜': f"{_tag}{_disp_tag}",
                                    '金額': f"{_amt_b:+.1f}億",
                                    'YT 7d': _yt_md,
                                })
                        if _sell_rows:
                            st.dataframe(_pd_d.DataFrame(_sell_rows), hide_index=True, use_container_width=True)

                    st.caption("YT 7d ×N = 近 7 日節目提及次數（🟢正面 / 🔴負面 / ⚪中性）。機構買 + YT 高提及 = 強共振 thesis 候選。")
            except Exception as _wc_md_err:
                st.warning(f"市場主流 flow 載入失敗: {_wc_md_err}")

    st.markdown("---")
    st.caption("💡 品質選股掃描: `python scanner_job.py --mode qm` | 價值掃描: `python scanner_job.py --mode value`")

