"""AI 報告 view (Phase D 從 app.py 抽出)

對應 app_mode == 'ai_reports' 的整段邏輯，包含 2 個 tab:
- 生成報告 (job 放 session_state，背景 thread 跑完寫回結果)
- 報告庫 (列出所有歷史報告 + 預覽)
"""

import logging
import threading
import time

import pandas as pd
import streamlit as st

from ui_helpers import _ai_report_job_lock, _ai_report_worker, validate_ticker

logger = logging.getLogger(__name__)


def render_ai_reports():
    """渲染 AI 研究報告庫 mode (2 tabs)。"""
    # ====================================================================
    #  AI 研究報告庫
    # ====================================================================
    from ai_report import (
        generate_report as _gen_report,
        save_report as _save_report,
        load_report_index as _load_index,
        load_report_content as _load_content,
        delete_report as _delete_report,
    )

    _report_tab_gen, _report_tab_lib = st.tabs(["✏️ 生成報告", "📚 報告庫"])

    # --- Tab 1: Generate ---
    with _report_tab_gen:
        # ==========================================================
        # [NEW] 背景生成：job 放 session_state，thread 跑完寫回結果
        # 切換 tab / app_mode 都不中斷；每次 rerun 檢查 job 狀態
        # ==========================================================
        _job = st.session_state.get('ai_report_job')

        # --- 處理已完成的 job ---
        if _job and _job.get('status') == 'done':
            _res = _job.get('result') or {}
            st.success(f"✅ **{_job['ticker']}** 報告生成完成！已儲存到報告庫（ID: `{_res.get('rid', 'N/A')}`）")
            st.info("請切到「📚 報告庫」tab 查看報告內容。")
            if st.button("清除通知並繼續", key='ai_clear_done'):
                del st.session_state['ai_report_job']
                st.rerun()

        elif _job and _job.get('status') == 'error':
            st.error(f"❌ **{_job['ticker']}** 生成失敗")
            with st.expander("錯誤訊息", expanded=True):
                st.code(_job.get('result') or "(無訊息)", language=None)
            if st.button("清除通知並重試", key='ai_clear_err'):
                del st.session_state['ai_report_job']
                st.rerun()

        # --- 顯示執行中的 banner + 自動刷新 ---
        if _job and _job.get('status') == 'running':
            _elapsed = int(time.time() - _job['start_time'])
            _mm, _ss = _elapsed // 60, _elapsed % 60
            st.warning(f"⏳ 正在生成 **{_job['ticker']}** 的研究報告... (已過 {_mm} 分 {_ss} 秒)")
            st.info("💡 可以安心切換到其他頁面，生成會在背景繼續進行。回到此頁會看到進度。")
            with st.expander("進度", expanded=True):
                # H4: 用 list() 快照避免 worker thread append 時 iterate 出錯
                with _ai_report_job_lock:
                    _progress_snapshot = list(_job.get('progress', []))
                for _msg in _progress_snapshot:
                    st.write(f"• {_msg}")
            # Auto-refresh every 2s to update elapsed + progress
            time.sleep(2)
            st.rerun()

        # --- 輸入區（執行中時禁用） ---
        st.markdown("輸入股票代號，Claude AI 將根據系統所有數據生成深度研究報告。")
        _is_running = _job is not None and _job.get('status') == 'running'

        _col_t, _col_f = st.columns([3, 2])
        with _col_t:
            _ai_ticker = st.text_input(
                "股票代號", placeholder="例: 2330, AAPL",
                key='ai_report_ticker',
                disabled=_is_running,
            )
        with _col_f:
            _format_labels = {
                'html': '📊 互動儀表板 (HTML)',
                'md': '📝 傳統報告 (Markdown)',
            }
            _ai_format = st.radio(
                "產出格式",
                options=['html', 'md'],
                format_func=lambda x: _format_labels[x],
                key='ai_report_format',
                disabled=_is_running,
                horizontal=False,
            )

        # 「🤖 生成研究報告」(本地 CLI) 按鈕隱藏 -- 只保留「產生 Prompt 貼 claude.ai」流程
        # (本地 CLI 會吃 Agent SDK Credit；生成邏輯保留待用，僅不顯示按鈕)
        _gen_clicked = False
        _prompt_clicked = st.button("📋 產生 Prompt (貼到 claude.ai)", type="primary",
                                    key='ai_prompt_btn', disabled=_is_running,
                                    help="組裝 prompt 複製貼到 claude.ai 網頁手動跑（用訂閱 quota，不吃 Agent SDK Credit）")

        if _gen_clicked:
            if not _ai_ticker or not _ai_ticker.strip():
                st.error("請輸入股票代號")
            else:
                _ai_ticker = _ai_ticker.strip().upper()
                is_valid, err_msg = validate_ticker(_ai_ticker)
                if not is_valid:
                    st.error(f"代號格式不正確: {err_msg}")
                else:
                    _new_job = {
                        'ticker': _ai_ticker,
                        'status': 'running',
                        'start_time': time.time(),
                        'progress': [],
                        'result': None,
                        'format': _ai_format,
                    }
                    st.session_state['ai_report_job'] = _new_job
                    _t = threading.Thread(
                        target=_ai_report_worker,
                        args=(_new_job, _ai_ticker, _ai_format),
                        daemon=True,
                    )
                    _t.start()
                    st.rerun()

        if _prompt_clicked:
            if not _ai_ticker or not _ai_ticker.strip():
                st.error("請輸入股票代號")
            else:
                _ai_ticker = _ai_ticker.strip().upper()
                is_valid, err_msg = validate_ticker(_ai_ticker)
                if not is_valid:
                    st.error(f"代號格式不正確: {err_msg}")
                else:
                    from ai_report_pipeline import assemble_prompt_only
                    with st.spinner(f"組裝 {_ai_ticker} prompt 中（10-30 秒，無 LLM call）..."):
                        _r = assemble_prompt_only(_ai_ticker, fmt=_ai_format)
                    if not _r['ok']:
                        st.error(f"組裝失敗: {_r['error']}")
                    else:
                        st.session_state['ai_prompt_result'] = {
                            'ticker': _ai_ticker,
                            'format': _ai_format,
                            'prompt': _r['prompt'],
                            'elapsed_s': _r['elapsed_s'],
                        }

        # --- 顯示組好的 prompt（如果有）---
        _pr = st.session_state.get('ai_prompt_result')
        if _pr:
            _fmt_label = '📊 儀表板 (HTML JSON)' if _pr['format'] == 'html' else '📝 Markdown'
            st.success(
                f"✅ **{_pr['ticker']}** prompt 組裝完成 (`{_fmt_label}`, "
                f"{len(_pr['prompt']):,} chars, {_pr['elapsed_s']:.1f}s)"
            )
            st.caption(
                "💡 點下方「📋 一鍵複製」按鈕複製 prompt → 貼到 claude.ai 對話框"
                "（建議用 Opus 4.7 / Extended Thinking）。Claude 回傳的內容貼到本頁下方"
                "「📥 貼回 claude.ai 輸出」區塊即可存到報告庫。"
            )

            # JS 一鍵複製按鈕 (navigator.clipboard.writeText)
            import streamlit.components.v1 as components
            import html as _htmlmod
            _esc_prompt = _htmlmod.escape(_pr['prompt'])  # 防 </script> 注入
            _btn_html = f"""
            <textarea id="prompt_src_{_pr['ticker']}" style="position:absolute;left:-9999px;">{_esc_prompt}</textarea>
            <button id="copy_btn_{_pr['ticker']}"
                    style="background:#FF4B4B;color:white;border:none;padding:10px 20px;
                           border-radius:8px;font-size:16px;cursor:pointer;font-weight:600;">
              📋 一鍵複製 Prompt 到剪貼簿
            </button>
            <span id="copy_status_{_pr['ticker']}" style="margin-left:12px;color:#0f9d58;font-weight:600;"></span>
            <script>
              document.getElementById("copy_btn_{_pr['ticker']}").addEventListener("click", async () => {{
                const src = document.getElementById("prompt_src_{_pr['ticker']}");
                const status = document.getElementById("copy_status_{_pr['ticker']}");
                try {{
                  if (navigator.clipboard && navigator.clipboard.writeText) {{
                    await navigator.clipboard.writeText(src.value);
                  }} else {{
                    src.style.left = "0"; src.select(); document.execCommand("copy"); src.style.left = "-9999px";
                  }}
                  status.textContent = "✅ 已複製 " + src.value.length.toLocaleString() + " chars";
                  setTimeout(() => {{ status.textContent = ""; }}, 4000);
                }} catch (e) {{
                  status.textContent = "❌ 複製失敗: " + e.message;
                  status.style.color = "#d93025";
                }}
              }});
            </script>
            """
            components.html(_btn_html, height=60)

            # 仍保留 st.code 給「想自己選取或檢視內容」的場景
            with st.expander("📄 顯示 prompt 全文（檢視/手動選取）", expanded=False):
                st.code(_pr['prompt'], language=None)

            _col_dl, _col_clr = st.columns([1, 1])
            with _col_dl:
                _ext = 'json.txt' if _pr['format'] == 'html' else 'md.txt'
                st.download_button(
                    "💾 下載 .txt (備援)",
                    data=_pr['prompt'],
                    file_name=f"prompt_{_pr['ticker']}_{_pr['format']}.{_ext}",
                    mime='text/plain',
                    key='ai_prompt_dl',
                )
            with _col_clr:
                if st.button("🗑️ 清除 prompt 顯示", key='ai_prompt_clear', width='stretch'):
                    del st.session_state['ai_prompt_result']
                    st.rerun()

        # === 貼回區塊：永遠顯示 ===
        # HTML 模式：從 JSON meta.ticker 自動抓代號（claude.ai 真值優先），form ticker 是 MD fallback
        st.markdown("---")
        st.markdown("#### 📥 貼回 claude.ai 輸出")
        # HTML 格式底下再切「JSON 灌本地模板」vs「整頁 HTML 直存」(2026-05-22 新增)
        _paste_html_mode = 'json'  # 預設給 MD 模式用（不會讀到）
        if _ai_format == 'html':
            _paste_html_mode = st.radio(
                "貼回類型",
                options=['json', 'fullhtml'],
                format_func=lambda x: {
                    'json': '📐 JSON 灌本地模板 (預設)',
                    'fullhtml': '📄 整頁 HTML (claude.ai 已生好)',
                }[x],
                key='ai_paste_html_mode',
                horizontal=True,
            )
        if _ai_format == 'html' and _paste_html_mode == 'json':
            st.caption(
                "把 claude.ai 的 JSON 回傳貼到下方 → 點「處理並儲存」會自動解析 + 灌模板 + 存到報告庫。"
                "**股票代號直接從 JSON `meta.ticker` 抓**（不靠上方表單）。"
            )
            _paste_placeholder = '{"meta": {"ticker": "CRCL", ...}, ...}'
        elif _ai_format == 'html' and _paste_html_mode == 'fullhtml':
            _paste_ticker_show = _ai_ticker.strip().upper() if _ai_ticker and _ai_ticker.strip() else "(未填)"
            st.caption(
                f"把 claude.ai 直接生好的完整 HTML 頁面貼到下方 → 直接存檔，不再灌本地模板。"
                f"**整頁 HTML 模式必須先在上方輸入股票代號**（=`{_paste_ticker_show}`），用作存檔代號。"
            )
            _paste_placeholder = '<!DOCTYPE html><html>...</html>'
        else:
            _paste_ticker_show = _ai_ticker.strip().upper() if _ai_ticker and _ai_ticker.strip() else "(未填)"
            st.caption(
                f"把 claude.ai 的 markdown 報告貼到下方 → 點「處理並儲存」存到報告庫。"
                f"**Markdown 模式必須先在上方輸入股票代號**（=`{_paste_ticker_show}`），無法從純文字自動抓。"
            )
            _paste_placeholder = 'markdown 報告全文'
        _paste = st.text_area(
            "貼上 claude.ai 輸出",
            value="",
            height=200,
            placeholder=_paste_placeholder,
            key='ai_paste_textarea',
        )
        if st.button("📥 處理並儲存到報告庫", type='primary', key='ai_paste_save_btn',
                     disabled=_is_running):
            if not _paste or not _paste.strip():
                st.error("請先貼上 claude.ai 輸出")
            else:
                try:
                    if _ai_format == 'html' and _paste_html_mode == 'json':
                        from ai_report import render_html_from_claude_output, save_report_html
                        # HTML 路徑：先 ticker fallback 給 render 用做 title 預設，實際存檔用 JSON meta.ticker
                        _fallback_ticker = (_ai_ticker.strip().upper() if _ai_ticker and _ai_ticker.strip() else 'UNKNOWN')
                        with st.spinner("解析 JSON + 灌模板..."):
                            _ok, _html_or_err, _json_data = render_html_from_claude_output(
                                _fallback_ticker, _paste
                            )
                        if not _ok:
                            st.error(f"處理失敗: {_html_or_err}")
                        else:
                            _meta_ticker = str((_json_data.get('meta') or {}).get('ticker', '')).strip().upper()
                            if not _meta_ticker:
                                st.error("JSON `meta.ticker` 為空，請確認 claude.ai 回傳完整")
                            else:
                                # 提示跨代號不一致 (form 填 X 但 claude 回 Y)
                                if _ai_ticker and _ai_ticker.strip() and _ai_ticker.strip().upper() != _meta_ticker:
                                    st.info(
                                        f"ℹ️ 上方表單填的是 `{_ai_ticker.strip().upper()}`，但 JSON `meta.ticker`=`{_meta_ticker}`；"
                                        f"以 JSON 為準存成 **{_meta_ticker}** 報告。"
                                    )
                                _rid = save_report_html(
                                    _meta_ticker, _html_or_err,
                                    json_data=_json_data,
                                )
                                st.success(f"✅ 已存到報告庫 (ID: `{_rid}`)，切到「📚 報告庫」tab 查看")
                                st.session_state.pop('ai_paste_textarea', None)
                    elif _ai_format == 'html' and _paste_html_mode == 'fullhtml':
                        # 整頁 HTML 直存：不解析 JSON、不灌模板、不寫 sidecar；ticker 只能靠上方表單
                        _save_ticker = _ai_ticker.strip().upper() if _ai_ticker else ""
                        if not _save_ticker:
                            st.error("整頁 HTML 模式請在上方「股票代號」欄輸入代號")
                        else:
                            is_valid, err_msg = validate_ticker(_save_ticker)
                            if not is_valid:
                                st.error(f"代號格式不正確: {err_msg}")
                            else:
                                _paste_head = _paste.strip()[:200].lower()
                                if not (_paste_head.startswith('<!doctype') or _paste_head.startswith('<html')):
                                    st.warning("⚠️ 內容不像完整 HTML（沒 `<!DOCTYPE` 或 `<html` 開頭），仍會存檔但可能無法正常顯示。")
                                from ai_report import save_report_html
                                _rid = save_report_html(_save_ticker, _paste, json_data=None)
                                st.success(f"✅ 已存到報告庫 (ID: `{_rid}`)，切到「📚 報告庫」tab 查看")
                                st.session_state.pop('ai_paste_textarea', None)
                    else:
                        # MD 路徑：無法自動抓 ticker，必須靠 form
                        _save_ticker = _ai_ticker.strip().upper() if _ai_ticker else ""
                        if not _save_ticker:
                            st.error("Markdown 模式請在上方「股票代號」欄輸入代號")
                        else:
                            is_valid, err_msg = validate_ticker(_save_ticker)
                            if not is_valid:
                                st.error(f"代號格式不正確: {err_msg}")
                            else:
                                from ai_report import save_report
                                _rid = save_report(_save_ticker, _paste)
                                st.success(f"✅ 已存到報告庫 (ID: `{_rid}`)，切到「📚 報告庫」tab 查看")
                                st.session_state.pop('ai_paste_textarea', None)
                except Exception as _save_err:
                    st.error(f"儲存失敗: {type(_save_err).__name__}: {_save_err}")

    # --- Tab 2: Library ---
    with _report_tab_lib:
        _index = _load_index()

        if not _index:
            st.info("報告庫是空的。請先在「✏️ 生成報告」tab 生成報告。")
        else:
            # Filter
            _all_tickers = sorted(set(r['ticker'] for r in _index))
            _filter_ticker = st.selectbox(
                "篩選股票", ['全部'] + _all_tickers, key='report_filter_ticker')

            _filtered = _index if _filter_ticker == '全部' else [
                r for r in _index if r['ticker'] == _filter_ticker]
            _filtered = sorted(_filtered, key=lambda x: x.get('date', '') + x.get('time', ''), reverse=True)

            st.caption(f"共 {len(_filtered)} 篇報告")

            # Report list
            _list_rows = []
            for _r in _filtered:
                _fmt = _r.get('format', 'md')
                _fmt_label = '📊 儀表板' if _fmt == 'html' else '📝 Markdown'
                _list_rows.append({
                    '日期': f"{_r.get('date', '')} {_r.get('time', '')[:5]}",
                    '股票': _r['ticker'],
                    '格式': _fmt_label,
                    '觸發分數': _r.get('trigger_score'),
                    '趨勢分數': _r.get('trend_score'),
                    'ID': _r['report_id'],
                })
            if _list_rows:
                st.dataframe(pd.DataFrame(_list_rows), width='stretch', hide_index=True)

            # Report viewer
            def _opt_label(r):
                _f = r.get('format', 'md')
                _badge = '📊' if _f == 'html' else '📝'
                return f"{_badge} {r.get('date', '')} {r['ticker']}"

            _report_options = [_opt_label(r) for r in _filtered]
            _report_ids = [r['report_id'] for r in _filtered]
            _report_formats = [r.get('format', 'md') for r in _filtered]

            if _report_options:
                _sel_idx = st.selectbox(
                    "選擇報告", range(len(_report_options)),
                    format_func=lambda i: _report_options[i],
                    key='report_viewer_sel',
                )
                _sel_id = _report_ids[_sel_idx]
                _sel_fmt = _report_formats[_sel_idx]
                _sel_content = _load_content(_sel_id)

                if _sel_content:
                    st.markdown("---")

                    if _sel_fmt == 'html':
                        # 工具列：在瀏覽器開啟 + 下載
                        from ai_report import get_report_filepath as _get_fp
                        _fp = _get_fp(_sel_id)

                        _c1, _c2, _c3 = st.columns([2, 2, 6])
                        with _c1:
                            if _fp and st.button("🌐 在瀏覽器開啟", key='html_open_btn', type='primary'):
                                import webbrowser
                                webbrowser.open(f"file:///{_fp.replace(chr(92), '/')}")
                        with _c2:
                            st.download_button(
                                "💾 下載 HTML",
                                data=_sel_content,
                                file_name=f"{_sel_id}.html",
                                mime='text/html',
                                key='html_download_btn',
                            )

                        st.caption("💡 如顯示不全請點「在瀏覽器開啟」看完整版（無高度限制）")
                        # 內嵌 iframe 預覽 — 固定高 2600px、關閉內部 scrolling 消除雙層 scrollbar。
                        # Streamlit 的 components.v1.html 不會實際響應 postMessage(streamlit:setFrameHeight)，
                        # 所以改走「給夠高的固定 height + scrolling=False」，多數報告 2600 可完整顯示；
                        # 超長報告（極端多 risk item 等）走「在瀏覽器開啟」按鈕。
                        import streamlit.components.v1 as _components
                        _components.html(_sel_content, height=2600, scrolling=False)
                    else:
                        # 報告含 <span style="color:..."> 顏色標記，需允許 HTML 才能正確渲染
                        st.markdown(_sel_content, unsafe_allow_html=True)

                    st.markdown("---")
                    st.caption("此報告由 Claude AI 基於系統數據自動生成，僅供參考，不構成投資建議。")

                    if st.button("🗑️ 刪除此報告", key='report_delete_btn'):
                        _delete_report(_sel_id)
                        st.success("報告已刪除")
                        st.rerun()

