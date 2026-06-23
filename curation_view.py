"""題材策展 view (多市場 TW/US 半自動化)。

設計見 memory/project_multimarket_theme_curation.md。
- 按鈕觸發 → subprocess.Popen detached 跑 tools/curate_themes_pipeline.py (背景，不卡 UI、跨 session)
- 狀態落 disk (data_cache/curation/<market>/status.json)，每次載入讀 disk 三態渲染
- REVIEW UI 出現 = 跑完；表格 diff + checkbox approve → apply_diff 寫回 canonical
- 高共識 added 預勾；removed / new_themes 強制人工 (不預勾)
"""
import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

REPO = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO / "tools"))

from curate_themes_pipeline import (  # noqa: E402
    paths, read_status, write_status, apply_diff, load_canonical,
)
import json  # noqa: E402

SCRIPT = REPO / "tools" / "curate_themes_pipeline.py"
MARKETS = [("tw", "🇹🇼 台股"), ("us", "🇺🇸 美股")]


def _launch_pipeline(market: str) -> None:
    """detached 背景啟動 pipeline。Windows 用 DETACHED_PROCESS 讓子程序survive 父 (Streamlit)。"""
    work = paths(market)["work"]
    work.mkdir(parents=True, exist_ok=True)
    log_f = (work / "launch.log").open("w", encoding="utf-8")
    kwargs = dict(stdout=log_f, stderr=subprocess.STDOUT, cwd=str(REPO))
    if os.name == "nt":
        DETACHED_PROCESS = 0x00000008
        kwargs["creationflags"] = DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    subprocess.Popen([sys.executable, str(SCRIPT), "--market", market], **kwargs)
    # 立刻寫 running，避免 launch 到 pipeline 接手前的空窗顯示舊狀態
    write_status(market, status="running", stage="launching", progress=["[UI] 已送出背景任務..."],
                 diff_path=None, summary=None, error=None)


def _render_diff_section(market: str, diff: dict) -> None:
    """REVIEW UI：表格 diff + approve checkbox → apply。"""
    summary = diff.get("summary", {})
    st.success(
        f"🎯 **REVIEW** — {dict(MARKETS).get(market, market)} 策展 diff 已就緒 "
        f"(新增 {summary.get('added', 0)} / 移除 {summary.get('removed', 0)} / 新題材 {summary.get('new_themes', 0)})"
    )
    st.caption(f"產生時間: {diff.get('generated_at', '?')}　|　高共識新增已預勾，移除與新題材需人工勾選")

    approved_added, approved_removed, approved_new = set(), set(), set()

    # --- Added ---
    added = diff.get("added", [])
    st.markdown(f"#### ➕ 新增候選 ({len(added)})")
    if added:
        df_add = pd.DataFrame([{
            "approve": bool(a.get("auto_suggest")),
            "ticker": a["ticker"], "name": a.get("name", ""),
            "題材": f"{a.get('theme_name_zh', '')} ({a['theme_id']})",
            "tier": a.get("tier", ""), "votes": a.get("votes", 0),
            "conf": a.get("confidence", 0),
            "共識": a.get("proposed_by", ""),
            "sources": ", ".join(a.get("sources", []) or [])[:80],
            "_key": f"{a['ticker']}|{a['theme_id']}",
        } for a in added])
        edited = st.data_editor(
            df_add.drop(columns=["_key"]), key=f"add_editor_{market}",
            hide_index=True, width="stretch",
            column_config={
                "approve": st.column_config.CheckboxColumn("✓", width="small"),
                "votes": st.column_config.NumberColumn("票", width="small"),
                "conf": st.column_config.NumberColumn("信心", width="small"),
            },
            disabled=[c for c in df_add.columns if c not in ("approve", "_key")],
        )
        for i, ok in enumerate(edited["approve"].tolist()):
            if ok:
                approved_added.add(df_add.iloc[i]["_key"])
    else:
        st.caption("(無新增候選)")

    # --- Removed ---
    removed = diff.get("removed", [])
    st.markdown(f"#### ➖ 移除候選 ({len(removed)})　[需人工確認]")
    if removed:
        df_rm = pd.DataFrame([{
            "approve": False,
            "ticker": r["ticker"],
            "題材": f"{r.get('theme_name_zh', '')} ({r['theme_id']})",
            "原因": r.get("reason", ""),
            "_key": f"{r['ticker']}|{r['theme_id']}",
        } for r in removed])
        edited_rm = st.data_editor(
            df_rm.drop(columns=["_key"]), key=f"rm_editor_{market}",
            hide_index=True, width="stretch",
            column_config={"approve": st.column_config.CheckboxColumn("✓移除", width="small")},
            disabled=[c for c in df_rm.columns if c != "approve"],
        )
        for i, ok in enumerate(edited_rm["approve"].tolist()):
            if ok:
                approved_removed.add(df_rm.iloc[i]["_key"])
    else:
        st.caption("(無移除候選)")

    # --- New themes ---
    new_themes = diff.get("new_themes", [])
    st.markdown(f"#### 🆕 新題材提議 ({len(new_themes)})　[需人工確認]")
    for nt in new_themes:
        tid = nt.get("theme_id", "?")
        members = [s.get("ticker") for tier in ("tier1", "tier2") for s in nt.get(tier, [])]
        if st.checkbox(
            f"**{nt.get('theme_name_zh', tid)}** ({tid}) — {len(members)} 檔: {', '.join(map(str, members))}",
            key=f"newtheme_{market}_{tid}",
        ):
            approved_new.add(tid)
        if nt.get("rationale"):
            st.caption(f"　理由: {nt['rationale'][:200]}")
    if not new_themes:
        st.caption("(無新題材提議)")

    # --- Actions ---
    st.markdown("---")
    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("✅ 套用勾選變更 (寫回 canonical)", type="primary", key=f"apply_{market}",
                     width="stretch"):
            n = len(approved_added) + len(approved_removed) + len(approved_new)
            if n == 0:
                st.warning("沒有勾選任何變更")
            else:
                res = apply_diff(market, approved_added, approved_removed, approved_new)
                st.success(f"已寫回 {Path(res['canonical_path']).name}: {res['applied']}")
                time.sleep(1.2)
                st.rerun()
    with c2:
        if st.button("🗑️ 放棄此 diff (不套用)", key=f"discard_{market}", width="stretch"):
            paths(market)["diff"].unlink(missing_ok=True)
            write_status(market, status="idle", stage="idle", diff_path=None)
            st.rerun()


def _render_market_panel(market: str, label: str) -> None:
    status = read_status(market)
    p = paths(market)
    canonical = load_canonical(market)
    n_themes = len(canonical.get("themes", []))
    st.caption(f"{label}　canonical: {n_themes} themes　|　{paths(market)['work']}")

    _state = (status or {}).get("status")

    # --- running: banner + 暫停鈕 + 自動刷新 ---
    if _state == "running":
        stage = status.get("stage", "")
        st.warning(f"⏳ 策展 pipeline 執行中... (stage: {stage}) — 可關閉分頁，跑完回來會看到 REVIEW")
        _stop_file = p["work"] / "STOP"
        if _stop_file.exists():
            st.info("⏸️ 已送出暫停請求，等手邊任務跑完就會停（之後可按『接續 resume』續跑）。")
        elif st.button("⏸️ 暫停 (做完手邊任務後停)", key=f"stop_{market}",
                       help="送出暫停請求：進行中的任務做完就停、已完成的會存檔；之後可接續 resume，不重做"):
            _stop_file.write_text("stop", encoding="utf-8")
            st.rerun()
        with st.expander("進度", expanded=True):
            for line in status.get("progress", [])[-30:]:
                st.write(f"• {line}")
        time.sleep(2)
        st.rerun()
        return

    # --- error ---
    if _state == "error":
        st.error(f"❌ pipeline 失敗: {status.get('error', '(無訊息)')}")
        with st.expander("進度"):
            for line in status.get("progress", []):
                st.write(f"• {line}")
        if st.button(f"🔄 重試 ({market})", key=f"retry_{market}"):
            _launch_pipeline(market)
            st.rerun()
        return

    # --- done + diff 存在 → REVIEW UI ---
    if _state == "done" and p["diff"].exists():
        try:
            diff = json.loads(p["diff"].read_text(encoding="utf-8"))
            _render_diff_section(market, diff)
            return
        except Exception as e:
            st.error(f"diff.json 讀取失敗: {e}")

    # --- paused (手動 stop / 帳號額度 429) → resume 按鈕 ---
    if _state == "paused":
        _su = status.get("summary", {})
        st.warning(f"⏸️ 已暫停（{_su.get('reason', '')}）：完成 {_su.get('completed', '?')}/{_su.get('total', '?')} 任務。"
                   "已完成的任務已存檔，按下方按鈕會**接續 resume**（不重做）。")

    # --- idle / applied / paused / 無 status → 觸發鈕 ---
    if _state == "applied":
        st.info(f"✅ 上次已套用: {status.get('applied', {})}　({status.get('finished_at', '')})")
    _btn_label = "▶ 接續策展 (resume)" if _state == "paused" else f"▶ 觸發策展刷新 ({label})"
    if st.button(_btn_label, type="primary", key=f"trigger_{market}",
                 help="背景跑每題材並行研究 + diff，跑完此頁會出現 REVIEW；期間可離開。暫停後再按=從中斷處接續"):
        _launch_pipeline(market)
        st.rerun()


def render_curation() -> None:
    st.subheader("🎨 多市場題材策展")
    st.caption("半自動：按鈕觸發背景 3-agent 研究 → 產生 diff → 人工 REVIEW approve 寫回 canonical。"
               "可關分頁隨時回來；REVIEW UI 出現即代表跑完。")
    tabs = st.tabs([label for _, label in MARKETS])
    for tab, (market, label) in zip(tabs, MARKETS):
        with tab:
            _render_market_panel(market, label)
