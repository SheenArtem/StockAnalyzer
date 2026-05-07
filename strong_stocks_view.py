"""
強勢股日報 view (Streamlit)

讀 data/strong_stocks_reports/YYYY-MM-DD.{html,pdf} 列出歷史報告，
預設最新一份，inline 渲染 HTML + 提供 PDF 下載。
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

REPORTS_DIR = Path(__file__).resolve().parent / "data" / "strong_stocks_reports"
LATEST_JSON = Path(__file__).resolve().parent / "data" / "latest" / "strong_stocks_daily.json"


def _list_reports() -> list[Path]:
    """List YYYY-MM-DD.html files, sorted newest first."""
    if not REPORTS_DIR.exists():
        return []
    return sorted(REPORTS_DIR.glob("*.html"), reverse=True)


def _load_metadata() -> dict | None:
    """Latest scan metadata for top-bar display."""
    if not LATEST_JSON.exists():
        return None
    try:
        with LATEST_JSON.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def render_strong_stocks() -> None:
    st.title("📰 強勢股日報")

    reports = _list_reports()
    if not reports:
        st.warning(
            "尚無強勢股日報。請等下次 scanner 排程跑完，或手動執行：\n"
            "```\n"
            "python tools/strong_stocks_daily.py\n"
            "python tools/strong_stocks_ai_analysis.py\n"
            "python tools/strong_stocks_render.py\n"
            "```"
        )
        return

    # --- Date selector ---
    date_labels = [r.stem for r in reports]
    col1, col2 = st.columns([3, 1])
    with col1:
        selected_label = st.selectbox(
            f"日期 (共 {len(reports)} 份報告)",
            options=date_labels,
            index=0,
            key="strong_stocks_date",
        )
    with col2:
        st.caption("最新報告由排程於 TUE-SAT 00:00 自動產出")

    selected_html = REPORTS_DIR / f"{selected_label}.html"
    selected_pdf = REPORTS_DIR / f"{selected_label}.pdf"

    # --- Metadata strip (only shown for the latest report; older reports
    #     may have been rendered with a different `latest` snapshot) ---
    is_latest = (selected_label == date_labels[0])
    if is_latest:
        meta = _load_metadata()
        if meta:
            sector_covered = sum(
                1 for r in (meta.get("twse_top", []) + meta.get("tpex_top", []))
                if r.get("primary_sector")
            )
            inst_covered = sum(
                1 for r in (meta.get("twse_top", []) + meta.get("tpex_top", []))
                if r.get("inst_net_buy_today_shares") is not None
            )
            total = len(meta.get("twse_top", [])) + len(meta.get("tpex_top", []))
            ai_meta = meta.get("ai_analysis_meta", {}) or {}
            mc1, mc2, mc3, mc4 = st.columns(4)
            ref_d = meta.get("ref_date") or meta.get("scan_date", "?")
            mc1.metric("資料日 (ref)", ref_d, help="所有欄位對齊的交易日 (OHLCV cache 共識)")
            mc2.metric("族群覆蓋", f"{sector_covered}/{total}")
            mc3.metric("法人覆蓋", f"{inst_covered}/{total}")
            ai_warn = len(ai_meta.get("validation_warnings", []) or [])
            mc4.metric("AI 警告", ai_warn, delta=None if ai_warn == 0 else "需注意")
            ai_at = ai_meta.get("generated_at")
            scan_d = meta.get("scan_date")
            cap_parts = []
            if scan_d and scan_d != ref_d:
                cap_parts.append(f"掃描日 {scan_d}")
            if ai_at:
                cap_parts.append(f"AI 分析產出於 {ai_at}")
            if cap_parts:
                st.caption(" | ".join(cap_parts))

    # --- PDF download ---
    if selected_pdf.exists():
        with selected_pdf.open("rb") as f:
            st.download_button(
                label=f"📥 下載 PDF ({selected_pdf.stat().st_size // 1024} KB)",
                data=f.read(),
                file_name=selected_pdf.name,
                mime="application/pdf",
            )
    else:
        st.caption("⚠️ 此日期的 PDF 不存在（只有 HTML）")

    st.divider()

    # --- Inline HTML render ---
    try:
        html = selected_html.read_text(encoding="utf-8")
    except Exception as e:
        st.error(f"HTML 讀取失敗: {e}")
        return

    # iframe height: 強勢股日報 12 欄 × 30 row + AI 5 段論述, 估 4000-5000px
    components.html(html, height=4800, scrolling=True)
