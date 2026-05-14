"""
強勢股報告 view (Streamlit) — Daily + Weekly (2026-05-14)

Mode (radio at top):
  - 📅 日報：讀 data/strong_stocks_reports/YYYY-MM-DD.html
  - 📊 週報：讀 data/strong_stocks_reports/YYYY-Www.html
            上方加 informational tier banner (週度 scoring 未經 IC 驗證)
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

REPO = Path(__file__).resolve().parent
REPORTS_DIR = REPO / "data" / "strong_stocks_reports"
LATEST_DAILY_JSON = REPO / "data" / "latest" / "strong_stocks_daily.json"
LATEST_WEEKLY_JSON = REPO / "data" / "latest" / "strong_stocks_weekly.json"

_DAILY_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_WEEKLY_PATTERN = re.compile(r"^\d{4}-W\d{2}$")


def _list_daily_reports() -> list[Path]:
    """List YYYY-MM-DD.html, newest first."""
    if not REPORTS_DIR.exists():
        return []
    return sorted(
        (p for p in REPORTS_DIR.glob("*.html") if _DAILY_PATTERN.match(p.stem)),
        reverse=True,
    )


def _list_weekly_reports() -> list[Path]:
    """List YYYY-Www.html, newest first."""
    if not REPORTS_DIR.exists():
        return []
    return sorted(
        (p for p in REPORTS_DIR.glob("*.html") if _WEEKLY_PATTERN.match(p.stem)),
        reverse=True,
    )


def _load_metadata(report_type: str) -> dict | None:
    """Latest scan metadata for top-bar display."""
    p = LATEST_WEEKLY_JSON if report_type == "weekly" else LATEST_DAILY_JSON
    if not p.exists():
        return None
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _render_daily_metadata_strip(meta: dict) -> None:
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
        model = ai_meta.get("model", "claude")
        n_news = ai_meta.get("news_articles_injected", 0)
        cap_parts.append(f"AI ({model}, {n_news} news) 產出於 {ai_at}")
    if cap_parts:
        st.caption(" | ".join(cap_parts))


def _render_weekly_metadata_strip(meta: dict) -> None:
    sector_covered = sum(
        1 for r in (meta.get("twse_top", []) + meta.get("tpex_top", []))
        if r.get("primary_sector")
    )
    inst_covered = sum(
        1 for r in (meta.get("twse_top", []) + meta.get("tpex_top", []))
        if r.get("inst_net_5d_shares") is not None
    )
    total = len(meta.get("twse_top", [])) + len(meta.get("tpex_top", []))
    ai_meta = meta.get("ai_analysis_meta", {}) or {}
    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("週次", meta.get("week_label", "?"),
               help=f"{meta.get('week_start','')} ~ {meta.get('week_end','')}")
    mc2.metric("Universe", meta.get("universe_size", "-"),
               help="本週週 K + chip 信號驗證後納入評分的股票數")
    mc3.metric("族群覆蓋", f"{sector_covered}/{total}")
    mc4.metric("5 日法人覆蓋", f"{inst_covered}/{total}")
    cap_parts = []
    ai_at = ai_meta.get("generated_at")
    if ai_at:
        model = ai_meta.get("model", "claude")
        n_news = ai_meta.get("news_articles_injected", 0)
        cap_parts.append(f"AI ({model}, {n_news} news) 產出於 {ai_at}")
    if cap_parts:
        st.caption(" | ".join(cap_parts))


def render_strong_stocks() -> None:
    st.title("🌟 強勢股報告")

    # Mode radio (top)
    report_type = st.radio(
        "報告類型",
        options=["daily", "weekly"],
        format_func=lambda x: "📅 日報" if x == "daily" else "📊 週報",
        horizontal=True,
        key="strong_stocks_report_type",
    )

    if report_type == "weekly":
        st.warning(
            "⚠️ **探索功能 — informational tier**：週度 scoring 尚未經 IC 驗證，"
            "僅供盤勢回顧探索；不接 paper_trade / 出場邏輯。"
            "累積 3-6 個月後將補 IC 驗證。",
            icon="⚠️",
        )

    reports = _list_weekly_reports() if report_type == "weekly" else _list_daily_reports()
    if not reports:
        if report_type == "weekly":
            st.info(
                "尚無強勢股週報。請等週日 12:00 排程，或手動執行：\n"
                "```\n"
                "python tools/strong_stocks_weekly_screener.py\n"
                "python tools/strong_stocks_ai_analysis.py --weekly\n"
                "python tools/strong_stocks_render.py --weekly\n"
                "```"
            )
        else:
            st.info(
                "尚無強勢股日報。請等下次 scanner 排程跑完，或手動執行：\n"
                "```\n"
                "python tools/strong_stocks_daily.py\n"
                "python tools/strong_stocks_ai_analysis.py\n"
                "python tools/strong_stocks_render.py\n"
                "```"
            )
        return

    # --- Date / week selector ---
    date_labels = [r.stem for r in reports]
    col1, col2 = st.columns([3, 1])
    with col1:
        label_text = "週次" if report_type == "weekly" else "日期"
        selected_label = st.selectbox(
            f"{label_text} (共 {len(reports)} 份報告)",
            options=date_labels,
            index=0,
            key=f"strong_stocks_{report_type}_select",
        )
    with col2:
        if report_type == "weekly":
            st.caption("週報由排程於週日 12:00 自動產出")
        else:
            st.caption("日報由排程於 TUE-SAT 00:00 自動產出")

    selected_html = REPORTS_DIR / f"{selected_label}.html"
    selected_pdf = REPORTS_DIR / f"{selected_label}.pdf"

    # --- Metadata strip (only for latest, since 'latest' JSON snapshot matches it) ---
    is_latest = (selected_label == date_labels[0])
    if is_latest:
        meta = _load_metadata(report_type)
        if meta:
            if report_type == "weekly":
                _render_weekly_metadata_strip(meta)
            else:
                _render_daily_metadata_strip(meta)

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
        st.caption("⚠️ 此份的 PDF 不存在（只有 HTML）")

    st.divider()

    # --- Inline HTML render ---
    try:
        html = selected_html.read_text(encoding="utf-8")
    except Exception as e:
        st.error(f"HTML 讀取失敗: {e}")
        return

    # iframe height: 日報 12 欄 × 30 row + AI 5 段論述 → 4800px
    #                週報 13 欄 + banner + AI 5 段 → ~5200px
    iframe_height = 5200 if report_type == "weekly" else 4800
    components.html(html, height=iframe_height, scrolling=True)
