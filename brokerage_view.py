"""
brokerage_view.py — 台股投顧 YT 追蹤 (sidebar mode: brokerage_yt)

完全獨立於 Mode D screener / market scan 等既有功能。資料來源:
  - data/yt_brokerage_mentions.parquet  (mention-level, 1 mention = 1 row)
  - data/yt_brokerage_videos.parquet    (video-level, 含 analyst_view/recommended_action 等)

UI 結構：
  Section A — 投顧公司 + 時間視窗 selectbox + disclaimer
  Tab 1 — 整體看板（該投顧聚合）
  Tab 2 — 分析師個別 (selectbox 切換)
  Tab 3 — 個股反查 (ticker → 該投顧覆蓋)

合規：頁首紅色警語，AI 報告不接此資料源 (參考 plans/swirling-swimming-stallman.md)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent
MENTIONS_PATH = REPO / "data" / "yt_brokerage_mentions.parquet"
VIDEOS_PATH = REPO / "data" / "yt_brokerage_videos.parquet"


# --- Data loading ----------------------------------------------------------

@st.cache_data(ttl=300)
def _load_mentions() -> pd.DataFrame:
    if not MENTIONS_PATH.exists():
        return pd.DataFrame()
    df = pd.read_parquet(MENTIONS_PATH)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    return df


@st.cache_data(ttl=300)
def _load_videos() -> pd.DataFrame:
    if not VIDEOS_PATH.exists():
        return pd.DataFrame()
    df = pd.read_parquet(VIDEOS_PATH)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    return df


def _filter_window(df: pd.DataFrame, days: int) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return df
    latest = df["date"].max()
    cutoff = latest - timedelta(days=days - 1)
    return df[df["date"] >= cutoff].copy()


# --- Section A: 投顧/時間視窗 + disclaimer ---------------------------------

def _render_header_and_filters(df_v: pd.DataFrame) -> tuple[str, int]:
    """回傳 (brokerage_key, window_days)"""
    st.markdown("# 📺 投顧追蹤 — 台股投顧 YT 內容彙整")
    st.error(
        "⚠️ **投顧分析師資料僅供參考**，分析師觀點不等於本系統建議。"
        "點位 / 個股推薦皆為分析師個人意見，使用者須自行判斷風險。"
        "本系統 **不代為下單**、**不接 AI 報告**。"
    )

    if df_v.empty:
        st.info("尚無投顧 YT 資料。請先跑 `run_yt_brokerage_sync.bat`。")
        return "", 7

    available_broks = sorted(df_v["brokerage"].dropna().unique().tolist())
    brok_names = {
        bk: df_v[df_v["brokerage"] == bk]["brokerage_name"].iloc[0]
        for bk in available_broks
        if not df_v[df_v["brokerage"] == bk].empty
    }
    label_map = {bk: f"{brok_names.get(bk, bk)} ({bk})" for bk in available_broks}

    col_a, col_b, col_c = st.columns([2, 1, 2])
    with col_a:
        brokerage = st.selectbox(
            "投顧公司",
            options=available_broks,
            format_func=lambda x: label_map.get(x, x),
            key="brokerage_select",
        )
    with col_b:
        window_days = st.selectbox(
            "時間視窗",
            options=[7, 14, 30, 60],
            index=0,
            format_func=lambda d: f"近 {d} 日",
            key="brokerage_window",
        )
    with col_c:
        # 資料新鮮度
        sub = df_v[df_v["brokerage"] == brokerage] if brokerage else df_v
        if not sub.empty:
            latest = sub["date"].max()
            video_count = len(sub)
            st.caption(
                f"資料截至：**{latest}** / 累計 **{video_count}** 部影片 "
                f"/ 分析師 **{sub['analyst_key'].nunique()}** 位"
            )

    return brokerage, window_days


# --- Tab 1: 整體看板 -------------------------------------------------------

def _render_overview_tab(df_m: pd.DataFrame, df_v: pd.DataFrame,
                        brokerage: str, window_days: int):
    if not brokerage:
        return
    df_mr = _filter_window(df_m[df_m["brokerage"] == brokerage], window_days)
    df_vr = _filter_window(df_v[df_v["brokerage"] == brokerage], window_days)

    if df_mr.empty:
        st.info(f"近 {window_days} 日無 mention 資料。")
        return

    # --- 1. mention 熱度 leaderboard ---
    st.markdown(f"### 🔥 個股提及熱度 (近 {window_days} 日)")
    agg = df_mr[df_mr["ticker"] != ""].groupby(["ticker", "name"]).agg(
        mention_count=("video_id", "count"),
        analyst_count=("analyst_key", "nunique"),
        sentiment_avg=("sentiment", "mean"),
        confidence_avg=("confidence", "mean"),
    ).sort_values("mention_count", ascending=False).reset_index()
    if not agg.empty:
        agg["sentiment_avg"] = agg["sentiment_avg"].round(2)
        agg["confidence_avg"] = agg["confidence_avg"].round(0).astype(int)
        st.dataframe(
            agg.head(20).rename(columns={
                "ticker": "代號", "name": "公司",
                "mention_count": "提及次數", "analyst_count": "分析師數",
                "sentiment_avg": "平均情緒", "confidence_avg": "平均信心",
            }),
            use_container_width=True, hide_index=True,
        )
    else:
        st.caption("（無有效 ticker mention）")

    # --- 2. theme 熱度 ---
    st.markdown(f"### 🏷️ 題材熱度 (近 {window_days} 日)")
    if not df_vr.empty:
        theme_counts: dict[str, int] = {}
        for themes in df_vr["themes_discussed"]:
            for t in themes if isinstance(themes, (list, tuple)) else []:
                t = (t or "").strip()
                if t:
                    theme_counts[t] = theme_counts.get(t, 0) + 1
        if theme_counts:
            theme_df = pd.DataFrame(
                sorted(theme_counts.items(), key=lambda x: -x[1])[:20],
                columns=["題材", "影片數"],
            )
            st.dataframe(theme_df, use_container_width=True, hide_index=True)
        else:
            st.caption("（無題材資料）")

    # --- 3. recommended_action 共識 ---
    st.markdown(f"### 🎯 分析師動作共識 (近 {window_days} 日, 每影片 1 票)")
    if not df_vr.empty and "recommended_action" in df_vr.columns:
        action_counts = df_vr["recommended_action"].value_counts()
        action_counts = action_counts[action_counts.index != ""]
        if not action_counts.empty:
            st.bar_chart(action_counts)
        else:
            st.caption("（無動作資料）")

    # --- 4. macro_views 文字流 ---
    st.markdown(f"### 🗣️ 分析師大盤觀點 (近 {window_days} 日)")
    macro_rows = df_vr[
        (df_vr["analyst_view"].fillna("") != "") |
        (df_vr["macro_views"].fillna("") != "")
    ].sort_values("date", ascending=False)
    for _, row in macro_rows.head(20).iterrows():
        with st.container():
            st.markdown(
                f"**{row['date']}** | **{row['analyst_name']}** "
                f"({row.get('recommended_action', '')})"
            )
            if row.get("analyst_view"):
                st.markdown(f"- 大盤: {row['analyst_view']}")
            if row.get("macro_views"):
                st.markdown(f"- 美股/Fed: {row['macro_views']}")
            if row.get("risk_warning"):
                st.markdown(f"- ⚠️ 風險: {row['risk_warning']}")

    # --- 5. 點位累計 (entry/stop/target) ---
    priced = df_mr[
        (df_mr["entry"].notna()) | (df_mr["stop"].notna()) | (df_mr["target"].notna())
    ]
    if not priced.empty:
        st.markdown(f"### 💰 分析師點位整理 (近 {window_days} 日)")
        st.dataframe(
            priced[["date", "analyst_name", "ticker", "name", "entry", "stop",
                    "target", "timeframe", "thesis"]].rename(columns={
                "date": "日期", "analyst_name": "分析師",
                "ticker": "代號", "name": "公司",
                "entry": "進場", "stop": "停損", "target": "目標",
                "timeframe": "週期", "thesis": "論述",
            }),
            use_container_width=True, hide_index=True,
        )


# --- Tab 2: 分析師個別 -----------------------------------------------------

def _render_analyst_tab(df_m: pd.DataFrame, df_v: pd.DataFrame,
                       brokerage: str, window_days: int):
    if not brokerage:
        return
    df_mb = df_m[df_m["brokerage"] == brokerage]
    df_vb = df_v[df_v["brokerage"] == brokerage]
    if df_vb.empty:
        st.info("無資料。")
        return

    analysts = (df_vb.groupby(["analyst_key", "analyst_name"])
                .agg(video_count=("video_id", "nunique"),
                     latest=("date", "max"))
                .reset_index()
                .sort_values("latest", ascending=False))

    options = analysts["analyst_key"].tolist()
    label_map = {
        r["analyst_key"]: f"{r['analyst_name']} ({r['video_count']} 部, 最新 {r['latest']})"
        for _, r in analysts.iterrows()
    }
    sel = st.selectbox(
        "選擇分析師",
        options=options,
        format_func=lambda x: label_map.get(x, x),
        key="brokerage_analyst_select",
    )
    if not sel:
        return

    df_ma = _filter_window(df_mb[df_mb["analyst_key"] == sel], window_days)
    df_va = _filter_window(df_vb[df_vb["analyst_key"] == sel], window_days)

    if df_va.empty:
        st.info(f"該分析師近 {window_days} 日無資料。")
        return

    name = df_va["analyst_name"].iloc[0]
    st.markdown(f"## 👤 {name}（近 {window_days} 日）")

    # 影片摘要列表
    st.markdown("### 📹 影片列表")
    for _, row in df_va.sort_values("date", ascending=False).iterrows():
        with st.expander(
            f"{row['date']} — {row['title'][:50]} "
            f"({row.get('recommended_action', '')})",
            expanded=False,
        ):
            if row.get("analyst_view"):
                st.markdown(f"**大盤觀點**: {row['analyst_view']}")
            if row.get("macro_views"):
                st.markdown(f"**美股/Fed**: {row['macro_views']}")
            if row.get("risk_warning"):
                st.markdown(f"**⚠️ 風險**: {row['risk_warning']}")
            themes = row.get("themes_discussed", [])
            if isinstance(themes, (list, tuple)) and len(themes) > 0:
                st.markdown(f"**題材**: {', '.join(themes)}")
            video_url = f"https://www.youtube.com/watch?v={row['video_id']}"
            st.markdown(f"[🔗 看原影片]({video_url})")

            # 該影片的 mentions
            v_mentions = df_ma[df_ma["video_id"] == row["video_id"]]
            if not v_mentions.empty:
                st.dataframe(
                    v_mentions[["ticker", "name", "sentiment", "confidence",
                                "entry", "stop", "target", "timeframe", "thesis"]]
                    .rename(columns={
                        "ticker": "代號", "name": "公司",
                        "sentiment": "情緒", "confidence": "信心",
                        "entry": "進場", "stop": "停損", "target": "目標",
                        "timeframe": "週期", "thesis": "論述",
                    }),
                    use_container_width=True, hide_index=True,
                )

    # 該分析師近期 ticker 熱度
    if not df_ma.empty:
        st.markdown(f"### 🔥 該分析師近 {window_days} 日 mention 熱度")
        agg = df_ma[df_ma["ticker"] != ""].groupby(["ticker", "name"]).agg(
            count=("video_id", "count"),
            sentiment_avg=("sentiment", "mean"),
        ).sort_values("count", ascending=False).reset_index()
        if not agg.empty:
            agg["sentiment_avg"] = agg["sentiment_avg"].round(2)
            st.dataframe(
                agg.head(15).rename(columns={
                    "ticker": "代號", "name": "公司",
                    "count": "次數", "sentiment_avg": "平均情緒",
                }),
                use_container_width=True, hide_index=True,
            )


# --- Tab 3: 個股反查 -------------------------------------------------------

def _render_ticker_lookup_tab(df_m: pd.DataFrame, df_v: pd.DataFrame,
                              brokerage: str, window_days: int):
    if not brokerage:
        return
    st.markdown("### 🔍 個股反查 — 該投顧公司分析師覆蓋")
    ticker = st.text_input(
        "輸入股票代號 (e.g. 2330 / 2454)",
        key="brokerage_ticker_input",
    ).strip()
    if not ticker:
        st.caption("輸入代號以查詢該投顧公司分析師對該股的歷史討論。")
        return

    df_mr = _filter_window(
        df_m[(df_m["brokerage"] == brokerage) & (df_m["ticker"] == ticker)],
        window_days,
    )
    if df_mr.empty:
        st.warning(f"近 {window_days} 日該投顧無分析師討論過 {ticker}。")
        return

    name = df_mr["name"].iloc[0] if not df_mr.empty else ticker
    st.markdown(f"#### {ticker} {name} — 近 {window_days} 日討論")

    col1, col2, col3 = st.columns(3)
    col1.metric("提及次數", len(df_mr))
    col2.metric("覆蓋分析師", df_mr["analyst_key"].nunique())
    col3.metric("平均情緒", f"{df_mr['sentiment'].mean():+.2f}")

    st.dataframe(
        df_mr.sort_values("date", ascending=False)[[
            "date", "analyst_name", "sentiment", "confidence",
            "entry", "stop", "target", "timeframe", "thesis", "video_id",
        ]].rename(columns={
            "date": "日期", "analyst_name": "分析師",
            "sentiment": "情緒", "confidence": "信心",
            "entry": "進場", "stop": "停損", "target": "目標",
            "timeframe": "週期", "thesis": "論述", "video_id": "影片 ID",
        }),
        use_container_width=True, hide_index=True,
    )


# --- Public entry point ----------------------------------------------------

def render_brokerage_yt():
    df_m = _load_mentions()
    df_v = _load_videos()

    brokerage, window_days = _render_header_and_filters(df_v)
    if df_v.empty:
        return

    tab1, tab2, tab3 = st.tabs(["📊 整體看板", "👤 分析師個別", "🔍 個股反查"])
    with tab1:
        _render_overview_tab(df_m, df_v, brokerage, window_days)
    with tab2:
        _render_analyst_tab(df_m, df_v, brokerage, window_days)
    with tab3:
        _render_ticker_lookup_tab(df_m, df_v, brokerage, window_days)
