"""
YT Guest Credibility Analysis (2026-04-25, Phase 3 #10 提早啟動)

把 YT mention × ohlcv forward return 對齊,group by guest 算 credibility 指標。
Backfill 4 個月後執行,輸出排名表。

Methodology:
- 每筆 mention (guest, ticker, date, sentiment) → 算 ticker 在 (date, date+H 日) 報酬
- 同一影片多個 guest 都歸屬該 mention (VTT 沒講者標記,只能粗略歸屬)
- Aggregate by guest:
    sample_size: 該 guest 出現的 mention 總數
    hit_rate: forward return > 0 比率
    mean_return: 平均 forward return
    sharpe_proxy: mean / std
    sentiment_aligned_hit: sentiment +1 後續真漲 / sentiment -1 後續真跌 比率

Limitations:
- 不知道哪句話誰講,所有 guest 共享 mention
- 樣本相關性 (同題材週期 mentions)
- 4 個月對冷門 guest 仍 underpowered

CLI:
  python tools/yt_guest_credibility_analysis.py            # 跑 H=20 日
  python tools/yt_guest_credibility_analysis.py --horizon 60
  python tools/yt_guest_credibility_analysis.py --min-mentions 10
"""
from __future__ import annotations

import argparse
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
MENTION_PANEL = REPO / "data" / "sector_tags_dynamic.parquet"
VIDEO_PANEL = REPO / "data" / "yt_videos_panel.parquet"
OHLCV_TW = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"
OUT_CSV = REPO / "reports" / "yt_guest_credibility.csv"
OUT_MD = REPO / "reports" / "yt_guest_credibility.md"

# Guest 名字 normalization: 短稱/暱稱/藝名/敬稱 → 統一全名
#
# 兩類 alias:
#   (A) 後綴匹配 / 敬稱對應 — 程式即可推斷,但仍需手動維護 (避免誤觸不同人)
#   (B) ⚠️ 藝名 / 暱稱 — 必須人工查證才能 merge (例: 連乾文=阿文師)
#
# 維護 SOP (新影片進來後):
#   1. 跑 yt_guest_credibility_analysis.py 看 84+ guest 列表
#   2. 找疑似同人 (後綴匹配 / 同 video_id 但分開 / n=videos 完全一致)
#   3. (B) 類藝名上網確認 (Wiki / 節目官網) 後加進 map
#   4. 不確定的寧可不 merge
GUEST_ALIAS_MAP = {
    # (A) 後綴匹配 / 敬稱對應
    "志誠": "張志誠",
    "明翰": "蔡明翰",
    "奇芬": "林奇芬",
    "冠嶔": "王冠嶔",
    "建承": "陳建承",
    "建承老師": "陳建承",
    "昱衡": "劉昱衡",
    "俊敏": "葉俊敏",
    "俊敏老師": "葉俊敏",
    "葉俊敏": "葉俊敏",  # identity (確保 case)
    "慶龍": "孫慶龍",
    "嘉明大哥": "孫嘉明",      # 2026-04-26 user 確認 = 孫嘉明 (非嘉偉老師)
    "嘉明": "孫嘉明",
    "蕙慈老師": "蕙慈",
    "蜀芳老師": "蜀芳",
    "林忠哥": "林忠",
    "奎國老師": "奎國",
    "博傑": "許博傑",
    "明哲": "謝明哲",          # 2026-04-26 user 確認 (vs 許明哲/謝明志 不同人)
    # (B) 藝名 / 暱稱 — 人工查證後加入 (2026-04-26 A audit confirmed 9+1)
    "連乾文": "阿文師",        # 阿文師本名
    "昆仁":   "陳昆仁",        # 中視財經早點名分析師
    "庭皓":   "游庭皓",        # 財經皓角頻道主
    "紫東":   "黃紫東",        # 運達投顧
    "毓棠":   "許毓棠",        # 永誠國際投顧
    "其展":   "李其展",        # 鈔錢部署常態,Yahoo 股市專欄
    "聖傑":   "林聖傑",        # 2019 錢線百分百影片標題明確
    "智霖":   "陳智霖",        # 亨達證券投顧
    "正華":   "蔡正華",        # 大來國際投顧
    "子昂":   "陳子昂",        # 資策會 MIC 資深總監
}

# Host 黑名單: 主持人不算 guest 評等 (沒在發布投資觀點)
GUEST_HOST_BLACKLIST = {
    "祝華",  # 主持人 (2026-04-26 user 確認)
}


def normalize_guest(name: str) -> str:
    """正規化 guest 名字: alias map 統一短稱 → 全名;其他維持原樣。"""
    n = (name or "").strip()
    return GUEST_ALIAS_MAP.get(n, n)


def load_inputs():
    if not MENTION_PANEL.exists() or not VIDEO_PANEL.exists():
        raise FileNotFoundError(f"Need {MENTION_PANEL.name} + {VIDEO_PANEL.name}")
    if not OHLCV_TW.exists():
        raise FileNotFoundError(f"Need {OHLCV_TW}")

    mentions = pd.read_parquet(MENTION_PANEL)
    videos = pd.read_parquet(VIDEO_PANEL)
    ohlcv = pd.read_parquet(OHLCV_TW, columns=['stock_id', 'date', 'Close'])
    ohlcv['date'] = pd.to_datetime(ohlcv['date']).dt.date

    print(f"  mentions: {len(mentions)}", file=sys.stderr)
    print(f"  videos:   {len(videos)}", file=sys.stderr)
    print(f"  ohlcv:    {len(ohlcv)} rows / {ohlcv['stock_id'].nunique()} tickers",
          file=sys.stderr)
    return mentions, videos, ohlcv


def attach_forward_return(mentions: pd.DataFrame, ohlcv: pd.DataFrame,
                          horizon: int) -> pd.DataFrame:
    """每筆 mention 加 close_at_mention 跟 close_at_mention+H 跟 forward_return."""
    df = mentions.copy()
    df['date'] = pd.to_datetime(df['date']).dt.date

    # build lookup: (ticker, date) -> close
    ohlcv_idx = ohlcv.set_index(['stock_id', 'date'])['Close']

    def lookup_close(ticker, target_date):
        try:
            return float(ohlcv_idx[(ticker, target_date)])
        except KeyError:
            # 找最近的可用日期 (<= target)
            sub = ohlcv[(ohlcv['stock_id'] == ticker) & (ohlcv['date'] <= target_date)]
            if sub.empty:
                return None
            return float(sub.sort_values('date').iloc[-1]['Close'])

    rows = []
    for _, m in df.iterrows():
        sid = str(m['ticker']).strip()
        if not sid:
            continue
        d0 = m['date']
        d1 = d0 + timedelta(days=horizon)
        c0 = lookup_close(sid, d0)
        c1 = lookup_close(sid, d1)
        if c0 is None or c1 is None or c0 <= 0:
            continue
        ret = (c1 / c0 - 1.0)
        rows.append({
            'video_id': m['video_id'],
            'date': d0,
            'ticker': sid,
            'sentiment': int(m['sentiment']),
            'close_d0': c0,
            'close_dH': c1,
            'forward_return': ret,
        })
    return pd.DataFrame(rows)


def expand_by_guest(returns_df: pd.DataFrame, videos: pd.DataFrame) -> pd.DataFrame:
    """每筆 mention × 每位該影片的 guest 展開成多筆."""
    vmap = {}
    for _, v in videos.iterrows():
        gs = list(v['guests']) if v['guests'] is not None else []
        # 正規化 + 過濾主持人 + 去重 (同影片同 alias 對應到同人時不雙計)
        normed = [normalize_guest(str(g)) for g in gs if str(g).strip()]
        normed = [g for g in normed if g not in GUEST_HOST_BLACKLIST]
        vmap[v['video_id']] = list(dict.fromkeys(normed))  # preserve order, dedupe

    rows = []
    for _, r in returns_df.iterrows():
        guests = vmap.get(r['video_id'], [])
        if not guests:
            continue
        for g in guests:
            rows.append({**r.to_dict(), 'guest': g})
    return pd.DataFrame(rows)


def aggregate_by_guest(expanded: pd.DataFrame, min_mentions: int) -> pd.DataFrame:
    """每個 guest 統計 hit_rate / mean / sharpe."""
    if expanded.empty:
        return pd.DataFrame()

    agg = expanded.groupby('guest').agg(
        mentions=('forward_return', 'count'),
        unique_tickers=('ticker', 'nunique'),
        unique_videos=('video_id', 'nunique'),
        mean_return=('forward_return', 'mean'),
        std_return=('forward_return', 'std'),
        hit_rate=('forward_return', lambda s: (s > 0).mean()),
    ).reset_index()

    # sentiment-aligned hit (sentiment +1 → return > 0 / sentiment -1 → return < 0)
    aligned = []
    for g, sub in expanded.groupby('guest'):
        bullish = sub[sub['sentiment'] > 0]
        bearish = sub[sub['sentiment'] < 0]
        b_correct = (bullish['forward_return'] > 0).sum() if len(bullish) else 0
        s_correct = (bearish['forward_return'] < 0).sum() if len(bearish) else 0
        total_directional = len(bullish) + len(bearish)
        aligned_hit = (b_correct + s_correct) / total_directional if total_directional else None
        aligned.append({
            'guest': g,
            'directional_n': total_directional,
            'sentiment_aligned_hit': aligned_hit,
        })
    aligned_df = pd.DataFrame(aligned)

    out = agg.merge(aligned_df, on='guest', how='left')
    out['sharpe_proxy'] = out['mean_return'] / out['std_return']
    out = out[out['mentions'] >= min_mentions].copy()
    out = out.sort_values('mean_return', ascending=False)
    return out


def write_report(out: pd.DataFrame, horizon: int, min_mentions: int):
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False, encoding='utf-8-sig')
    print(f"  written CSV: {OUT_CSV}", file=sys.stderr)

    lines = []
    lines.append(f"# YT Guest Credibility (H={horizon}d, min mentions={min_mentions})")
    lines.append("")
    lines.append(f"Total guests passed threshold: **{len(out)}**")
    lines.append("")
    if out.empty:
        lines.append("(無 guest 達 min mentions 門檻)")
    else:
        lines.append("| Guest | Mentions | Tickers | Videos | Mean Ret | Hit Rate | Sentiment-Aligned | Sharpe |")
        lines.append("|-------|----------|---------|--------|----------|----------|-------------------|--------|")
        for _, r in out.iterrows():
            sa = f"{r['sentiment_aligned_hit']:.0%}" if pd.notna(r['sentiment_aligned_hit']) else "-"
            lines.append(
                f"| {r['guest']} | {int(r['mentions'])} | {int(r['unique_tickers'])} | "
                f"{int(r['unique_videos'])} | {r['mean_return']:+.2%} | {r['hit_rate']:.0%} | "
                f"{sa} | {r['sharpe_proxy']:+.2f} |"
            )

    OUT_MD.write_text("\n".join(lines), encoding='utf-8')
    print(f"  written MD:  {OUT_MD}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", type=int, default=20, help="forward return 天數 (default 20)")
    ap.add_argument("--min-mentions", type=int, default=5,
                    help="最少 mention 數才列入排名 (default 5)")
    args = ap.parse_args()

    print(f"=== YT Guest Credibility (H={args.horizon}d) ===", file=sys.stderr)
    mentions, videos, ohlcv = load_inputs()

    print(f"\nStep 1: 計算 forward return (H={args.horizon}d) ...", file=sys.stderr)
    returns_df = attach_forward_return(mentions, ohlcv, args.horizon)
    print(f"  matched mentions with returns: {len(returns_df)}", file=sys.stderr)

    print(f"\nStep 2: 對齊 guest (每影片多 guest 共享 mention) ...", file=sys.stderr)
    expanded = expand_by_guest(returns_df, videos)
    print(f"  guest-mention rows: {len(expanded)}", file=sys.stderr)

    print(f"\nStep 3: aggregate by guest (min mentions={args.min_mentions}) ...",
          file=sys.stderr)
    out = aggregate_by_guest(expanded, args.min_mentions)
    print(f"  guests passed threshold: {len(out)}", file=sys.stderr)

    print(f"\nStep 4: 寫出 report ...", file=sys.stderr)
    write_report(out, args.horizon, args.min_mentions)

    if not out.empty:
        print("\n=== TOP 5 by mean return ===", file=sys.stderr)
        for _, r in out.head(5).iterrows():
            print(f"  {r['guest']:<10s} n={int(r['mentions']):<3d} "
                  f"mean={r['mean_return']:+.2%} hit={r['hit_rate']:.0%} "
                  f"sharpe={r['sharpe_proxy']:+.2f}", file=sys.stderr)
        print("\n=== BOTTOM 5 ===", file=sys.stderr)
        for _, r in out.tail(5).iterrows():
            print(f"  {r['guest']:<10s} n={int(r['mentions']):<3d} "
                  f"mean={r['mean_return']:+.2%} hit={r['hit_rate']:.0%} "
                  f"sharpe={r['sharpe_proxy']:+.2f}", file=sys.stderr)


if __name__ == "__main__":
    main()
