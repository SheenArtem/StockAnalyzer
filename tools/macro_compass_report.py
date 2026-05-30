"""
macro_compass_report.py -- 總經大盤風向 AI 報告產生器

流程：
  1. 收集所有 panel 資料（FRED / breadth / systemic chip / sentiment / valuation）
  2. 組裝統一 context（含當前值 + 30 天歷史 + 百分位 + 警戒線）
  3. 平行呼叫 Claude Opus + Gemini 3.1 Pro
  4. Claude Sonnet council 統整為單一 HTML 報告
  5. 報告必含「資料缺口建議」段，回頭指引下一輪要補哪些指標
  6. 存 data/macro_reports/YYYY-MM-DD_HHMMSS.html

LLM 規範 (CLAUDE.md):
  - Claude: --model opus --allowedTools "*" (timeout 600s)
  - Gemini: gemini-3.1-pro-preview (timeout 900s)
  - Council 統整: --model sonnet --allowedTools "WebSearch,WebFetch" (timeout 600s)

執行：
  python tools/macro_compass_report.py [--no-gemini] [--no-claude]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

DATA = REPO / "data"
OUT_DIR = DATA / "macro_reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CLAUDE_CLI = shutil.which("claude") or "claude"
GEMINI_CLI = shutil.which("gemini") or "gemini"

CLAUDE_OPUS_TIMEOUT = 600
CLAUDE_SONNET_TIMEOUT = 600
GEMINI_TIMEOUT = 900


# ============================================================
#  資料收集
# ============================================================

def _safe_read_parquet(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception as e:
        logger.warning("read failed %s: %s", path, e)
        return None


def _format_series_summary(df: pd.DataFrame, col: str, n_recent: int = 30) -> str:
    """格式化一個 column 的近 30 天 summary。"""
    if df is None or df.empty or col not in df.columns:
        return f"  {col}: N/A"
    s = df[col].dropna()
    if s.empty:
        return f"  {col}: N/A"
    last = s.iloc[-1]
    n = min(n_recent, len(s))
    recent = s.tail(n)
    p_now = (s <= last).mean() * 100  # 全期百分位
    return (f"  {col}: 當前 {last:.4g} "
            f"(全期百分位 {p_now:.0f}%；"
            f"近{n}天 min={recent.min():.4g} max={recent.max():.4g} "
            f"avg={recent.mean():.4g})")


def collect_context() -> str:
    """組裝給 LLM 的完整 panel context。"""
    lines = [
        "=" * 70,
        f"市場日期：{datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 70,
        "",
    ]

    # FRED macro
    fred = _safe_read_parquet(DATA / "macro" / "fred_panel.parquet")
    lines.append("### A. 國際 macro / 信用 / 流動性 (FRED, 2010-2026)")
    if fred is not None and not fred.empty:
        fred = fred.sort_values('date').reset_index(drop=True)
        lines.append(f"資料日期 last={fred['date'].iloc[-1]}")
        for col in ['hy_oas', 'hy_oas_rank', 'yield_curve_10y_2y', 'yield_curve_10y_3m',
                    'dxy_close', 'dxy_chg_4w', 'vix_close', 'fed_bs_trillion', 'fed_bs_chg_4w']:
            lines.append(_format_series_summary(fred, col))
    else:
        lines.append("  (尚未建立 - 執行 tools/fetch_fred_macro.py)")
    lines.append("")

    # Breadth
    breadth = _safe_read_parquet(DATA / "breadth" / "tw_breadth.parquet")
    lines.append("### B. 台股市場廣度 (1548 檔聚合, 2006-2026)")
    if breadth is not None and not breadth.empty:
        breadth = breadth.sort_values('date').reset_index(drop=True)
        lines.append(f"資料日期 last={breadth['date'].iloc[-1]}")
        for col in ['advances', 'declines', 'adl', 'mcclellan_oscillator',
                    'ad_ratio', 'breadth_thrust_10d', 'new_high_minus_low',
                    'new_highs_52w', 'new_lows_52w']:
            lines.append(_format_series_summary(breadth, col))
    else:
        lines.append("  (尚未建立 - 執行 tools/build_tw_breadth.py)")
    lines.append("")

    # Systemic chip
    sys_chip = _safe_read_parquet(DATA / "macro" / "systemic_chip.parquet")
    lines.append("### C. 機構撤退訊號 / Systemic Chip (2016-2026)")
    if sys_chip is not None and not sys_chip.empty:
        sys_chip = sys_chip.sort_values('date').reset_index(drop=True)
        lines.append(f"資料日期 last={sys_chip['date'].iloc[-1]}")
        for col in ['sbl_total', 'foreign_holding_avg', 'foreign_holding_chg_4w',
                    'sbl_change_4w_pct', 'margin_to_index_ratio', 'margin_ratio_z_252d',
                    'short_to_long_ratio', 'pcr_oi']:
            lines.append(_format_series_summary(sys_chip, col))
        last = sys_chip.iloc[-1]
        lines.append(f"  flags: A={last.get('group_a_flag')} ({last.get('group_a_reason', '')}) | "
                     f"B={last.get('group_b_flag')} ({last.get('group_b_reason', '')}) | "
                     f"D={last.get('group_d_flag')}")
    else:
        lines.append("  (尚未建立 - 執行 tools/build_systemic_chip_panel.py)")
    lines.append("")

    # Banner risk
    try:
        from market_banner import _get_banner_data
        banner = _get_banner_data()
        risk = (banner or {}).get('risk_score', {}) or {}
        if risk.get('composite') is not None:
            lines.append("### D. Banner 綜合風險 (v3 calibration, 2002-2026)")
            lines.append(f"  composite={risk.get('composite'):.1f} zone={risk.get('zone')}")
            for sig, info in (risk.get('breakdown') or {}).items():
                lines.append(f"  {sig}: rank={info.get('rank')} weight={info.get('weight')}")
            lines.append("")
    except Exception as e:
        logger.warning("banner data fetch failed: %s", e)

    # Sentiment 既有
    pcr = _safe_read_parquet(DATA / "sentiment" / "pcr_history.parquet")
    if pcr is not None and not pcr.empty and 'pcr_oi' in pcr.columns:
        lines.append("### E. 情緒/期權 (pcr_history)")
        for col in ['pcr_oi', 'pcr_volume']:
            if col in pcr.columns:
                lines.append(_format_series_summary(pcr, col))
        lines.append("")

    # 最後 7 天 trend table
    lines.append("### F. 近 7 個交易日重點指標 trend")
    if breadth is not None:
        last7 = breadth.tail(7)[['date', 'advances', 'declines', 'mcclellan_oscillator',
                                  'new_high_minus_low']].to_string(index=False)
        lines.append("廣度:")
        lines.append(last7)
        lines.append("")
    if fred is not None:
        last7 = fred.tail(7)[['date', 'hy_oas', 'yield_curve_10y_2y',
                              'vix_close', 'dxy_close']].to_string(index=False)
        lines.append("Macro:")
        lines.append(last7)

    return "\n".join(lines)


# ============================================================
#  Prompt 組裝
# ============================================================

def build_prompt(context: str, fmt: str = "html") -> str:
    """組裝報告 prompt。

    fmt='html'：本地 LLM pipeline 用（要求輸出 HTML body 內嵌 iframe）。
    fmt='md'  ：使用者複製到 claude.ai 用（要求輸出 Markdown，網頁端較好讀）。
    兩版段落內容相同，只有標題語法 + 輸出格式指示不同（2026-05-30 加 md 匯出）。
    """
    if fmt == "md":
        directive = ("請產出一份「總經大盤風向研究報告」，用 **Markdown** 表達"
                     "（## 主標 / ### 次標 / 段落 / `-` 條列），內容必須包含以下五段：")
        def H(n, t):
            return f"## {n}. {t}"
        out_fmt_rule = "- 用台灣繁體中文，Markdown 格式輸出"
    else:
        directive = ("請產出一份「總經大盤風向研究報告」，內容必須包含以下五段"
                     "（用 HTML <h2>/<h3>/<p>/<ul> 表達，最後輸出整段乾淨的 HTML body 即可，"
                     "不要 <html>/<head>/<body> wrapper）：")
        def H(n, t):
            return f"<h2>{n}. {t}</h2>"
        out_fmt_rule = "- 用台灣繁體中文"

    return f"""你是一位資深總體經濟與量化研究員，以下是台股 + 美股大盤的當前完整資料面板。

【資料面板】
{context}

【任務】
{directive}

{H(1, "當前風險定調")}
- 5 階燈號：危機/嚴重/警戒/留意/安全 -- 給出明確選一
- 一句話定調 (50 字內)
- 主要驅動訊號 top 3 (依重要性排序，每條附「為何重要」)

{H(2, "1-4 週情境推演")}
- Scenario A (基本情境，機率 % 估計)：什麼會發生 + 觸發條件
- Scenario B (悲觀情境)：同上
- Scenario C (樂觀情境)：同上

{H(3, "訊號交叉驗證")}
- 哪些訊號互相印證？(例：HY OAS 高 + 廣度轉弱 + SBL 增 = 多重共振)
- 哪些訊號彼此衝突？怎麼解讀？
- 每個訊號的 false positive 風險 (歷史上幾次假警報？)

{H(4, "操作建議 (informational only, SOP-14)")}
- 部位水位建議區間（如 5-7 成）
- 避險工具建議（PUT / 反向 ETF / 提高現金）
- 進場/觀察條件 trigger price 或 indicator level
- 強調這是 informational tier，非自動 portfolio rebalance gate

{H(5, "資料缺口與下一步建議")}
這段最重要，請仔細思考：
- 當前 panel 缺什麼資料？(列 5-10 個指標)
- 哪些資料能讓 1-4 週 lead 更可靠？(列出具體 FRED ID / 資料源 / 取得方式)
- 哪些指標應該優先補（按 IC 預期 + 取得難度排序）
- 哪些訊號目前是 stub（如 group_c/e_flag = low），怎麼填補
- 是否有跨市場資料（中國/日本/歐洲）能加強？

【輸出規範】
{out_fmt_rule}
- 數字精準到小數點 2 位
- 文字嚴謹但不過度套話，避免空話
- 必須引用 panel 的具體數字（不能說「市場可能波動」這種空話）
- 第 5 段是真正的價值，不要敷衍
"""


def export_prompt(fmt: str = "md") -> Path:
    """只組 prompt 不呼叫任何 LLM，寫檔供使用者複製到 claude.ai 自行產生報告。
    （2026-05-30：避免本地 Opus CLI 消耗 Agent SDK Credit。）"""
    context = collect_context()
    prompt = build_prompt(context, fmt="md")  # claude.ai 端用 markdown 輸出較好讀
    if fmt == "json":
        out_path = OUT_DIR / "latest_prompt.json"
        out_path.write_text(json.dumps(
            {"generated_at": datetime.now().isoformat(timespec="seconds"),
             "panel_context": context,
             "report_prompt": prompt},
            ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        out_path = OUT_DIR / "latest_prompt.md"
        out_path.write_text(prompt, encoding="utf-8")
    logger.info("Prompt exported (%s, %d chars) -> %s", fmt, len(prompt), out_path)
    return out_path


# ============================================================
#  CLI 呼叫
# ============================================================

def call_claude_opus(prompt: str) -> tuple[bool, str]:
    """呼叫 Claude Opus CLI。"""
    logger.info("Calling Claude Opus (timeout=%ds)...", CLAUDE_OPUS_TIMEOUT)
    try:
        result = subprocess.run(
            [CLAUDE_CLI, "-p",
             "--model", "opus",
             "--effort", "xhigh",  # 2026-05-21: 必須 CLI 帶 (settings.json effortLevel 不影響 -p)
             "--allowedTools", "*",
             "--output-format", "text"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=CLAUDE_OPUS_TIMEOUT,
            encoding='utf-8',
        )
        if result.returncode != 0:
            return False, f"Claude exit {result.returncode}: {result.stderr.strip()}"
        return True, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, f"Claude timeout {CLAUDE_OPUS_TIMEOUT}s"
    except FileNotFoundError:
        return False, "Claude CLI not found"


def call_gemini(prompt: str) -> tuple[bool, str]:
    """呼叫 Gemini CLI（gemini-3.1-pro-preview）。"""
    logger.info("Calling Gemini 3.1 Pro (timeout=%ds)...", GEMINI_TIMEOUT)
    try:
        result = subprocess.run(
            [GEMINI_CLI, "-p", prompt,
             "-m", "gemini-3.1-pro-preview", "-y"],
            capture_output=True,
            text=True,
            timeout=GEMINI_TIMEOUT,
            encoding='utf-8',
        )
        if result.returncode != 0:
            return False, f"Gemini exit {result.returncode}: {result.stderr.strip()[:500]}"
        return True, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, f"Gemini timeout {GEMINI_TIMEOUT}s"
    except FileNotFoundError:
        return False, "Gemini CLI not found"


def call_claude_sonnet_council(claude_out: str, gemini_out: str, context: str) -> tuple[bool, str]:
    """Council 統整：給 Sonnet 看兩家結果 → 統整出最終 HTML 報告。"""
    council_prompt = f"""你是研究 council 的主席。下面兩位研究員針對同一份 panel 各自產出了報告，請你統整出最終版。

【原始 panel 摘要】
{context[:3000]}

【研究員 A: Claude Opus】
{claude_out}

【研究員 B: Gemini 3.1 Pro】
{gemini_out}

【你的任務】
1. 整合兩家結論，明確指出「兩家共識點」與「分歧點 + 你的判讀」
2. 以 5 段結構輸出最終 HTML：1. 風險定調 / 2. 情境推演 / 3. 訊號交叉驗證 / 4. 操作建議 / 5. **資料缺口與下一步**
3. 最終 HTML body 只用 <h2>/<h3>/<p>/<ul>/<table>/<strong>/<em>，不要 <html>/<head>/<body> wrapper
4. 在最開頭加一個 <div class="meta"> 寫「兩家共識度 X/10」
5. 第 5 段必須具體列出 5-10 個建議補充的指標與資料源

開始輸出 HTML body：
"""
    logger.info("Calling Claude Sonnet council (timeout=%ds)...", CLAUDE_SONNET_TIMEOUT)
    try:
        result = subprocess.run(
            [CLAUDE_CLI, "-p",
             "--model", "sonnet",
             "--effort", "xhigh",  # 2026-05-21: 必須 CLI 帶
             "--allowedTools", "WebSearch,WebFetch",
             "--output-format", "text"],
            input=council_prompt,
            capture_output=True,
            text=True,
            timeout=CLAUDE_SONNET_TIMEOUT,
            encoding='utf-8',
        )
        if result.returncode != 0:
            return False, f"Sonnet exit {result.returncode}: {result.stderr.strip()}"
        return True, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, f"Sonnet timeout {CLAUDE_SONNET_TIMEOUT}s"


# ============================================================
#  HTML 包裝
# ============================================================

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<title>總經大盤風向 — {date}</title>
<style>
  body {{ font-family: 'Segoe UI', 'Microsoft JhengHei', sans-serif; max-width: 980px;
         margin: 30px auto; padding: 20px; line-height: 1.7; color: #222;
         background: #fafbfc; }}
  h1 {{ border-bottom: 3px solid #2c3e50; padding-bottom: 12px; color: #2c3e50; }}
  h2 {{ color: #2980b9; border-left: 5px solid #2980b9; padding-left: 12px;
        margin-top: 32px; }}
  h3 {{ color: #34495e; margin-top: 20px; }}
  .meta {{ background: #ecf0f1; padding: 12px 16px; border-radius: 6px;
           font-size: 0.95em; margin-bottom: 24px; }}
  .header-meta {{ font-size: 0.85em; color: #7f8c8d; margin-bottom: 24px; }}
  table {{ border-collapse: collapse; margin: 12px 0; }}
  table th, table td {{ border: 1px solid #bdc3c7; padding: 6px 10px; }}
  ul {{ padding-left: 22px; }}
  strong {{ color: #c0392b; }}
  em {{ color: #16a085; font-style: normal; font-weight: 600; }}
  .agent-block {{ border: 1px dashed #95a5a6; padding: 16px; margin-top: 30px;
                  border-radius: 6px; background: #fff; }}
  .footer {{ margin-top: 50px; padding-top: 16px; border-top: 1px solid #ddd;
             color: #7f8c8d; font-size: 0.85em; }}
</style>
</head>
<body>
<h1>🧭 總經大盤風向 AI 研究報告</h1>
<div class="header-meta">產出時間：{datetime} | 報告 ID：{rid} | informational tier (SOP-14)</div>

{council_html}

<details>
  <summary><strong>原始 LLM 回答 (兩家研究員獨立產出)</strong></summary>
  <div class="agent-block">
    <h3>📘 Claude Opus</h3>
    <div>{claude_html}</div>
  </div>
  <div class="agent-block">
    <h3>📗 Gemini 3.1 Pro</h3>
    <div>{gemini_html}</div>
  </div>
</details>

<div class="footer">
  資料來源：FRED / TWSE / FinMind / TDCC / TAIFEX / 主計處<br>
  注意：此報告為 informational tier (SOP-14)，僅供研究參考，<strong>不是自動 portfolio rebalance gate</strong>，<br>
  也不構成投資建議。任何下單決策請佐以個股分析、風險容忍度與獨立判斷。
</div>
</body>
</html>"""


def _to_html_safe(text: str) -> str:
    """簡易 markdown -> HTML 轉換 (基本 fallback)；如果 LLM 已輸 HTML 就保留。"""
    if "<h2" in text or "<p>" in text:
        return text
    # markdown 簡轉
    import html
    text = html.escape(text)
    lines = text.split("\n")
    out = []
    in_list = False
    for ln in lines:
        ln_strip = ln.strip()
        if ln_strip.startswith("## "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h2>{ln_strip[3:]}</h2>")
        elif ln_strip.startswith("### "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h3>{ln_strip[4:]}</h3>")
        elif ln_strip.startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{ln_strip[2:]}</li>")
        elif ln_strip == "":
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append("")
        else:
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<p>{ln_strip}</p>" if ln_strip else "")
    if in_list:
        out.append("</ul>")
    return "\n".join(out)


# ============================================================
#  主流程
# ============================================================

def run(use_claude: bool = True, use_gemini: bool = True, council: bool = True) -> Path:
    context = collect_context()
    prompt = build_prompt(context)
    logger.info("Prompt assembled: %d chars", len(prompt))

    claude_ok, claude_out = (False, "(disabled)")
    gemini_ok, gemini_out = (False, "(disabled)")

    # 平行呼叫
    threads = []
    results = {}

    if use_claude:
        def _run_claude():
            results['claude'] = call_claude_opus(prompt)
        t = threading.Thread(target=_run_claude)
        t.start()
        threads.append(t)

    if use_gemini:
        def _run_gemini():
            results['gemini'] = call_gemini(prompt)
        t = threading.Thread(target=_run_gemini)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    if 'claude' in results:
        claude_ok, claude_out = results['claude']
        logger.info("Claude result: ok=%s len=%d", claude_ok, len(claude_out))
    if 'gemini' in results:
        gemini_ok, gemini_out = results['gemini']
        logger.info("Gemini result: ok=%s len=%d", gemini_ok, len(gemini_out))

    # Council
    if council and claude_ok and gemini_ok:
        council_ok, council_out = call_claude_sonnet_council(claude_out, gemini_out, context)
    else:
        # Fallback：哪家成功就用哪家當 council
        council_ok = claude_ok or gemini_ok
        council_out = claude_out if claude_ok else gemini_out
        logger.warning("Skipping council (claude_ok=%s gemini_ok=%s)", claude_ok, gemini_ok)

    # HTML 組裝
    rid = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    html_body = HTML_TEMPLATE.format(
        date=datetime.now().strftime('%Y-%m-%d'),
        datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        rid=rid,
        council_html=_to_html_safe(council_out) if council_ok else
            f'<div style="color:red"><b>Council 失敗：</b>{council_out}</div>',
        claude_html=_to_html_safe(claude_out) if claude_ok else
            f'<div style="color:gray">Claude 不可用：{claude_out}</div>',
        gemini_html=_to_html_safe(gemini_out) if gemini_ok else
            f'<div style="color:gray">Gemini 不可用：{gemini_out}</div>',
    )

    out_path = OUT_DIR / f"{rid}.html"
    out_path.write_text(html_body, encoding='utf-8')
    logger.info("Saved -> %s", out_path)

    # 同時寫一份 latest.html
    latest = OUT_DIR / "latest.html"
    latest.write_text(html_body, encoding='utf-8')

    # metadata
    meta = {
        'rid': rid,
        'datetime': datetime.now().isoformat(),
        'claude_ok': claude_ok,
        'gemini_ok': gemini_ok,
        'council_ok': council_ok,
        'context_chars': len(context),
        'prompt_chars': len(prompt),
        'claude_chars': len(claude_out) if claude_ok else 0,
        'gemini_chars': len(gemini_out) if gemini_ok else 0,
    }
    (OUT_DIR / f"{rid}.meta.json").write_text(json.dumps(meta, indent=2), encoding='utf-8')

    return out_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-claude', action='store_true')
    parser.add_argument('--no-gemini', action='store_true')
    parser.add_argument('--no-council', action='store_true')
    parser.add_argument('--export-prompt', action='store_true',
                        help='只匯出 prompt 供複製到 claude.ai，不呼叫本地 LLM')
    parser.add_argument('--format', choices=['md', 'json'], default='md',
                        help='--export-prompt 格式 (預設 md)')
    args = parser.parse_args()

    if args.export_prompt:
        out = export_prompt(fmt=args.format)
        sys.stdout.write(f"\n[OK] Prompt exported: {out}\n")
        sys.stdout.flush()
        return

    out = run(
        use_claude=not args.no_claude,
        use_gemini=not args.no_gemini,
        council=not args.no_council,
    )
    sys.stdout.write(f"\n[OK] Report saved: {out}\n")
    sys.stdout.flush()


if __name__ == '__main__':
    main()
