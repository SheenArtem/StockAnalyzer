"""
強勢股日報 AI 盤後分析 — Phase 3

讀 data/latest/strong_stocks_daily.json，呼叫 Claude Sonnet 產 5 段論述：
  1. market_summary       - 資金熱點總結
  2. sector_analysis      - 族群行情判斷 (含個股驅動原因 + 新聞催化劑)
  3. chase_warnings       - 追高風險提示
  4. watchlist            - 潛力觀察名單
  5. overall_risk         - 整體風險提醒

LLM 規範 (CLAUDE.md): claude --model sonnet, timeout 600s

輸出: data/latest/strong_stocks_daily.json (in-place 加 'ai_analysis' field)

Usage:
  python tools/strong_stocks_ai_analysis.py
  python tools/strong_stocks_ai_analysis.py --dry-run    # 印 prompt 不打 LLM
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logger = logging.getLogger(__name__)

INPUT_PATH = REPO / "data" / "latest" / "strong_stocks_daily.json"
_CLAUDE_CLI = shutil.which("claude") or "claude"
CLAUDE_TIMEOUT = 600


# ============================================================
# Build prompt
# ============================================================
def _format_row(r: dict[str, Any]) -> str:
    sid = r.get("stock_id", "")
    name = r.get("name", "")
    sector = r.get("primary_sector") or "-"
    chg = r.get("change_pct", 0)
    vol_ratio = r.get("volume_ratio_5d")
    abn = "[爆量]" if r.get("is_abnormal_volume") else ""
    chg5 = r.get("change_pct_5d")
    inst = r.get("inst_net_buy_today_shares")
    margin = r.get("margin_net_today_shares")
    sbl = r.get("sbl_sell_today_shares")
    dt_pct = r.get("day_trade_pct")
    score = r.get("trigger_score", 0)

    vol_ratio_str = f"{vol_ratio}x" if vol_ratio is not None else "-"
    chg5_str = f"{chg5:+.1f}%" if chg5 is not None else "-"
    inst_str = f"{inst:+,}張" if inst is not None else "N/A"
    margin_str = f"{margin:+,}張" if margin is not None else "N/A"
    sbl_str = f"{sbl:,}張" if sbl is not None else "N/A"
    dt_str = f"{dt_pct}%" if dt_pct is not None else "-"

    return (
        f"| {sid} | {name} | {sector} | {chg:+.1f}% | "
        f"{vol_ratio_str}{abn} | {chg5_str} | {inst_str} | "
        f"{margin_str} | {dt_str} | {sbl_str} | {score} |"
    )


def build_prompt(daily: dict[str, Any]) -> str:
    twse_top = daily.get("twse_top", [])
    tpex_top = daily.get("tpex_top", [])
    scan_date = daily.get("scan_date", "?")

    header = "| 代號 | 名稱 | 族群 | 漲幅 | 量比 | 近5日 | 法人(張) | 融資(張) | 當沖% | 借券賣(張) | 評分 |"
    sep = "|---|---|---|---|---|---|---|---|---|---|---|"

    twse_table = "\n".join([header, sep] + [_format_row(r) for r in twse_top])
    tpex_table = "\n".join([header, sep] + [_format_row(r) for r in tpex_top])

    # 收集所有合法 stock_id 給 validator
    all_ids = sorted({str(r["stock_id"]) for r in twse_top + tpex_top})

    prompt = f"""你是台股盤後分析師。以下是 {scan_date} 的強勢股 Top 15 上市 + Top 15 上櫃。請產出結構化 JSON 五段論述。

## 上市強勢股 Top 15
{twse_table}

## 上櫃強勢股 Top 15
{tpex_table}

---

## 任務

產出以下 JSON schema（只回傳 JSON，不要前後說明文字、不要 markdown fence）：

```json
{{
  "market_summary": "300-400 字，講今日資金熱點、主流題材、量價齊揚 / 法人加碼觀察、是否屬於結構性轉型 vs 短線反彈",
  "sector_analysis": [
    {{
      "sector_emoji": "🚀",
      "sector_name": "族群名（例如 AI 與半導體基礎建設）",
      "stocks": [
        {{
          "stock_id": "2327",
          "name": "國巨",
          "driver": "簡述驅動原因 (1-2 句)",
          "news_catalyst": "新聞利多催化劑 (1-2 句)"
        }}
      ]
    }}
  ],
  "chase_warnings": [
    {{
      "stock_id": "3485",
      "name": "敘豐",
      "change_pct": 9.9,
      "reason": "為何不宜追價（量比 / 籌碼 / 5日漲幅 / 小型股流動性）"
    }}
  ],
  "watchlist": [
    {{
      "stock_id": "2327",
      "name": "國巨",
      "reason": "為何放入潛力觀察（基本面 / 法人 / 題材護城河）"
    }}
  ],
  "overall_risk": "150 字以內，提醒利率 / 地緣 / 短期獲利了結壓力 / 操作心法"
}}
```

## 籌碼欄位解讀指引（重要）

- **法人**：當日三大法人合計買賣超 (張)，正值大代表機構追價，負值代表機構出脫。
- **融資**：當日融資增減 (張) = 融資買進 − 融資賣出。**正值大 = 散戶槓桿追價（容易過熱）**，負值 = 散戶縮手。
- **當沖%**：當日當沖量佔總成交量比例。**≥ 50% = 投機度極高**，30-50% 正常活絡，<30% 多為實質交易。
- **借券賣**：當日借券賣出張數。**≥ 100 張 = 機構押寶下跌**（空頭力道警示），數值越大越警戒。

## 約束 (重要)

1. **數字一致性**：所有 stock_id, change_pct, vol_ratio, change_pct_5d, inst, margin, day_trade, sbl 數字必須與表格完全一致；不准編造。
2. **合法 stock_id 集**：只能引用以下檔之一 → {",".join(all_ids)}
3. **族群判斷**：依表格 "族群" 欄位 + 你對台股題材的認知歸納 2-4 大族群；族群名稱要中文且具體（例如「AI 伺服器」「矽光子 / CPO」）。
4. **每族群 2-3 檔**：sector_analysis 每個族群挑該族群代表性最強的 2-3 檔，總計 6-10 檔。
5. **追高風險 3-5 檔**：優先挑符合下列任一籌碼警訊的個股：
   - 漲幅 ≥ 20% 暴漲
   - 量比異常（≥ 3x 或 < 1.0 卻仍上榜）
   - 5日漲幅與今日漲幅背離
   - **融資爆增 ≥ 1000 張**（散戶追高陷阱）
   - **當沖比 ≥ 50%**（投機度極高）
   - **借券賣大 ≥ 200 張**（機構押下跌）
6. **潛力觀察 3-5 檔**：須符合下列至少一條：
   - (a) 法人買超 ≥ 100 張且融資不過熱（增減 < 1000 張）
   - (b) 量比 1.2-2.0x（健康放量、非異常爆量）且當沖 < 40%
   - (c) 評分 ≥ 80 且借券賣 < 50 張（無顯著空方押寶）
7. **driver / news_catalyst / reason 必須引用具體籌碼數字**：例如「法人買超 +21,107 張為全場最大」「融資增 +8,998 張籌碼鬆動」「借券賣 977 張機構押寶下跌」「當沖 58% 投機度高」。
8. **新聞催化劑**：若不確定具體新聞，寫「籌碼面 / 法人加碼 / 題材熱度」即可，不准編造未發生事件。
9. **語言**：繁體中文。
10. **格式**：純 JSON，不要 ```json fence、不要前後解釋。

立即產出 JSON。
"""
    return prompt


# ============================================================
# Claude Sonnet call
# ============================================================
def call_claude_sonnet(prompt: str) -> tuple[str, str | None]:
    """同 tools/news_theme_extract.py 的呼叫慣例。"""
    cmd = [_CLAUDE_CLI, "-p", "--model", "sonnet", "--output-format", "json"]
    try:
        result = subprocess.run(
            cmd,
            input=prompt,
            capture_output=True, text=True, timeout=CLAUDE_TIMEOUT,
            encoding="utf-8", errors="replace",
            shell=False,
        )
    except subprocess.TimeoutExpired:
        return "", f"claude CLI timeout after {CLAUDE_TIMEOUT}s"
    except FileNotFoundError:
        return "", "claude CLI not found"
    if result.returncode != 0:
        return result.stdout or "", f"claude exit {result.returncode}: {result.stderr[:300]}"
    try:
        envelope = json.loads(result.stdout)
        text = envelope.get("result", "")
        if envelope.get("is_error"):
            return text, f'claude is_error=true (status={envelope.get("api_error_status")})'
        return text, None
    except json.JSONDecodeError:
        return result.stdout, None


# ============================================================
# Parse + validate
# ============================================================
def parse_json_response(text: str) -> dict[str, Any] | None:
    """容錯 parse: strip markdown fence + 找第一個 { ... }。"""
    s = text.strip()
    if s.startswith("```"):
        lines = s.split("\n")
        s = "\n".join(lines[1:-1] if len(lines) >= 3 else lines)
        if s.startswith("json"):
            s = s[4:].lstrip()
    # 找第一個 { 配對的 }
    start = s.find("{")
    if start < 0:
        return None
    depth = 0
    in_str = False
    escape = False
    end = -1
    for i in range(start, len(s)):
        c = s[i]
        if escape:
            escape = False
            continue
        if c == "\\":
            escape = True
            continue
        if c == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end < 0:
        return None
    try:
        return json.loads(s[start:end + 1])
    except json.JSONDecodeError as e:
        logger.warning("JSON parse failed: %s", e)
        return None


def validate_ai_output(
    ai: dict[str, Any], legal_ids: set[str]
) -> tuple[dict[str, Any], list[str]]:
    """檢查 schema 完整 + stock_id 合法。回傳 (cleaned, warnings)."""
    warnings: list[str] = []
    cleaned: dict[str, Any] = {
        "market_summary": "",
        "sector_analysis": [],
        "chase_warnings": [],
        "watchlist": [],
        "overall_risk": "",
    }

    # market_summary
    ms = ai.get("market_summary", "")
    if not isinstance(ms, str) or len(ms) < 50:
        warnings.append("market_summary missing or too short")
    cleaned["market_summary"] = str(ms)

    # sector_analysis
    sectors = ai.get("sector_analysis", [])
    if not isinstance(sectors, list):
        warnings.append("sector_analysis not list")
        sectors = []
    for sec in sectors:
        if not isinstance(sec, dict):
            continue
        sec_stocks = []
        for s in sec.get("stocks", []) or []:
            if not isinstance(s, dict):
                continue
            sid = str(s.get("stock_id", ""))
            if sid not in legal_ids:
                warnings.append(f"sector_analysis: illegal stock_id {sid}")
                continue
            sec_stocks.append({
                "stock_id": sid,
                "name": str(s.get("name", "")),
                "driver": str(s.get("driver", "")),
                "news_catalyst": str(s.get("news_catalyst", "")),
            })
        if sec_stocks:
            cleaned["sector_analysis"].append({
                "sector_emoji": str(sec.get("sector_emoji", "")),
                "sector_name": str(sec.get("sector_name", "")),
                "stocks": sec_stocks,
            })

    # chase_warnings
    for w in ai.get("chase_warnings", []) or []:
        if not isinstance(w, dict):
            continue
        sid = str(w.get("stock_id", ""))
        if sid not in legal_ids:
            warnings.append(f"chase_warnings: illegal stock_id {sid}")
            continue
        cleaned["chase_warnings"].append({
            "stock_id": sid,
            "name": str(w.get("name", "")),
            "change_pct": w.get("change_pct"),
            "reason": str(w.get("reason", "")),
        })

    # watchlist
    for w in ai.get("watchlist", []) or []:
        if not isinstance(w, dict):
            continue
        sid = str(w.get("stock_id", ""))
        if sid not in legal_ids:
            warnings.append(f"watchlist: illegal stock_id {sid}")
            continue
        cleaned["watchlist"].append({
            "stock_id": sid,
            "name": str(w.get("name", "")),
            "reason": str(w.get("reason", "")),
        })

    # overall_risk
    ovr = ai.get("overall_risk", "")
    if not isinstance(ovr, str) or len(ovr) < 30:
        warnings.append("overall_risk missing or too short")
    cleaned["overall_risk"] = str(ovr)

    return cleaned, warnings


# ============================================================
# Main
# ============================================================
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=INPUT_PATH)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print prompt only, do not call LLM")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.input.exists():
        print(f"[ERROR] {args.input} not found. Run strong_stocks_daily.py first.",
              file=sys.stderr)
        return 1

    with args.input.open("r", encoding="utf-8") as f:
        daily = json.load(f)

    prompt = build_prompt(daily)
    print(f"[INFO] Prompt {len(prompt)} chars, scan_date={daily.get('scan_date')}")

    if args.dry_run:
        debug_path = args.input.parent / "strong_stocks_ai_prompt.txt"
        debug_path.write_text(prompt, encoding="utf-8")
        print(f"[DRY-RUN] Prompt saved to {debug_path}")
        return 0

    print("[INFO] Calling Claude Sonnet (timeout 600s)...")
    output, err = call_claude_sonnet(prompt)
    if err:
        print(f"[ERROR] Claude call failed: {err}", file=sys.stderr)
        return 2

    ai = parse_json_response(output)
    if ai is None:
        print("[ERROR] Failed to parse LLM JSON output", file=sys.stderr)
        # 保留 raw 給人工 debug
        debug_path = args.input.parent / "strong_stocks_ai_raw.txt"
        debug_path.write_text(output, encoding="utf-8")
        print(f"[INFO] Raw output saved to {debug_path}", file=sys.stderr)
        return 3

    legal_ids = {str(r["stock_id"]) for r in daily.get("twse_top", []) + daily.get("tpex_top", [])}
    cleaned, warnings = validate_ai_output(ai, legal_ids)

    if warnings:
        print(f"[WARN] {len(warnings)} validation issues:")
        for w in warnings[:10]:
            print(f"  - {w}")

    # In-place 加 ai_analysis field
    daily["ai_analysis"] = cleaned
    daily["ai_analysis_meta"] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "model": "claude sonnet",
        "validation_warnings": warnings,
    }
    with args.input.open("w", encoding="utf-8") as f:
        json.dump(daily, f, ensure_ascii=False, indent=2)

    print(
        f"[OK] AI analysis written to {args.input} "
        f"(sectors={len(cleaned['sector_analysis'])}, "
        f"warnings={len(cleaned['chase_warnings'])}, "
        f"watchlist={len(cleaned['watchlist'])})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
