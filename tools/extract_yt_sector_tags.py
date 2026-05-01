"""
E1 Stage 2: 從 YT VTT 萃取 ticker / sector tag / sentiment (JSON 結構化)

Dual LLM (Gemini + Claude 備援):
1. 主: Gemini CLI with gemini-3.1-pro-preview (長 context + 便宜)
2. 備: Claude CLI Sonnet (若 Gemini 429/超時/JSON invalid 才用)

Schema:
- 每集 VTT → 一份 JSON (含 mentions[], themes_discussed[], guests[], macro_views)
- 儲存到 data_cache/yt_extracts/<show_key>/<date>_<video_id>.json

Ticker validation:
- 依 data/sector_tags_manual.json 檢查 ticker 是否為已知台股（suspicious flag）
- 不強制 reject 新股 (可能 IPO 還沒進手動清單)

CLI:
    python tools/extract_yt_sector_tags.py <vtt_path>                # 單檔
    python tools/extract_yt_sector_tags.py --all                     # 全部未處理 VTT
    python tools/extract_yt_sector_tags.py --show money_deploy       # 特定節目
    python tools/extract_yt_sector_tags.py <vtt_path> --llm claude   # 強制用 Claude
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
VTT_ROOT = REPO / "data_cache" / "yt_transcripts"
OUT_ROOT = REPO / "data_cache" / "yt_extracts"
SECTOR_TAGS_FILE = REPO / "data" / "sector_tags_manual.json"

GEMINI_MODEL = "gemini-3.1-pro-preview"  # LLM 規範 (2026-05-01)：Gemini 一律 3.1-pro-preview
GEMINI_FALLBACK_MODEL = None  # None = default model
CLAUDE_MODEL_FLAG = "--model=sonnet"  # LLM 規範 (2026-05-01)：News 解析用 Sonnet


class TokenExhaustedError(Exception):
    """LLM token / quota / rate limit 用完。caller 應該 sleep 後重試同一集,不要寫 error JSON。"""


class LLMTimeoutError(Exception):
    """LLM CLI subprocess 超時。caller 應該短暫 sleep 後重試同一集,有上限 (避免無限 retry)。"""


# 偵測 LLM CLI 在 stderr / stdout 報配額用完的訊號
# 注意: 不要加純數字 pattern (如 "429"),會誤觸中文 JSON 內的股價/數字。
# "too many requests" 已涵蓋 HTTP 429 的英文訊息,不需另外加 "429"。
TOKEN_EXHAUSTED_PATTERNS = (
    "credit balance is too low",
    "usage limit reached",
    "rate_limit",
    "rate limit",
    "quota exceeded",
    "too many requests",
    "resource_exhausted",
)


def is_token_exhausted(err_msg: str) -> bool:
    if not err_msg:
        return False
    low = err_msg.lower()
    return any(p in low for p in TOKEN_EXHAUSTED_PATTERNS)

PROMPT_TEMPLATE = """你是台股財經節目內容分析員。stdin 會給你一集節目的自動字幕 (VTT 含時間碼,可忽略),請萃取結構化資訊。

節目: {show_name}
日期: {date}
Video ID: {video_id}
標題: {title}

**CRITICAL: 僅輸出合法 JSON (RFC 8259)**
- 所有 key 必須用雙引號 " 包起來
- String 必須用雙引號
- 不要 markdown ```fence
- 不要任何前言/後語/標題/說明
- 不要 action（不存 memory、不呼叫工具）
- 輸出必須可直接 json.loads() 解析

schema:

{{
  "guests": ["來賓名字1", "來賓名字2"],
  "mentions": [
    {{
      "ticker": "4位數字股票代碼 (KY 股也是 4 位數,不加 .TW)",
      "name": "公司名",
      "sentiment": "+1 | 0 | -1",
      "tag": ["產業題材1"],
      "thesis": "一句話摘要節目對這檔的討論觀點",
      "confidence": 0-100
    }}
  ],
  "themes_discussed": ["整集討論的產業題材"],
  "macro_views": "節目對大盤/Fed/利率/美中政策的整體看法,一句話;無則空字串"
}}

規則:
- Ticker 必須 4 位數 (e.g. "3017","4958"),無法確認代碼時 ticker 留 "" 但保留 name
- Sentiment: +1 看好 / 0 中立討論 / -1 看空
- Confidence: 對此 mention 分類 (題材 + sentiment 正確度) 的信心度 0-100
- 只萃取「**實質被討論**」的個股 — 主持人/來賓明確評論過的,一閃而過帶過的權值股不算
- 不要幻覺,VTT 沒提到就不寫
- Tag 請用常見題材名稱: AI 散熱 / CPO / HBM / ASIC / ASIC 設計服務 / Apple 供應鏈 / AI 伺服器 ODM / AI 伺服器電源 / CCL / ABF 載板 / PCB 硬板 / 先進封測 / CoWoS / BMC / 矽智財 / 手機 SoC / AI PC / 網通 / EV / SiC / 矽晶圓 / 光學元件 / 機器人 / 其他 (自定名稱)
"""


def load_known_tickers() -> dict[str, str]:
    """從 sector_tags_manual.json 載入已知 ticker -> name map."""
    if not SECTOR_TAGS_FILE.exists():
        return {}
    data = json.loads(SECTOR_TAGS_FILE.read_text(encoding="utf-8"))
    known = {}
    for theme in data.get("themes", []):
        for tier in ("tier1", "tier2"):
            for stock in theme.get(tier, []):
                known[stock["ticker"]] = stock["name"]
    return known


def parse_vtt_filename(vtt_path: Path) -> dict:
    """
    解出 date / video_id / title。
    YT video_id 固定 11 字元 (含 _, -)，所以用 position 切比 split 穩。
    檔名格式: YYYYMMDD_<11-char-videoid>_<title>.zh-XX[.zh-XX].vtt
    """
    # Strip all .vtt and lang suffixes (could have 1 or 2 dots before vtt)
    stem = vtt_path.stem
    # Strip potential nested lang codes like .zh-Hant-zh-Hant or .zh-TW
    while "." in stem and stem.split(".")[-1].startswith("zh"):
        stem = stem.rsplit(".", 1)[0]

    # Expected format: YYYYMMDD_<videoid>_<title>
    # Date = first 8 chars, then "_", then 11-char videoid, then "_", then title
    if len(stem) < 20 or stem[8] != "_":
        return {"date": "", "video_id": "", "title": stem}

    date_str = stem[:8]
    rest = stem[9:]  # skip first underscore

    # video_id = 11 chars (YT standard)
    if len(rest) < 12 or rest[11] != "_":
        # Fallback to split
        parts = rest.split("_", 1)
        video_id = parts[0]
        title = parts[1] if len(parts) > 1 else ""
    else:
        video_id = rest[:11]
        title = rest[12:]

    # Normalize YYYYMMDD -> YYYY-MM-DD
    if len(date_str) == 8 and date_str.isdigit():
        date_iso = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    else:
        date_iso = date_str

    return {"date": date_iso, "video_id": video_id, "title": title}


def call_gemini(prompt: str, vtt_text: str, model: str | None, timeout: int = 900) -> tuple[str, str | None]:  # 15 min per LLM 規範

    """
    VTT 透過 stdin 傳 (避開 CLI argv 長度上限)。
    shell=True 必須：Windows 上 gemini CLI 是 gemini.cmd (npm global)，shell=False 找不到。
    Prompt 用 env var 傳避免 shell 解引號地雷。
    model=None → 用 default model (限流較鬆)。
    """
    import os
    env = os.environ.copy()
    env["YT_EXTRACT_PROMPT"] = prompt

    model_flag = f"-m {model} " if model else ""
    cmd = f'gemini {model_flag}-p "%YT_EXTRACT_PROMPT%" -y'

    try:
        result = subprocess.run(
            cmd, input=vtt_text, capture_output=True, text=True, timeout=timeout,
            encoding="utf-8", errors="replace", shell=True, env=env,
        )
    except subprocess.TimeoutExpired:
        raise LLMTimeoutError(f"gemini CLI timeout after {timeout}s")

    if result.returncode != 0:
        err_msg = f"gemini exit {result.returncode}: {result.stderr[:500]}"
        if is_token_exhausted(result.stderr or ""):
            raise TokenExhaustedError(err_msg)
        return result.stdout or "", err_msg
    # 檢查 stderr 是否有配額警告（即使 exit=0）
    if is_token_exhausted(result.stderr or ""):
        raise TokenExhaustedError(f"gemini stderr quota: {result.stderr[:500]}")
    return result.stdout, None


def call_claude(prompt: str, vtt_text: str, timeout: int = 600) -> tuple[str, str | None]:  # 10 min per LLM 規範

    """VTT + prompt 合併傳 stdin 避開 Windows argv 限制。

    BUG FIX 2026-04-25: 之前漏帶 --model flag 導致 Claude CLI 走 user default
    model (可能 Opus 4.7),token 消耗 ~5x Sonnet。現強制帶 CLAUDE_MODEL_FLAG。
    """
    combined = f"{prompt}\n\n--- 以下為 VTT 字幕 ---\n{vtt_text}"
    try:
        result = subprocess.run(
            f'claude -p {CLAUDE_MODEL_FLAG}',
            input=combined,
            capture_output=True, text=True, timeout=timeout,
            encoding="utf-8", errors="replace",
            shell=True,
        )
    except subprocess.TimeoutExpired:
        raise LLMTimeoutError(f"claude CLI timeout after {timeout}s")

    if result.returncode != 0:
        err_msg = f"claude exit {result.returncode}: {result.stderr[:500]}"
        if is_token_exhausted((result.stderr or "") + " " + (result.stdout or "")):
            raise TokenExhaustedError(err_msg)
        return result.stdout or "", err_msg
    # 即使 exit=0,Claude CLI 偶爾會把 quota 警告塞 stderr / stdout
    combined_stderr_stdout = (result.stderr or "") + " " + (result.stdout or "")
    if is_token_exhausted(combined_stderr_stdout):
        raise TokenExhaustedError(f"claude quota: {combined_stderr_stdout[:500]}")
    return result.stdout, None


def extract_json_from_output(output: str) -> dict | None:
    """嘗試從 LLM output 拉出 JSON (容錯 markdown fence / 前後文字)."""
    # 1. 直接嘗試 parse
    try:
        return json.loads(output.strip())
    except json.JSONDecodeError:
        pass

    # 2. 嘗試找 ```json ... ``` fence
    fence_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", output, re.DOTALL)
    if fence_match:
        try:
            return json.loads(fence_match.group(1))
        except json.JSONDecodeError:
            pass

    # 3. 嘗試找第一個 { 到最後一個 }
    start = output.find("{")
    end = output.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(output[start:end + 1])
        except json.JSONDecodeError:
            pass

    return None


def validate_and_annotate(parsed: dict, known_tickers: dict[str, str]) -> dict:
    """檢查 ticker 真實性,加 suspicious 標記."""
    mentions = parsed.get("mentions", [])
    for m in mentions:
        ticker = (m.get("ticker") or "").strip()
        if ticker and ticker not in known_tickers:
            m["ticker_suspicious"] = True
        # Normalize sentiment to string
        if isinstance(m.get("sentiment"), int):
            m["sentiment"] = str(m["sentiment"]) if m["sentiment"] < 0 else f"+{m['sentiment']}"
    return parsed


def extract(vtt_path: Path, prefer: str = "gemini", known_tickers: dict | None = None) -> dict:
    """主 extract 流程: 讀 VTT -> LLM -> JSON 驗證."""
    if known_tickers is None:
        known_tickers = load_known_tickers()

    meta = parse_vtt_filename(vtt_path)
    show_key = vtt_path.parent.name
    vtt_text = vtt_path.read_text(encoding="utf-8", errors="replace")

    # 若 VTT 太大 (>200KB) 截斷尾端 (通常是 ASR 重複片段)
    if len(vtt_text) > 200_000:
        vtt_text = vtt_text[:200_000] + "\n...(truncated)"

    show_name_map = {
        "money100": "錢線百分百",
        "money_deploy": "鈔錢部署",
    }
    show_name = show_name_map.get(show_key, show_key)

    prompt = PROMPT_TEMPLATE.format(
        show_name=show_name, date=meta["date"], video_id=meta["video_id"], title=meta["title"],
    )

    # 1. 主 LLM (token 用完由 caller sleep retry,不在此自動 fallback Gemini)
    output, err = "", None
    model_used = ""
    if prefer == "gemini":
        output, err = call_gemini(prompt, vtt_text, GEMINI_MODEL)
        model_used = GEMINI_MODEL
    else:
        output, err = call_claude(prompt, vtt_text)
        model_used = "claude-sonnet"

    # 2. Parse JSON
    parsed = extract_json_from_output(output) if not err else None

    if parsed is None:
        return {
            "schema_version": 1,
            "show_key": show_key,
            "show_name": show_name,
            "date": meta["date"],
            "video_id": meta["video_id"],
            "title": meta["title"],
            "error": "both LLMs failed to return valid JSON",
            "last_error": err,
            "extracted_at": datetime.now().isoformat(timespec="seconds"),
            "mentions": [],
        }

    # 4. Validate + annotate
    parsed = validate_and_annotate(parsed, known_tickers)

    # 5. Add metadata
    parsed["schema_version"] = 1
    parsed["show_key"] = show_key
    parsed["show_name"] = show_name
    parsed["date"] = meta["date"]
    parsed["video_id"] = meta["video_id"]
    parsed["title"] = meta["title"]
    parsed["extracted_by_model"] = model_used
    parsed["extracted_at"] = datetime.now().isoformat(timespec="seconds")
    return parsed


def list_pending(show_key: str | None = None) -> list[Path]:
    """列出未處理 (無對應 .json) 的 VTT 檔.

    2026-04-25 修改: 同 video 多 subtitle variant 只取 zh-Hant,避免雙語重複跑 LLM。
    優先序: .zh-Hant.vtt > .zh-Hant-zh-Hant.vtt > .zh-Hant-zh-TW.vtt > 其他
    完全跳過 .zh-Hans-*.vtt (簡體版,內容跟繁體一樣只是字符不同)
    """
    shows = [show_key] if show_key else ["money100", "money_deploy"]
    pending = []
    for sk in shows:
        vtt_dir = VTT_ROOT / sk
        if not vtt_dir.exists():
            continue
        out_dir = OUT_ROOT / sk
        out_dir.mkdir(parents=True, exist_ok=True)

        # Group by video_id, pick preferred variant
        by_video: dict[str, list[Path]] = {}
        for vtt in sorted(vtt_dir.glob("*.vtt")):
            name = vtt.name
            # Skip 簡體 variants entirely
            if ".zh-Hans" in name:
                continue
            try:
                meta = parse_vtt_filename(vtt)
            except Exception:
                continue
            by_video.setdefault(meta['video_id'], []).append(vtt)

        for vid, vtts in by_video.items():
            # Prefer .zh-Hant.vtt (simplest), then zh-Hant-zh-Hant, then others
            def _pref_key(p: Path) -> int:
                n = p.name
                if n.endswith(".zh-Hant.vtt"):
                    return 0
                if ".zh-Hant-zh-Hant" in n:
                    return 1
                if ".zh-Hant-zh-TW" in n:
                    return 2
                return 3
            chosen = sorted(vtts, key=_pref_key)[0]
            meta = parse_vtt_filename(chosen)
            out_file = out_dir / f"{meta['date']}_{meta['video_id']}.json"
            if out_file.exists():
                # 之前 LLM 失敗留下的 error JSON 視為 pending,要重跑;讀不開的也重跑
                try:
                    existing = json.loads(out_file.read_text(encoding="utf-8"))
                    if "error" not in existing:
                        continue
                except (json.JSONDecodeError, OSError):
                    pass
            pending.append(chosen)
    return pending


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vtt_path", nargs="?", help="單一 VTT 檔路徑")
    ap.add_argument("--all", action="store_true", help="處理全部未萃取的 VTT")
    ap.add_argument("--show", choices=["money100", "money_deploy"],
                    help="只處理特定節目")
    ap.add_argument("--llm", choices=["gemini", "claude"], default="claude",
                    help="優先 LLM (default claude,失敗自動 fallback Gemini). 2026-04-24 實測 Gemini (含 pro-preview) 不穩定遵守 JSON 指令傾向輸 markdown")
    ap.add_argument("--stdout", action="store_true", help="輸出到 stdout 不存檔 (debug)")
    ap.add_argument("--limit", type=int, default=None, help="限制處理 N 個 (test 用)")
    ap.add_argument("--token-retry-sleep-min", type=int, default=30,
                    help="LLM token / quota 用完時 sleep 幾分鐘後重抓同一集 (default 30,保證該集不漏)")
    args = ap.parse_args()

    known_tickers = load_known_tickers()
    print(f"Known tickers: {len(known_tickers)}", file=sys.stderr)

    targets: list[Path] = []
    if args.vtt_path:
        targets = [Path(args.vtt_path)]
    elif args.all or args.show:
        targets = list_pending(args.show)
        print(f"Pending VTTs: {len(targets)}", file=sys.stderr)
        if args.limit:
            targets = targets[:args.limit]
            print(f"Limited to first {args.limit}", file=sys.stderr)
    else:
        ap.print_help()
        sys.exit(1)

    sleep_sec = max(args.token_retry_sleep_min, 1) * 60
    timeout_max_retry = 3
    successes = 0
    for vtt in targets:
        print(f"\n>> {vtt.name}", file=sys.stderr)
        parsed = None
        attempt = 0
        timeout_count = 0
        while True:
            attempt += 1
            try:
                parsed = extract(vtt, prefer=args.llm, known_tickers=known_tickers)
                break
            except TokenExhaustedError as e:
                wake_at = datetime.now().timestamp() + sleep_sec
                wake_iso = datetime.fromtimestamp(wake_at).isoformat(timespec="seconds")
                print(f"  [TOKEN EXHAUSTED attempt {attempt}] {e}", file=sys.stderr)
                print(f"  Sleeping {args.token_retry_sleep_min} min, retry same VTT at {wake_iso}",
                      file=sys.stderr)
                time.sleep(sleep_sec)
                continue
            except LLMTimeoutError as e:
                timeout_count += 1
                if timeout_count >= timeout_max_retry:
                    print(f"  [TIMEOUT GIVEUP after {timeout_count} attempts] {e}", file=sys.stderr)
                    break
                print(f"  [TIMEOUT attempt {timeout_count}/{timeout_max_retry}] {e}, sleep 60s and retry",
                      file=sys.stderr)
                time.sleep(60)
                continue
            except Exception as e:
                print(f"  [ERROR] {type(e).__name__}: {e}", file=sys.stderr)
                break
        if parsed is None:
            continue

        if args.stdout:
            print(json.dumps(parsed, ensure_ascii=False, indent=2))
        else:
            show_key = vtt.parent.name
            out_dir = OUT_ROOT / show_key
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{parsed['date']}_{parsed['video_id']}.json"
            out_file.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"  -> {out_file.name} "
                  f"({len(parsed.get('mentions', []))} mentions, "
                  f"model={parsed.get('extracted_by_model', '?')})",
                  file=sys.stderr)

        if "error" not in parsed:
            successes += 1

    print(f"\n== Summary: {successes}/{len(targets)} OK ==", file=sys.stderr)
    sys.exit(0 if successes == len(targets) else 1)


if __name__ == "__main__":
    main()
