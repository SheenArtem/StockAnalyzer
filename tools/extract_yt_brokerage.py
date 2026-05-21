"""
Brokerage YT Stage 2: 投顧分析師 VTT -> 結構化 JSON (Claude Sonnet)

跟 extract_yt_sector_tags.py (電視節目 pipeline) 獨立但**重用 LLM utility**：
- 重用: call_claude / extract_json_from_output / TokenExhaustedError / LLMTimeoutError
- 不共用: PROMPT_TEMPLATE / VTT_ROOT / OUT_ROOT / list_pending / extract()

Schema (top-level keys) — 跟電視節目 schema 平行但增加投顧獨有欄位:
- brokerage / analyst_key / analyst_name / host_name (新增)
- mentions[].entry / .stop / .target / .timeframe (新增)
- analyst_view / recommended_action / risk_warning (新增)
- 沿用: themes_discussed / macro_views / mentions[ticker/name/sentiment/tag/thesis/confidence]

LLM 規範: --model=sonnet + 600s timeout (CLAUDE.md News 解析規範)
輸出: data_cache/yt_brokerage_extracts/<brokerage>/<analyst_key>/<date>_<video_id>.json

CLI:
    python tools/extract_yt_brokerage.py <vtt_path>                # 單檔
    python tools/extract_yt_brokerage.py --all                     # 全部未萃取
    python tools/extract_yt_brokerage.py --brokerage moore         # 限定投顧
    python tools/extract_yt_brokerage.py --analyst moore_guo       # 限定分析師
    python tools/extract_yt_brokerage.py <vtt> --stdout            # debug 不存檔
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# 重用既有 LLM utility (CLAUDE.md「避免 rework」原則)
sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract_yt_sector_tags import (  # noqa: E402
    call_claude,
    extract_json_from_output,
    is_token_exhausted,
    load_known_tickers,
    parse_vtt_filename,
    TokenExhaustedError,
    LLMTimeoutError,
)
from fetch_yt_brokerage import BROKERAGES  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
VTT_ROOT = REPO / "data_cache" / "yt_brokerage_transcripts"
OUT_ROOT = REPO / "data_cache" / "yt_brokerage_extracts"
SCHEMA_VERSION = 1

# A/B 比較 (2026-05-21) 結果: codex (GPT-5.5 reasoning=medium) 速度 4-6x Claude Sonnet
# + 覆蓋更廣 (多抓 24 個真實討論的供應鏈個股) + ticker code 較準 (Sonnet 把華邦電抓成 4958
# 應為 2344)。幻覺率 8% 但同類股 prompt 加強後可壓低。
# 策略: codex primary, Claude Sonnet (effort xhigh) fallback (codex 超時 / JSON 解不出時切換)。
CODEX_REASONING = "medium"  # minimal | low | medium | high | xhigh
CODEX_TIMEOUT = 600

# Sticky fallback (2026-05-21): codex quota 用完時若每支 VTT 都再試一次 codex 才
# fallback Sonnet,每支多花 30-60 秒等 timeout/quota error。連續 N 次失敗後切 sticky
# 模式直接走 Sonnet,但每 PROBE_INTERVAL_SEC 秒 probe 一次 codex,恢復就切回。
STICKY_THRESHOLD = 3            # 連續 N 次 codex 失敗 -> sticky
PROBE_INTERVAL_SEC = 600        # sticky 期間每 10 分鐘 probe 一次 codex
_codex_consecutive_fails = 0
_codex_last_fail_ts: float = 0.0
_codex_sticky_until_logged = False


def _should_try_codex() -> tuple[bool, str]:
    """回傳 (try_codex, reason)。sticky 期間僅在 probe 時間到才回 True。"""
    global _codex_consecutive_fails, _codex_last_fail_ts
    if _codex_consecutive_fails < STICKY_THRESHOLD:
        return True, "normal"
    # sticky 期間
    elapsed = time.time() - _codex_last_fail_ts
    if elapsed >= PROBE_INTERVAL_SEC:
        return True, f"probe (sticky {_codex_consecutive_fails} fails, last {elapsed/60:.1f}min ago)"
    return False, f"sticky ({_codex_consecutive_fails} fails, probe in {(PROBE_INTERVAL_SEC-elapsed)/60:.1f}min)"


def _record_codex_result(success: bool) -> None:
    global _codex_consecutive_fails, _codex_last_fail_ts, _codex_sticky_until_logged
    if success:
        if _codex_consecutive_fails >= STICKY_THRESHOLD:
            sys.stderr.write(
                f"  [STICKY RECOVERY] codex 重新可用,清除 sticky 狀態 "
                f"(之前連續失敗 {_codex_consecutive_fails} 次)\n"
            )
            sys.stderr.flush()
        _codex_consecutive_fails = 0
        _codex_sticky_until_logged = False
    else:
        _codex_consecutive_fails += 1
        _codex_last_fail_ts = time.time()
        if _codex_consecutive_fails == STICKY_THRESHOLD and not _codex_sticky_until_logged:
            sys.stderr.write(
                f"  [STICKY ENTER] codex 連續失敗 {STICKY_THRESHOLD} 次,切 sticky 模式 "
                f"(每 {PROBE_INTERVAL_SEC//60} 分鐘 probe 一次)\n"
            )
            sys.stderr.flush()
            _codex_sticky_until_logged = True


def call_codex(prompt: str, vtt_text: str,
               timeout: int = CODEX_TIMEOUT,
               reasoning_effort: str = CODEX_REASONING) -> tuple[str, str | None]:
    """呼叫 codex exec (GPT-5.5)。回傳 (model_output_text, error_msg)。

    - shell=True 因為 codex 是 npm 包裝 .cmd (Windows PATH 透過 shell 才找得到)
    - 用 -o output-last-message 避開 codex banner / tokens used 後綴
    - 超時/exit!=0 視為 fallback signal
    """
    combined = f"{prompt}\n\n--- 以下為 VTT 字幕 ---\n{vtt_text}"
    last_msg_file = REPO / f"data_cache/codex_last_msg_{int(time.time()*1000)}.txt"
    last_msg_file.parent.mkdir(parents=True, exist_ok=True)

    cmd = (
        f'codex exec --skip-git-repo-check '
        f'--dangerously-bypass-approvals-and-sandbox '
        f'-o "{last_msg_file}" '
        f'-c model_reasoning_effort={reasoning_effort} -'
    )
    try:
        result = subprocess.run(
            cmd, input=combined, capture_output=True, text=True,
            timeout=timeout, encoding="utf-8", errors="replace",
            shell=True,
        )
    except subprocess.TimeoutExpired:
        if last_msg_file.exists():
            last_msg_file.unlink()
        return "", f"codex timeout {timeout}s"

    output = ""
    if last_msg_file.exists():
        try:
            output = last_msg_file.read_text(encoding="utf-8", errors="replace")
        except Exception:
            pass
        try:
            last_msg_file.unlink()
        except Exception:
            pass
    if not output:
        output = result.stdout or ""

    if result.returncode != 0:
        return output, f"codex exit {result.returncode}: {(result.stderr or '')[:300]}"
    return output, None


PROMPT_TEMPLATE = """你是台股投顧分析師頻道內容萃取員。stdin 會給你一集影片自動/人工字幕 (VTT 含時間碼,可忽略),請萃取結構化資訊。

投顧公司: {brokerage_name}
分析師: {analyst_name}
日期: {date}
Video ID: {video_id}
標題: {title}

影片結構: 通常 1 位「主分析師」(就是 {analyst_name}) 主講 + 1 位「主持人」輔助提問。
跟電視節目「主持人 + 多輪流來賓」不同,你要分清楚誰是分析師、誰是主持人。

**CRITICAL: 僅輸出合法 JSON (RFC 8259)**
- 所有 key 必須用雙引號 " 包起來
- String 必須用雙引號
- 不要 markdown ```fence
- 不要任何前言/後語/標題/說明
- 不要 action (不存 memory、不呼叫工具)
- 輸出必須可直接 json.loads() 解析

schema:

{{
  "host_name": "主持人名字 (e.g. Lily / 德瑜) — 若整集無主持人留空字串",
  "analyst_view": "分析師對今日台股大盤的整體看法,一句話 (e.g. 短線回測4萬點是買點)",
  "recommended_action": "加碼 | 持有 | 觀望 | 減碼 | 空手",
  "risk_warning": "分析師明講的風險點 (e.g. 輝達財報利多出盡可能多殺多),無則空字串",
  "macro_views": "節目對美股/Fed/利率/美中政策的整體看法,一句話;無則空字串",
  "themes_discussed": ["整集討論的產業題材列表"],
  "mentions": [
    {{
      "ticker": "4位數字股票代碼 (KY 股也是 4 位數,不加 .TW)",
      "name": "公司名",
      "sentiment": "+1 | 0 | -1",
      "tag": ["產業題材1"],
      "thesis": "一句話摘要分析師對這檔的觀點 (e.g. 國巨從低點抱到現在最高大賺74%)",
      "confidence": 0-100,
      "entry": 進場價 (float 或 null),
      "stop": 停損價 (float 或 null),
      "target": 目標價 (float 或 null),
      "timeframe": "intraday | swing | position | unspecified"
    }}
  ]
}}

規則:
- Ticker 必須 4 位數 (e.g. "3017","4958"),無法確認代碼時 ticker 留 "" 但保留 name
- Sentiment: +1 看好 / 0 中立討論 / -1 看空
- Confidence: 對此 mention 分類 (題材 + sentiment 正確度) 的信心度 0-100
- 只萃取「**實質被分析師討論**」的個股 — 主持人/分析師明確評論過的,一閃而過帶過的權值股不算
- entry/stop/target 只在分析師**明確講出價位**時填,否則 null
- timeframe: 短線當沖 → intraday / 波段 1-4 週 → swing / 長線 > 1 月 → position / 沒講清楚 → unspecified
- 不要幻覺,VTT 沒提到就不寫
- **關鍵反幻覺規則**: 公司名 **必須在字幕原文出現過** 才能列。不要因為「同類股」「供應鏈」「題材接近」就自行擴充列出。
  例如分析師討論「漢唐 (2404)」時,你不能順便列「亞翔 (6139)」即使兩者都是台積電廠房工程股 — 除非字幕原文也明確提到「亞翔」。
  違反此規則的 mention 應該整筆移除。
- **Ticker 對應正確性**: 公司中文名 → 4 位數 ticker 對應務必精準。
  常見易錯: 華邦電=2344 (非 4958), 臻鼎-KY=4958, 環球晶=6488, 中砂=1560, 漢唐=2404。
  若不確定 ticker 對應正確,寧可留 ticker="" 也不要硬猜。
- Tag 請用常見題材名稱: AI 散熱 / CPO / HBM / ASIC / ASIC 設計服務 / Apple 供應鏈 / AI 伺服器 ODM / AI 伺服器電源 / CCL / ABF 載板 / PCB 硬板 / 先進封測 / CoWoS / BMC / 矽智財 / 手機 SoC / AI PC / 網通 / EV / SiC / 矽晶圓 / 光學元件 / 機器人 / 其他 (自定名稱)
"""


def validate_and_annotate(parsed: dict, known_tickers: dict[str, str]) -> dict:
    """檢查 ticker 真實性 + 正規化 sentiment + 確保 mention 內所有欄位都有。"""
    mentions = parsed.get("mentions", [])
    for m in mentions:
        ticker = (m.get("ticker") or "").strip()
        if ticker and ticker not in known_tickers:
            m["ticker_suspicious"] = True
        # Normalize sentiment to string
        s = m.get("sentiment")
        if isinstance(s, int):
            m["sentiment"] = str(s) if s < 0 else f"+{s}"
        # 確保新欄位都存在 (允許 null)
        for k in ("entry", "stop", "target"):
            if k not in m:
                m[k] = None
            elif isinstance(m[k], str):
                # LLM 偶爾回字串 ("550" / "N/A") - 嘗試 coerce
                try:
                    m[k] = float(m[k])
                except (TypeError, ValueError):
                    m[k] = None
        m.setdefault("timeframe", "unspecified")
    return parsed


def extract(vtt_path: Path, brokerage: str, analyst_key: str,
            known_tickers: dict | None = None) -> dict:
    """主 extract 流程: 讀 VTT -> Claude Sonnet -> JSON 驗證。"""
    if known_tickers is None:
        known_tickers = load_known_tickers()

    meta = parse_vtt_filename(vtt_path)
    info = BROKERAGES[brokerage]["analysts"][analyst_key]
    analyst_name = info["name"]
    brokerage_name = BROKERAGES[brokerage]["name"]

    vtt_text = vtt_path.read_text(encoding="utf-8", errors="replace")
    if len(vtt_text) > 200_000:
        vtt_text = vtt_text[:200_000] + "\n...(truncated)"

    prompt = PROMPT_TEMPLATE.format(
        brokerage_name=brokerage_name,
        analyst_name=analyst_name,
        date=meta["date"],
        video_id=meta["video_id"],
        title=meta["title"],
    )

    # Strategy (2026-05-21 A/B 後): codex GPT-5.5 primary, Claude Sonnet fallback
    # + sticky fallback: codex 連續失敗 STICKY_THRESHOLD 次切 sticky 模式直接走 Sonnet,
    #   每 PROBE_INTERVAL_SEC 秒 probe 一次 codex 恢復狀況
    try_codex, reason = _should_try_codex()
    parsed = None
    output, err = "", None
    model_used = "claude-sonnet"

    if try_codex:
        if reason != "normal":
            sys.stderr.write(f"  [PROBE] {reason}\n")
            sys.stderr.flush()
        output, err = call_codex(prompt, vtt_text)
        parsed = extract_json_from_output(output) if output else None
        if parsed is not None:
            _record_codex_result(success=True)
            model_used = "codex-gpt-5.5"
        else:
            _record_codex_result(success=False)
            sys.stderr.write(
                f"  [FALLBACK] codex failed (err={err!r}), retrying with Claude Sonnet...\n"
            )
            sys.stderr.flush()

    if parsed is None:
        # 兩種狀況走到這: 1) sticky 跳過 codex, 2) codex 失敗 fallback
        output, err = call_claude(prompt, vtt_text)
        model_used = "claude-sonnet"
        parsed = extract_json_from_output(output) if not err else None

    base_meta = {
        "schema_version": SCHEMA_VERSION,
        "brokerage": brokerage,
        "brokerage_name": brokerage_name,
        "analyst_key": analyst_key,
        "analyst_name": analyst_name,
        "date": meta["date"],
        "video_id": meta["video_id"],
        "title": meta["title"],
        "extracted_by_model": model_used,
        "extracted_at": datetime.now().isoformat(timespec="seconds"),
    }

    if parsed is None:
        return {
            **base_meta,
            "error": "Both codex and Claude Sonnet failed to return valid JSON",
            "last_error": err,
            "mentions": [],
        }

    parsed = validate_and_annotate(parsed, known_tickers)
    parsed.update(base_meta)  # base_meta 蓋掉 LLM 任意產的 brokerage/date 等
    return parsed


def _parse_brokerage_analyst_from_path(vtt_path: Path) -> tuple[str, str]:
    """從路徑反推 brokerage / analyst_key (data_cache/yt_brokerage_transcripts/<brok>/<analyst>/x.vtt)"""
    parts = vtt_path.resolve().parts
    # 找 "yt_brokerage_transcripts" 位置
    for i, p in enumerate(parts):
        if p == "yt_brokerage_transcripts" and i + 2 < len(parts):
            return parts[i + 1], parts[i + 2]
    raise ValueError(f"cannot parse brokerage/analyst from path: {vtt_path}")


def list_pending(brokerage: str | None = None,
                 analyst_key: str | None = None) -> list[tuple[Path, str, str]]:
    """列出未萃取 VTT。回傳 [(vtt_path, brokerage, analyst_key)]。

    同 video 多語言只取 zh-TW 優先 (人工)，再 zh > zh-Hant > zh-Hans。
    """
    pending: list[tuple[Path, str, str]] = []

    brokerages = [brokerage] if brokerage else list(BROKERAGES.keys())
    for bk in brokerages:
        if bk not in BROKERAGES:
            continue
        analysts = [analyst_key] if analyst_key else list(BROKERAGES[bk]["analysts"].keys())
        for ak in analysts:
            if ak not in BROKERAGES[bk]["analysts"]:
                continue
            vtt_dir = VTT_ROOT / bk / ak
            if not vtt_dir.exists():
                continue
            out_dir = OUT_ROOT / bk / ak
            out_dir.mkdir(parents=True, exist_ok=True)

            # group by video_id
            by_video: dict[str, list[Path]] = {}
            for vtt in sorted(vtt_dir.glob("*.vtt")):
                try:
                    meta = parse_vtt_filename(vtt)
                except Exception:
                    continue
                if not meta.get("video_id"):
                    continue
                by_video.setdefault(meta["video_id"], []).append(vtt)

            for vid, vtts in by_video.items():
                def _pref(p: Path) -> int:
                    n = p.name
                    if n.endswith(".zh-TW.vtt"):
                        return 0
                    if n.endswith(".zh.vtt"):
                        return 1
                    if n.endswith(".zh-Hant.vtt"):
                        return 2
                    if ".zh-Hant" in n:
                        return 3
                    if ".zh-Hans" in n:
                        return 9  # 簡體最後
                    return 5
                chosen = sorted(vtts, key=_pref)[0]
                meta = parse_vtt_filename(chosen)
                out_file = out_dir / f"{meta['date']}_{meta['video_id']}.json"
                if out_file.exists():
                    try:
                        existing = json.loads(out_file.read_text(encoding="utf-8"))
                        if "error" not in existing:
                            continue
                    except (json.JSONDecodeError, OSError):
                        pass
                pending.append((chosen, bk, ak))
    return pending


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vtt_path", nargs="?", help="單一 VTT 檔路徑")
    ap.add_argument("--all", action="store_true", help="處理全部未萃取 VTT")
    ap.add_argument("--brokerage", type=str, default=None,
                    choices=list(BROKERAGES.keys()), help="限定投顧")
    ap.add_argument("--analyst", type=str, default=None, help="限定分析師 (e.g. moore_guo)")
    ap.add_argument("--stdout", action="store_true", help="輸出到 stdout 不存檔 (debug)")
    ap.add_argument("--limit", type=int, default=None, help="限制處理 N 個 (test 用)")
    ap.add_argument("--token-retry-sleep-min", type=int, default=30,
                    help="LLM token exhausted 時 sleep 幾分鐘 (default 30)")
    args = ap.parse_args()

    known_tickers = load_known_tickers()
    print(f"Known tickers: {len(known_tickers)}", file=sys.stderr)

    targets: list[tuple[Path, str, str]] = []
    if args.vtt_path:
        p = Path(args.vtt_path)
        bk, ak = _parse_brokerage_analyst_from_path(p)
        targets = [(p, bk, ak)]
    elif args.all or args.brokerage or args.analyst:
        targets = list_pending(args.brokerage, args.analyst)
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

    for vtt, bk, ak in targets:
        print(f"\n>> [{bk}/{ak}] {vtt.name}", file=sys.stderr)
        parsed = None
        attempt = 0
        timeout_count = 0
        while True:
            attempt += 1
            try:
                parsed = extract(vtt, bk, ak, known_tickers=known_tickers)
                break
            except TokenExhaustedError as e:
                wake_iso = datetime.fromtimestamp(
                    datetime.now().timestamp() + sleep_sec
                ).isoformat(timespec="seconds")
                print(f"  [TOKEN EXHAUSTED attempt {attempt}] {e}", file=sys.stderr)
                print(f"  Sleeping {args.token_retry_sleep_min} min, retry same VTT at {wake_iso}",
                      file=sys.stderr)
                time.sleep(sleep_sec)
                continue
            except LLMTimeoutError as e:
                timeout_count += 1
                if timeout_count >= timeout_max_retry:
                    print(f"  [TIMEOUT GIVEUP after {timeout_count} attempts] {e}",
                          file=sys.stderr)
                    break
                print(f"  [TIMEOUT {timeout_count}/{timeout_max_retry}] {e}, sleep 60s",
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
            out_dir = OUT_ROOT / bk / ak
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{parsed['date']}_{parsed['video_id']}.json"
            out_file.write_text(json.dumps(parsed, ensure_ascii=False, indent=2),
                                encoding="utf-8")
            print(
                f"  -> {out_file.name} "
                f"({len(parsed.get('mentions', []))} mentions, "
                f"action={parsed.get('recommended_action', '?')}, "
                f"model={parsed.get('extracted_by_model', '?')})",
                file=sys.stderr,
            )

        if "error" not in parsed:
            successes += 1

    print(f"\n== Summary: {successes}/{len(targets)} OK ==", file=sys.stderr)
    sys.exit(0 if successes == len(targets) else 1)


if __name__ == "__main__":
    main()
