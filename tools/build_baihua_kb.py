"""白話投資 FB 貼文 -> 知識庫建置器 (2026-07-16)

讀 data/baihua/raw/posts.jsonl（fetch_baihua_fb.py 產出的 raw innerText），
逐篇餵 Claude Sonnet 清洗 + 結構化，產出：
    data/baihua/kb/<NNNN>_<title>.md   # 每篇一檔（frontmatter + 摘要 + 重點 + 純正文 + 原文連結）
    data/baihua/kb/INDEX.md            # 全庫索引表（依日期）
    data/baihua/kb/THEMES.md           # 依主題分組地圖

LLM 規範 (CLAUDE.md news/short-form): claude `--model sonnet --effort xhigh`
    `--output-format json`，timeout 600s。（本檔屬短文清洗/萃取，非 AI 報告。）

增量：已在 kb/ 有對應 md 的貼文（依 raw id）預設跳過；--rebuild 全重建。
併發：ThreadPoolExecutor（對齊 curate_themes_pipeline 的 call_claude 模式）。
容錯：單篇失敗寫 kb/_fail/，不中斷全批；--dry-run 走 regex-only 不呼叫 LLM。

CLI:
    python tools/build_baihua_kb.py                 # 增量建置
    python tools/build_baihua_kb.py --rebuild        # 全部重跑
    python tools/build_baihua_kb.py --dry-run        # 不呼叫 LLM（regex 清洗 + stub metadata，驗證管線）
    python tools/build_baihua_kb.py --limit 3        # 只處理前 N 篇（測試）
    python tools/build_baihua_kb.py --concurrency 4  # 同時幾個 claude
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("baihua_kb")
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

REPO = Path(__file__).resolve().parents[1]
RAW_JSONL = REPO / "data" / "baihua" / "raw" / "posts.jsonl"
KB_DIR = REPO / "data" / "baihua" / "kb"
FAIL_DIR = KB_DIR / "_fail"
META_DIR = KB_DIR / "_meta"                  # 每篇 LLM meta 快取（供 --render-only 免 LLM 重繪）
STATE = KB_DIR / ".processed.json"          # id -> 產出檔名，增量判斷用

MODEL_FLAG = "--model sonnet --effort xhigh --output-format json"
LLM_TIMEOUT = 600
DEFAULT_CONCURRENCY = 4

THEME_VOCAB = ["投資心法", "總經分析", "產業研究", "交易心理", "風險管理",
               "資產配置", "市場觀察", "個股評析", "讀者問答", "其他"]


# ---------------------------------------------------------------- regex 清洗
_HEAD_RE = re.compile(r"^白話投資\s*\n.*?\n\s*·\s*\n", re.DOTALL)   # 作者名 + 時間 + · 前綴
_TAIL_RE = re.compile(r"\n所有心情[：:].*$", re.DOTALL)             # 心情/讚/留言/分享 尾巴
_SEEMORE_RE = re.compile(r"\s*…?\s*查看更多\s*")


def regex_clean(text: str) -> str:
    t = _HEAD_RE.sub("", text)
    t = _TAIL_RE.sub("", t)
    t = _SEEMORE_RE.sub("", t)
    return t.strip()


def extract_bracket_title(body: str) -> Optional[str]:
    """作者慣例：正文首段常是「（標題）」全形括號。抽出當標題。"""
    m = re.match(r"\s*[（(]([^）)]{4,60})[）)]", body)
    return m.group(1).strip() if m else None


# ---------------------------------------------------------------- claude CLI
def call_claude(prompt: str, timeout: int = LLM_TIMEOUT) -> tuple[str, Optional[str]]:
    """對齊 curate_themes_pipeline.call_claude：reader thread 抽乾 pipe，timeout 整樹 kill。"""
    kwargs = dict(stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                  text=True, encoding="utf-8", errors="replace", shell=True)
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    proc = subprocess.Popen(f"claude -p {MODEL_FLAG}", **kwargs)
    chunks: list[str] = []

    def _drain():
        try:
            for line in proc.stdout:
                chunks.append(line)
        except Exception:
            pass

    reader = threading.Thread(target=_drain, daemon=True)
    reader.start()
    try:
        proc.stdin.write(prompt)
        proc.stdin.close()
    except (OSError, ValueError):
        pass
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        _kill_tree(proc.pid)
        try:
            proc.wait(15)
        except Exception:
            pass
        reader.join(5)
        return "".join(chunks), f"claude timeout {timeout}s"
    reader.join(10)
    return _unwrap_envelope("".join(chunks))


def _kill_tree(pid: int) -> None:
    try:
        if os.name == "nt":
            subprocess.run(f"taskkill /F /T /PID {pid}", shell=True, capture_output=True, timeout=30)
        else:
            import signal
            os.killpg(os.getpgid(pid), signal.SIGKILL)
    except Exception as e:
        log.warning("kill_tree %s failed: %s", pid, e)


def _unwrap_envelope(raw: str) -> tuple[str, Optional[str]]:
    s = raw.strip()
    try:
        env = json.loads(s)
    except json.JSONDecodeError:
        return raw, None
    if isinstance(env, dict) and env.get("is_error"):
        res = str(env.get("result", ""))
        if env.get("api_error_status") == 429 or "limit" in res.lower():
            return "", "RATE_LIMIT"
        return res, f"claude is_error ({env.get('stop_reason')})"
    if isinstance(env, dict) and "result" in env:
        return env.get("result", ""), None
    return raw, None


def _extract_json(s: str) -> Optional[dict]:
    if not s:
        return None
    m = re.search(r"\{.*\}", s, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    return None


# ---------------------------------------------------------------- prompt
def build_prompt(raw_text: str, date_label: Optional[str]) -> str:
    vocab = " / ".join(THEME_VOCAB)
    return f"""你是財經內容編輯。以下是 Facebook 粉專「白話投資」單篇貼文的 raw 文字
（含作者名、發文時間、心情數、讚/留言/分享等 UI 雜訊）。請清洗並結構化。

抓取時的相對時間標記：{date_label or "(無)"}

嚴格只輸出一個 JSON 物件（不要 markdown、不要多餘文字），欄位：
- "title": 標題。作者常以全形括號「（…）」開頭當標題，優先採用；否則自擬 ≤30 字精簡標題。
- "date_iso": 若能從內文明確推斷發文日期回 "YYYY-MM-DD"，否則 null（勿臆測）。
- "themes": 陣列，1-3 個主題標籤，優先從此分類選：{vocab}；必要時可補自訂標籤。
- "tickers": 陣列，內文提及的股票/ETF/標的（代號或名稱）；無則空陣列。
- "summary": 2-4 句繁中摘要，抓核心論點。
- "takeaways": 陣列，3-6 條重點（每條 ≤40 字繁中）。
- "one_liner": 一句話 hook，≤30 字繁中。
- "cleaned_body": 去除所有 UI 雜訊（作者名/時間/·/心情數/讚/留言/分享/「查看更多」/留言預覽）
  後的**純正文**，保留原始段落換行；不要改寫、不要摘要、不要加註，逐字保留作者原文。

raw 文字：
<<<
{raw_text}
>>>"""


# ---------------------------------------------------------------- 產檔
def _safe_filename(title: str, maxlen: int = 40) -> str:
    t = re.sub(r'[\\/:*?"<>|\n\r\t]+', " ", title).strip()
    t = re.sub(r"\s+", " ", t)
    return t[:maxlen].strip() or "untitled"


def _meta_path(rec: dict) -> Path:
    safe = re.sub(r"[^0-9A-Za-z]+", "_", rec.get("id", "x"))[:60]
    return META_DIR / f"{safe}.json"


def _norm_title(title: str) -> str:
    """去除作者慣用的外層全形/半形括號，避免 LLM 兩次輸出含/不含括號產生不同檔名。"""
    t = (title or "未命名").strip()
    t = re.sub(r"^[（(]\s*", "", t)
    t = re.sub(r"\s*[）)]$", "", t)
    return t.strip() or "未命名"


def write_md(seq: int, rec: dict, meta: dict) -> Path:
    title = _norm_title(meta.get("title"))
    # 先清同 seq 舊檔（LLM 標題可能變動 → 防孤兒重複檔）
    for old in KB_DIR.glob(f"{seq:04d}_*.md"):
        try:
            old.unlink()
        except Exception:
            pass
    fname = f"{seq:04d}_{_safe_filename(title)}.md"
    path = KB_DIR / fname
    themes = meta.get("themes") or []
    tickers = meta.get("tickers") or []
    takeaways = meta.get("takeaways") or []
    permalink = rec.get("permalink") or ""
    date_iso = meta.get("date_iso") or ""
    date_label = rec.get("date_label") or ""
    body = meta.get("cleaned_body") or regex_clean(rec.get("text", ""))

    fm = [
        "---",
        f"title: {title}",
        f"date_iso: {date_iso}",
        f"date_label: {date_label}",
        f"themes: [{', '.join(themes)}]",
        f"tickers: [{', '.join(str(x) for x in tickers)}]",
        f"source: {permalink}",
        f"raw_id: {rec.get('id','')}",
        "---",
        "",
        f"# {title}",
        "",
        f"> {meta.get('one_liner','')}",
        "",
        "## 摘要",
        "",
        meta.get("summary", ""),
        "",
        "## 重點",
        "",
    ]
    fm += [f"- {t}" for t in takeaways]
    fm += ["", "## 原文", "", body, ""]
    # 原始擷取（未清洗，逐字保留）— 摺疊備份，防 LLM 清洗誤刪、可對照
    raw_text = rec.get("text", "")
    fm += ["<details>", "<summary>原始擷取（未清洗，逐字保留）</summary>", "",
           "````text", raw_text, "````", "", "</details>", ""]
    if permalink:
        fm += ["---", f"原文連結：{permalink}", ""]
    path.write_text("\n".join(fm), encoding="utf-8")
    return path


def _stub_meta(rec: dict) -> dict:
    body = regex_clean(rec.get("text", ""))
    title = extract_bracket_title(body) or (body[:24] + "…")
    return {"title": title, "date_iso": None, "themes": ["其他"], "tickers": [],
            "summary": "(dry-run stub)", "takeaways": ["(dry-run)"],
            "one_liner": title, "cleaned_body": body}


# ---------------------------------------------------------------- 單篇 pipeline
def process_one(seq: int, rec: dict, dry_run: bool) -> tuple[Optional[dict], Optional[str]]:
    raw = rec.get("text", "")
    if dry_run:
        meta = _stub_meta(rec)
    else:
        pre = regex_clean(raw)                     # 先 regex 去頭尾雜訊省 token
        prompt = build_prompt(pre or raw, rec.get("date_label"))
        out, err = call_claude(prompt)
        if err:
            FAIL_DIR.mkdir(parents=True, exist_ok=True)
            (FAIL_DIR / f"{seq:04d}_{rec.get('id','x')[:20]}.txt").write_text(
                f"ERR: {err}\n\n{out or ''}", encoding="utf-8")
            return None, err
        meta = _extract_json(out)
        if not meta:
            FAIL_DIR.mkdir(parents=True, exist_ok=True)
            (FAIL_DIR / f"{seq:04d}_{rec.get('id','x')[:20]}.txt").write_text(out or "", encoding="utf-8")
            return None, "parse fail"
        meta.setdefault("cleaned_body", pre)
    # 快取 meta（供 --render-only 免 LLM 重繪）
    META_DIR.mkdir(parents=True, exist_ok=True)
    _meta_path(rec).write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
    path = write_md(seq, rec, meta)
    return {"seq": seq, "id": rec.get("id"), "file": path.name,
            "title": meta.get("title"), "date_iso": meta.get("date_iso"),
            "date_label": rec.get("date_label"), "themes": meta.get("themes") or [],
            "one_liner": meta.get("one_liner", ""), "permalink": rec.get("permalink"),
            "text_len": len(raw)}, None


# ---------------------------------------------------------------- 索引
def build_index(records: list[dict]) -> None:
    # seq＝抓取順序＝新→舊（FB feed 最新在前）。升序＝最新在上。date_iso 多數缺值不可靠，不用它排序。
    ordered = sorted(records, key=lambda r: r.get("seq", 0))

    lines = ["# 白話投資 知識庫 — 索引", "",
             f"共 {len(records)} 篇。來源：Facebook 粉專「白話投資」。", "",
             "| 日期 | 標題 | 主題 | 一句話 | 檔案 |", "|---|---|---|---|---|"]
    for r in ordered:
        d = r.get("date_iso") or r.get("date_label") or "-"
        themes = " / ".join(r.get("themes") or [])
        title = _norm_title(r.get("title")).replace("|", "/")
        one = (r.get("one_liner") or "").replace("|", "/")
        lines.append(f"| {d} | {title} | {themes} | {one} | [{r['file']}]({r['file']}) |")
    (KB_DIR / "INDEX.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    # 主題地圖
    by_theme: dict[str, list[dict]] = {}
    for r in records:
        for th in (r.get("themes") or ["其他"]):
            by_theme.setdefault(th, []).append(r)
    tlines = ["# 白話投資 知識庫 — 主題地圖", ""]
    for th in sorted(by_theme, key=lambda k: -len(by_theme[k])):
        tlines.append(f"## {th}（{len(by_theme[th])} 篇）")
        tlines.append("")
        for r in sorted(by_theme[th], key=lambda x: x.get("seq", 0)):
            d = r.get("date_iso") or r.get("date_label") or "-"
            tlines.append(f"- [{_norm_title(r.get('title'))}]({r['file']}) — {d}")
        tlines.append("")
    (KB_DIR / "THEMES.md").write_text("\n".join(tlines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------- 主流程
def load_posts() -> list[dict]:
    if not RAW_JSONL.exists():
        log.error("找不到 %s。請先跑 tools/fetch_baihua_fb.py --scrape", RAW_JSONL)
        return []
    rows = []
    for line in RAW_JSONL.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows


def load_state() -> dict:
    if STATE.exists():
        try:
            return json.loads(STATE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def main() -> int:
    ap = argparse.ArgumentParser(description="白話投資知識庫建置器")
    ap.add_argument("--rebuild", action="store_true", help="全部重跑（忽略已處理狀態）")
    ap.add_argument("--dry-run", action="store_true", help="不呼叫 LLM（regex 清洗 + stub）")
    ap.add_argument("--render-only", action="store_true", help="不呼叫 LLM，從 _meta/ 快取重繪所有 md + 索引")
    ap.add_argument("--limit", type=int, default=0, help="只處理前 N 篇（0=全部）")
    ap.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    args = ap.parse_args()

    KB_DIR.mkdir(parents=True, exist_ok=True)
    posts = load_posts()
    if not posts:
        return 1
    if args.limit:
        posts = posts[:args.limit]

    if args.render_only:
        records = []
        miss = 0
        for seq, rec in enumerate(posts):
            mp = _meta_path(rec)
            if not mp.exists():
                miss += 1
                continue
            meta = json.loads(mp.read_text(encoding="utf-8"))
            path = write_md(seq, rec, meta)
            records.append({"seq": seq, "id": rec.get("id"), "file": path.name,
                            "title": meta.get("title"), "date_iso": meta.get("date_iso"),
                            "date_label": rec.get("date_label"), "themes": meta.get("themes") or [],
                            "one_liner": meta.get("one_liner", ""), "permalink": rec.get("permalink")})
        build_index(records)
        log.info("[RENDER-ONLY] 重繪 %d 篇（缺 meta %d）→ %s", len(records), miss, KB_DIR / "INDEX.md")
        return 0

    state = {} if args.rebuild else load_state()
    prior_len = {r.get("id"): r.get("text_len", 0)
                 for r in (state.get("__records__", []) if isinstance(state, dict) else [])}

    def _needs(p: dict) -> bool:
        pid = p.get("id")
        if args.rebuild or pid not in state:
            return True
        # 原文顯著變長（see-more 展開 / 貼文被編輯）→ 重新清洗
        return len(p.get("text", "")) > prior_len.get(pid, 0) * 1.15

    todo = [(i, p) for i, p in enumerate(posts) if _needs(p)]
    log.info("posts=%d 待處理=%d (已處理 %d) dry_run=%s conc=%d",
             len(posts), len(todo), len(posts) - len(todo), args.dry_run, args.concurrency)

    records: list[dict] = []
    # 保留既有已處理 record（重建索引需要全集）
    prior = state.get("__records__", []) if isinstance(state, dict) else []
    if not args.rebuild:
        done_ids = {r.get("id") for _, p in [(i, p) for i, p in enumerate(posts)] for r in prior}
        records.extend(prior)

    ok = fail = 0
    if args.dry_run or args.concurrency <= 1:
        for seq, rec in todo:
            r, err = process_one(seq, rec, args.dry_run)
            if r:
                records.append(r); ok += 1
            else:
                fail += 1
                log.warning("seq=%d FAIL: %s", seq, err)
    else:
        with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            futs = {ex.submit(process_one, seq, rec, False): seq for seq, rec in todo}
            for fut in as_completed(futs):
                r, err = fut.result()
                if r:
                    records.append(r); ok += 1
                    log.info("[OK] %s", r["file"])
                else:
                    fail += 1
                    log.warning("seq=%d FAIL: %s", futs[fut], err)

    # 去重（同 id 保留最新）後建索引
    dedup = {r["id"]: r for r in records}
    records = list(dedup.values())
    build_index(records)

    new_state = {r["id"]: r["file"] for r in records}
    new_state["__records__"] = records
    STATE.write_text(json.dumps(new_state, ensure_ascii=False, indent=1), encoding="utf-8")
    log.info("[DONE] ok=%d fail=%d 總索引=%d → %s", ok, fail, len(records), KB_DIR / "INDEX.md")
    return 0 if fail == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
