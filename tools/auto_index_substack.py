"""
auto_index_substack.py — 使用 Claude CLI 自動補索引

由 sync_substack.py 在偵測到新文時呼叫，或手動執行。

流程：
  1. 掃描 C:\\ClaudeCode\\Normal\\substack_posts\\ 找未在 INDEX.md 的新文
  2. 對每篇新文呼叫 Claude CLI 萃取 metadata (category / tags / summary / actionable / permanence)
  3. 依日期插入 INDEX.md 正確位置（newest first）
  4. 失敗的文章 log 出來，不中斷其他文章處理

Claude CLI:
  claude -p --output-format text  < prompt_with_article_content

Robustness:
  - 單一文章失敗 → log + skip，不影響其他
  - 回傳格式不對 → 標記 pending，log 提示人工補
  - INDEX 寫入用 tmp + rename（atomic）
"""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# 強制 stdout UTF-8
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

POSTS_DIR = Path(r"C:\ClaudeCode\Normal\substack_posts")
INDEX_FILE = Path(__file__).resolve().parents[1] / "knowledge" / "songfen" / "INDEX.md"
AUDIT_LOG = INDEX_FILE.parent / ".auto_added.log"

_CLAUDE_CLI = shutil.which("claude") or "claude"
# LLM 規範 (2026-05-01)：metadata 萃取屬 News 類，用 Sonnet + 10 min timeout
_PER_ARTICLE_TIMEOUT = 600
_ARTICLE_CAP = 25000        # chars，超過截斷


EXTRACTION_PROMPT = """你是一位台/美股機構分析師的文章索引員。我會給你一篇宋分《美股送分題》Substack 文章，請**只萃取 metadata**，不用寫評論或結論。

請嚴格依照下列格式輸出（6 行，鍵值對，無其他文字）：

CATEGORY: <methodology | memo | deep | psych | methodology,psych | deep,methodology>
TAGS: <2-5 個關鍵字，逗號分隔，全小寫英文 kebab-case>
SUMMARY: <1-2 句話，繁中，不超過 80 字>
ACTIONABLE: <最多 3 點，分號「；」分隔，繁中>
PERMANENCE: <permanent | semi-permanent | time-sensitive>

分類說明：
- methodology：純方法論/教學類（類似底層系統系列、投資流程）
- memo：時效性市場觀點（宋分分析師備忘錄系列）
- deep：產業/主題深度研究（市場解碼系列）
- psych：投資心法/行為（停損、抄底、獲利了結）

Permanence 判斷：
- permanent：1-3 年後仍適用的方法論/心法
- semi-permanent：半年內有效的深度研究
- time-sensitive：3 個月內有效的備忘錄、當期觀點

嚴格規則：
- 只輸出那 6 行鍵值對，不要任何前言/總結/markdown
- SUMMARY 中文具體，不要空泛；避開「探討/分享」這類虛字
- ACTIONABLE 具體可執行，不要像「參考一下」「值得注意」

---

文章內容：

"""


def _log(msg: str):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {msg}", flush=True)


def _audit(msg: str):
    AUDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with AUDIT_LOG.open("a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}\n")


def parse_index_dates(index_text: str) -> set[str]:
    return set(re.findall(r"^### (\d{4}-\d{2}-\d{2})\s", index_text, re.MULTILINE))


def list_new_files(index_text: str) -> list[Path]:
    if not POSTS_DIR.exists():
        return []
    indexed = parse_index_dates(index_text)
    new = []
    for f in sorted(POSTS_DIR.glob("*.md")):
        if re.match(r"^\d{4}-\d{2}-\d{2}_Note_", f.name):
            continue
        if f.stat().st_size < 2048:
            continue
        m = re.match(r"^(\d{4}-\d{2}-\d{2})_", f.name)
        if not m:
            continue
        if m.group(1) not in indexed:
            new.append(f)
    return new


def extract_title(filename: str) -> str:
    """檔名格式 YYYY-MM-DD_《標題》.md → 《標題》"""
    stem = Path(filename).stem
    m = re.match(r"^\d{4}-\d{2}-\d{2}_(.+)$", stem)
    if not m:
        return stem
    return m.group(1).strip()


def call_claude(article_text: str) -> str | None:
    """呼叫 Claude CLI，回傳 stdout 或 None（失敗）。"""
    prompt = EXTRACTION_PROMPT + article_text[:_ARTICLE_CAP]
    try:
        r = subprocess.run(
            [_CLAUDE_CLI, "-p",
             "--model", "sonnet",
             "--output-format", "text"],
            input=prompt,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=_PER_ARTICLE_TIMEOUT,
        )
        if r.returncode != 0:
            _log(f"    Claude CLI exit={r.returncode}: {(r.stderr or '')[:200]}")
            return None
        out = (r.stdout or "").strip()
        if not out:
            _log("    Claude CLI returned empty output")
            return None
        return out
    except subprocess.TimeoutExpired:
        _log(f"    Claude CLI timeout ({_PER_ARTICLE_TIMEOUT}s)")
        return None
    except FileNotFoundError:
        _log("    Claude CLI not found in PATH")
        return None
    except Exception as e:
        _log(f"    Claude CLI error: {e}")
        return None


def parse_cli_output(out: str) -> dict | None:
    """解析 6 行鍵值對 → dict，失敗回 None。"""
    required = {"CATEGORY", "TAGS", "SUMMARY", "ACTIONABLE", "PERMANENCE"}
    result = {}
    for line in out.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip().upper()
        val = val.strip()
        if key in required and val:
            result[key] = val
    if not required.issubset(result.keys()):
        missing = required - result.keys()
        _log(f"    parse failed, missing: {missing}")
        return None
    # 驗證 category / permanence
    cat_valid = {"methodology", "memo", "deep", "psych"}
    cat_parts = [c.strip() for c in result["CATEGORY"].split(",")]
    if not all(c in cat_valid for c in cat_parts):
        _log(f"    invalid category: {result['CATEGORY']}")
        return None
    perm_valid = {"permanent", "semi-permanent", "time-sensitive"}
    if result["PERMANENCE"] not in perm_valid:
        _log(f"    invalid permanence: {result['PERMANENCE']}")
        return None
    return result


def format_entry(date_str: str, title: str, parsed: dict) -> str:
    """格式化成與現有 INDEX 一致的條目。尾行加 auto-generated 標記供審計。"""
    actionables = [a.strip() for a in parsed["ACTIONABLE"].split("；") if a.strip()]
    # 轉回「；」分隔（UI 一致）
    act_line = "；".join(actionables)
    return (
        f"### {date_str} {title}\n"
        f"- **Category:** {parsed['CATEGORY']}\n"
        f"- **Tags:** {parsed['TAGS']}\n"
        f"- **Summary:** {parsed['SUMMARY']}\n"
        f"- **Actionable:** {act_line}\n"
        f"- **Permanence:** {parsed['PERMANENCE']}\n"
        f"<!-- auto-generated {datetime.now().strftime('%Y-%m-%d')} -->\n"
    )


def insert_entry(index_text: str, entry_md: str, date_str: str) -> str:
    """
    Insert entry before the first ### block with date <= date_str.
    INDEX.md 結構：... ## Articles (by date, newest first) ... ### YYYY-MM-DD ...
    若找不到任何 ### 於 date_str 之後，就插在 "## Articles" 段之首。
    """
    lines = index_text.splitlines(keepends=True)
    out = []
    inserted = False
    in_articles_section = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("## Articles"):
            in_articles_section = True
            out.append(line)
            i += 1
            continue
        if line.startswith("## ") and in_articles_section and not line.startswith("## Articles"):
            # Entered next major section (e.g. "## Current Themes")
            if not inserted:
                out.append(entry_md + "\n")
                inserted = True
            in_articles_section = False
            out.append(line)
            i += 1
            continue
        if in_articles_section and not inserted:
            m = re.match(r"^### (\d{4}-\d{2}-\d{2})\s", line)
            if m and m.group(1) < date_str:
                # 在這個 ### 之前插入（因為 newest first，新文要在比它舊的 ### 前）
                out.append(entry_md + "\n")
                inserted = True
                out.append(line)
                i += 1
                continue
        out.append(line)
        i += 1

    if not inserted:
        # 找不到比它舊的 ###，追加在 Articles section 末尾或整份末尾
        out.append("\n" + entry_md)

    return "".join(out)


def atomic_write(path: Path, content: str):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def main():
    _log("=== auto_index_substack start ===")

    if not INDEX_FILE.exists():
        _log(f"[ERROR] INDEX.md missing: {INDEX_FILE}")
        return 2

    index_text = INDEX_FILE.read_text(encoding="utf-8")
    new_files = list_new_files(index_text)
    if not new_files:
        _log("[OK] no new files; nothing to do.")
        return 0

    _log(f"[INFO] {len(new_files)} new file(s) to index via Claude CLI")

    added = 0
    failed = []
    for f in new_files:
        m = re.match(r"^(\d{4}-\d{2}-\d{2})_", f.name)
        if not m:
            continue
        date_str = m.group(1)
        title = extract_title(f.name)
        _log(f"  - processing {f.name}")

        try:
            article_text = f.read_text(encoding="utf-8")
        except Exception as e:
            _log(f"    read failed: {e}")
            failed.append(f.name)
            continue

        cli_out = call_claude(article_text)
        if cli_out is None:
            failed.append(f.name)
            continue
        parsed = parse_cli_output(cli_out)
        if parsed is None:
            _log(f"    raw CLI output:\n{cli_out[:500]}")
            failed.append(f.name)
            continue

        entry_md = format_entry(date_str, title, parsed)
        # 重新讀 INDEX（前一輪寫入可能已改變）
        index_text = INDEX_FILE.read_text(encoding="utf-8")
        new_index = insert_entry(index_text, entry_md, date_str)
        atomic_write(INDEX_FILE, new_index)
        _audit(f"AUTO_ADD {f.name} -> category={parsed['CATEGORY']} perm={parsed['PERMANENCE']}")
        _log(f"    inserted into INDEX.md (category={parsed['CATEGORY']})")
        added += 1

    _log(f"[SUMMARY] added={added} failed={len(failed)}")
    if failed:
        _log("[WARN] failed files need manual review:")
        for name in failed:
            _log(f"    - {name}")
    _log("=== auto_index_substack done ===")
    return 0 if not failed else 3


if __name__ == "__main__":
    sys.exit(main())
