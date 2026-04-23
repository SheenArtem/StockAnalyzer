"""
sync_substack.py — Substack 文章下載 + 索引重建

用途：
  1. 呼叫 C:\\ClaudeCode\\Normal\\download_substack.py 抓最新文章（只新增不覆蓋）
  2. 掃描 substack_posts 資料夾，比對現有 INDEX.md 文章列表
  3. 若有新檔 → 提示需人工補索引（新文摘要/分類需讀內容，程式自動化風險大）
  4. 若沒新檔 → 印一行「up to date」結束

為什麼不全自動補索引？
  摘要 / 分類 / actionable_points 需要讀理解文章內容，LLM 呼叫成本高且
  可能失真。人工觸發比排程全自動更可控 —— 有新文時印出檔名，由使用者
  下次開 Claude Code 時請求補 INDEX 即可。

排程整合：
  run_scanner.bat 每日 22:00 呼叫本腳本。即使失敗也不影響 scanner 主流程
  （exit code 不 propagate 出去）。

Robustness First：
  - 所有網路/subprocess 失敗 log + 繼續，不讓排程掛掉
  - Fail loud：新檔出現時在 stdout 明確列出，並寫入 scanner.log
"""
from __future__ import annotations

import io
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# 強制 stdout UTF-8 — 避免 cp950 console 印中文失敗
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

DOWNLOADER = Path(r"C:\ClaudeCode\Normal\download_substack.py")
POSTS_DIR = Path(r"C:\ClaudeCode\Normal\substack_posts")
PUB_URL = "https://openbookandeasypoint.substack.com"

INDEX_FILE = Path(__file__).resolve().parents[1] / "knowledge" / "songfen" / "INDEX.md"


def _log(msg: str):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {msg}", flush=True)


def run_downloader() -> int:
    """呼叫原作者的 download_substack.py 抓新文。"""
    if not DOWNLOADER.exists():
        _log(f"[WARN] downloader not found: {DOWNLOADER}")
        return 1
    _log(f"running downloader: {DOWNLOADER} --all {PUB_URL}")
    try:
        r = subprocess.run(
            [sys.executable, str(DOWNLOADER), "--all", PUB_URL,
             "--output", str(POSTS_DIR)],
            capture_output=True, text=True, timeout=300,
            encoding="utf-8", errors="replace",
        )
        if r.stdout:
            for line in r.stdout.strip().splitlines():
                _log(f"  {line}")
        if r.returncode != 0:
            _log(f"[WARN] downloader exit={r.returncode}")
            if r.stderr:
                _log(f"  stderr: {r.stderr[:500]}")
        return r.returncode
    except subprocess.TimeoutExpired:
        _log("[WARN] downloader timeout (300s)")
        return 124
    except Exception as e:
        _log(f"[WARN] downloader failed: {e}")
        return 2


def list_post_files() -> list[Path]:
    """列出 substack_posts 所有 .md 檔（2024 年之後的）。"""
    if not POSTS_DIR.exists():
        _log(f"[ERROR] posts dir missing: {POSTS_DIR}")
        return []
    return sorted(POSTS_DIR.glob("*.md"))


def parse_index_filenames() -> set[str]:
    """從 INDEX.md 日期標頭 + 標題推測已索引的檔名集合。
    標頭格式：### YYYY-MM-DD 《...》
    INDEX 條目用 date + partial title 匹配，比對時只取 date 前綴（寬鬆比對）。
    """
    if not INDEX_FILE.exists():
        return set()
    text = INDEX_FILE.read_text(encoding="utf-8")
    # 擷取所有 ### YYYY-MM-DD 開頭
    dates = set(re.findall(r"^### (\d{4}-\d{2}-\d{2})\s", text, re.MULTILINE))
    return dates


def find_new_posts(all_files: list[Path], indexed_dates: set[str]) -> list[Path]:
    """檔名以 YYYY-MM-DD_ 開頭，未在索引中的視為新文。
    排除：Substack Note（短文，檔名 YYYY-MM-DD_Note_xxx.md 或 < 2KB）。
    """
    new = []
    for f in all_files:
        # 排除 Notes
        if re.match(r"^\d{4}-\d{2}-\d{2}_Note_", f.name):
            continue
        if f.stat().st_size < 2048:
            continue
        m = re.match(r"^(\d{4}-\d{2}-\d{2})_", f.name)
        if not m:
            continue
        date = m.group(1)
        if date not in indexed_dates:
            new.append(f)
    return new


def main():
    _log("=== Substack sync start ===")
    _log(f"posts dir: {POSTS_DIR}")
    _log(f"index file: {INDEX_FILE}")

    # Step 1: download new posts (best-effort)
    run_downloader()

    # Step 2: find files not yet in INDEX.md
    all_files = list_post_files()
    indexed_dates = parse_index_filenames()
    _log(f"total posts on disk: {len(all_files)}  already indexed: {len(indexed_dates)}")

    new_posts = find_new_posts(all_files, indexed_dates)
    if not new_posts:
        _log("[OK] index is up to date; no new posts.")
        _log("=== Substack sync done ===")
        return 0

    _log(f"[INFO] {len(new_posts)} new post(s) detected; auto-indexing via Claude CLI:")
    for f in new_posts:
        _log(f"  + {f.name}")
    _log("")

    # Step 3: delegate to auto_index_substack (uses Claude CLI)
    auto_indexer = Path(__file__).with_name("auto_index_substack.py")
    if not auto_indexer.exists():
        _log(f"[WARN] auto indexer not found: {auto_indexer}")
        _log("Action required: manually add new posts to INDEX.md")
        _log("=== Substack sync done ===")
        return 0

    try:
        r = subprocess.run(
            [sys.executable, str(auto_indexer)],
            timeout=1800,  # 30 min budget for all new posts
            capture_output=False,  # stream logs directly
        )
        _log(f"auto_index_substack exit={r.returncode}")
    except subprocess.TimeoutExpired:
        _log("[WARN] auto_index_substack timeout (30 min)")
    except Exception as e:
        _log(f"[WARN] auto_index_substack failed: {e}")

    _log("=== Substack sync done ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
