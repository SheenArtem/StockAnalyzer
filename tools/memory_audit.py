"""
Memory cleanup stale audit (read-only).

掃 ~/.claude/projects/C--GIT-StockAnalyzer/memory/MEMORY.md 每行索引，
對描述含「pending 字眼」的條目，grep 對應 link 檔的「完工 marker」，
報告可能 stale 的 mismatch。不自動修。

觸發時機：
    1. User 喊「整理記憶」前先跑（feedback_memory_cleanup_sop.md phase 1）
    2. 整理完跑第二次驗證 mismatch=0

Usage:
    python tools/memory_audit.py
    python tools/memory_audit.py --verbose      # 列每條檢查細節
    python tools/memory_audit.py --quiet        # 只列 mismatch
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

# Windows cp950 codepage 不支援 ✅ / 中文，強制 utf-8
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

MEMORY_DIR = (
    Path.home()
    / ".claude"
    / "projects"
    / "C--GIT-StockAnalyzer"
    / "memory"
)
MEMORY_INDEX = MEMORY_DIR / "MEMORY.md"

# 「描述還說 pending」的字眼（命中即 flag）
PENDING_PATTERNS = [
    r"\bP[1-4]\b(?!\s*[+]\s*P)",  # P1/P2/P3/P4 但不抓 "P1+P2+P3 已落地"
    r"未做",
    r"未開工",
    r"未完成",
    r"待開工",
    r"未驗",
    r"\bTODO\b",
    r"actionable",
    r"\binflight\b",
    r"in[ _]progress",
    r"剩\s*#?\w",  # 「剩 #2/#3」「剩營業槓桿」
    r"低優先",
]

# link 檔的「完工 marker」
COMPLETION_PATTERNS = [
    r"✅",
    r"已完成",
    r"已落地",
    r"D\s*歸檔",
    r"已驗",
    r"completed",
    r"完成\s*20\d{2}",  # 「完成 2026-04-29」
    r"commit\s*[`\"]?[a-f0-9]{6,40}",
]

# MEMORY.md 索引 line 格式: `- [Title](file.md) — description`
INDEX_LINE_RE = re.compile(r"^-\s+\[([^\]]+)\]\(([^)]+\.md)\)\s*[—–-]\s*(.+)$")


@dataclass
class IndexEntry:
    line_no: int
    title: str
    link: str
    description: str
    raw: str


@dataclass
class Mismatch:
    entry: IndexEntry
    pending_hit: str          # 描述觸發的 pending pattern
    completion_evidence: list  # link 檔 grep 命中的完工 marker (取前 3 行)


def parse_index(path: Path) -> list[IndexEntry]:
    out = []
    for i, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        m = INDEX_LINE_RE.match(raw)
        if not m:
            continue
        title, link, desc = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        out.append(IndexEntry(i, title, link, desc, raw))
    return out


def scan_pending(desc: str) -> str | None:
    """描述中是否含 pending 字眼？回傳第一個 hit pattern。"""
    for pat in PENDING_PATTERNS:
        if re.search(pat, desc):
            return pat
    return None


def scan_completion(text: str, max_evidence: int = 3) -> list[str]:
    """link 檔內容是否含完工 marker？回傳前 N 個 hit lines。"""
    evidence = []
    for line in text.splitlines():
        for pat in COMPLETION_PATTERNS:
            if re.search(pat, line):
                evidence.append(line.strip())
                break
        if len(evidence) >= max_evidence:
            break
    return evidence


def audit(verbose: bool = False, quiet: bool = False) -> int:
    if not MEMORY_INDEX.exists():
        print(f"[ERROR] MEMORY.md not found: {MEMORY_INDEX}", file=sys.stderr)
        return 2

    entries = parse_index(MEMORY_INDEX)
    mismatches: list[Mismatch] = []
    missing_links: list[IndexEntry] = []

    for entry in entries:
        link_path = MEMORY_DIR / entry.link
        if not link_path.exists():
            missing_links.append(entry)
            continue

        pending_hit = scan_pending(entry.description)
        if not pending_hit:
            if verbose:
                print(f"  [OK]   L{entry.line_no:3d} {entry.title}")
            continue

        text = link_path.read_text(encoding="utf-8")
        evidence = scan_completion(text)
        if evidence:
            mismatches.append(Mismatch(entry, pending_hit, evidence))
        elif verbose:
            print(f"  [PEND] L{entry.line_no:3d} {entry.title} (pending '{pending_hit}', no completion in file)")

    # 輸出
    print("=" * 70)
    print(f"Memory audit @ {MEMORY_INDEX}")
    print(f"Total index lines: {len(entries)}")
    print(f"Missing links:     {len(missing_links)}")
    print(f"Stale mismatches:  {len(mismatches)}  (描述像 pending 但檔內有完工 marker)")
    print("=" * 70)

    if missing_links:
        print("\n[MISSING LINKS]")
        for e in missing_links:
            print(f"  L{e.line_no:3d} → {e.link}  ({e.title})")

    if mismatches:
        print("\n[STALE MISMATCHES]")
        for m in mismatches:
            e = m.entry
            print(f"\n  L{e.line_no:3d} {e.title}")
            print(f"      file:        {e.link}")
            print(f"      pending hit: '{m.pending_hit}' in description")
            print(f"      description: {e.description[:120]}{'...' if len(e.description) > 120 else ''}")
            print(f"      file evidence ({len(m.completion_evidence)} lines):")
            for ev in m.completion_evidence:
                print(f"        > {ev[:100]}{'...' if len(ev) > 100 else ''}")

    if not quiet:
        if mismatches or missing_links:
            print("\n⚠️  人工 checklist：對每條 mismatch 確認 — 若已完成 → 改 MEMORY.md 描述（mutation 不 append）；若仍 pending → 在 link 檔加說明釐清。")
        else:
            print("\n✅ 沒有發現 stale mismatch / missing link。")

    return 1 if (mismatches or missing_links) else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", "-v", action="store_true", help="列每條檢查細節")
    parser.add_argument("--quiet", "-q", action="store_true", help="只列 mismatch")
    args = parser.parse_args()
    return audit(verbose=args.verbose, quiet=args.quiet)


if __name__ == "__main__":
    sys.exit(main())
