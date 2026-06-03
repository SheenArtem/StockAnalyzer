"""publish_reports.py -- 把報告庫的 HTML 同步發佈到 sa-reports (GitHub Pages)。

把 data/ai_reports + data/macro_reports 的 *.html 複製到 sa-reports repo 的
ai/ 與 macro/ 子資料夾，重生封面頁 index.html，commit 後 push。
認證走 git credential manager (使用者已快取的 GitHub 帳號)，不需要 token。

用法:
    python tools/publish_reports.py            # 同步 + commit + push
    python tools/publish_reports.py --dry-run   # 只複製 + 重生 index，不 commit/push
    python tools/publish_reports.py --repo D:/somewhere/sa-reports   # 覆寫發佈 repo 路徑

線上入口: https://sheenartem.github.io/sa-reports/
"""
from __future__ import annotations

import argparse
import html
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"

# 發佈目標 repo (sa-reports)。預設為 StockAnalyzer 的同層目錄。
DEFAULT_PUBLISH_REPO = REPO.parent / "sa-reports"
PUBLISH_REPO_URL = "https://github.com/SheenArtem/sa-reports.git"
PAGES_URL = "https://sheenartem.github.io/sa-reports/"

# 來源分類 -> sa-reports 子資料夾
SOURCES = {
    "ai": DATA / "ai_reports",      # 個股 AI 報告
    "macro": DATA / "macro_reports",  # 總經風向報告
}

# index 封面頁分類標題
CATEGORY_TITLES = {"ai": "個股 AI 報告", "macro": "總經 Macro Compass"}


# ---------------------------------------------------------------------------
# index.html 產生
# ---------------------------------------------------------------------------
def _parse_ai(fn: str) -> tuple[str, str, str]:
    """TICKER_YYYYMMDD_HHMMSS.html -> (title, date, time)。"""
    m = re.match(r"(.+?)_(\d{8})_(\d{6})\.html$", fn)
    if not m:
        return fn[:-5], "", ""
    tk, d, t = m.groups()
    return tk, f"{d[:4]}-{d[4:6]}-{d[6:8]}", f"{t[:2]}:{t[2:4]}"


def _parse_macro(fn: str) -> tuple[str, str, str]:
    """YYYY-MM-DD_HHMMSS.html / latest.html -> (title, date, time)。"""
    if fn == "latest.html":
        return "最新", "", "latest"
    m = re.match(r"(\d{4}-\d{2}-\d{2})_(\d{6})\.html$", fn)
    if not m:
        return fn[:-5], "", ""
    d, t = m.groups()
    return "Macro Compass", d, f"{t[:2]}:{t[2:4]}"


_PARSERS = {"ai": _parse_ai, "macro": _parse_macro}


def _card(href: str, title: str, sub: str) -> str:
    return (f'<a class="card" href="{html.escape(href)}">'
            f'<div class="t">{html.escape(title)}</div>'
            f'<div class="s">{html.escape(sub)}</div></a>')


def build_index(publish_repo: Path) -> str:
    """掃 publish_repo 下各子資料夾的 *.html，產生深色封面頁。"""
    sections = []
    counts = {}
    for cat in SOURCES:
        sub = publish_repo / cat
        if not sub.is_dir():
            counts[cat] = 0
            continue
        parser = _PARSERS[cat]
        rows = sorted(
            ((f.name,) + parser(f.name) for f in sub.glob("*.html")),
            key=lambda x: (x[2], x[3]), reverse=True,
        )
        counts[cat] = len(rows)
        cards = "\n".join(
            _card(f"{cat}/{fn}", title, f"{d} {tm}".strip() or "latest")
            for fn, title, d, tm in rows
        )
        sections.append(f'<h2>{html.escape(CATEGORY_TITLES[cat])}</h2>\n'
                        f'<div class="grid">\n{cards}\n</div>')

    meta = " · ".join(f"{CATEGORY_TITLES[c]} {counts.get(c, 0)} 份" for c in SOURCES)
    body = "\n".join(sections)
    return f"""<!DOCTYPE html>
<html lang="zh-Hant"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex">
<title>StockAnalyzer Reports</title>
<style>
:root{{color-scheme:dark}}
*{{box-sizing:border-box}}
body{{margin:0;font-family:-apple-system,"Segoe UI",system-ui,sans-serif;background:#0e1116;color:#e6edf3;padding:32px 20px;max-width:980px;margin:0 auto}}
h1{{font-size:24px;margin:0 0 4px}}
.meta{{color:#8b949e;font-size:13px;margin-bottom:28px}}
h2{{font-size:15px;color:#8b949e;text-transform:uppercase;letter-spacing:.05em;margin:32px 0 12px;border-bottom:1px solid #21262d;padding-bottom:8px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px}}
.card{{display:block;background:#161b22;border:1px solid #21262d;border-radius:10px;padding:14px 16px;text-decoration:none;color:inherit;transition:.15s}}
.card:hover{{border-color:#388bfd;background:#1c2230}}
.card .t{{font-weight:600;font-size:16px}}
.card .s{{color:#8b949e;font-size:12px;margin-top:4px}}
</style></head><body>
<h1>StockAnalyzer Reports</h1>
<div class="meta">{html.escape(meta)}</div>
{body}
</body></html>"""


# ---------------------------------------------------------------------------
# 同步 + git
# ---------------------------------------------------------------------------
def _run_git(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=str(cwd),
                          capture_output=True, text=True)


def ensure_repo(publish_repo: Path) -> None:
    """publish repo 不存在就 clone (走 credential manager)。失敗 raise。"""
    if (publish_repo / ".git").is_dir():
        return
    r = subprocess.run(["git", "clone", PUBLISH_REPO_URL, str(publish_repo)],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"clone 失敗: {r.stderr.strip()}")


def sync_files(publish_repo: Path) -> int:
    """複製各來源 *.html 到 publish_repo/<cat>/。回傳複製檔數。"""
    n = 0
    for cat, src in SOURCES.items():
        if not src.is_dir():
            print(f"[skip] 來源不存在: {src}")
            continue
        dst = publish_repo / cat
        dst.mkdir(parents=True, exist_ok=True)
        for f in src.glob("*.html"):
            shutil.copy2(f, dst / f.name)
            n += 1
    return n


def publish(repo: Path = DEFAULT_PUBLISH_REPO, dry_run: bool = False) -> dict:
    """同步 + 重生 index + (非 dry-run 才) commit/push。

    回傳 {ok, message, url, copied, pushed}；不 sys.exit，供 UI / CLI 共用。
    """
    publish_repo = Path(repo).resolve()
    try:
        if not dry_run:
            ensure_repo(publish_repo)
        elif not (publish_repo / ".git").is_dir():
            return {"ok": False, "message": f"dry-run 需 repo 已存在: {publish_repo}",
                    "url": PAGES_URL, "copied": 0, "pushed": False}

        copied = sync_files(publish_repo)
        (publish_repo / "index.html").write_text(build_index(publish_repo), encoding="utf-8")

        if dry_run:
            return {"ok": True, "message": f"[dry-run] 複製 {copied} 份 HTML + 重生 index，未 push。",
                    "url": PAGES_URL, "copied": copied, "pushed": False}

        status = _run_git(["status", "--porcelain"], publish_repo)
        if not status.stdout.strip():
            return {"ok": True, "message": f"無變更（{copied} 份已是最新），跳過 push。",
                    "url": PAGES_URL, "copied": copied, "pushed": False}

        _run_git(["add", "-A"], publish_repo)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        commit = _run_git(["-c", "commit.gpgsign=false", "commit",
                           "-m", f"Publish reports {ts}"], publish_repo)
        if commit.returncode != 0:
            return {"ok": False, "message": f"commit 失敗: {commit.stdout.strip()} {commit.stderr.strip()}",
                    "url": PAGES_URL, "copied": copied, "pushed": False}

        push = _run_git(["push"], publish_repo)
        if push.returncode != 0:
            return {"ok": False, "message": f"push 失敗 (檢查 git 認證): {push.stderr.strip()}",
                    "url": PAGES_URL, "copied": copied, "pushed": False}

        return {"ok": True, "message": f"已發佈 {copied} 份報告，Pages 約 30-60 秒後更新。",
                "url": PAGES_URL, "copied": copied, "pushed": True}
    except Exception as e:  # clone 失敗等
        return {"ok": False, "message": f"發佈失敗: {e}",
                "url": PAGES_URL, "copied": 0, "pushed": False}


def main() -> None:
    ap = argparse.ArgumentParser(description="發佈報告 HTML 到 sa-reports (GitHub Pages)")
    ap.add_argument("--repo", type=Path, default=DEFAULT_PUBLISH_REPO,
                    help=f"sa-reports repo 路徑 (預設 {DEFAULT_PUBLISH_REPO})")
    ap.add_argument("--dry-run", action="store_true",
                    help="只複製 + 重生 index，不 commit/push")
    args = ap.parse_args()

    res = publish(repo=args.repo, dry_run=args.dry_run)
    print(("[OK] " if res["ok"] else "[FATAL] ") + res["message"])
    print(f"線上入口: {res['url']}")
    if not res["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
