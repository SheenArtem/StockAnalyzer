"""
強勢股日報 + 週報 渲染 — Phase 4 (+ 2026-05-14 加 --weekly)

讀 input JSON + 對應 Jinja2 template 產出 HTML + PDF.

模式:
  Daily (default):
    input  = data/latest/strong_stocks_daily.json
    template = templates/strong_stocks_daily.html.j2
    output = data/strong_stocks_reports/YYYY-MM-DD.{html,pdf}

  Weekly (--weekly):
    input  = data/latest/strong_stocks_weekly.json
    template = templates/strong_stocks_weekly.html.j2
    output = data/strong_stocks_reports/YYYY-Www.{html,pdf}  (ISO 週次)

PDF 用 playwright headless Chromium 印.

Usage:
  python tools/strong_stocks_render.py
  python tools/strong_stocks_render.py --weekly
  python tools/strong_stocks_render.py --no-pdf      # 只出 HTML
  python tools/strong_stocks_render.py --output-dir custom/path
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logger = logging.getLogger(__name__)

INPUT_PATH = REPO / "data" / "latest" / "strong_stocks_daily.json"
TEMPLATE_PATH = REPO / "templates" / "strong_stocks_daily.html.j2"
WEEKLY_INPUT_PATH = REPO / "data" / "latest" / "strong_stocks_weekly.json"
WEEKLY_TEMPLATE_PATH = REPO / "templates" / "strong_stocks_weekly.html.j2"
OUTPUT_DIR = REPO / "data" / "strong_stocks_reports"


def to_roc(date_str: str) -> str:
    """2026-05-06 -> 115/05/06."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"
    except (ValueError, TypeError):
        return date_str or ""


def render_html(daily: dict, template_path: Path = TEMPLATE_PATH) -> str:
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        autoescape=select_autoescape(["html", "j2"]),
    )
    template = env.get_template(template_path.name)

    scan_date = daily.get("scan_date", "")
    ref_date = daily.get("ref_date", "") or scan_date
    # Weekly extra fields (None for daily mode)
    return template.render(
        scan_date=scan_date,
        scan_date_roc=to_roc(scan_date),
        ref_date=ref_date,
        ref_date_roc=to_roc(ref_date),
        week_label=daily.get("week_label", ""),
        week_start=daily.get("week_start", ""),
        week_end=daily.get("week_end", ""),
        universe_size=daily.get("universe_size", "-"),
        informational_tier=daily.get("informational_tier", False),
        informational_caveat=daily.get("informational_caveat", ""),
        generated_at=daily.get(
            "generated_at",
            datetime.now().isoformat(timespec="seconds"),
        ),
        twse_top=daily.get("twse_top", []),
        tpex_top=daily.get("tpex_top", []),
        ai_analysis=daily.get("ai_analysis"),
    )


def render_pdf(html_path: Path, pdf_path: Path) -> bool:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[WARN] playwright not installed, skip PDF", file=sys.stderr)
        return False

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            page = browser.new_page()
            page.goto(html_path.as_uri())
            page.pdf(
                path=str(pdf_path),
                format="A4",
                landscape=True,  # 12 columns → 橫式比較不擠
                margin={"top": "12mm", "bottom": "12mm",
                        "left": "8mm", "right": "8mm"},
                print_background=True,
            )
            browser.close()
        return True
    except Exception as e:
        print(f"[ERROR] PDF render failed: {e}", file=sys.stderr)
        return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=None,
                        help="輸入 JSON; --weekly 預設 weekly JSON, 否則 daily")
    parser.add_argument("--weekly", action="store_true",
                        help="週報模式 (檔名 YYYY-Www, 用 weekly template)")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--no-pdf", action="store_true",
                        help="Skip PDF, only emit HTML")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Mode dispatch
    if args.weekly:
        if args.input is None:
            args.input = WEEKLY_INPUT_PATH
        template_path = WEEKLY_TEMPLATE_PATH
    else:
        if args.input is None:
            args.input = INPUT_PATH
        template_path = TEMPLATE_PATH

    if not args.input.exists():
        print(f"[ERROR] {args.input} not found", file=sys.stderr)
        return 1
    if not template_path.exists():
        print(f"[ERROR] template {template_path} not found", file=sys.stderr)
        return 1

    with args.input.open("r", encoding="utf-8") as f:
        daily = json.load(f)

    # File name:
    #   Daily:  YYYY-MM-DD (ref_date)
    #   Weekly: YYYY-Www  (ISO week label)
    if args.weekly:
        file_stem = daily.get("week_label") or datetime.now().strftime("%Y-W%W")
    else:
        file_stem = (daily.get("ref_date") or daily.get("scan_date")
                     or datetime.now().strftime("%Y-%m-%d"))
    args.output_dir.mkdir(parents=True, exist_ok=True)

    html_path = args.output_dir / f"{file_stem}.html"
    pdf_path = args.output_dir / f"{file_stem}.pdf"

    html = render_html(daily, template_path=template_path)
    html_path.write_text(html, encoding="utf-8")
    print(f"[OK] HTML: {html_path}")

    if not args.no_pdf:
        if render_pdf(html_path, pdf_path):
            print(f"[OK] PDF:  {pdf_path}")
        else:
            return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
