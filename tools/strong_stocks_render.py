"""
強勢股日報 渲染 — Phase 4

讀 data/latest/strong_stocks_daily.json + templates/strong_stocks_daily.html.j2
產出 data/strong_stocks_reports/YYYY-MM-DD.{html,pdf}

PDF 用 playwright headless Chromium 印（已是 ui-test-rd 依賴，無新依賴）。

Usage:
  python tools/strong_stocks_render.py
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
OUTPUT_DIR = REPO / "data" / "strong_stocks_reports"


def to_roc(date_str: str) -> str:
    """2026-05-06 -> 115/05/06."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"
    except (ValueError, TypeError):
        return date_str or ""


def render_html(daily: dict) -> str:
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_PATH.parent)),
        autoescape=select_autoescape(["html", "j2"]),
    )
    template = env.get_template(TEMPLATE_PATH.name)

    scan_date = daily.get("scan_date", "")
    return template.render(
        scan_date=scan_date,
        scan_date_roc=to_roc(scan_date),
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
    parser.add_argument("--input", type=Path, default=INPUT_PATH)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--no-pdf", action="store_true",
                        help="Skip PDF, only emit HTML")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.input.exists():
        print(f"[ERROR] {args.input} not found", file=sys.stderr)
        return 1

    with args.input.open("r", encoding="utf-8") as f:
        daily = json.load(f)

    scan_date = daily.get("scan_date") or datetime.now().strftime("%Y-%m-%d")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    html_path = args.output_dir / f"{scan_date}.html"
    pdf_path = args.output_dir / f"{scan_date}.pdf"

    html = render_html(daily)
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
