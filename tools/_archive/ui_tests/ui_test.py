"""
UI Self-Test Script (Playwright)
Usage: python tools/ui_test.py [stock_id] [--keep]

- Launches Streamlit in background
- Opens headless Chromium, inputs stock, clicks analyze
- Takes screenshots for each tab
- Saves to tools/screenshots/ (auto-cleaned unless --keep)
"""

import subprocess
import sys
import os
import time
import shutil
import signal
from pathlib import Path

# Screenshot output directory
SCREENSHOT_DIR = Path(__file__).parent / "screenshots"


def cleanup_screenshots():
    """Remove old screenshots."""
    if SCREENSHOT_DIR.exists():
        shutil.rmtree(SCREENSHOT_DIR)
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)


def run_test(stock_id="2330", keep=False):
    if not keep:
        cleanup_screenshots()
    else:
        SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    # Start Streamlit in background
    print(f"Starting Streamlit server...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app.py",
         "--server.headless", "true", "--server.port", "8599",
         "--browser.gatherUsageStats", "false"],
        cwd=str(Path(__file__).parent.parent),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
    )

    try:
        # Wait for server to start
        print("Waiting for server to start...")
        time.sleep(6)

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1400, "height": 900})

            # Navigate to Streamlit
            print("Navigating to Streamlit...")
            page.goto("http://localhost:8599", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=30000)

            # Take initial screenshot
            page.screenshot(path=str(SCREENSHOT_DIR / "00_initial.png"))
            print("  Saved: 00_initial.png")

            # Find and fill the stock input
            # Streamlit text_input renders as <input> inside a div
            input_el = page.locator("input[type='text']").first
            if input_el:
                input_el.fill(stock_id)
                print(f"  Entered stock: {stock_id}")
                page.screenshot(path=str(SCREENSHOT_DIR / "01_input.png"))

            # Click the analyze button
            analyze_btn = page.get_by_text("開始分析")
            if analyze_btn.count() > 0:
                analyze_btn.first.click()
                print("  Clicked: analyze button")

                # Wait for analysis to complete (spinner disappears)
                print("  Waiting for analysis...")
                page.wait_for_timeout(15000)  # 15 sec for data fetch + compute
                page.wait_for_load_state("networkidle", timeout=60000)
                page.wait_for_timeout(3000)  # Extra settle time

            # Screenshot main analysis
            page.screenshot(path=str(SCREENSHOT_DIR / "02_analysis.png"), full_page=True)
            print("  Saved: 02_analysis.png")

            # Click each tab and screenshot
            tab_names = ["週K", "日K", "籌碼面", "基本面", "情緒/期權", "除息/營收"]
            for i, tab_name in enumerate(tab_names):
                try:
                    tab = page.get_by_role("tab", name=tab_name)
                    if tab.count() > 0:
                        tab.first.click()
                        page.wait_for_timeout(3000)
                        fname = f"03_tab{i+1}_{tab_name.replace('/', '_')}.png"
                        page.screenshot(path=str(SCREENSHOT_DIR / fname), full_page=True)
                        print(f"  Saved: {fname}")
                except Exception as e:
                    print(f"  Tab '{tab_name}' error: {e}")

            browser.close()

        print(f"\nAll screenshots saved to: {SCREENSHOT_DIR}")
        print(f"Total: {len(list(SCREENSHOT_DIR.glob('*.png')))} files")

    finally:
        # Kill Streamlit
        print("Stopping Streamlit server...")
        if os.name == 'nt':
            proc.send_signal(signal.CTRL_BREAK_EVENT)
            time.sleep(1)
            proc.kill()
        else:
            proc.terminate()
        proc.wait(timeout=5)
        print("Done.")


if __name__ == "__main__":
    stock = sys.argv[1] if len(sys.argv) > 1 else "2330"
    keep = "--keep" in sys.argv
    run_test(stock, keep)
