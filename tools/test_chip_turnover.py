"""
籌碼面「周轉率」欄位驗證腳本
驗證 ⚡ 當沖週轉概況 區塊有 4 個 metric（含「周轉率」）
"""
import subprocess
import sys
import os
import time
import signal
from pathlib import Path

SCREENSHOT_DIR = Path(__file__).parent / "screenshots"
SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)


def run_test(stock_id="2330"):
    print(f"Starting Streamlit server on port 8599...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app.py",
         "--server.headless", "true", "--server.port", "8599",
         "--browser.gatherUsageStats", "false"],
        cwd=str(Path(__file__).parent.parent),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
    )

    try:
        print("Waiting for server (8s)...")
        time.sleep(8)
        # Drain stdout so buffer doesn't block
        import threading
        server_log = []
        def _drain(p):
            for line in p.stdout:
                try:
                    server_log.append(line.decode("utf-8", errors="replace").rstrip())
                except Exception:
                    pass
        t = threading.Thread(target=_drain, args=(proc,), daemon=True)
        t.start()

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1400, "height": 900})

            page.goto("http://localhost:8599", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=30000)
            page.screenshot(path=str(SCREENSHOT_DIR / "chip_00_initial.png"))
            print("  Saved: chip_00_initial.png")

            # Fill stock input
            input_el = page.locator("input[type='text']").first
            input_el.fill(stock_id)
            page.screenshot(path=str(SCREENSHOT_DIR / "chip_01_input.png"))

            # Click analyze
            analyze_btn = page.get_by_text("開始分析")
            if analyze_btn.count() > 0:
                analyze_btn.first.click()
                print(f"  Clicked analyze for {stock_id}, waiting for tabs to appear (up to 90s)...")
                # Wait for tabs to appear — indicates analysis complete
                try:
                    page.wait_for_selector("[role='tab']", timeout=90000)
                    print("  Tabs appeared!")
                except Exception:
                    print("  [WARN] Tabs did not appear in 90s, taking screenshot anyway")
                page.wait_for_timeout(3000)
            else:
                print("  ERROR: 找不到「開始分析」按鈕")
                page.screenshot(path=str(SCREENSHOT_DIR / "chip_error_no_btn.png"))
                browser.close()
                return

            page.screenshot(path=str(SCREENSHOT_DIR / "chip_02_after_analyze.png"), full_page=False)
            print("  Saved: chip_02_after_analyze.png")

            # Find tabs — try scrolling down first
            page.evaluate("window.scrollTo(0, 400)")
            page.wait_for_timeout(1000)

            tabs = page.get_by_role("tab")
            tab_count = tabs.count()
            print(f"  Found {tab_count} tab(s)")

            # Try to click 籌碼面
            chip_tab = page.get_by_role("tab", name="籌碼面")
            if chip_tab.count() > 0:
                chip_tab.first.scroll_into_view_if_needed()
                chip_tab.first.click()
                print("  Clicked 籌碼面 tab")
                page.wait_for_timeout(4000)
                page.screenshot(path=str(SCREENSHOT_DIR / "chip_03_chip_tab_top.png"), full_page=False)
                print("  Saved: chip_03_chip_tab_top.png")

                # Scroll down to find ⚡ 當沖週轉概況
                page.evaluate("window.scrollBy(0, 2000)")
                page.wait_for_timeout(1000)
                page.screenshot(path=str(SCREENSHOT_DIR / "chip_04_scroll_mid.png"), full_page=False)
                print("  Saved: chip_04_scroll_mid.png")

                page.evaluate("window.scrollBy(0, 2000)")
                page.wait_for_timeout(1000)
                page.screenshot(path=str(SCREENSHOT_DIR / "chip_05_scroll_lower.png"), full_page=False)
                print("  Saved: chip_05_scroll_lower.png")

                page.evaluate("window.scrollBy(0, 2000)")
                page.wait_for_timeout(1000)
                page.screenshot(path=str(SCREENSHOT_DIR / "chip_06_scroll_bottom.png"), full_page=False)
                print("  Saved: chip_06_scroll_bottom.png")

                # Full page screenshot of chip tab
                chip_tab.first.click()
                page.wait_for_timeout(3000)
                page.screenshot(path=str(SCREENSHOT_DIR / "chip_07_fullpage.png"), full_page=True)
                print("  Saved: chip_07_fullpage.png (full_page)")

                # Try to find ⚡ text on page
                turnover_el = page.get_by_text("周轉率")
                if turnover_el.count() > 0:
                    print(f"  [FOUND] '周轉率' element count: {turnover_el.count()}")
                    turnover_el.first.scroll_into_view_if_needed()
                    page.wait_for_timeout(500)
                    page.screenshot(path=str(SCREENSHOT_DIR / "chip_08_turnover_focus.png"), full_page=False)
                    print("  Saved: chip_08_turnover_focus.png")
                else:
                    print("  [WARN] '周轉率' text NOT found on page")

                # Check for ⚡ 當沖週轉概況
                section = page.get_by_text("當沖週轉概況")
                if section.count() > 0:
                    print(f"  [FOUND] '當沖週轉概況' section")
                    section.first.scroll_into_view_if_needed()
                    page.wait_for_timeout(500)
                    page.screenshot(path=str(SCREENSHOT_DIR / "chip_09_section_focus.png"), full_page=False)
                    print("  Saved: chip_09_section_focus.png")
                else:
                    print("  [WARN] '當沖週轉概況' section NOT found on page")

            else:
                print("  [FAIL] 籌碼面 tab not found")
                page.screenshot(path=str(SCREENSHOT_DIR / "chip_error_notab.png"), full_page=True)

            browser.close()

        print(f"\nScreenshots: {SCREENSHOT_DIR}")
        files = sorted(SCREENSHOT_DIR.glob("chip_*.png"))
        print(f"Total chip screenshots: {len(files)}")

    finally:
        # Print server log tail
        time.sleep(0.5)
        if server_log:
            print("\n--- Streamlit server log (last 30 lines) ---")
            for line in server_log[-30:]:
                print(line)
            print("--- end ---\n")
        else:
            print("[INFO] Server log empty (server may not have started)")

        print("Stopping Streamlit...")
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
    run_test(stock)
