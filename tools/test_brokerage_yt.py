"""
Playwright smoke test for brokerage_yt mode.
Usage: python tools/test_brokerage_yt.py
"""
import subprocess
import sys
import time
import os
from pathlib import Path

# Force UTF-8 stdout to avoid cp950 encode errors on Windows console
if sys.stdout.encoding != "utf-8":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

REPO = Path(__file__).resolve().parent.parent
SCREENSHOTS = REPO / "reports" / "screenshots"
SCREENSHOTS.mkdir(parents=True, exist_ok=True)
PORT = 18800
APP = str(REPO / "app.py")


def start_streamlit():
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", APP,
         "--server.port", str(PORT),
         "--server.headless", "true",
         "--server.runOnSave", "false",
         "--browser.gatherUsageStats", "false"],
        cwd=str(REPO),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    # Wait for server ready
    deadline = time.time() + 40
    while time.time() < deadline:
        try:
            import urllib.request
            urllib.request.urlopen(f"http://localhost:{PORT}/_stcore/health", timeout=2)
            print(f"[OK] Streamlit up on port {PORT}")
            return proc
        except Exception:
            time.sleep(1)
    raise RuntimeError("Streamlit failed to start within 40s")


def run_test():
    from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1400, "height": 900},
                                  locale="zh-TW")
        page = ctx.new_page()
        page.goto(f"http://localhost:{PORT}", timeout=30000)
        page.wait_for_load_state("networkidle", timeout=30000)
        time.sleep(3)

        # --- Acceptance 1: sidebar has ?ïÈ°ßËøΩËπ§ radio ---
        sidebar = page.locator('[data-testid="stSidebar"]')
        content = sidebar.inner_text()
        a1 = "?ïÈ°ßËøΩËπ§" in content
        print(f"[{'PASS' if a1 else 'FAIL'}] A1 sidebar has ?ïÈ°ßËøΩËπ§: {a1}")

        # --- Click the radio for brokerage_yt ---
        radio_options = page.locator('[data-testid="stSidebar"] label')
        clicked = False
        for i in range(radio_options.count()):
            lbl = radio_options.nth(i)
            txt = lbl.inner_text()
            if "?ïÈ°ß" in txt:
                lbl.click()
                clicked = True
                print(f"[OK] Clicked radio: {txt.strip()}")
                break
        if not clicked:
            print("[FAIL] Could not find ?ïÈ°ßËøΩËπ§ radio label")

        page.wait_for_load_state("networkidle", timeout=20000)
        time.sleep(3)

        # Screenshot: full page after mode switch
        shot0 = str(SCREENSHOTS / "brokerage_yt_00_mode_switch.png")
        page.screenshot(path=shot0, full_page=True)
        print(f"[SHOT] {shot0}")

        # --- Acceptance 2: title + disclaimer ---
        body = page.inner_text("body")
        a2_title = "?ïÈ°ßËøΩËπ§" in body
        a2_disc = any(kw in body for kw in ["?≤Ê?", "‰∏ç‰ª£Ë°?, "?çË≤¨", "disclaimer", "Disclaimer"])
        print(f"[{'PASS' if a2_title else 'FAIL'}] A2 title ?ïÈ°ßËøΩËπ§: {a2_title}")
        print(f"[{'PASS' if a2_disc else 'WARN'}] A2 disclaimer present: {a2_disc}")

        # --- Acceptance 3: selectbox shows ?©ÁàæË≠âÂà∏ ---
        a3 = "?©Áàæ" in body
        print(f"[{'PASS' if a3 else 'FAIL'}] A3 ?©ÁàæË≠âÂà∏ in page: {a3}")

        # --- Find tabs ---
        tabs = page.locator('[data-testid="stTab"]')
        tab_count = tabs.count()
        print(f"[INFO] Found {tab_count} tabs")

        # --- Tab 1: ?¥È??ãÊùø ---
        for i in range(tab_count):
            t = tabs.nth(i)
            if "?¥È?" in t.inner_text() or "?ãÊùø" in t.inner_text():
                t.click()
                time.sleep(2)
                break
        page.wait_for_load_state("networkidle", timeout=15000)
        shot1 = str(SCREENSHOTS / "brokerage_yt_01_tab_overview.png")
        page.screenshot(path=shot1, full_page=True)
        print(f"[SHOT] {shot1}")
        body1 = page.inner_text("body")
        tickers = [t for t in ["2330", "2454", "2408", "2327"] if t in body1]
        a4_ticker = len(tickers) >= 2
        a4_chart = any(k in body1 for k in ["mention", "?®Ëñ¶", "ËßÄÈª?, "?±Â∫¶", "Âª∫Ë≠∞"])
        print(f"[{'PASS' if a4_ticker else 'FAIL'}] A4 tickers in Tab1 {tickers}: {a4_ticker}")
        print(f"[{'PASS' if a4_chart else 'WARN'}] A4 chart content: {a4_chart}")

        # --- Tab 2: ?ÜÊ?Â∏´ÂÄãÂà• ---
        for i in range(tab_count):
            t = tabs.nth(i)
            if "?ÜÊ?Â∏? in t.inner_text():
                t.click()
                time.sleep(2)
                break
        page.wait_for_load_state("networkidle", timeout=15000)
        shot2 = str(SCREENSHOTS / "brokerage_yt_02_tab_analyst.png")
        page.screenshot(path=shot2, full_page=True)
        print(f"[SHOT] {shot2}")
        body2 = page.inner_text("body")
        analysts = [n for n in ["?≠Âì≤Ê¶?, "?â‰???, "?≥Ê?‰ª?] if n in body2]
        a5 = len(analysts) >= 1
        print(f"[{'PASS' if a5 else 'FAIL'}] A5 analysts in Tab2 {analysts}: {a5}")

        # --- Tab 3: ?ãËÇ°?çÊü• ---
        for i in range(tab_count):
            t = tabs.nth(i)
            if "?çÊü•" in t.inner_text() or "?ãËÇ°" in t.inner_text():
                t.click()
                time.sleep(2)
                break
        page.wait_for_load_state("networkidle", timeout=15000)
        # Try to type 2330 in text input
        inp = page.locator('input[type="text"]').first
        try:
            inp.fill("2330")
            inp.press("Enter")
            time.sleep(2)
        except Exception as e:
            print(f"[WARN] Could not fill input: {e}")
        shot3 = str(SCREENSHOTS / "brokerage_yt_03_tab_stock_lookup.png")
        page.screenshot(path=shot3, full_page=True)
        print(f"[SHOT] {shot3}")
        body3 = page.inner_text("body")
        a6 = "2330" in body3
        print(f"[{'PASS' if a6 else 'FAIL'}] A6 2330 in Tab3: {a6}")

        # --- Acceptance 7: no exception ---
        a7 = "Error" not in body and "Traceback" not in body and "Exception" not in body
        print(f"[{'PASS' if a7 else 'FAIL'}] A7 no Python exception: {a7}")

        browser.close()
        return all([a1, a2_title, a3, a4_ticker, a5, a6, a7])


if __name__ == "__main__":
    proc = None
    try:
        proc = start_streamlit()
        ok = run_test()
        print(f"\n[RESULT] {'ALL PASS' if ok else 'SOME FAIL'}")
        sys.exit(0 if ok else 1)
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback; traceback.print_exc()
        sys.exit(2)
    finally:
        if proc:
            proc.terminate()
            proc.wait(timeout=5)
