"""
UI smoke test for Mode D tab (screener mode, 5th tab).
Usage: python tools/ui_test_mode_d.py

Steps:
1. Launch Streamlit on port 8599 (headless)
2. Open Chromium, navigate to app
3. Click app_mode radio -> screener (auto selector)
4. Click "Mode D" tab
5. Screenshot each of the 3 subtabs
6. Kill Streamlit
"""

import subprocess
import sys
import os
import time
import signal
from pathlib import Path

# Force UTF-8 stdout so emoji in print() don't crash on CP950 Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SCREENSHOT_DIR = Path(__file__).parent / "screenshots"
APP_DIR = Path(__file__).parent.parent
PORT = 8599


def run():
    # Clean screenshots
    import shutil
    if SCREENSHOT_DIR.exists():
        shutil.rmtree(SCREENSHOT_DIR)
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    print("Starting Streamlit on port 8599...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app.py",
         "--server.headless", "true",
         "--server.port", str(PORT),
         "--browser.gatherUsageStats", "false"],
        cwd=str(APP_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0,
    )

    results = {}

    try:
        print("Waiting 8s for server...")
        time.sleep(8)

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1400, "height": 900})

            print("Navigating to http://localhost:8599 ...")
            page.goto(f"http://localhost:{PORT}", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=30000)
            time.sleep(2)

            page.screenshot(path=str(SCREENSHOT_DIR / "d00_initial.png"))
            print("  Saved: d00_initial.png")

            # Switch to screener mode: find the radio button group
            # Streamlit radio renders label text; try clicking the screener option
            screener_labels = ["🔍 自動選股", "自動選股"]
            switched = False
            for label in screener_labels:
                btns = page.get_by_text(label, exact=False)
                if btns.count() > 0:
                    btns.first.click()
                    page.wait_for_timeout(3000)
                    page.wait_for_load_state("networkidle", timeout=20000)
                    page.wait_for_timeout(2000)
                    switched = True
                    print(f"  Clicked screener mode via label: {repr(label)}")
                    break

            if not switched:
                print("  WARN: could not find screener mode radio, trying to proceed anyway")

            page.screenshot(path=str(SCREENSHOT_DIR / "d01_screener_mode.png"), full_page=False)
            print("  Saved: d01_screener_mode.png")

            # Click the "Mode D" tab
            mode_d_tab = page.get_by_role("tab", name="🎯 Mode D")
            if mode_d_tab.count() == 0:
                # fallback partial match
                mode_d_tab = page.get_by_text("Mode D", exact=False)

            if mode_d_tab.count() > 0:
                mode_d_tab.first.click()
                page.wait_for_timeout(3000)
                page.wait_for_load_state("networkidle", timeout=20000)
                page.wait_for_timeout(2000)
                print("  Clicked: Mode D tab")
            else:
                print("  ERROR: Mode D tab not found")
                page.screenshot(path=str(SCREENSHOT_DIR / "d_fail_no_mode_d.png"), full_page=True)
                results['Mode D tab'] = 'FAIL - tab not found'
                browser.close()
                return results

            page.screenshot(path=str(SCREENSHOT_DIR / "d02_mode_d_landing.png"), full_page=False)
            print("  Saved: d02_mode_d_landing.png")

            # Check for red error banners on landing
            err_on_landing = _check_error(page)
            if err_on_landing:
                print(f"  ERROR on Mode D landing: {err_on_landing}")
                results['Mode D landing'] = f'FAIL - {err_on_landing}'
            else:
                results['Mode D landing'] = 'PASS'

            # --- Subtab 1: 今日 Pick ---
            sub1 = page.get_by_role("tab", name="📋 今日 Pick")
            if sub1.count() == 0:
                sub1 = page.get_by_text("今日 Pick", exact=False)
            if sub1.count() > 0:
                sub1.first.click()
                page.wait_for_timeout(3000)
                page.screenshot(path=str(SCREENSHOT_DIR / "d03_sub1_today_pick.png"), full_page=False)
                print("  Saved: d03_sub1_today_pick.png")
                err = _check_error(page)
                # Check for dataframe (table) presence
                has_table = page.locator("div[data-testid='stDataFrame']").count() > 0
                if err:
                    results['Sub1 今日Pick'] = f'FAIL - {err}'
                elif not has_table:
                    results['Sub1 今日Pick'] = 'WARN - no dataframe found'
                else:
                    results['Sub1 今日Pick'] = 'PASS - dataframe rendered'
            else:
                results['Sub1 今日Pick'] = 'FAIL - subtab not found'
            print(f"  {results.get('Sub1 今日Pick')}")

            # --- Subtab 2: YT 熱度榜 ---
            sub2 = page.get_by_role("tab", name="📺 YT 熱度榜")
            if sub2.count() == 0:
                sub2 = page.get_by_text("YT 熱度榜", exact=False)
            if sub2.count() > 0:
                sub2.first.click()
                page.wait_for_timeout(3000)
                page.screenshot(path=str(SCREENSHOT_DIR / "d04_sub2_yt_hot.png"), full_page=False)
                print("  Saved: d04_sub2_yt_hot.png")
                err = _check_error(page)
                has_table = page.locator("div[data-testid='stDataFrame']").count() > 0
                if err:
                    results['Sub2 YT熱度榜'] = f'FAIL - {err}'
                elif not has_table:
                    results['Sub2 YT熱度榜'] = 'WARN - no dataframe found'
                else:
                    results['Sub2 YT熱度榜'] = 'PASS - dataframe rendered'
            else:
                results['Sub2 YT熱度榜'] = 'FAIL - subtab not found'
            print(f"  {results.get('Sub2 YT熱度榜')}")

            # --- Subtab 3: C1 拐點清單 ---
            sub3 = page.get_by_role("tab", name="📈 C1 拐點清單")
            if sub3.count() == 0:
                sub3 = page.get_by_text("C1 拐點清單", exact=False)
            if sub3.count() > 0:
                sub3.first.click()
                page.wait_for_timeout(3000)
                page.screenshot(path=str(SCREENSHOT_DIR / "d05_sub3_c1_tilt.png"), full_page=False)
                print("  Saved: d05_sub3_c1_tilt.png")
                err = _check_error(page)
                has_table = page.locator("div[data-testid='stDataFrame']").count() > 0
                if err:
                    results['Sub3 C1拐點'] = f'FAIL - {err}'
                elif not has_table:
                    results['Sub3 C1拐點'] = 'WARN - no dataframe found'
                else:
                    results['Sub3 C1拐點'] = 'PASS - dataframe rendered'
            else:
                results['Sub3 C1拐點'] = 'FAIL - subtab not found'
            print(f"  {results.get('Sub3 C1拐點')}")

            # --- Subtab 4: Thesis Panel ---
            sub4 = page.get_by_role("tab", name="🎯 Thesis Panel")
            if sub4.count() == 0:
                sub4 = page.get_by_text("Thesis Panel", exact=False)
            if sub4.count() > 0:
                sub4.first.click()
                page.wait_for_timeout(3000)
                page.screenshot(path=str(SCREENSHOT_DIR / "d06_sub4_thesis_panel.png"), full_page=True)
                print("  Saved: d06_sub4_thesis_panel.png")
                err = _check_error(page)
                # Thesis Panel has 3 st.info placeholders; check at least one info box
                has_info = page.locator("div[data-testid='stAlert']").count() > 0
                if err:
                    results['Sub4 Thesis Panel'] = f'FAIL - {err}'
                elif not has_info:
                    results['Sub4 Thesis Panel'] = 'WARN - no info/placeholder boxes found'
                else:
                    info_count = page.locator("div[data-testid='stAlert']").count()
                    results['Sub4 Thesis Panel'] = f'PASS - {info_count} info box(es) rendered'
            else:
                results['Sub4 Thesis Panel'] = 'FAIL - subtab not found'
            print(f"  {results.get('Sub4 Thesis Panel')}")

            # Full page final shot
            page.screenshot(path=str(SCREENSHOT_DIR / "d07_final_full.png"), full_page=True)
            print("  Saved: d07_final_full.png")

            browser.close()

    finally:
        print("Stopping Streamlit...")
        if os.name == 'nt':
            proc.send_signal(signal.CTRL_BREAK_EVENT)
            time.sleep(1)
            proc.kill()
        else:
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            pass
        print("Done.")

    return results


def _check_error(page):
    """Return first error text found on page, or None."""
    # Streamlit exception box
    exc = page.locator("div[data-testid='stException']")
    if exc.count() > 0:
        return exc.first.inner_text()[:200]
    # Red alert / error
    err_alert = page.locator("div[data-testid='stAlert'][data-baseweb='notification']")
    for i in range(err_alert.count()):
        el = err_alert.nth(i)
        kind = el.get_attribute("kind") or ""
        if "error" in kind.lower():
            return el.inner_text()[:200]
    return None


if __name__ == "__main__":
    results = run()
    print("\n===== RESULT SUMMARY =====")
    all_pass = True
    for k, v in results.items():
        status = "PASS" if v.startswith("PASS") else ("WARN" if v.startswith("WARN") else "FAIL")
        if status != "PASS":
            all_pass = False
        print(f"[{status}] {k}: {v}")
    print("==========================")
    if not all_pass:
        sys.exit(1)
