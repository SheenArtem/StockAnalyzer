"""
對 port 8501 的既有 dev server 做籌碼面周轉率驗證
不啟動新 Streamlit，直接截圖
"""
import sys
import time
from pathlib import Path

SCREENSHOT_DIR = Path(__file__).parent / "screenshots"
SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
BASE_URL = "http://localhost:8501"


def run_test(stock_id="2330"):
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 900})

        print(f"Navigating to {BASE_URL}...")
        page.goto(BASE_URL, timeout=30000)
        page.wait_for_load_state("networkidle", timeout=30000)
        page.screenshot(path=str(SCREENSHOT_DIR / "t01_initial.png"))
        print("  Saved: t01_initial.png")

        # Check version
        version_el = page.get_by_text("v2026.04.22")
        if version_el.count() > 0:
            print(f"  [OK] Version found: {version_el.first.inner_text()}")
        else:
            print("  [WARN] Version element not found")

        # Fill stock input
        input_el = page.locator("input[type='text']").first
        input_el.fill(stock_id)
        print(f"  Filled: {stock_id}")

        # Click analyze
        analyze_btn = page.get_by_text("開始分析")
        if analyze_btn.count() == 0:
            print("  [ERROR] No analyze button found")
            page.screenshot(path=str(SCREENSHOT_DIR / "t_error_no_btn.png"), full_page=True)
            browser.close()
            return

        analyze_btn.first.click()
        print(f"  Clicked 開始分析, waiting for tabs (up to 120s)...")

        try:
            page.wait_for_selector("[role='tab']", timeout=120000)
            print("  [OK] Tabs appeared!")
        except Exception:
            print("  [FAIL] Tabs never appeared within 120s")
            page.screenshot(path=str(SCREENSHOT_DIR / "t_fail_no_tabs.png"), full_page=False)
            browser.close()
            return

        page.wait_for_timeout(2000)
        page.screenshot(path=str(SCREENSHOT_DIR / "t02_after_analyze.png"), full_page=False)
        print("  Saved: t02_after_analyze.png")

        # List all tabs
        tabs = page.get_by_role("tab")
        tab_count = tabs.count()
        print(f"  Tab count: {tab_count}")
        for i in range(tab_count):
            print(f"    Tab[{i}]: {tabs.nth(i).inner_text()}")

        # Click 籌碼面 tab
        chip_tab = page.get_by_role("tab", name="籌碼面")
        if chip_tab.count() == 0:
            # Try partial match
            for i in range(tab_count):
                txt = tabs.nth(i).inner_text()
                if "籌碼" in txt:
                    tabs.nth(i).click()
                    print(f"  Clicked tab[{i}] = '{txt}'")
                    break
            else:
                print("  [FAIL] 籌碼面 tab not found")
                browser.close()
                return
        else:
            chip_tab.first.click()
            print("  Clicked 籌碼面 tab")

        page.wait_for_timeout(3000)
        page.screenshot(path=str(SCREENSHOT_DIR / "t03_chip_tab.png"), full_page=False)
        print("  Saved: t03_chip_tab.png")

        # Scroll down to find ⚡ 當沖週轉概況
        found_section = False
        for scroll_i in range(8):
            section = page.get_by_text("當沖週轉概況")
            if section.count() > 0:
                section.first.scroll_into_view_if_needed()
                page.wait_for_timeout(500)
                page.screenshot(path=str(SCREENSHOT_DIR / f"t04_section_found.png"), full_page=False)
                print(f"  [FOUND] 當沖週轉概況 at scroll step {scroll_i}")
                print(f"  Saved: t04_section_found.png")
                found_section = True
                break
            page.evaluate("window.scrollBy(0, 1500)")
            page.wait_for_timeout(500)

        if not found_section:
            print("  [FAIL] 當沖週轉概況 section not found after scrolling")
            page.screenshot(path=str(SCREENSHOT_DIR / "t04_chip_fullpage.png"), full_page=True)
            print("  Saved: t04_chip_fullpage.png (full page)")

        # Check for 周轉率 text
        to_el = page.get_by_text("周轉率")
        to_count = to_el.count()
        print(f"  '周轉率' element count: {to_count}")

        if to_count > 0:
            to_el.first.scroll_into_view_if_needed()
            page.wait_for_timeout(500)
            page.screenshot(path=str(SCREENSHOT_DIR / "t05_turnover_focus.png"), full_page=False)
            print("  Saved: t05_turnover_focus.png")
            # Get surrounding text for value
            parent_text = to_el.first.evaluate("el => el.closest('[data-testid]')?.innerText || el.parentElement?.innerText || 'N/A'")
            print(f"  周轉率 context: {parent_text!r}")
        else:
            print("  [FAIL] '周轉率' not found in DOM")

        # Check for N/A (shares outstanding missing)
        na_el = page.get_by_text("N/A")
        if na_el.count() > 0:
            print(f"  [WARN] N/A found ({na_el.count()} occurrences) — may include 周轉率=N/A")
        else:
            print("  [OK] No N/A on visible page")

        browser.close()

    print(f"\nScreenshots: {SCREENSHOT_DIR}")
    files = sorted(SCREENSHOT_DIR.glob("t*.png"))
    print(f"Total: {len(files)} screenshots")


if __name__ == "__main__":
    stock = sys.argv[1] if len(sys.argv) > 1 else "2330"
    run_test(stock)
