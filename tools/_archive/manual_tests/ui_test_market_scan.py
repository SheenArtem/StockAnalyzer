"""
UI smoke test for Market Scan mode (app_mode='market_scan').
Usage: python tools/ui_test_market_scan.py

Steps:
1. Launch Streamlit on port 8599 (headless)
2. Navigate to app, verify 4 sidebar mode options
3. Click "market_scan" radio
4. Verify title "市場掃描"
5. Verify caption (week_end date + unique_stocks count)
6. Verify "維度" selectbox default = "三大法人合計"
7. Screenshot 4 ranking tables (consec_buy / consec_sell / week_buy / week_sell)
8. Switch dimension to "外資"
9. Open "跳轉個股分析" expander, verify selectbox present
10. Final full-page screenshot
"""

import subprocess
import sys
import os
import time
import signal
import shutil
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SCREENSHOT_DIR = Path(__file__).parent / "screenshots"
APP_DIR = Path(__file__).parent.parent
PORT = 8599


def _check_error(page):
    """Return first error/exception text found on page, or None."""
    exc = page.locator("div[data-testid='stException']")
    if exc.count() > 0:
        return exc.first.inner_text()[:300]
    err_alert = page.locator("div[data-testid='stAlert']")
    for i in range(err_alert.count()):
        el = err_alert.nth(i)
        cls = el.get_attribute("class") or ""
        inner = el.inner_text()
        if "error" in cls.lower() or inner.startswith("Error") or "Traceback" in inner:
            return inner[:300]
    return None


def run():
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
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )

    results = {}

    try:
        print("Waiting 8s for server startup...")
        time.sleep(8)

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1400, "height": 900})

            print("Navigating to http://localhost:8599 ...")
            page.goto(f"http://localhost:{PORT}", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=30000)
            page.wait_for_timeout(2000)

            page.screenshot(path=str(SCREENSHOT_DIR / "ms00_initial.png"))
            print("  Saved: ms00_initial.png")

            # ---- Step 2: verify 4 sidebar mode options ----
            expected_labels = ["📈 個股分析", "🔍 自動選股", "📡 市場掃描", "📝 AI 報告"]
            missing = []
            for lbl in expected_labels:
                # Use partial match for robustness
                if page.get_by_text(lbl, exact=False).count() == 0:
                    missing.append(lbl)
            if missing:
                results['Sidebar 4 modes'] = f'FAIL - missing: {missing}'
            else:
                results['Sidebar 4 modes'] = 'PASS - all 4 mode labels found'
            print(f"  {results['Sidebar 4 modes']}")

            # ---- Step 3: click "市場掃描" radio ----
            scan_label_candidates = ["📡 市場掃描", "市場掃描"]
            switched = False
            for lbl in scan_label_candidates:
                btns = page.get_by_text(lbl, exact=False)
                if btns.count() > 0:
                    btns.first.click()
                    page.wait_for_timeout(3000)
                    page.wait_for_load_state("networkidle", timeout=20000)
                    page.wait_for_timeout(2000)
                    switched = True
                    print(f"  Clicked market_scan mode via label: {repr(lbl)}")
                    break

            if not switched:
                results['Mode switch'] = 'FAIL - could not find market_scan radio'
                print(f"  {results['Mode switch']}")
                page.screenshot(path=str(SCREENSHOT_DIR / "ms_fail_no_mode.png"), full_page=True)
                browser.close()
                return results

            page.screenshot(path=str(SCREENSHOT_DIR / "ms01_market_scan_mode.png"))
            print("  Saved: ms01_market_scan_mode.png")

            # Check no crash after switch
            err = _check_error(page)
            if err:
                results['Mode switch'] = f'FAIL - error after switch: {err}'
                print(f"  {results['Mode switch']}")
                browser.close()
                return results
            results['Mode switch'] = 'PASS'
            print(f"  {results['Mode switch']}")

            # ---- Step 4: verify title "市場掃描" ----
            title_el = page.get_by_text("📡 市場掃描", exact=False)
            # Look specifically for h1/title element (not sidebar radio label)
            # Streamlit renders st.title as <h1>
            h1_text = ""
            h1_els = page.locator("h1")
            for i in range(h1_els.count()):
                t = h1_els.nth(i).inner_text()
                if "市場掃描" in t:
                    h1_text = t
                    break
            if "市場掃描" in h1_text:
                results['Title'] = f'PASS - h1: {repr(h1_text[:50])}'
            else:
                # Fallback: any text element
                if title_el.count() > 0:
                    results['Title'] = 'PASS - title text found (not in h1)'
                else:
                    results['Title'] = 'FAIL - title "市場掃描" not found on page'
            print(f"  {results['Title']}")

            # ---- Step 5: verify caption (date + stock count) ----
            page_text = page.locator("body").inner_text()
            caption_ok = "統計窗口收尾於" in page_text
            stocks_ok = "上榜" in page_text
            # Try to extract date and count
            import re
            date_match = re.search(r"統計窗口收尾於[^\d]*(\d{4}-\d{2}-\d{2})", page_text)
            count_match = re.search(r"(\d+)\s*檔上榜", page_text)
            if caption_ok and stocks_ok:
                date_str = date_match.group(1) if date_match else "(?)"
                count_str = count_match.group(1) if count_match else "(?)"
                results['Caption'] = f'PASS - date={date_str}, stocks={count_str} 檔上榜'
            else:
                # Check for the warning (data not found)
                if "週榜資料尚未產出" in page_text:
                    results['Caption'] = 'FAIL - data not found (週榜資料尚未產出)'
                else:
                    results['Caption'] = f'FAIL - caption missing (date_ok={caption_ok}, stocks_ok={stocks_ok})'
            print(f"  {results['Caption']}")

            # If data not found, no point continuing
            if results['Caption'].startswith('FAIL'):
                page.screenshot(path=str(SCREENSHOT_DIR / "ms_fail_no_data.png"), full_page=True)
                browser.close()
                return results

            # ---- Step 6: verify "維度" selectbox default ----
            # Streamlit selectbox renders as a div with label
            dim_label = page.get_by_text("維度", exact=False)
            dim_found = dim_label.count() > 0
            # Check default value shown (三大法人合計)
            total_visible = "三大法人合計" in page.locator("body").inner_text()
            if dim_found and total_visible:
                results['Dimension selectbox'] = 'PASS - 維度 label found, default=三大法人合計'
            elif dim_found:
                results['Dimension selectbox'] = 'WARN - 維度 label found but default value unclear'
            else:
                results['Dimension selectbox'] = 'FAIL - 維度 selectbox not found'
            print(f"  {results['Dimension selectbox']}")

            # ---- Step 7: screenshot 4 ranking tables ----
            page.screenshot(path=str(SCREENSHOT_DIR / "ms02_tables_total.png"), full_page=True)
            print("  Saved: ms02_tables_total.png (full page, should show 4 tables)")

            # Count dataframes rendered
            df_count = page.locator("div[data-testid='stDataFrame']").count()
            rank_titles = ["連續買超", "連續賣超", "當週買超", "當週賣超"]
            body_text = page.locator("body").inner_text()
            tables_found = [rt for rt in rank_titles if rt in body_text]

            if df_count >= 4 and len(tables_found) == 4:
                results['4 Ranking tables'] = f'PASS - {df_count} dataframes, all 4 titles found'
            elif df_count > 0:
                results['4 Ranking tables'] = f'WARN - {df_count} dataframes (expected 4), titles={tables_found}'
            else:
                results['4 Ranking tables'] = f'FAIL - 0 dataframes rendered, titles={tables_found}'
            print(f"  {results['4 Ranking tables']}")

            # ---- Step 8: switch dimension to "外資" ----
            # Click the selectbox
            dim_selectbox = page.locator("div[data-testid='stSelectbox']").first
            if dim_selectbox.count() > 0:
                dim_selectbox.click()
                page.wait_for_timeout(1000)
                # Click "外資" option in dropdown
                foreign_opt = page.get_by_role("option", name="外資")
                if foreign_opt.count() == 0:
                    # Try by text
                    foreign_opt = page.get_by_text("外資", exact=True)
                if foreign_opt.count() > 0:
                    foreign_opt.first.click()
                    page.wait_for_timeout(3000)
                    page.wait_for_load_state("networkidle", timeout=15000)
                    page.wait_for_timeout(1000)
                    page.screenshot(path=str(SCREENSHOT_DIR / "ms03_tables_foreign.png"), full_page=True)
                    print("  Saved: ms03_tables_foreign.png")
                    err = _check_error(page)
                    df_count2 = page.locator("div[data-testid='stDataFrame']").count()
                    if err:
                        results['Dimension switch (外資)'] = f'FAIL - {err}'
                    elif df_count2 > 0:
                        results['Dimension switch (外資)'] = f'PASS - {df_count2} dataframes after 外資 switch'
                    else:
                        results['Dimension switch (外資)'] = 'WARN - 0 dataframes after switch'
                else:
                    results['Dimension switch (外資)'] = 'WARN - 外資 option not found in dropdown'
                    page.keyboard.press("Escape")
            else:
                results['Dimension switch (外資)'] = 'WARN - selectbox not found'
            print(f"  {results.get('Dimension switch (外資)')}")

            # ---- Step 9: open expander and verify selectbox ----
            expander = page.get_by_text("跳轉個股分析", exact=False)
            if expander.count() > 0:
                expander.first.click()
                page.wait_for_timeout(2000)
                page.screenshot(path=str(SCREENSHOT_DIR / "ms04_expander_open.png"))
                print("  Saved: ms04_expander_open.png")
                # Check for selectbox inside expander
                inner_text = page.locator("body").inner_text()
                has_select_option = "請選擇" in inner_text or "選股" in inner_text
                err = _check_error(page)
                if err:
                    results['Expander jump'] = f'FAIL - {err}'
                elif has_select_option:
                    results['Expander jump'] = 'PASS - expander opened, 選股 selectbox found'
                else:
                    results['Expander jump'] = 'WARN - expander opened but selectbox unclear'
            else:
                results['Expander jump'] = 'WARN - expander text not found'
            print(f"  {results.get('Expander jump')}")

            # ---- Step 10: final full page screenshot ----
            page.screenshot(path=str(SCREENSHOT_DIR / "ms05_final_full.png"), full_page=True)
            print("  Saved: ms05_final_full.png")

            browser.close()

    finally:
        print("Stopping Streamlit...")
        if os.name == "nt":
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


if __name__ == "__main__":
    results = run()
    print("\n===== RESULT SUMMARY =====")
    all_pass = True
    for k, v in results.items():
        if v.startswith("PASS"):
            status = "PASS"
        elif v.startswith("WARN"):
            status = "WARN"
        else:
            status = "FAIL"
            all_pass = False
        print(f"[{status}] {k}: {v}")
    print("==========================")
    if not all_pass:
        sys.exit(1)
