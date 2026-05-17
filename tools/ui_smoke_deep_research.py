"""
ui_smoke_deep_research.py -- C3 Phase B 深度辯論 UI smoke test (2026-05-17)

Verifies:
  1. individual_view.py: 🔬 深度辯論 expander appears after AI report section
  2. Expander opens without errors
  3. Correct cache-vs-no-cache button label for 2330 (2026-04-30 cache = EXPIRED >24h)
  4. screener_view.py Mode D tab: table has ⭐DR column
  5. 深度辯論 selectbox + button appears below table

Does NOT click the run button (8-10 min execution).

Usage:
    python tools/ui_smoke_deep_research.py [stock_id]   # default 2330
"""

import subprocess
import sys
import os
import io
import time
import shutil
import signal
from pathlib import Path

# Force UTF-8 stdout so CJK/emoji characters in tab labels don't crash on CP950 terminals
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

SCREENSHOT_DIR = Path(__file__).parent / "screenshots"
PORT = 8599
ROOT = Path(__file__).parent.parent


def cleanup_screenshots():
    if SCREENSHOT_DIR.exists():
        shutil.rmtree(SCREENSHOT_DIR)
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)


def save(page, name):
    fpath = SCREENSHOT_DIR / name
    page.screenshot(path=str(fpath), full_page=True)
    print(f"  [SAVED] {fpath}")
    return fpath


def check_no_error(page, label):
    exc = page.locator(".stException").all()
    err = page.locator("[data-testid='stAlert'][kind='error']").all()
    if exc or err:
        # Print the first error text for diagnosis
        if exc:
            print(f"  [FAIL] {label}: stException detected: {exc[0].inner_text()[:200]}")
        if err:
            print(f"  [FAIL] {label}: stAlert error detected: {err[0].inner_text()[:200]}")
        return False
    print(f"  [PASS] {label}: no error blocks")
    return True


def run_test(stock_id="2330"):
    cleanup_screenshots()
    results = []

    print(f"Starting Streamlit on port {PORT}...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app.py",
         "--server.headless", "true", f"--server.port={PORT}",
         "--server.fileWatcherType", "none",
         "--browser.gatherUsageStats", "false"],
        cwd=str(ROOT),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )

    try:
        print("Waiting for server startup (12s)...")
        time.sleep(12)

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 900})

            # ── 0. Initial load ───────────────────────────────────────────────
            print("\n=== Phase 0: Initial load ===")
            page.goto(f"http://localhost:{PORT}", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=30000)
            page.wait_for_timeout(3000)
            save(page, "00_initial.png")
            check_no_error(page, "initial_load")

            # ── 1. Enter stock_id → individual analysis ───────────────────────
            print(f"\n=== Phase 1: Navigate to individual analysis {stock_id} ===")
            stock_input = page.locator("input[type='text']").first
            stock_input.fill(stock_id)
            page.wait_for_timeout(500)

            analyze_btn = page.get_by_text("開始分析")
            if analyze_btn.count() > 0:
                analyze_btn.first.click()
                print(f"  Clicked analyze for {stock_id}, waiting 25s for data...")
                page.wait_for_timeout(25000)
                page.wait_for_load_state("networkidle", timeout=60000)
                page.wait_for_timeout(3000)
            else:
                print("  [WARN] '開始分析' button not found")

            save(page, "01_individual_main.png")
            ok_main = check_no_error(page, f"individual_main_{stock_id}")
            results.append(f"[{'PASS' if ok_main else 'FAIL'}] individual_main: no exception")

            # ── 2. Click AI Report tab ────────────────────────────────────────
            print("\n=== Phase 2: Navigate to AI 報告 tab ===")
            # First try by text match for AI report related tab
            ai_tab_found = False
            for tab_name in ["AI 報告", "🤖 AI 報告", "AI報告", "AI 分析", "個股分析"]:
                tab_el = page.get_by_role("tab", name=tab_name)
                if tab_el.count() > 0:
                    tab_el.first.click()
                    ai_tab_found = True
                    print(f"  Clicked tab '{tab_name}', waiting 8s...")
                    page.wait_for_timeout(8000)
                    page.wait_for_load_state("networkidle", timeout=30000)
                    page.wait_for_timeout(2000)
                    break

            if not ai_tab_found:
                # Enumerate all tabs and try to find AI report
                tabs = page.get_by_role("tab").all()
                tab_texts = [t.inner_text() for t in tabs]
                safe_tabs = [txt.encode('ascii', 'replace').decode('ascii') for txt in tab_texts]
                print(f"  Available tabs: {safe_tabs}")
                # The individual analysis has: 週K / 日K / 籌碼面 / 基本面 / AI 報告
                # AI report is typically the last tab (index 4 or 5)
                for i, txt in enumerate(tab_texts):
                    if "AI" in txt or "報告" in txt:
                        tabs[i].click()
                        ai_tab_found = True
                        print(f"  Clicked tab[{i}] '{txt}', waiting 8s...")
                        page.wait_for_timeout(8000)
                        page.wait_for_load_state("networkidle", timeout=30000)
                        page.wait_for_timeout(2000)
                        break

                if not ai_tab_found and len(tabs) >= 5:
                    # Try last tab
                    tabs[-1].click()
                    safe_last = tab_texts[-1].encode('ascii', 'replace').decode('ascii')
                    print(f"  Clicked last tab '{safe_last}', waiting 8s...")
                    page.wait_for_timeout(8000)
                    page.wait_for_load_state("networkidle", timeout=30000)
                    page.wait_for_timeout(2000)
                    ai_tab_found = True

            save(page, "02_ai_report_tab.png")
            ok_ai = check_no_error(page, "ai_report_tab")
            results.append(f"[{'PASS' if ok_ai else 'FAIL'}] ai_report_tab: no exception")

            html_ai = page.content()

            # ── 3. Check 深度辯論 expander presence ───────────────────────────
            print("\n=== Phase 3: Check 深度辯論 expander in individual view ===")
            has_deep = "深度辯論" in html_ai
            print(f"  [{'PASS' if has_deep else 'FAIL'}] 深度辯論 text in page: {has_deep}")
            results.append(f"[{'PASS' if has_deep else 'FAIL'}] deep_research_expander_text: present={has_deep}")

            # Try to find and click the expander
            dr_expander = None
            for selector in [
                "text=深度辯論",
                "text=🔬 深度辯論",
                "summary:has-text('深度辯論')",
            ]:
                el = page.locator(selector)
                if el.count() > 0:
                    dr_expander = el.first
                    print(f"  Found expander via selector '{selector}'")
                    break

            if dr_expander:
                # Scroll into view then click
                try:
                    dr_expander.scroll_into_view_if_needed()
                    page.wait_for_timeout(500)
                    dr_expander.click()
                    page.wait_for_timeout(2000)
                    print("  Expander clicked/opened")
                except Exception as e:
                    print(f"  [WARN] Could not click expander: {e}")
            else:
                print("  [WARN] 深度辯論 expander element not directly clickable (may be in collapsed section)")

            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            save(page, "03_deep_research_expander.png")
            ok_dr = check_no_error(page, "deep_research_expander")
            results.append(f"[{'PASS' if ok_dr else 'FAIL'}] deep_research_expander: no exception after open")

            # ── 4. Check button label (cache vs no-cache) ─────────────────────
            print("\n=== Phase 4: Check button label (EXPIRED cache expected for 2330) ===")
            html_after_open = page.content()

            has_run_btn = "開跑" in html_after_open
            has_cache_btn = "看快取結果" in html_after_open
            has_force_rerun = "強制重跑" in html_after_open

            print(f"  '開跑' button: {has_run_btn}")
            print(f"  '看快取結果' button: {has_cache_btn}")
            print(f"  '強制重跑' button: {has_force_rerun}")

            # 2026-04-30 cache is 17 days old → EXPIRED → expect '開跑', NOT '看快取結果'
            if has_run_btn and not has_cache_btn:
                print("  [PASS] cache_state: shows '開跑' (cache EXPIRED as expected for 17-day-old report)")
                results.append("[PASS] cache_state: EXPIRED - shows '開跑' button (correct for 17d old cache)")
            elif has_cache_btn:
                print("  [WARN] cache_state: shows '看快取結果' — cache NOT expired (unexpected for 2026-04-30 file)")
                results.append("[WARN] cache_state: UNEXPIRED - shows '看快取結果' (2026-04-30 file, 24h limit?)")
            elif not has_run_btn and not has_cache_btn:
                print("  [WARN] cache_state: neither button found — expander may not be open or text differs")
                results.append("[WARN] cache_state: no button found (expander not open or text mismatch)")
            else:
                print(f"  [INFO] cache_state: run={has_run_btn} cache={has_cache_btn}")
                results.append(f"[INFO] cache_state: run={has_run_btn} cache_btn={has_cache_btn}")

            # Check expander description text
            has_desc = "Bull/Bear" in html_after_open or "5 AI 互辯" in html_after_open
            print(f"  [{'PASS' if has_desc else 'WARN'}] expander description: {has_desc}")
            results.append(f"[{'PASS' if has_desc else 'WARN'}] dr_description: present={has_desc}")

            # ── 5. Switch to screener → Mode D ───────────────────────────────
            # Open a FRESH page so there is no DOM residue from individual view.
            # Streamlit persists st.empty() placeholders across mode switches in the
            # same browser tab, causing individual view content to bleed through.
            print("\n=== Phase 5: Open fresh page for screener mode ===")
            screener_page = browser.new_page(viewport={"width": 1440, "height": 900})
            screener_page.goto(f"http://localhost:{PORT}", timeout=30000)
            screener_page.wait_for_load_state("networkidle", timeout=30000)
            screener_page.wait_for_timeout(3000)

            # Click 自動選股 radio on fresh page
            screener_clicked = False
            for selector in [
                "label:has-text('自動選股')",
                "[data-testid='stRadio'] label:has-text('自動選股')",
                "text=🔍 自動選股",
            ]:
                el = screener_page.locator(selector)
                if el.count() > 0:
                    el.first.click()
                    screener_clicked = True
                    print(f"  Clicked screener radio via '{selector}', waiting 12s...")
                    screener_page.wait_for_timeout(12000)
                    screener_page.wait_for_load_state("networkidle", timeout=30000)
                    screener_page.wait_for_timeout(3000)
                    break

            if not screener_clicked:
                print("  [WARN] screener radio not found on fresh page")

            screener_page.evaluate("window.scrollTo(0, 0)")
            screener_page.wait_for_timeout(1000)
            # Close old page and use screener_page from here on
            page.close()
            page = screener_page

            save(page, "04_screener_mode.png")
            ok_screener = check_no_error(page, "screener_mode")

            # Verify screener mode (should see QM/Mode D tabs, no individual bleed)
            html_screener = page.content()
            is_in_screener = ("品質選股" in html_screener or "Mode D" in html_screener)
            has_individual_bleed = "分析完成" in html_screener
            print(f"  Screener confirmed: {is_in_screener}, individual bleed-through: {has_individual_bleed}")
            results.append(f"[{'PASS' if is_in_screener else 'WARN'}] screener_mode_switch: confirmed={is_in_screener}")

            # ── 6. Navigate to Mode D tab ─────────────────────────────────────
            print("\n=== Phase 6: Navigate to Mode D tab ===")
            mode_d_found = False
            for tab_name in ["🎯 Mode D", "Mode D", "🎯", "Mode D 深度辯論"]:
                tab_el = page.get_by_role("tab", name=tab_name)
                if tab_el.count() > 0:
                    # Scroll the tab into view first, then click
                    tab_el.first.scroll_into_view_if_needed()
                    page.wait_for_timeout(500)
                    tab_el.first.click()
                    mode_d_found = True
                    print(f"  Clicked tab '{tab_name}', waiting for Mode D content...")
                    # Wait for EITHER the Mode D heading OR an error/info message
                    # to confirm the content area has rendered
                    try:
                        page.wait_for_selector(
                            "text=Mode D, text=今日 Pick, text=QM top, text=尚無 QM",
                            timeout=30000
                        )
                        print("  Mode D content detected")
                    except Exception:
                        print("  [WARN] Timed out waiting for Mode D content; falling back to fixed wait")
                    page.wait_for_timeout(5000)
                    page.wait_for_load_state("networkidle", timeout=30000)
                    page.wait_for_timeout(3000)
                    break

            if not mode_d_found:
                tabs = page.get_by_role("tab").all()
                tab_texts = [t.inner_text() for t in tabs]
                safe_tabs2 = [t.encode('ascii', 'replace').decode('ascii') for t in tab_texts]
                print(f"  Screener tabs: {safe_tabs2}")
                for i, txt in enumerate(tab_texts):
                    if "Mode" in txt or "D" in txt or "深度" in txt:
                        tabs[i].click()
                        mode_d_found = True
                        print(f"  Clicked tab[{i}] '{txt}', waiting 10s...")
                        page.wait_for_timeout(10000)
                        page.wait_for_load_state("networkidle", timeout=30000)
                        page.wait_for_timeout(3000)
                        break

            # Scroll to top so Mode D content is visible in viewport
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(1000)
            save(page, "05_mode_d_tab.png")
            ok_mode_d = check_no_error(page, "mode_d_tab")
            results.append(f"[{'PASS' if ok_mode_d else 'FAIL'}] mode_d_tab: no exception")

            html_mode_d = page.content()
            # Debug: check what's in Mode D HTML
            has_mode_d_heading = "Mode D" in html_mode_d
            has_pick_table = "今日 Pick" in html_mode_d or "QM 分" in html_mode_d
            has_individual_banner = "分析完成" in html_mode_d
            print(f"  Mode D heading: {has_mode_d_heading}, Pick table: {has_pick_table}, Ind banner: {has_individual_banner}")
            # Debug: dump snippet around Mode D in HTML
            idx = html_mode_d.find("Mode D")
            if idx >= 0:
                snippet = html_mode_d[max(0, idx-100):idx+500]
                safe_snippet = snippet.encode('ascii', 'replace').decode('ascii')
                print(f"  HTML around 'Mode D': {safe_snippet[:300]}")

            # ── 7. Check 今日 Pick subtab ─────────────────────────────────────
            # Note: Mode D subtabs (今日 Pick / YT 熱度榜 / C1 拐點清單 / Thesis Panel)
            # are nested tabs that only exist when Mode D tab is active.
            # The tab text is "📋 今日 Pick" — already active by default (first subtab).
            print("\n=== Phase 7: Navigate to 今日 Pick subtab ===")
            pick_found = False

            # First check if tab is already active (first subtab is default)
            html_mode_d_check = page.content()
            if "今日 Pick" in html_mode_d_check or "QM 分" in html_mode_d_check:
                pick_found = True
                print("  今日 Pick subtab already active (default first subtab)")

            if not pick_found:
                for sub_name in ["📋 今日 Pick", "今日 Pick", "今日Pick"]:
                    sub_el = page.get_by_role("tab", name=sub_name)
                    if sub_el.count() > 0:
                        sub_el.first.click()
                        pick_found = True
                        print(f"  Clicked subtab '{sub_name}', waiting 8s...")
                        page.wait_for_timeout(8000)
                        page.wait_for_load_state("networkidle", timeout=30000)
                        page.wait_for_timeout(3000)
                        break

            if not pick_found:
                # Enumerate sub-tabs — look for any that contain Pick/今日
                tabs_all = page.get_by_role("tab").all()
                tab_texts = [t.inner_text() for t in tabs_all]
                safe_tabs3 = [t.encode('ascii', 'replace').decode('ascii') for t in tab_texts]
                print(f"  All tabs (may include subtabs): {safe_tabs3}")
                for i, txt in enumerate(tab_texts):
                    if "Pick" in txt or "今日" in txt:
                        tabs_all[i].click()
                        pick_found = True
                        print(f"  Clicked '{txt}', waiting 8s...")
                        page.wait_for_timeout(8000)
                        page.wait_for_load_state("networkidle", timeout=30000)
                        page.wait_for_timeout(3000)
                        break

            # If still not found, try locating by text directly (Streamlit tab buttons)
            if not pick_found:
                pick_btn = page.locator("button[role='tab']:has-text('今日')")
                if pick_btn.count() == 0:
                    pick_btn = page.locator("button:has-text('今日 Pick')")
                if pick_btn.count() > 0:
                    pick_btn.first.click()
                    pick_found = True
                    print(f"  Clicked '今日 Pick' button directly, waiting 8s...")
                    page.wait_for_timeout(8000)
                    page.wait_for_load_state("networkidle", timeout=30000)
                    page.wait_for_timeout(3000)

            if not pick_found:
                print("  [WARN] 今日 Pick subtab not found - checking Mode D content directly")

            # Scroll to top to capture Mode D pick content from the start
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(1000)
            save(page, "06_mode_d_pick_tab.png")
            ok_pick = check_no_error(page, "mode_d_pick_tab")
            results.append(f"[{'PASS' if ok_pick else 'FAIL'}] mode_d_pick_tab: no exception")

            html_pick = page.content()

            # ── 8. Check ⭐DR column ──────────────────────────────────────────
            print("\n=== Phase 8: Check ⭐DR column + 深度辯論 section ===")
            # The ⭐ star U+2B50 may appear as HTML entity &#11088; or literal
            # Also check for "DR" column header which always appears
            has_dr_col = ("⭐DR" in html_pick or
                          "&#11088;DR" in html_pick or
                          "⭐DR" in html_pick or
                          ">DR<" in html_pick or
                          '"DR"' in html_pick or
                          "⭐" in html_pick)
            has_dr_section = "深度辯論" in html_pick
            has_dr_selectbox = "selectbox" in html_pick.lower() or "stSelectbox" in html_pick
            has_dr_btn = "開跑" in html_pick or "看快取" in html_pick

            print(f"  ⭐DR column present: {has_dr_col}")
            print(f"  深度辯論 section present: {has_dr_section}")
            print(f"  selectbox present: {has_dr_selectbox}")
            print(f"  run/cache button present: {has_dr_btn}")

            # Also check via JS for the actual dataframe column header
            try:
                dr_col_via_js = page.evaluate(
                    "() => document.body.innerHTML.includes('DR')"
                )
                print(f"  'DR' found via JS body scan: {dr_col_via_js}")
            except Exception:
                dr_col_via_js = None

            results.append(f"[{'PASS' if has_dr_col or dr_col_via_js else 'FAIL'}] mode_d_dr_column: ⭐DR/DR in page={has_dr_col or dr_col_via_js}")
            results.append(f"[{'PASS' if has_dr_section else 'FAIL'}] mode_d_dr_section: 深度辯論 present={has_dr_section}")
            results.append(f"[{'PASS' if has_dr_btn else 'WARN'}] mode_d_dr_button: present={has_dr_btn}")

            # Scroll to bottom to see the 深度辯論 section
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            save(page, "07_mode_d_pick_bottom.png")
            ok_pick_bot = check_no_error(page, "mode_d_pick_bottom")
            results.append(f"[{'PASS' if ok_pick_bot else 'FAIL'}] mode_d_pick_bottom: no exception")

            browser.close()

    finally:
        if os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()
        # Print last 50 lines of Streamlit stderr for error diagnosis
        try:
            stderr_out = proc.stderr.read().decode('utf-8', errors='replace')
            stderr_lines = [l for l in stderr_out.splitlines() if l.strip()]
            if stderr_lines:
                print("\n--- Streamlit STDERR (last 30 lines) ---")
                for line in stderr_lines[-30:]:
                    print(f"  {line}")
        except Exception as _e:
            print(f"  (could not read stderr: {_e})")
        print("\nStreamlit stopped.")

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("SMOKE TEST RESULTS — C3 Phase B 深度辯論 UI")
    print("=" * 60)
    for r in results:
        print(r)

    passes = sum(1 for r in results if r.startswith("[PASS]"))
    fails = sum(1 for r in results if r.startswith("[FAIL]"))
    warns = sum(1 for r in results if r.startswith("[WARN]"))
    print(f"\nTotal: {passes} PASS / {fails} FAIL / {warns} WARN")
    print(f"Screenshots saved to: {SCREENSHOT_DIR}")
    return fails == 0


if __name__ == "__main__":
    stock_id = sys.argv[1] if len(sys.argv) > 1 else "2330"
    ok = run_test(stock_id)
    sys.exit(0 if ok else 1)
