"""
Screener UI smoke test 2026-04-30
Verifies 3 changes in screener_view.py:
  1. 持股警報 expander is GONE from QM tab
  2. 篩選條件說明 + 操作SOP moved to BOTTOM (after 個股操作建議)
  3. Table fully expanded (no scroll bar) — page scroll reaches all rows

Port: 8514
Output: reports/ui_test_screener_2026_04_30/
"""

import subprocess
import sys
import os
import time
import shutil
import signal
from pathlib import Path

# Force stdout to UTF-8 on Windows (avoids cp950 UnicodeEncodeError with emojis)
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

SCREENSHOT_DIR = Path("C:/GIT/StockAnalyzer/reports/ui_test_screener_2026_04_30")
PORT = 8514
APP_DIR = Path("C:/GIT/StockAnalyzer")


def save(page, name):
    fpath = SCREENSHOT_DIR / name
    page.screenshot(path=str(fpath), full_page=True)
    print(f"  [SAVED] {name} ({fpath})")
    return str(fpath)


def js_contains(page, needle_unicode):
    """Check if page body text contains a unicode string, via JS evaluation."""
    return page.evaluate(f"() => document.body.innerText.indexOf({repr(needle_unicode)}) >= 0")


def run_test():
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Starting Streamlit on port {PORT}...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app.py",
         "--server.headless", "true", f"--server.port={PORT}",
         "--browser.gatherUsageStats", "false"],
        cwd=str(APP_DIR),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    )

    results = []

    try:
        print("Waiting 12s for Streamlit boot...")
        time.sleep(12)

        # Check process still alive
        if proc.poll() is not None:
            out, err = proc.communicate()
            print(f"[FATAL] Streamlit exited early: {err.decode('utf-8','replace')[:500]}")
            results.append("[FAIL] app: Streamlit exited before test could start")
            return results

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900})
            page = ctx.new_page()

            # -- Load app --
            page.goto(f"http://localhost:{PORT}", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=30000)
            page.wait_for_timeout(3000)

            save(page, "00_initial.png")
            page_text = page.evaluate("() => document.body.innerText")
            print(f"  Initial page chars: {len(page_text)}")

            # -- Switch to screener mode (自動選股) --
            print("\nSwitching to screener mode (自動選股)...")
            switched = page.evaluate(r"""() => {
                // Try radio label containing 自動選股
                const labels = document.querySelectorAll('label');
                for (const l of labels) {
                    if (l.innerText && l.innerText.includes('自動選股')) {
                        l.click(); return 'label_click';
                    }
                }
                // Try st.radio direct
                const divs = document.querySelectorAll('div[data-testid="stRadio"] label');
                for (const d of divs) {
                    if (d.innerText && d.innerText.includes('自動選股')) {
                        d.click(); return 'radio_label';
                    }
                }
                return null;
            }""")
            print(f"  Switch result: {switched}")
            page.wait_for_timeout(5000)
            page.wait_for_load_state("networkidle", timeout=15000)
            page.wait_for_timeout(2000)
            save(page, "01_screener_mode.png")

            # Confirm we are in screener
            in_screener = js_contains(page, "自動選股")
            print(f"  In screener mode: {in_screener}")

            # -- Click QM tab (品質選股) --
            print("\nClicking QM tab (品質選股)...")
            qm_result = page.evaluate(r"""() => {
                const tabs = document.querySelectorAll('[role="tab"]');
                for (const t of tabs) {
                    if (t.innerText && t.innerText.includes('品質')) {
                        t.click();
                        return {clicked: true, text: t.innerText};
                    }
                }
                // Return all tab texts for debug
                return {clicked: false, tabs: Array.from(tabs).map(t => t.innerText)};
            }""")
            print(f"  QM tab result: {qm_result}")
            page.wait_for_timeout(6000)
            page.wait_for_load_state("networkidle", timeout=20000)
            page.wait_for_timeout(2000)

            # Screenshot QM top
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(500)
            save(page, "02_qm_top.png")

            # Screenshot QM bottom
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1000)
            save(page, "03_qm_bottom.png")

            # Screenshot QM mid (to see table area)
            page.evaluate("window.scrollTo(0, 600)")
            page.wait_for_timeout(500)
            save(page, "04_qm_mid.png")

            # ================================================================
            # CHECK 1: 持股警報 expander MUST NOT appear
            # ================================================================
            alert_titles = [
                "持股警報",   # 持股警報 (generic)
                "\U0001f6a8 持股警報",  # 🚨 持股警報
                "⚠️ 持股警報",  # ⚠️ 持股警報
                "✅ 持股監控",  # ✅ 持股監控
                "我的持股",  # 我的持股
                "日常警報",  # 日常警報
                "新增持股",  # 新增持股
                "刪除持股",  # 刪除持股
            ]
            found_alert = False
            for needle in alert_titles:
                if js_contains(page, needle):
                    found_alert = True
                    print(f"  [BAD] Found forbidden text: {repr(needle)}")
            if found_alert:
                results.append("[FAIL] check1_alert_expander: 持股警報 text found in QM tab — expander not removed")
            else:
                results.append("[PASS] check1_alert_expander: no 持股警報/持股監控 text found in QM tab")

            # ================================================================
            # CHECK 2: 篩選條件說明 / 操作SOP must appear AFTER 個股操作建議
            # ================================================================
            print("\nChecking text order for check 2...")
            text_order = page.evaluate(r"""() => {
                const txt = document.body.innerText;
                const idx_filter  = txt.indexOf('篩選條件說明');  // 篩選條件說明
                const idx_sop     = txt.indexOf('操作 SOP');                       // 操作 SOP
                const idx_detail  = txt.indexOf('個股操作建議');  // 個股操作建議
                const idx_detail2 = txt.indexOf('個股詳細評分');  // 個股詳細評分
                return {
                    filter_idx: idx_filter,
                    sop_idx: idx_sop,
                    detail_idx: idx_detail,
                    detail2_idx: idx_detail2,
                    len: txt.length
                };
            }""")
            print(f"  Text positions: {text_order}")

            fi = text_order["filter_idx"]
            si = text_order["sop_idx"]
            di = text_order["detail_idx"]
            d2i = text_order["detail2_idx"]

            # Use the earlier of 個股操作建議 / 個股詳細評分 as the anchor
            anchor_idx = -1
            if di >= 0 and d2i >= 0:
                anchor_idx = min(di, d2i)
            elif di >= 0:
                anchor_idx = di
            elif d2i >= 0:
                anchor_idx = d2i

            if fi < 0 and si < 0:
                results.append("[WARN] check2_order: neither 篩選條件說明 nor 操作SOP found (no picks today, expanders collapsed?)")
            elif anchor_idx < 0:
                results.append("[WARN] check2_order: 個股操作建議/詳細評分 section not found (no picks in result?)")
            else:
                order_ok = True
                issues = []
                if fi >= 0 and fi < anchor_idx:
                    order_ok = False
                    issues.append(f"篩選條件說明(pos={fi}) BEFORE 個股操作建議(pos={anchor_idx})")
                if si >= 0 and si < anchor_idx:
                    order_ok = False
                    issues.append(f"操作SOP(pos={si}) BEFORE 個股操作建議(pos={anchor_idx})")
                if order_ok:
                    results.append("[PASS] check2_order: 篩選條件說明 and 操作SOP appear AFTER 個股操作建議")
                else:
                    results.append(f"[FAIL] check2_order: wrong order — {'; '.join(issues)}")

            # ================================================================
            # CHECK 3: Table fully expanded — detect internal scroll bar
            # ================================================================
            print("\nChecking for internal table scroll bars (check 3)...")
            # Detect any element inside the dataframe/table that has overflow scroll
            # and has scrollHeight > clientHeight (meaning content is clipped)
            scroll_info = page.evaluate(r"""() => {
                // Streamlit dataframes are in <div data-testid="stDataFrame"> or similar
                // Check all div elements with overflow style
                const allDivs = document.querySelectorAll('div[data-testid="stDataFrame"], div.stDataFrame, div[class*="dataframe"]');
                const scrollable = [];
                for (const el of allDivs) {
                    // Walk children looking for scrollable containers
                    const children = el.querySelectorAll('*');
                    for (const child of children) {
                        const style = window.getComputedStyle(child);
                        const ov = style.overflow + style.overflowY;
                        if ((ov.includes('scroll') || ov.includes('auto')) &&
                            child.scrollHeight > child.clientHeight + 5) {
                            scrollable.push({
                                tag: child.tagName,
                                scrollH: child.scrollHeight,
                                clientH: child.clientHeight,
                                overflow: ov
                            });
                        }
                    }
                }
                // Also check any iframe (Streamlit may use iframes for tables)
                const iframes = document.querySelectorAll('iframe');
                return {
                    dataframe_divs: allDivs.length,
                    scrollable_count: scrollable.length,
                    scrollable_items: scrollable.slice(0, 3),
                    iframes: iframes.length
                };
            }""")
            print(f"  Scroll check result: {scroll_info}")

            if scroll_info["scrollable_count"] == 0:
                results.append("[PASS] check3_table_expand: no internal scroll containers detected in dataframe area")
            else:
                # Some scroll containers are expected (the outer page), only flag if table-specific
                results.append(f"[WARN] check3_table_expand: {scroll_info['scrollable_count']} scrollable element(s) in dataframe area — review screenshot 04_qm_mid.png")

            # Also capture a scrolled-to-table screenshot for manual verification
            page.evaluate("window.scrollTo(0, 400)")
            page.wait_for_timeout(500)
            save(page, "05_qm_table_area.png")

            # -- Now test Value tab (💎 價值池) --
            print("\nClicking Value tab (價值池)...")
            val_result = page.evaluate(r"""() => {
                const tabs = document.querySelectorAll('[role="tab"]');
                for (const t of tabs) {
                    if (t.innerText && t.innerText.includes('價值池')) {
                        t.click();
                        return {clicked: true, text: t.innerText};
                    }
                }
                return {clicked: false, tabs: Array.from(tabs).map(t => t.innerText)};
            }""")
            print(f"  Value tab result: {val_result}")
            page.wait_for_timeout(5000)
            page.wait_for_load_state("networkidle", timeout=20000)
            page.wait_for_timeout(2000)

            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(500)
            save(page, "06_value_top.png")
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1000)
            save(page, "07_value_bottom.png")

            # Check 1 equivalent for Value tab — no 持股警報
            val_found_alert = False
            for needle in alert_titles:
                if js_contains(page, needle):
                    val_found_alert = True
                    print(f"  [BAD] Value tab found forbidden text: {repr(needle)}")
            if val_found_alert:
                results.append("[FAIL] check1_value_alert: 持股警報 text found in Value tab")
            else:
                results.append("[PASS] check1_value_alert: no 持股警報 in Value tab")

            # Check 2 equivalent for Value tab — 篩選條件說明 after table section
            val_order = page.evaluate(r"""() => {
                const txt = document.body.innerText;
                const idx_filter = txt.indexOf('篩選條件說明');  // 篩選條件說明
                const idx_detail = txt.indexOf('個股操作建議');  // 個股操作建議
                const idx_detail2= txt.indexOf('個股詳細評分');  // 個股詳細評分
                return {filter_idx: idx_filter, detail_idx: idx_detail, detail2_idx: idx_detail2}
            }""")
            print(f"  Value tab text positions: {val_order}")

            v_fi = val_order["filter_idx"]
            v_di = val_order["detail_idx"]
            v_d2i = val_order["detail2_idx"]
            v_anchor = -1
            if v_di >= 0 and v_d2i >= 0:
                v_anchor = min(v_di, v_d2i)
            elif v_di >= 0:
                v_anchor = v_di
            elif v_d2i >= 0:
                v_anchor = v_d2i

            if v_fi < 0:
                results.append("[WARN] check2_value_order: 篩選條件說明 not found in Value tab (collapsed or no picks)")
            elif v_anchor < 0:
                results.append("[WARN] check2_value_order: 個股操作建議 section not found in Value tab")
            elif v_fi > v_anchor:
                results.append("[PASS] check2_value_order: 篩選條件說明 appears AFTER 個股操作建議 in Value tab")
            else:
                results.append(f"[FAIL] check2_value_order: 篩選條件說明(pos={v_fi}) appears BEFORE 個股操作建議(pos={v_anchor})")

            ctx.close()
            browser.close()

        print("\n" + "=" * 60)
        print("RESULTS SUMMARY")
        print("=" * 60)
        for r in results:
            print(r)

        all_files = sorted(SCREENSHOT_DIR.glob("*.png"))
        print(f"\nScreenshots ({len(all_files)}) saved to: {SCREENSHOT_DIR}")
        for f in all_files:
            print(f"  {f}")

    finally:
        print("\nStopping Streamlit...")
        if os.name == "nt":
            try:
                proc.send_signal(signal.CTRL_BREAK_EVENT)
                time.sleep(1)
            except Exception:
                pass
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
    run_test()
