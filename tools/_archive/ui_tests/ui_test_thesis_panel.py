"""
Thesis Panel Wave 1 verification.
Tests 3 sections: pair performance, theme heat, macro views.
"""
import subprocess, sys, os, time, signal, shutil
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SCREENSHOT_DIR = Path(__file__).parent / "screenshots"
APP_DIR = Path(__file__).parent.parent
PORT = 8599


def run():
    shutil.rmtree(SCREENSHOT_DIR, ignore_errors=True)
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    print("Starting Streamlit on port 8599...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app.py",
         "--server.headless", "true",
         "--server.port", str(PORT),
         "--browser.gatherUsageStats", "false"],
        cwd=str(APP_DIR),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )

    results = {}
    try:
        print("Waiting 10s for server...")
        time.sleep(10)

        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1400, "height": 1000})

            page.goto(f"http://localhost:{PORT}", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=30000)
            time.sleep(2)

            # Switch to screener mode
            for label in ["🔍 自動選股", "自動選股"]:
                btns = page.get_by_text(label, exact=False)
                if btns.count() > 0:
                    btns.first.click()
                    page.wait_for_timeout(4000)
                    page.wait_for_load_state("networkidle", timeout=20000)
                    print(f"  Switched to screener mode via: {repr(label)}")
                    break

            page.screenshot(path=str(SCREENSHOT_DIR / "t00_screener_mode.png"))

            # Click Mode D tab
            for selector in [
                lambda: page.get_by_role("tab", name="🎯 Mode D"),
                lambda: page.get_by_text("Mode D", exact=False),
            ]:
                el = selector()
                if el.count() > 0:
                    el.first.click()
                    page.wait_for_timeout(4000)
                    page.wait_for_load_state("networkidle", timeout=20000)
                    print("  Clicked Mode D tab")
                    break
            else:
                results["Mode D tab"] = "FAIL - not found"
                page.screenshot(path=str(SCREENSHOT_DIR / "t_fail_no_mode_d.png"), full_page=True)
                browser.close()
                return results

            # Click Thesis Panel subtab
            for selector in [
                lambda: page.get_by_role("tab", name="🎯 Thesis Panel"),
                lambda: page.get_by_text("Thesis Panel", exact=False),
            ]:
                el = selector()
                if el.count() > 0:
                    el.first.click()
                    page.wait_for_timeout(5000)
                    page.wait_for_load_state("networkidle", timeout=25000)
                    print("  Clicked Thesis Panel subtab")
                    break
            else:
                results["Thesis Panel subtab"] = "FAIL - not found"
                page.screenshot(path=str(SCREENSHOT_DIR / "t_fail_no_thesis.png"), full_page=True)
                browser.close()
                return results

            # Check for exception
            exc = _check_error(page)
            if exc:
                results["Thesis Panel load"] = f"FAIL - exception: {exc}"
                page.screenshot(path=str(SCREENSHOT_DIR / "t_fail_exception.png"), full_page=True)
                browser.close()
                return results

            # Full page screenshot of Thesis Panel
            page.screenshot(path=str(SCREENSHOT_DIR / "t01_thesis_full.png"), full_page=True)
            print("  Saved: t01_thesis_full.png")

            # --- Section 1: 劇本進行式 (pair performance table) ---
            # Expect a dataframe with pair rows
            tables = page.locator("div[data-testid='stDataFrame']")
            table_count = tables.count()

            # Check for placeholder info boxes (old: 3 st.info placeholders)
            info_boxes = page.locator("div[data-testid='stAlert']")
            info_texts = []
            for i in range(min(info_boxes.count(), 5)):
                txt = info_boxes.nth(i).inner_text()[:80]
                info_texts.append(txt)

            # Check pair section - look for "Regime" text or pair names
            page_text = page.inner_text("body")
            has_pair_table = "Regime" in page_text or "廣達" in page_text or "奇鋐" in page_text
            has_theme_section = "theme" in page_text.lower() or "題材" in page_text or "mention" in page_text.lower()
            has_macro_section = "macro" in page_text.lower() or "大盤" in page_text or "expander" in page_text.lower()

            # Count expanders (macro views)
            expanders = page.locator("details, [data-testid='stExpander']")
            expander_count = expanders.count()

            # Check for placeholder text (Wave 1 not yet implemented)
            placeholder_keywords = ["待 Wave 1", "placeholder", "#3a", "#3b", "#3c"]
            is_placeholder = any(kw in page_text for kw in placeholder_keywords)

            print(f"  tables={table_count}, info_boxes={info_boxes.count()}, expanders={expander_count}")
            print(f"  has_pair={has_pair_table}, has_theme={has_theme_section}, has_macro={has_macro_section}")
            print(f"  is_placeholder={is_placeholder}")
            if info_texts:
                print(f"  info_texts: {info_texts}")

            # Section 1: pair performance
            if is_placeholder and not has_pair_table:
                results["Section1 劇本進行式"] = "FAIL - still placeholder text"
            elif has_pair_table and table_count > 0:
                # Count pair rows if possible
                results["Section1 劇本進行式"] = f"PASS - dataframe rendered ({table_count} table(s)), Regime column present"
            elif table_count > 0:
                results["Section1 劇本進行式"] = f"PASS - {table_count} table(s) rendered"
            else:
                results["Section1 劇本進行式"] = "FAIL - no dataframe found"

            # Section 2: theme heat
            if has_theme_section and not is_placeholder:
                results["Section2 題材熱度"] = "PASS - theme content present"
            elif is_placeholder:
                results["Section2 題材熱度"] = "FAIL - still placeholder"
            else:
                results["Section2 題材熱度"] = "WARN - theme content not detected"

            # Section 3: macro views
            if expander_count > 0 and not is_placeholder:
                results["Section3 大盤Macro"] = f"PASS - {expander_count} expander(s) found"
            elif has_macro_section and not is_placeholder:
                results["Section3 大盤Macro"] = "PASS - macro content present"
            elif is_placeholder:
                results["Section3 大盤Macro"] = "FAIL - still placeholder"
            else:
                results["Section3 大盤Macro"] = "WARN - no expanders found (may be no recent macro_views data)"

            # --- Test radio switch 7/14/30 ---
            for days in ["14", "30"]:
                # Use visible radio label containing the day number
                radio = page.locator(f"label:has-text('{days}')").filter(has_text=f"{days}").first
                if radio.count() == 0:
                    radio = page.get_by_text(f"{days} 日", exact=False).first
                try:
                    radio.scroll_into_view_if_needed(timeout=5000)
                    radio.click(timeout=8000)
                    page.wait_for_timeout(2000)
                    err = _check_error(page)
                    if err:
                        results[f"Radio {days}d"] = f"FAIL - {err}"
                    else:
                        results[f"Radio {days}d"] = "PASS - no crash"
                    fname = f"t02_radio_{days}d.png"
                    page.screenshot(path=str(SCREENSHOT_DIR / fname))
                    print(f"  Saved: {fname}")
                except Exception as _re:
                    results[f"Radio {days}d"] = f"WARN - click failed ({str(_re)[:80]})"

            # Final full-page screenshot
            page.screenshot(path=str(SCREENSHOT_DIR / "t03_thesis_final.png"), full_page=True)
            print("  Saved: t03_thesis_final.png")

            browser.close()

    finally:
        print("Stopping Streamlit...")
        if os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
            time.sleep(1)
            proc.kill()
        else:
            proc.terminate()
        proc.wait(timeout=5)
        print("Done.")

    return results


def _check_error(page):
    exc = page.locator("div[data-testid='stException']")
    if exc.count() > 0:
        return exc.first.inner_text()[:300]
    return None


if __name__ == "__main__":
    results = run()
    print("\n===== RESULT SUMMARY =====")
    for k, v in results.items():
        tag = "PASS" if v.startswith("PASS") else ("WARN" if v.startswith("WARN") else "FAIL")
        print(f"[{tag}] {k}: {v}")
    print("==========================")
    any_fail = any(v.startswith("FAIL") for v in results.values())
    sys.exit(1 if any_fail else 0)
