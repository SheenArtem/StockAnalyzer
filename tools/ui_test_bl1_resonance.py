"""
BL-1 Resonance Smoke Test
Scene: screener mode -- QM tab + Value TW tab resonance column verification

Usage: python tools/ui_test_bl1_resonance.py [--keep] [--port PORT]

Checks:
1. Both tabs render without Python errors
2. QM table has resonance column
3. Value TW table has resonance column
4. version shows v2026.04.22.x
"""

import subprocess
import sys
import os
import time
import shutil
from pathlib import Path

SCREENSHOT_DIR = Path(__file__).parent / "screenshots"
PROJECT_DIR = Path(__file__).parent.parent
DEFAULT_PORT = 8599


def sp(s):
    """Safe print: replace un-encodable chars for Windows CP950 terminals."""
    try:
        print(s)
    except UnicodeEncodeError:
        print(s.encode(sys.stdout.encoding or "ascii", errors="replace").decode(
            sys.stdout.encoding or "ascii"))


def cleanup_screenshots():
    if SCREENSHOT_DIR.exists():
        shutil.rmtree(SCREENSHOT_DIR)
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)


def run_test(keep=False, port=DEFAULT_PORT):
    if not keep:
        cleanup_screenshots()
    else:
        SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    results = []  # list of (label_ascii, ok: bool, detail_ascii)

    sp(f"[BL-1] Starting Streamlit server (port {port})...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app.py",
         "--server.headless", "true",
         "--server.port", str(port),
         "--browser.gatherUsageStats", "false"],
        cwd=str(PROJECT_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )

    try:
        sp("[BL-1] Waiting for server to start (10s)...")
        time.sleep(10)

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1400, "height": 900})

            # --- connect ---
            sp(f"[BL-1] Connecting to http://localhost:{port} ...")
            page.goto(f"http://localhost:{port}", timeout=30000)
            # wait until sidebar is present (app initialized)
            page.wait_for_selector(
                "section[data-testid='stSidebar']", timeout=60000
            )
            page.wait_for_timeout(3000)
            page.screenshot(path=str(SCREENSHOT_DIR / "00_initial.png"))
            sp("  Saved: 00_initial.png")

            # --- version check ---
            page_text = page.inner_text("body")
            version_ok = "v2026.04.22" in page_text
            results.append((
                "version v2026.04.22.x",
                version_ok,
                "version string found" if version_ok else "version NOT found in page",
            ))

            # --- switch to screener mode ---
            sp("[BL-1] Switching to screener mode...")
            # The radio label contains CJK; use locator filter
            screener_label = page.locator("label").filter(has_text="自動選股")
            if screener_label.count() > 0:
                screener_label.first.click()
                sp("  Clicked screener mode radio label")
            else:
                # fallback: click by visible text
                page.get_by_text("自動選股").first.click()
                sp("  Clicked screener mode by text")

            # wait for screener tabs to appear
            sp("[BL-1] Waiting for screener tabs to render...")
            try:
                page.wait_for_selector(
                    "button[role='tab']", timeout=20000
                )
                page.wait_for_timeout(3000)
            except Exception:
                sp("  WARN: tab selector timeout, proceeding anyway")

            page.screenshot(path=str(SCREENSHOT_DIR / "01_screener_mode.png"))
            sp("  Saved: 01_screener_mode.png")

            # ================================================================
            # Tab 1: QM
            # ================================================================
            sp("[BL-1] Opening QM tab...")
            # tab name contains shield emoji + CJK
            qm_tab = page.get_by_role("tab", name="品質選股")
            if qm_tab.count() == 0:
                # broader match
                qm_tab = page.locator("button[role='tab']").filter(has_text="品質")
            if qm_tab.count() > 0:
                qm_tab.first.click()
                sp("  Clicked QM tab")
                page.wait_for_timeout(5000)
                try:
                    page.wait_for_load_state("networkidle", timeout=20000)
                except Exception:
                    pass
                page.wait_for_timeout(2000)
                page.screenshot(
                    path=str(SCREENSHOT_DIR / "02_qm_tab.png"),
                    full_page=True,
                )
                sp("  Saved: 02_qm_tab.png")

                qm_text = page.inner_text("body")
                # Also grab all DOM text including aria labels on dataframe cells
                qm_html = page.content()

                no_error = "Traceback" not in qm_text and "AttributeError" not in qm_text and "TypeError" not in qm_text
                results.append(("qm_tab: no Python error", no_error,
                                "clean render" if no_error else "exception detected in page"))

                # Streamlit st.dataframe column headers may not appear in innerText
                # Check both innerText and HTML source for the column name
                has_resonance = "共振" in qm_text or "共振" in qm_html
                results.append(("qm_tab: resonance column exists", has_resonance,
                                "column header found in DOM" if has_resonance else "column NOT found in DOM/text"))

                has_data = ("掃描日期" in qm_text or "scan_date" in qm_text
                            or "掃描日期" in qm_html)
                results.append(("qm_tab: table data present", has_data,
                                "scan_date caption found" if has_data else "no data caption — may be empty"))

                # Scroll down to dataframe and screenshot the column header row
                # QM tab has: 持股警報 expander (~300px) + 精選 expander (~300px) + 排序 + table
                page.evaluate("window.scrollTo(0, 1400)")
                page.wait_for_timeout(1000)
                page.screenshot(
                    path=str(SCREENSHOT_DIR / "04_qm_table_top.png"),
                    clip={"x": 240, "y": 0, "width": 1160, "height": 900},
                )
                sp("  Saved: 04_qm_table_top.png")

            else:
                results.append(("qm_tab: tab found", False, "QM tab element not found"))

            # ================================================================
            # Tab 2: Value TW
            # ================================================================
            sp("[BL-1] Opening Value TW tab...")
            # tab name contains diamond emoji + "價值 (台股)"
            val_tab = page.locator("button[role='tab']").filter(has_text="價值")
            if val_tab.count() == 0:
                val_tab = page.locator("button[role='tab']").filter(has_text="Value")
            if val_tab.count() > 0:
                val_tab.first.click()
                sp("  Clicked Value TW tab")
                page.wait_for_timeout(5000)
                try:
                    page.wait_for_load_state("networkidle", timeout=20000)
                except Exception:
                    pass
                page.wait_for_timeout(2000)
                page.screenshot(
                    path=str(SCREENSHOT_DIR / "03_value_tw_tab.png"),
                    full_page=True,
                )
                sp("  Saved: 03_value_tw_tab.png")

                val_text = page.inner_text("body")
                val_html = page.content()

                no_error_v = "Traceback" not in val_text and "AttributeError" not in val_text and "TypeError" not in val_text
                results.append(("value_tw_tab: no Python error", no_error_v,
                                "clean render" if no_error_v else "exception detected in page"))

                has_resonance_v = "共振" in val_text or "共振" in val_html
                results.append(("value_tw_tab: resonance column exists", has_resonance_v,
                                "column header found in DOM" if has_resonance_v else "column NOT found in DOM/text"))

                has_data_v = ("掃描日期" in val_text or "scan_date" in val_text
                              or "掃描日期" in val_html)
                results.append(("value_tw_tab: table data present", has_data_v,
                                "scan_date caption found" if has_data_v else "no data caption — may be empty"))

                page.evaluate("window.scrollTo(0, 500)")
                page.wait_for_timeout(1000)
                page.screenshot(
                    path=str(SCREENSHOT_DIR / "05_value_table_top.png"),
                    clip={"x": 240, "y": 0, "width": 1160, "height": 900},
                )
                sp("  Saved: 05_value_table_top.png")

            else:
                results.append(("value_tw_tab: tab found", False, "Value TW tab element not found"))

            browser.close()

    finally:
        sp("[BL-1] Stopping Streamlit server...")
        if os.name == "nt":
            import signal as _sig
            try:
                proc.send_signal(_sig.CTRL_BREAK_EVENT)
            except Exception:
                pass
            time.sleep(1)
            try:
                proc.kill()
            except Exception:
                pass
        else:
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            pass
        sp("[BL-1] Server stopped.")

    # ================================================================
    # Report
    # ================================================================
    sp("\n" + "=" * 60)
    sp("BL-1 Resonance Smoke Test - Results")
    sp("=" * 60)
    pass_count = sum(1 for _, ok, _ in results if ok)
    fail_count = len(results) - pass_count
    for name, ok, detail in results:
        tag = "[PASS]" if ok else "[FAIL]"
        sp(f"{tag} {name}: {detail}")
    sp("-" * 60)
    sp(f"Total: {pass_count} PASS / {fail_count} FAIL")
    sp(f"Screenshots: {SCREENSHOT_DIR}")
    sp("=" * 60)

    return fail_count == 0


if __name__ == "__main__":
    keep = "--keep" in sys.argv
    port_arg = DEFAULT_PORT
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--port" and i + 2 <= len(sys.argv) - 1:
            try:
                port_arg = int(sys.argv[i + 2])
            except ValueError:
                pass
    success = run_test(keep=keep, port=port_arg)
    sys.exit(0 if success else 1)
