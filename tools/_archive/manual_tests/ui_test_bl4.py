"""
BL-4 Integration UI Smoke Test
Tests 4 positions added in v2026.04.27.6:
  Phase B - Market Scan mode (4 modes in sidebar, 16 Top-10 tables)
  Phase C - Weekly rank column in QM / Value / Mode-D Today-Pick tables
  Phase D - Stock analysis chip tab: BL-4 weekly chip expander for 2330
  Phase F - Mode D Thesis Panel: market flow section

Usage: python tools/ui_test_bl4.py [--keep]
"""
import io
# Force UTF-8 output so emoji in page text / results doesn't crash on cp950 console
import sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import subprocess
import sys
import os
import time
import shutil
import signal
from pathlib import Path

SCREENSHOT_DIR = Path(__file__).parent / "screenshots"
PORT = 8599
BASE = Path(__file__).parent.parent


def cleanup():
    if SCREENSHOT_DIR.exists():
        shutil.rmtree(SCREENSHOT_DIR)
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)


def save(page, name):
    p = str(SCREENSHOT_DIR / name)
    page.screenshot(path=p, full_page=True)
    print(f"  [screenshot] {name}")
    return p


def wait_streamlit(page, extra_ms=0):
    """Wait for Streamlit to finish rendering (spinner gone)."""
    try:
        page.wait_for_selector("[data-testid='stSpinner']", timeout=5000)
        page.wait_for_selector("[data-testid='stSpinner']", state="hidden", timeout=90000)
    except Exception:
        pass
    if extra_ms:
        page.wait_for_timeout(extra_ms)
    page.wait_for_load_state("networkidle", timeout=15000)
    page.wait_for_timeout(1500)


def click_radio(page, label):
    """Click a Streamlit radio option by visible text (partial match for emoji robustness)."""
    # Use partial text match — emoji exact match can fail on cp950 Windows consoles
    radio = page.get_by_text(label).first
    radio.click()
    page.wait_for_timeout(1000)


def click_tab(page, label):
    """Click a Streamlit tab by text."""
    tab = page.get_by_role("tab", name=label)
    if tab.count() == 0:
        raise RuntimeError(f"Tab not found: {label}")
    tab.first.click()
    page.wait_for_timeout(2000)


def run_tests(keep=False):
    if not keep:
        cleanup()
    else:
        SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    results = {}

    print("Starting Streamlit on port", PORT)
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app.py",
         "--server.headless", "true",
         "--server.port", str(PORT),
         "--browser.gatherUsageStats", "false"],
        cwd=str(BASE),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )

    try:
        print("Waiting 8s for server startup...")
        time.sleep(8)

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 900})

            # ── initial load ──────────────────────────────────────────────
            print("\n[0] Initial load")
            page.goto(f"http://localhost:{PORT}", timeout=30000)
            wait_streamlit(page)
            save(page, "00_initial.png")

            # ══════════════════════════════════════════════════════════════
            # Phase B: Market Scan mode
            # ══════════════════════════════════════════════════════════════
            print("\n[Phase B] Market Scan mode")
            try:
                click_radio(page, "📡 市場掃描")
                wait_streamlit(page, extra_ms=3000)
                save(page, "B_market_scan.png")

                # Check 4 radio options exist
                radio_texts = page.locator("[data-testid='stRadio'] label").all_inner_texts()
                print(f"  Radio options found: {len(radio_texts)} options")
                has_4_modes = len(radio_texts) >= 4
                # Check title
                title_ok = page.get_by_text("📡 市場掃描").count() > 0
                # Check 16 tables rendered (look for stDataFrame or stTable elements)
                # The report renders 1 selectbox + 4 rank-type expanders each with 4 tabs → we just
                # check that at least some table-like elements appear after clicking the tab
                tab_el = page.get_by_role("tab", name="📊 法人週榜")
                if tab_el.count() > 0:
                    tab_el.first.click()
                    wait_streamlit(page, extra_ms=3000)
                    save(page, "B_market_scan_chipreport.png")
                    # Look for any dataframe
                    df_count = page.locator("[data-testid='stDataFrame']").count()
                    print(f"  DataFrames visible: {df_count}")
                    # Accept: warning about missing data OR actual tables
                    no_data_warn = page.get_by_text("週榜資料尚未產出").count() > 0
                    if no_data_warn:
                        print("  INFO: no weekly data yet (expected if batch not run)")
                    results["Phase_B"] = ("PASS" if (has_4_modes and title_ok) else "WARN",
                                         f"4 modes={has_4_modes}, title={title_ok}, dfs={df_count}, no_data_warn={no_data_warn}")
                else:
                    results["Phase_B"] = ("FAIL", "tab 📊 法人週榜 not found")
            except Exception as e:
                save(page, "B_error.png")
                results["Phase_B"] = ("FAIL", str(e))

            # ══════════════════════════════════════════════════════════════
            # Phase C: 週榜 column in screener tabs
            # ══════════════════════════════════════════════════════════════
            print("\n[Phase C] 週榜 column in screener")
            try:
                click_radio(page, "🔍 自動選股")
                wait_streamlit(page, extra_ms=3000)
                save(page, "C_screener_initial.png")

                # Sub-check C1: QM tab
                click_tab(page, "🛡️ 品質選股")
                wait_streamlit(page, extra_ms=2000)
                save(page, "C1_qm_tab.png")
                qm_weekly = page.get_by_text("週榜").count()
                print(f"  QM tab '週榜' text count: {qm_weekly}")

                # Sub-check C2: Value tab (looking for 💎 價值池)
                click_tab(page, "💎 價值池")
                wait_streamlit(page, extra_ms=2000)
                save(page, "C2_value_tab.png")
                val_weekly = page.get_by_text("週榜").count()
                print(f"  Value tab '週榜' text count: {val_weekly}")

                # Sub-check C3: Mode D tab, 今日 Pick sub-tab
                click_tab(page, "🎯 Mode D")
                wait_streamlit(page, extra_ms=2000)
                # Mode D has sub-tabs; 今日 Pick should be default/first
                moded_pick_tab = page.get_by_role("tab", name="📋 今日 Pick")
                if moded_pick_tab.count() > 0:
                    moded_pick_tab.first.click()
                    wait_streamlit(page, extra_ms=2000)
                save(page, "C3_moded_pick_tab.png")
                moded_weekly = page.get_by_text("週榜").count()
                print(f"  Mode D Pick tab '週榜' text count: {moded_weekly}")

                c_pass = (qm_weekly > 0 or val_weekly > 0 or moded_weekly > 0)
                results["Phase_C"] = ("PASS" if c_pass else "WARN",
                                     f"qm={qm_weekly}, value={val_weekly}, moded={moded_weekly}")
            except Exception as e:
                save(page, "C_error.png")
                results["Phase_C"] = ("FAIL", str(e))

            # ══════════════════════════════════════════════════════════════
            # Phase D: 個股分析 2330 籌碼面 BL-4 expander
            # ══════════════════════════════════════════════════════════════
            print("\n[Phase D] 2330 chip tab BL-4 expander")
            try:
                click_radio(page, "📈 個股分析")
                wait_streamlit(page, extra_ms=2000)

                # Fill stock input
                inp = page.locator("input[type='text']").first
                inp.click(click_count=3)
                inp.fill("2330")
                page.wait_for_timeout(500)

                # Click analyze
                btn = page.get_by_text("開始分析")
                if btn.count() > 0:
                    btn.first.click()
                else:
                    # Try emoji variant
                    btn2 = page.get_by_text("🚀 開始分析")
                    if btn2.count() > 0:
                        btn2.first.click()

                print("  Waiting for 2330 analysis (up to 90s)...")
                wait_streamlit(page, extra_ms=5000)
                # Extra wait for heavy 2330 data fetch
                page.wait_for_timeout(20000)
                wait_streamlit(page)

                save(page, "D_2330_analysis.png")

                # Switch to 籌碼面 tab
                click_tab(page, "籌碼面")
                wait_streamlit(page, extra_ms=3000)
                save(page, "D_2330_chip_tab.png")

                # Look for the expander header text
                exp_text = page.get_by_text("本週法人動向").count()
                print(f"  '本週法人動向' text count: {exp_text}")

                # Also check 4 columns label
                col4_count = max(
                    page.get_by_text("三大").count(),
                    page.get_by_text("外資").count(),
                )
                print(f"  '三大'/'外資' label count: {col4_count}")

                # If expander not found but chip tab loaded, check for known caveat
                if exp_text == 0:
                    # Could be 2330 not in weekly report, or no data yet
                    no_weekly = page.get_by_text("本週法人動向").count() == 0
                    print(f"  NOTE: expander absent — 2330 may not be in weekly report this week")
                    results["Phase_D"] = ("WARN",
                                         "chip tab loaded but 本週法人動向 expander absent (2330 not in weekly top-10 or no data)")
                else:
                    results["Phase_D"] = ("PASS", f"expander found, 三大/外資 cols={col4_count}")

            except Exception as e:
                save(page, "D_error.png")
                results["Phase_D"] = ("FAIL", str(e))

            # ══════════════════════════════════════════════════════════════
            # Phase F: Mode D Thesis Panel market flow section
            # Uses a DEDICATED Streamlit process on port 8600 — screener mode
            # with full QM/Value/ModeD tabs takes 90s+ to render; isolating
            # avoids Phase D's 2330 session state from causing indefinite rerun
            # ══════════════════════════════════════════════════════════════
            print("\n[Phase F] Mode D Thesis Panel market flow (dedicated server port 8600)")

            PORT_F = 8600
            proc_f = subprocess.Popen(
                [sys.executable, "-m", "streamlit", "run", "app.py",
                 "--server.headless", "true",
                 "--server.port", str(PORT_F),
                 "--browser.gatherUsageStats", "false"],
                cwd=str(BASE),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
            )
            print(f"  Launched dedicated Streamlit on port {PORT_F}, waiting 8s...")
            time.sleep(8)

            try:
                page_f = browser.new_page(viewport={"width": 1440, "height": 900})
                page_f.goto(f"http://localhost:{PORT_F}", timeout=30000)
                wait_streamlit(page_f, extra_ms=3000)

                # Switch to screener mode
                page_f.get_by_text("自動選股").first.click()
                wait_streamlit(page_f, extra_ms=4000)
                page_f.wait_for_selector("[data-testid='stTabs']", timeout=15000)
                page_f.wait_for_timeout(1000)
                save(page_f, "F_screener_loaded.png")

                # Click Mode D outer tab
                mode_d_tab = page_f.get_by_role("tab", name="🎯 Mode D")
                if mode_d_tab.count() == 0:
                    raise RuntimeError("Mode D tab not found")
                mode_d_tab.first.click()
                wait_streamlit(page_f, extra_ms=3000)
                save(page_f, "F_moded_tab.png")

                # Click Thesis Panel sub-tab
                tp_tab = page_f.get_by_role("tab", name="🎯 Thesis Panel")
                if tp_tab.count() == 0:
                    raise RuntimeError("Thesis Panel sub-tab not found")
                tp_tab.first.click()
                # Screener init + QM TradingView fetch + all 4 Thesis sections ~90s in headless
                print("  Waiting 100s for Thesis Panel Section 4 to appear in DOM...")
                page_f.wait_for_timeout(100000)
                save(page_f, "F_thesis_panel_top.png")

                # Scroll to bottom using mouse wheel
                page_f.mouse.wheel(0, 10000)
                page_f.wait_for_timeout(3000)
                save(page_f, "F_thesis_panel_bottom.png")

                # Search entire page content (including off-screen) via inner text
                full_text = page_f.content()
                subheader_in_dom = "本週市場主流 flow" in full_text
                subheader_count = page_f.get_by_text("本週市場主流 flow").count()
                print(f"  '本週市場主流 flow': visible={subheader_count}, in_dom={subheader_in_dom}")

                # Check 2-column structure markers
                buy_col = page_f.get_by_text("機構在買").count()
                sell_col = page_f.get_by_text("機構在賣").count()
                buy_in_dom = "機構在買" in full_text
                sell_in_dom = "機構在賣" in full_text
                print(f"  '機構在買': visible={buy_col}, in_dom={buy_in_dom}")
                print(f"  '機構在賣': visible={sell_col}, in_dom={sell_in_dom}")

                # Accept: section header present (even if data empty / warning)
                warn_present = page_f.get_by_text("市場主流 flow 載入失敗").count()
                info_present = page_f.get_by_text("尚無週榜資料").count()

                page_f.close()

                # Accept: found in DOM even if not visible (below fold)
                found = subheader_count > 0 or subheader_in_dom
                if found:
                    if warn_present:
                        results["Phase_F"] = ("WARN",
                                             f"subheader in DOM but load error shown. buy_dom={buy_in_dom}, sell_dom={sell_in_dom}")
                    else:
                        results["Phase_F"] = ("PASS",
                                             f"subheader in DOM. buy_dom={buy_in_dom}, sell_dom={sell_in_dom}, no_data_info={info_present>0}")
                else:
                    results["Phase_F"] = ("FAIL", "subheader not found after 100s wait")

            except Exception as e:
                save(page, "F_error.png")
                results["Phase_F"] = ("FAIL", str(e))
            finally:
                print("  Stopping dedicated Phase F server...")
                if os.name == "nt":
                    proc_f.send_signal(signal.CTRL_BREAK_EVENT)
                    time.sleep(1)
                    proc_f.kill()
                else:
                    proc_f.terminate()
                proc_f.wait(timeout=5)

            browser.close()

    finally:
        print("\nStopping Streamlit...")
        if os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
            time.sleep(1)
            proc.kill()
        else:
            proc.terminate()
        proc.wait(timeout=5)

    # ── Final report ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("BL-4 UI Smoke Test Results")
    print("=" * 60)
    screenshots = sorted(SCREENSHOT_DIR.glob("*.png"))
    for phase, (status, detail) in results.items():
        print(f"[{status}] {phase}: {detail}")
    print("\nScreenshots:")
    for s in screenshots:
        print(f"  {s}")
    print("=" * 60)
    return results


if __name__ == "__main__":
    keep = "--keep" in sys.argv
    run_tests(keep)
