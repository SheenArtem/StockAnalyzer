"""
notes_view.py Playwright validation script (2026-06-12)
Usage: python tools/test_notes_view.py
Output: test_screenshots/notes_tab_*.png
"""

import subprocess
import sys
import os
import time
import io
from pathlib import Path

SCREENSHOT_DIR = Path(__file__).resolve().parent.parent / "test_screenshots"
APP_ROOT = Path(__file__).resolve().parent.parent
PORT = 8603

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

RESULTS = []


def log(step, status, msg=""):
    line = "[%s] %s" % (status, step) + (": %s" % msg if msg else "")
    RESULTS.append(line)
    print(line)


def get_inner_text_safe(loc):
    try:
        return loc.inner_text(timeout=3000)
    except Exception:
        return ""


def click_by_text_in_page(page, text, exact=False):
    """Find any element containing text and click it."""
    if exact:
        els = page.get_by_text(text, exact=True)
    else:
        els = page.get_by_text(text)
    if els.count() > 0:
        els.first.click()
        return True
    return False


def main():
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    print("[INFO] Starting Streamlit on port %d ..." % PORT)
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app.py",
         "--server.headless", "true",
         "--server.port", str(PORT),
         "--browser.gatherUsageStats", "false"],
        cwd=str(APP_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )

    try:
        print("[INFO] Waiting for Streamlit to start (max 90s)...")
        import socket
        deadline = time.time() + 90
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", PORT), timeout=1):
                    print("[INFO] Port %d is open, waiting 5s more for app init..." % PORT)
                    time.sleep(5)
                    break
            except (OSError, ConnectionRefusedError):
                time.sleep(2)
        else:
            print("[ERROR] Streamlit did not start within 90s on port %d" % PORT)

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 900})

            # ---- 0. Load homepage ----
            try:
                page.goto(f"http://localhost:{PORT}", timeout=60000)
                page.wait_for_load_state("networkidle", timeout=60000)
                log("00_homepage_load", "PASS", "port %d OK" % PORT)
            except Exception as e:
                log("00_homepage_load", "FAIL", str(e))
                browser.close()
                return

            # ---- 1. sidebar radio count + click notes ----
            # DOM: radio options are <label data-baseweb="radio"> elements
            # inside [data-testid="stRadio"]
            try:
                # Wait until at least one radio option label appears
                page.wait_for_selector(
                    "label[data-baseweb='radio']", timeout=15000)
                time.sleep(2)  # give Streamlit a moment to finish rendering

                # Verify via JS evaluate first
                js_count = page.evaluate("""() => {
                    return document.querySelectorAll("label[data-baseweb='radio']").length;
                }""")
                log("step1_js_count", "INFO", "JS count=%d" % js_count)

                # Count all radio option labels (data-baseweb="radio")
                radio_option_labels = page.locator(
                    "label[data-baseweb='radio']")
                count = radio_option_labels.count()
                if count == 6:
                    log("step1_radio_count", "PASS", "%d radio options" % count)
                else:
                    log("step1_radio_count", "FAIL",
                        "expected 6, got %d" % count)

                # Click the notes option by finding the label containing "筆記"
                notes_clicked = False
                for i in range(count):
                    lbl = radio_option_labels.nth(i)
                    txt = get_inner_text_safe(lbl)
                    if "筆記" in txt:
                        lbl.click()
                        notes_clicked = True
                        log("step1_click_notes", "PASS",
                            "clicked option %d: %r" % (i, txt.strip()))
                        break

                if not notes_clicked:
                    # fallback: try the last radio label
                    if count > 0:
                        last_lbl = radio_option_labels.nth(count - 1)
                        txt = get_inner_text_safe(last_lbl)
                        last_lbl.click()
                        log("step1_click_notes", "WARN",
                            "notes not found by text, clicked last label: %r" % txt.strip())
                    else:
                        log("step1_click_notes", "FAIL", "no radio option labels found")

                time.sleep(3)
                page.wait_for_load_state("networkidle", timeout=15000)

            except Exception as e:
                log("step1_sidebar", "FAIL", str(e))

            # ---- 2. Notes list + view mode ----
            try:
                time.sleep(2)

                page_text = page.inner_text("body")
                titles_found = 0
                # Check for key fragments from note filenames
                for fragment in ["雙鴻", "榮剛", "興富發"]:
                    if fragment in page_text:
                        titles_found += 1

                if titles_found == 3:
                    log("step2_notes_list", "PASS", "all 3 note titles visible")
                elif titles_found > 0:
                    log("step2_notes_list", "WARN",
                        "only %d/3 note titles visible" % titles_found)
                else:
                    log("step2_notes_list", "FAIL",
                        "no note titles found in page text")

                # Check edit / delete buttons
                # Streamlit renders buttons with data-testid="stButton"
                all_btns = page.locator("[data-testid='stButton']")
                btn_count = all_btns.count()
                edit_found = False
                del_found = False
                for i in range(btn_count):
                    txt = get_inner_text_safe(all_btns.nth(i))
                    if "編輯" in txt:
                        edit_found = True
                    if "刪除" in txt:
                        del_found = True

                if edit_found and del_found:
                    log("step2_view_buttons", "PASS", "edit + delete buttons visible")
                else:
                    log("step2_view_buttons", "FAIL",
                        "edit=%s delete=%s" % (edit_found, del_found))

                md_count = page.locator("[data-testid='stMarkdown']").count()
                if md_count > 0:
                    log("step2_markdown_render", "PASS",
                        "%d markdown blocks" % md_count)
                else:
                    log("step2_markdown_render", "WARN",
                        "no stMarkdown blocks detected")

                path_view = str(SCREENSHOT_DIR / "notes_tab_view.png")
                page.screenshot(path=path_view, full_page=False)
                log("step2_screenshot", "PASS", path_view)

            except Exception as e:
                log("step2_list_view", "FAIL", str(e))
                try:
                    page.screenshot(
                        path=str(SCREENSHOT_DIR / "notes_tab_view_error.png"))
                except Exception:
                    pass

            # ---- 3. Click Edit -> editor mode -> Cancel ----
            try:
                # Find button containing "編輯"
                all_btns = page.locator("[data-testid='stButton']")
                edit_btn = None
                for i in range(all_btns.count()):
                    btn = all_btns.nth(i)
                    txt = get_inner_text_safe(btn)
                    if "編輯" in txt:
                        edit_btn = btn
                        break

                if edit_btn is not None:
                    edit_btn.click()
                    time.sleep(2)
                    page.wait_for_load_state("networkidle", timeout=10000)

                    # Detect editor elements
                    text_inputs = page.locator("[data-testid='stTextInput']")
                    textareas = page.locator("textarea")
                    save_found = False
                    cancel_found = False
                    cancel_btn_el = None
                    btns2 = page.locator("[data-testid='stButton']")
                    for i in range(btns2.count()):
                        btn = btns2.nth(i)
                        txt = get_inner_text_safe(btn)
                        if "儲存" in txt:
                            save_found = True
                        if txt.strip() == "取消":
                            cancel_found = True
                            cancel_btn_el = btn

                    ok_input = text_inputs.count() > 0
                    ok_textarea = textareas.count() > 0

                    if ok_input and ok_textarea and save_found and cancel_found:
                        log("step3_editor_ui", "PASS",
                            "title input + textarea + save + cancel all present")
                    else:
                        log("step3_editor_ui", "FAIL",
                            "input=%s textarea=%s save=%s cancel=%s" % (
                                ok_input, ok_textarea, save_found, cancel_found))

                    path_edit = str(SCREENSHOT_DIR / "notes_tab_edit.png")
                    page.screenshot(path=path_edit, full_page=False)
                    log("step3_screenshot", "PASS", path_edit)

                    if cancel_btn_el is not None:
                        cancel_btn_el.click()
                        time.sleep(2)
                        page.wait_for_load_state("networkidle", timeout=10000)
                        log("step3_cancel", "PASS", "cancel clicked, back to view mode")
                    else:
                        log("step3_cancel", "FAIL", "cancel button not found")
                else:
                    log("step3_edit_btn", "FAIL", "edit button not found")

            except Exception as e:
                log("step3_edit", "FAIL", str(e))
                try:
                    page.screenshot(
                        path=str(SCREENSHOT_DIR / "notes_tab_edit_error.png"))
                except Exception:
                    pass

            # ---- 4. Click new note -> empty editor -> Cancel ----
            try:
                new_btn = None
                btns3 = page.locator("[data-testid='stButton']")
                for i in range(btns3.count()):
                    btn = btns3.nth(i)
                    txt = get_inner_text_safe(btn)
                    if "新增" in txt:
                        new_btn = btn
                        break

                if new_btn is not None:
                    new_btn.click()
                    time.sleep(2)
                    page.wait_for_load_state("networkidle", timeout=10000)

                    # Check title input is empty
                    text_inputs = page.locator("[data-testid='stTextInput'] input")
                    empty_found = False
                    for i in range(text_inputs.count()):
                        inp = text_inputs.nth(i)
                        try:
                            aria = inp.get_attribute("aria-label") or ""
                            val = inp.input_value()
                            # The title input for a new note should be empty
                            if "標題" in aria or (val == "" and i == 0):
                                empty_found = True
                                break
                        except Exception:
                            continue

                    if empty_found:
                        log("step4_new_editor", "PASS", "empty editor present")
                    else:
                        log("step4_new_editor", "WARN",
                            "could not confirm empty title input (%d inputs found)"
                            % text_inputs.count())

                    path_new = str(SCREENSHOT_DIR / "notes_tab_new.png")
                    page.screenshot(path=path_new, full_page=False)
                    log("step4_screenshot", "PASS", path_new)

                    cancel_btn2 = None
                    btns4 = page.locator("[data-testid='stButton']")
                    for i in range(btns4.count()):
                        btn = btns4.nth(i)
                        txt = get_inner_text_safe(btn)
                        if txt.strip() == "取消":
                            cancel_btn2 = btn
                            break

                    if cancel_btn2 is not None:
                        cancel_btn2.click()
                        time.sleep(2)
                        page.wait_for_load_state("networkidle", timeout=10000)
                        log("step4_cancel", "PASS", "cancel new note OK")
                    else:
                        log("step4_cancel", "FAIL", "cancel button not found")
                else:
                    log("step4_new_btn", "FAIL", "new note button not found")

            except Exception as e:
                log("step4_new", "FAIL", str(e))
                try:
                    page.screenshot(
                        path=str(SCREENSHOT_DIR / "notes_tab_new_error.png"))
                except Exception:
                    pass

            # ---- 5. Click delete -> confirm dialog -> Cancel ----
            try:
                del_btn = None
                btns5 = page.locator("[data-testid='stButton']")
                for i in range(btns5.count()):
                    btn = btns5.nth(i)
                    txt = get_inner_text_safe(btn)
                    # Match "🗑 刪除" but NOT "確定刪除"
                    if "刪除" in txt and "確定" not in txt:
                        del_btn = btn
                        break

                if del_btn is not None:
                    del_btn.click()
                    time.sleep(2)
                    page.wait_for_load_state("networkidle", timeout=10000)

                    page_text2 = page.inner_text("body")
                    has_confirm_text = "確定刪除" in page_text2
                    has_error_alert = page.locator(
                        "[data-testid='stAlertMessage']").count() > 0
                    # Also check baseweb notification
                    if not has_error_alert:
                        has_error_alert = page.locator(
                            "[data-baseweb='notification']").count() > 0

                    # Check cancel button
                    cancel_btn3 = None
                    btns6 = page.locator("[data-testid='stButton']")
                    for i in range(btns6.count()):
                        btn = btns6.nth(i)
                        txt = get_inner_text_safe(btn)
                        if txt.strip() == "取消":
                            cancel_btn3 = btn
                            break

                    if has_confirm_text and has_error_alert:
                        log("step5_delete_confirm", "PASS",
                            "red alert + confirm text visible")
                    elif has_confirm_text:
                        log("step5_delete_confirm", "WARN",
                            "confirm text visible but stAlertMessage not detected "
                            "(may use different selector)")
                    else:
                        log("step5_delete_confirm", "FAIL",
                            "confirm dialog not found")

                    path_del = str(SCREENSHOT_DIR / "notes_tab_delete_confirm.png")
                    page.screenshot(path=path_del, full_page=False)
                    log("step5_screenshot", "PASS", path_del)

                    if cancel_btn3 is not None:
                        cancel_btn3.click()
                        time.sleep(2)
                        page.wait_for_load_state("networkidle", timeout=10000)
                        # Verify note still exists
                        page_text3 = page.inner_text("body")
                        still_has = any(f in page_text3 for f in ["雙鴻", "榮剛", "興富發"])
                        if still_has:
                            log("step5_cancel_delete", "PASS",
                                "note NOT deleted (still visible)")
                        else:
                            log("step5_cancel_delete", "WARN",
                                "cancelled but could not confirm note still exists")
                    else:
                        log("step5_cancel_delete", "FAIL",
                            "cancel button not found - did NOT click delete confirm")
                else:
                    log("step5_del_btn", "FAIL", "delete button not found")

            except Exception as e:
                log("step5_delete", "FAIL", str(e))
                try:
                    page.screenshot(
                        path=str(SCREENSHOT_DIR / "notes_tab_delete_confirm_error.png"))
                except Exception:
                    pass

            # ---- 6. Regression: switch back to individual analysis ----
            try:
                mode_radios = page.locator(
                    "[data-testid='stRadio'] label[data-baseweb='radio']")
                individual_clicked = False
                for i in range(mode_radios.count()):
                    lbl = mode_radios.nth(i)
                    txt = get_inner_text_safe(lbl)
                    if "個股分析" in txt:
                        lbl.click()
                        individual_clicked = True
                        break

                if not individual_clicked:
                    # Fallback: click first radio label
                    if mode_radios.count() > 0:
                        mode_radios.first.click()
                        log("step6_click_individual", "WARN",
                            "used fallback first-label click")
                    else:
                        log("step6_click_individual", "FAIL",
                            "no radio labels found")

                time.sleep(3)
                page.wait_for_load_state("networkidle", timeout=20000)

                page_text4 = page.inner_text("body")
                has_form = (
                    "開始分析" in page_text4
                    or "輸入股票代號" in page_text4
                    or "歷史紀錄" in page_text4
                )
                if has_form:
                    log("step6_regression_smoke", "PASS",
                        "individual analysis page rendered OK")
                else:
                    log("step6_regression_smoke", "WARN",
                        "switched back but expected text not found")

                path_reg = str(SCREENSHOT_DIR / "notes_regression_individual.png")
                page.screenshot(path=path_reg, full_page=False)
                log("step6_screenshot", "PASS", path_reg)

            except Exception as e:
                log("step6_regression", "FAIL", str(e))
                try:
                    page.screenshot(
                        path=str(SCREENSHOT_DIR / "notes_regression_error.png"))
                except Exception:
                    pass

            browser.close()

    finally:
        print("[INFO] Shutting down Streamlit...")
        if os.name == "nt":
            proc.terminate()
        else:
            import signal
            proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()
        print("[INFO] Done.")

    print("\n" + "=" * 60)
    print("NOTES VIEW UI TEST SUMMARY")
    print("=" * 60)
    for r in RESULTS:
        print(r)
    print("=" * 60)
    fail_cnt = sum(1 for r in RESULTS if r.startswith("[FAIL]"))
    warn_cnt = sum(1 for r in RESULTS if r.startswith("[WARN]"))
    print("FAIL=%d  WARN=%d  total=%d" % (fail_cnt, warn_cnt, len(RESULTS)))


if __name__ == "__main__":
    main()
