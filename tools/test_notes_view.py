"""
notes_view.py Playwright validation script (2026-06-12)
Usage: python tools/test_notes_view.py
Output: test_screenshots/notes_tab_*.png

⚠️ 2026-08-05 修 flaky：本測試曾在同一個 commit 連跑兩次得到不同結果
（一次 19 項全 PASS、一次 step3-5 四項 FAIL），一度被誤判成「筆記的編輯 /
刪除功能壞了」。真因是等待邏輯與兩個假陽性判據，見 wait_script_idle()
與 step4 / step5 的註解。
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


# ====================================================================
#  Streamlit rerun 等待 —— 本測試可信度的地基，別改回 networkidle
# ====================================================================
#
# ⚠️ `page.wait_for_load_state("networkidle")` 對 Streamlit 完全無效：
# rerun 的結果是走 WebSocket 推送的，不產生 HTTP request，所以 networkidle
# 幾乎立刻就滿足 —— 等於沒等。原本真正在等的只有 `time.sleep(2)`，而實測
# 點「編輯」後 textarea 要 1.83s 才出現，餘裕只剩 0.17s：機器稍慢就整批 FAIL，
# 而畫面其實只是還在 rerun（截圖右上角有 "Stop"、整頁灰化），功能是好的。
# 判據改用 Streamlit 自己的 <body data-test-script-state="running|notRunning">。

_SCRIPT_STATE_JS = """() => {
    const e = document.querySelector('[data-test-script-state]');
    return e ? e.getAttribute('data-test-script-state') : null;
}"""


def _script_state(page):
    return page.evaluate(_SCRIPT_STATE_JS)


def assert_script_state_marker(page):
    """啟動自檢：標記不存在就 fail loud，不要靜默退回 flaky。

    若 Streamlit 改掉這個 DOM 契約，wait_script_idle() 會永遠立刻回傳 True，
    測試看起來還是綠的 —— 那比紅的更危險，所以這裡直接炸。
    """
    state = _script_state(page)
    if state is None:
        raise RuntimeError(
            "data-test-script-state marker missing: Streamlit changed its DOM "
            "contract, so wait_script_idle() would silently no-op and this test "
            "would go back to being flaky. Fix the selector before trusting any "
            "result.")
    return state


def wait_script_idle(page, timeout=30.0, settle=0.4):
    """等這一輪 rerun 真的跑完（含連續 rerun）。逾時回 False，不丟例外。"""
    deadline = time.time() + timeout
    # rerun 可能還沒起跑，先給一個短窗口等它進 running（沒進去就是已經跑完了）
    enter_deadline = time.time() + 1.5
    while time.time() < enter_deadline:
        if _script_state(page) == "running":
            break
        time.sleep(0.05)
    stable_since = None
    while time.time() < deadline:
        if _script_state(page) == "notRunning":
            if stable_since is None:
                stable_since = time.time()
            elif time.time() - stable_since >= settle:
                return True
        else:
            stable_since = None  # 又開始跑了（連續 rerun），重新計 settle
        time.sleep(0.1)
    return False


def _port_in_use(port):
    import socket
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


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

    # ⚠️ port 已被佔用時絕不能繼續：Streamlit 會因 port 衝突退出（stderr 被
    # PIPE 吞掉，看不到），而下面的 create_connection 對「別人的」進程仍然
    # 連得上 —— 測試會靜默地對錯誤的 app 跑完，給出無意義的紅或綠。
    if _port_in_use(PORT):
        raise RuntimeError(
            "port %d is already in use. Kill the process holding it (or change "
            "PORT) — otherwise this test silently runs against that app instead "
            "of the one it just started." % PORT)

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
        deadline = time.time() + 90
        while time.time() < deadline:
            # 進程提早死掉就別再空等 90s —— 原本會等滿後只印一行 ERROR 然後
            # 繼續往下跑，後面每一項都 FAIL 卻看不出真因（stderr 全被吞了）。
            if proc.poll() is not None:
                err = (proc.stderr.read() or b"").decode("utf-8", "replace")
                raise RuntimeError(
                    "Streamlit exited early (code %s) without serving port %d.\n"
                    "--- stderr tail ---\n%s" % (proc.returncode, PORT, err[-2000:]))
            if _port_in_use(PORT):
                print("[INFO] Port %d is open, waiting for first render..." % PORT)
                break
            time.sleep(2)
        else:
            raise RuntimeError(
                "Streamlit did not open port %d within 90s" % PORT)

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 900})

            # ---- 0. Load homepage ----
            try:
                page.goto(f"http://localhost:{PORT}", timeout=60000)
                # 首次載入是真的 HTTP 資源，networkidle 在這裡仍然有意義；
                # 但「app 跑完了沒」一律看 data-test-script-state。
                page.wait_for_load_state("networkidle", timeout=60000)
                page.wait_for_selector("label[data-baseweb='radio']", timeout=60000)
                assert_script_state_marker(page)
                if not wait_script_idle(page, timeout=60.0):
                    log("00_first_render", "WARN", "still running after 60s")
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

                # Verify via JS evaluate first
                js_count = page.evaluate("""() => {
                    return document.querySelectorAll("label[data-baseweb='radio']").length;
                }""")
                log("step1_js_count", "INFO", "JS count=%d" % js_count)

                # Count all radio option labels (data-baseweb="radio")
                radio_option_labels = page.locator(
                    "label[data-baseweb='radio']")
                count = radio_option_labels.count()
                if count == 7:
                    log("step1_radio_count", "PASS", "%d radio options" % count)
                else:
                    log("step1_radio_count", "FAIL",
                        "expected 7, got %d" % count)

                # Click the knowledge-base option (label "📚 知識庫"; 2026-07-16 renamed from 筆記)
                notes_clicked = False
                for i in range(count):
                    lbl = radio_option_labels.nth(i)
                    txt = get_inner_text_safe(lbl)
                    if "知識庫" in txt:
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

                if not wait_script_idle(page):
                    log("step1_wait_idle", "WARN", "still running after 30s")

            except Exception as e:
                log("step1_sidebar", "FAIL", str(e))

            # ---- 2. Notes list + view mode ----
            try:
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

            # (step 2b 白話投資來源測試已於 2026-08-03 隨功能移除)

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
                    if not wait_script_idle(page):
                        log("step3_wait_idle", "WARN", "still running after 30s")

                    # Detect editor elements。標題輸入框要用 aria-label 精準定位 ——
                    # 左欄搜尋框也是 stTextInput，光數數量會把它算進來。
                    text_inputs = page.locator(
                        "[data-testid='stTextInput'] input[aria-label='標題']")
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
                    # 「編輯既有筆記」的定義是標題與內容都要帶進來 —— 只檢查
                    # 元素存在的話，開成空白的新增編輯器也會過。
                    title_val = (text_inputs.first.input_value()
                                 if ok_input else "")
                    body_val = (textareas.first.input_value()
                                if ok_textarea else "")
                    ok_prefill = bool(title_val.strip()) and bool(body_val.strip())

                    if ok_input and ok_textarea and save_found and cancel_found \
                            and ok_prefill:
                        log("step3_editor_ui", "PASS",
                            "editor prefilled (title=%r, %d chars body) "
                            "+ save + cancel" % (title_val[:24], len(body_val)))
                    else:
                        log("step3_editor_ui", "FAIL",
                            "input=%s textarea=%s save=%s cancel=%s prefill=%s "
                            "(title=%r body_len=%d)" % (
                                ok_input, ok_textarea, save_found, cancel_found,
                                ok_prefill, title_val[:24], len(body_val)))

                    path_edit = str(SCREENSHOT_DIR / "notes_tab_edit.png")
                    page.screenshot(path=path_edit, full_page=False)
                    log("step3_screenshot", "PASS", path_edit)

                    if cancel_btn_el is not None:
                        cancel_btn_el.click()
                        wait_script_idle(page)
                        # 真的回到瀏覽模式才算過（編輯器的 textarea 要消失）
                        if page.locator("textarea").count() == 0:
                            log("step3_cancel", "PASS",
                                "cancel clicked, back to view mode")
                        else:
                            log("step3_cancel", "FAIL",
                                "cancel clicked but editor textarea still present")
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
                    if not wait_script_idle(page):
                        log("step4_wait_idle", "WARN", "still running after 30s")

                    # ⚠️ 別用「第一個空的 input」當判據 —— 左欄搜尋框（2026-07-16
                    # 加的）就是第一個而且永遠是空的，原本的
                    # `"標題" in aria or (val == "" and i == 0)` 因此恆真，
                    # 讓這項永遠假 PASS：即使「新增」根本沒點開編輯器也照樣綠。
                    # 只認 aria-label='標題' 這個真正的標題輸入框。
                    title_input = page.locator(
                        "[data-testid='stTextInput'] input[aria-label='標題']")
                    has_title = title_input.count() > 0
                    title_val = title_input.first.input_value() if has_title else None
                    textarea_val = (page.locator("textarea").first.input_value()
                                    if page.locator("textarea").count() else None)

                    if has_title and title_val == "" and textarea_val == "":
                        log("step4_new_editor", "PASS",
                            "blank editor: title and body both empty")
                    else:
                        log("step4_new_editor", "FAIL",
                            "title_input=%s title=%r body=%r"
                            % (has_title, title_val,
                               (textarea_val or "")[:20] if textarea_val is not None
                               else None))

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
                        wait_script_idle(page)
                        if page.locator("textarea").count() == 0:
                            log("step4_cancel", "PASS", "cancel new note OK")
                        else:
                            log("step4_cancel", "FAIL",
                                "cancel clicked but editor textarea still present")
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
                    if not wait_script_idle(page):
                        log("step5_wait_idle", "WARN", "still running after 30s")

                    page_text2 = page.inner_text("body")
                    has_confirm_text = "確定刪除" in page_text2
                    # Streamlit 1.52 用 stAlert / stAlertContainer；
                    # 舊的 stAlertMessage 已不存在（2026-08-05 實測 count=0），
                    # 留著只會讓這項掉進 WARN 分支。
                    has_error_alert = any(
                        page.locator(sel).count() > 0
                        for sel in ("[data-testid='stAlert']",
                                    "[data-testid='stAlertContainer']",
                                    "[data-baseweb='notification']"))

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
                            "confirm text visible but no alert container matched "
                            "(stAlert / stAlertContainer / baseweb notification "
                            "all missing — Streamlit may have renamed them again)")
                    else:
                        log("step5_delete_confirm", "FAIL",
                            "confirm dialog not found")

                    path_del = str(SCREENSHOT_DIR / "notes_tab_delete_confirm.png")
                    page.screenshot(path=path_del, full_page=False)
                    log("step5_screenshot", "PASS", path_del)

                    # ⚠️ 這項不可只看「筆記還在不在」—— 筆記本來就沒被刪，所以
                    # 就算確認對話從未出現、取消也從未點到，它照樣會 PASS。
                    # 必須先確認確認對話真的出現過，再驗證取消把它收掉了。
                    if not has_confirm_text:
                        log("step5_cancel_delete", "FAIL",
                            "skipped: confirm dialog never appeared, so there was "
                            "nothing to cancel")
                    elif cancel_btn3 is not None:
                        cancel_btn3.click()
                        wait_script_idle(page)
                        page_text3 = page.inner_text("body")
                        dialog_gone = "確定刪除" not in page_text3
                        still_has = any(f in page_text3
                                        for f in ["雙鴻", "榮剛", "興富發"])
                        if dialog_gone and still_has:
                            log("step5_cancel_delete", "PASS",
                                "confirm dialog dismissed, note NOT deleted")
                        else:
                            log("step5_cancel_delete", "FAIL",
                                "dialog_gone=%s note_still_listed=%s"
                                % (dialog_gone, still_has))
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

                if not wait_script_idle(page, timeout=60.0):
                    log("step6_wait_idle", "WARN", "still running after 60s")

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
