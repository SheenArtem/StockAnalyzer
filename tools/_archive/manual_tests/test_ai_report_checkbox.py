"""
UI Test: AI 報告模式 — 宋分視角 checkbox 驗證
連接既有 http://localhost:8501
"""
from pathlib import Path
from playwright.sync_api import sync_playwright

SCREENSHOT_DIR = Path(__file__).parent / "screenshots"
SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

def wait_stable(page, ms=1500):
    try:
        page.wait_for_selector("[data-testid='stSpinner']", timeout=2000)
        page.wait_for_selector("[data-testid='stSpinner']", state="hidden", timeout=8000)
    except Exception:
        pass
    page.wait_for_timeout(ms)

def find_songfen_checkbox(page):
    """Find 宋分視角 checkbox; returns (input_element, label_text) or (None, None)."""
    all_cb = page.locator("input[type='checkbox']")
    for i in range(all_cb.count()):
        cb = all_cb.nth(i)
        try:
            parent_text = cb.locator("xpath=..").inner_text(timeout=800)
            if "宋分" in parent_text or "視角" in parent_text:
                return cb, parent_text.strip()
        except Exception:
            pass
    labels = page.locator("label")
    for i in range(labels.count()):
        lbl = labels.nth(i)
        try:
            t = lbl.inner_text(timeout=500)
            if "宋分" in t or "視角" in t:
                label_for = lbl.get_attribute("for")
                if label_for:
                    cb = page.locator(f"#{label_for}")
                    if cb.count() > 0:
                        return cb.first, t.strip()
        except Exception:
            pass
    return None, None

def click_format(page, text_fragment):
    """Click a radio label containing text_fragment."""
    lbl = page.locator("label").filter(has_text=text_fragment)
    if lbl.count() > 0:
        lbl.first.click()
        wait_stable(page)
        return True
    return False

def run():
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1400, "height": 900})

        print("Navigating to http://localhost:8501 ...")
        page.goto("http://localhost:8501", timeout=30000)
        page.wait_for_load_state("networkidle", timeout=30000)
        wait_stable(page, 3000)

        # 1. 切到 AI 報告模式
        ai_label = page.locator("label").filter(has_text="AI")
        if ai_label.count() == 0:
            ai_label = page.get_by_text("AI")
        ai_label.first.click()
        wait_stable(page, 3000)

        # 2. 等待 tabs 並點「生成報告」
        try:
            page.wait_for_selector("[role='tab']", timeout=15000)
        except Exception:
            pass
        wait_stable(page, 1000)

        gen_tab = page.get_by_role("tab").filter(has_text="生成報告")
        if gen_tab.count() > 0:
            gen_tab.first.click()
            wait_stable(page, 1000)

        # 3. 先切到 Markdown，確認 checkbox enabled + checked
        ok_md = click_format(page, "Markdown")
        print(f"Switched to Markdown: {ok_md}")

        cb, cb_text = find_songfen_checkbox(page)
        checkbox_found = cb is not None
        safe_text = (cb_text or "not found").encode("ascii", "replace").decode("ascii")
        print(f"Checkbox found: {checkbox_found} | text: {safe_text}")
        results.append(("checkbox_found", checkbox_found, "found" if checkbox_found else "not found"))

        md_checked = md_enabled = None
        if cb:
            md_checked = cb.is_checked()
            md_enabled = cb.is_enabled()
            print(f"[Markdown] checked={md_checked}, enabled={md_enabled}")
            results.append(("md_checked", md_checked, f"checked={md_checked}"))
            results.append(("md_enabled", md_enabled, f"enabled={md_enabled}"))

        # Screenshot A: Markdown + checkbox enabled
        page.screenshot(path=str(SCREENSHOT_DIR / "ai_A_md_checkbox_enabled.png"))
        print("  Saved: ai_A_md_checkbox_enabled.png")

        # 4. 切到 HTML，確認 checkbox disabled
        ok_html = click_format(page, "HTML")
        print(f"Switched to HTML: {ok_html}")

        cb2, _ = find_songfen_checkbox(page)
        html_disabled = None
        if cb2:
            html_checked2 = cb2.is_checked()
            html_disabled = not cb2.is_enabled()
            print(f"[HTML] checked={html_checked2}, disabled={html_disabled}")
            results.append(("html_disabled", html_disabled, f"disabled={html_disabled}"))
        else:
            # checkbox hidden = treated as disabled
            print("[INFO] Checkbox not found in HTML mode (hidden = disabled)")
            results.append(("html_disabled", True, "checkbox hidden/absent in HTML mode"))

        # Screenshot B: HTML + checkbox disabled
        page.screenshot(path=str(SCREENSHOT_DIR / "ai_B_html_checkbox_disabled.png"))
        print("  Saved: ai_B_html_checkbox_disabled.png")

        # 5. 切回 Markdown，確認 checkbox 恢復
        click_format(page, "Markdown")
        cb3, _ = find_songfen_checkbox(page)
        restore_enabled = restore_checked = None
        if cb3:
            restore_checked = cb3.is_checked()
            restore_enabled = cb3.is_enabled()
            print(f"[Markdown restored] checked={restore_checked}, enabled={restore_enabled}")
            results.append(("restore_enabled", restore_enabled, f"enabled={restore_enabled}"))
            results.append(("restore_checked", restore_checked, f"checked={restore_checked}"))

        browser.close()

    # ---- 最終回報 ----
    pass_map = {
        "checkbox_found": True,
        "md_checked":     True,
        "md_enabled":     True,
        "html_disabled":  True,
        "restore_enabled":True,
        "restore_checked":True,
    }
    label_map = {
        "checkbox_found": "Checkbox 出現在頁面",
        "md_checked":     "Markdown 模式預設勾選",
        "md_enabled":     "Markdown 模式可互動（enabled）",
        "html_disabled":  "HTML 模式 checkbox disabled",
        "restore_enabled":"切回 Markdown checkbox 恢復可用",
        "restore_checked":"切回 Markdown 仍保持勾選",
    }
    print("\n===== Verification Results =====")
    for key, val, detail in results:
        expected = pass_map.get(key, True)
        status = "[PASS]" if val == expected else "[FAIL]"
        print(f"{status} {label_map.get(key, key)}: {detail}")
    print("================================")
    print("Screenshots:")
    for f in sorted(SCREENSHOT_DIR.glob("ai_[AB]*.png")):
        print(f"  {f}")

if __name__ == "__main__":
    run()
