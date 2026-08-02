"""白話投資 FB 粉專文章抓取器 (2026-07-16)

抓 Facebook 粉專「白話投資」(profile.php?id=61582454913453) 全部貼文，落地
raw JSONL 供下游 build_baihua_kb Workflow 清洗成知識庫。

== 為什麼要登入 ==
FB 對未登入者有硬登入牆：捲約 19 篇即跳登入對話框卡死、永久連結也抓不到。
取得「全部」文章繞不開一次性登入。本工具用 Playwright persistent context，
把登入 cookie 存在本機獨立 profile (local/baihua_fb_profile/，已 gitignore)，
登入一次後所有抓取 / 日後增量更新自動沿用，不碰使用者日常 Chrome。

== 合規 ==
raw 原文 + 產出知識庫落在 data/baihua/ (已 gitignore)，只留本機，不推入
公開 GitHub repo。工具程式碼本身可版控。

== 清洗分工 ==
FB article innerText 混雜作者名/時間/讚留言分享/留言預覽。DOM 精準切 body 太脆，
故 scraper 只存「raw innerText + 結構化 permalink/日期/圖片」；正文抽取交給下游
LLM 清洗階段 (build_baihua_kb Workflow, Sonnet)。

CLI:
    # 步驟 1（人工，一次性）: 開瀏覽器登入 FB，登入後照終端指示按 Enter
    python tools/fetch_baihua_fb.py --login

    # 步驟 2: 抓取（增量 merge 進既有 JSONL）
    python tools/fetch_baihua_fb.py --scrape
    python tools/fetch_baihua_fb.py --scrape --headful      # 想看畫面
    python tools/fetch_baihua_fb.py --scrape --max-stall 8  # 捲到更底 (預設 6)
    python tools/fetch_baihua_fb.py --scrape --logged-out   # 不登入試抓 (只拿得到牆前 ~19 篇，驗證管線用)

輸出:
    data/baihua/raw/posts.jsonl        # 每行一貼文，增量 merge，永久 SoT
    data/baihua/raw/manifest.json      # 統計 (count / 日期範圍 / last_run)

Exit code:
    0  正常完成（已捲到底且停止增長）
    1  一般失敗
    2  未登入 / 登入未完成
    4  **抓取不完整** —— 撞到 --max-rounds 上限就被截斷，資料已落地但少了最舊的部分；
       提高 --max-rounds 重跑即可（增量 merge，不會重抓）
    5  posts.jsonl 有無法解析的行 —— 未抓取、未寫檔。抓取會整檔重寫，照舊執行等於
       永久刪掉那些行。修好那幾行，或明示 --drop-corrupt-lines（會先備份原檔）
    6  **一篇貼文都沒抽到** —— 選擇器全落空（FB DOM 改版）或整輪都撞登入牆。
       既有 posts.jsonl 未被改動。這不是「沒有新文章」
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("baihua_fb")

# stdout 轉 utf-8，避免 Windows cp950 印 zh-TW 爆 UnicodeEncodeError
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

REPO = Path(__file__).resolve().parents[1]
PROFILE_DIR = REPO / "local" / "baihua_fb_profile"     # 登入 cookie（gitignore: local/）
OUT_DIR = REPO / "data" / "baihua" / "raw"             # 原文（gitignore: data/baihua/）
POSTS_JSONL = OUT_DIR / "posts.jsonl"
MANIFEST = OUT_DIR / "manifest.json"

PAGE_URL = "https://www.facebook.com/profile.php?id=61582454913453"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

SEE_MORE = ["查看更多", "See more", "See More", "顯示更多", "…查看更多"]
# permalink 形態：/posts/、pfbid、story_fbid、permalink.php?story_fbid
PERMALINK_RE = re.compile(r"(/posts/|pfbid|story_fbid=|/permalink/|/videos/|/reel/)")


# ---------------------------------------------------------------- 登入偵測
def _logged_in(ctx) -> bool:
    """FB 登入後會種 c_user cookie。無 c_user = 未登入。"""
    try:
        return any(c.get("name") == "c_user" for c in ctx.cookies())
    except Exception:
        return False


def _cookie_names_from_disk() -> set:
    """不啟動瀏覽器，直接讀 profile 的 Cookies SQLite（immutable 唯讀，繞過瀏覽器鎖）。
    回 facebook.com 的 cookie 名稱集合；讀不到回空集合。"""
    import sqlite3
    db = PROFILE_DIR / "Default" / "Network" / "Cookies"
    if not db.exists():
        return set()
    uri = "file:" + db.as_posix() + "?mode=ro&immutable=1"
    try:
        con = sqlite3.connect(uri, uri=True)
        try:
            rows = con.execute(
                "SELECT name FROM cookies WHERE host_key LIKE '%facebook%'").fetchall()
        finally:
            con.close()
        return {r[0] for r in rows}
    except Exception:
        return set()


def check_login() -> int:
    """回報登入狀態（不啟動瀏覽器）。已登入 exit 0，未登入 exit 2。"""
    names = _cookie_names_from_disk()
    ok = "c_user" in names and "xs" in names
    if not PROFILE_DIR.exists():
        log.info("尚未建立登入 profile（從未登入）。")
        return 2
    log.info("facebook cookie=%d 個；c_user=%s xs=%s → %s",
             len(names), "c_user" in names, "xs" in names,
             "已登入" if ok else "未登入")
    return 0 if ok else 2


# ---------------------------------------------------------------- --login
def do_login(headful: bool = True, wait_secs: int = 600) -> int:
    """開瀏覽器讓使用者登入 FB。輪詢偵測 c_user，**一偵測到就自動優雅關閉**（不需按 Enter，
    不會卡著視窗）；偵測到後多等 2s 讓 cookie 落盤，再 ctx.close() 確保寫回磁碟。

    ⚠️ 瀏覽器開在「執行這支程式的機器」上（server 端）。若你從別台電腦用瀏覽器連 App，
    登入視窗仍會跳在這台 server；請在這台機器完成一次登入即可，之後抓取都是背景無視窗。
    """
    from playwright.sync_api import sync_playwright

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    log.info("開啟瀏覽器 (profile=%s)…請在視窗內完成 Facebook 登入（最多等 %d 分鐘）",
             PROFILE_DIR, wait_secs // 60)
    print("\n" + "=" * 64)
    print("  瀏覽器視窗已開啟，請完成 Facebook 登入（含兩步驟驗證）。")
    print("  登入成功後，本程式會自動偵測並關閉視窗，無需手動操作。")
    print("=" * 64 + "\n")
    ok = False
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            str(PROFILE_DIR), headless=not headful, user_agent=UA,
            locale="zh-TW", viewport={"width": 1360, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        try:
            page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            log.warning("開啟 facebook.com 失敗（可繼續等待登入）：%s", e)
        import time as _t
        waited = 0
        while waited < wait_secs:
            if _logged_in(ctx):
                ok = True
                break
            _t.sleep(3)
            waited += 3
            if waited % 30 == 0:
                log.info("等待登入中…（%ds / %ds）", waited, wait_secs)
        if ok:
            _t.sleep(2)   # 讓 c_user/xs 等 session cookie 落盤
        try:
            ctx.close()   # 優雅關閉 → flush cookies 到 SQLite
        except Exception:
            pass
    if ok:
        log.info("[OK] 偵測到登入（c_user），cookie 已存 %s", PROFILE_DIR)
        return 0
    log.error("[FAIL] %ds 內未偵測到登入。請重跑 --login 並確實完成登入。", wait_secs)
    return 1


# ---------------------------------------------------------------- cookie 匯入（遠端登入用）
def _parse_cookie_file(path: Path) -> list[dict]:
    """支援三種匯出格式，回 Playwright add_cookies 用的 dict list：
    - JSON 陣列（Cookie-Editor / EditThisCookie）：[{name,value,domain,path,expirationDate,...}]
    - Netscape cookies.txt（Get cookies.txt LOCALLY）：tab 分隔
    - Cookie 標頭字串（DevTools Network → 複製 Cookie: 請求標頭）：`c_user=..; xs=..; datr=..`
      ⚠️ 不要用 Console 的 document.cookie —— xs 是 HttpOnly 讀不到，會缺登入 session。
    JSON/Netscape 只保留 facebook 網域；標頭字串無網域資訊 → 一律當 .facebook.com。
    """
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    out: list[dict] = []

    def _samesite(v):
        m = {"lax": "Lax", "strict": "Strict", "none": "None", "no_restriction": "None"}
        return m.get(str(v).lower()) if v else None

    if text.startswith("[") or text.startswith("{"):
        data = json.loads(text)
        if isinstance(data, dict):
            data = data.get("cookies", [])
        for c in data:
            dom = c.get("domain", "")
            if "facebook" not in dom:
                continue
            ck = {"name": c["name"], "value": c["value"],
                  "domain": dom, "path": c.get("path", "/")}
            exp = c.get("expirationDate") or c.get("expires")
            if exp and float(exp) > 0:
                ck["expires"] = float(exp)
            if c.get("httpOnly") is not None:
                ck["httpOnly"] = bool(c["httpOnly"])
            if c.get("secure") is not None:
                ck["secure"] = bool(c["secure"])
            ss = _samesite(c.get("sameSite"))
            if ss:
                ck["sameSite"] = ss
            out.append(ck)
    elif "\t" in text:
        for line in text.splitlines():
            if not line.strip() or line.strip().startswith("#") and "HttpOnly_" not in line:
                continue
            raw = line
            http_only = False
            if raw.startswith("#HttpOnly_"):
                http_only = True
                raw = raw[len("#HttpOnly_"):]
            parts = raw.split("\t")
            if len(parts) < 7:
                continue
            dom, _flag, pth, secure, expiry, name, value = parts[:7]
            if "facebook" not in dom:
                continue
            ck = {"name": name, "value": value, "domain": dom, "path": pth or "/",
                  "secure": secure.upper() == "TRUE", "httpOnly": http_only}
            try:
                if float(expiry) > 0:
                    ck["expires"] = float(expiry)
            except ValueError:
                pass
            out.append(ck)
    else:
        # Cookie 標頭字串：可能帶 "Cookie:" 前綴，以 "; " 分隔
        s = text
        if s.lower().startswith("cookie:"):
            s = s[len("cookie:"):].strip()
        for part in s.split(";"):
            part = part.strip()
            if not part or "=" not in part:
                continue
            name, _, value = part.partition("=")
            name = name.strip()
            value = value.strip()
            if not name:
                continue
            out.append({"name": name, "value": value,
                        "domain": ".facebook.com", "path": "/", "secure": True})
    return out


def import_cookies(path_str: str) -> int:
    """把已登入瀏覽器匯出的 FB cookie 匯入本機 profile（遠端登入替代方案，免在 server 開視窗）。
    cookie 是你自己的 session、僅存本機 gitignore profile、可隨時登出撤銷。"""
    from playwright.sync_api import sync_playwright

    src = Path(path_str)
    if not src.exists():
        log.error("找不到 cookie 檔：%s", src)
        return 2
    try:
        cookies = _parse_cookie_file(src)
    except Exception as e:
        log.error("解析 cookie 檔失敗：%s", e)
        return 2
    if not cookies:
        log.error("檔內沒有 facebook 網域的 cookie（格式不符或匯錯網站）。")
        return 2
    names = {c["name"] for c in cookies}
    log.info("解析到 %d 個 facebook cookie：%s", len(cookies),
             ", ".join(sorted(names))[:200])
    if "c_user" not in names or "xs" not in names:
        log.warning("缺少 c_user/xs（登入 session cookie）— 匯入後可能仍未登入。"
                    "請確認在『已登入 FB』的分頁匯出。")

    # ⚠️ 沒到期日的 cookie 會被當 session cookie（只在記憶體、不落盤）→ 換 context 就消失。
    # 補一年後到期，強制寫入 profile 磁碟。
    import time as _t
    far = _t.time() + 365 * 24 * 3600
    for c in cookies:
        if not c.get("expires") or float(c.get("expires", 0)) <= 0:
            c["expires"] = far

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            str(PROFILE_DIR), headless=True, user_agent=UA, locale="zh-TW",
            args=["--disable-blink-features=AutomationControlled"])
        try:
            ctx.add_cookies(cookies)
        except Exception as e:
            log.error("add_cookies 失敗：%s", e)
            ctx.close()
            return 2
        _t.sleep(1)      # 讓 cookie 落盤
        ctx.close()      # 優雅關閉 flush 到 SQLite
    # 關閉後從磁碟重讀確認真的存下來（記憶體看得到不代表落盤）
    disk = _cookie_names_from_disk()
    ok = "c_user" in disk and "xs" in disk
    if ok:
        log.info("[OK] cookie 已寫入 profile 磁碟並確認 c_user/xs，登入生效。可以抓取了。")
        return 0
    log.error("[FAIL] cookie 未成功落盤（磁碟只讀到：%s）。可能 cookie 不完整或過期，請重新匯出。",
              ", ".join(sorted(disk))[:200] or "無")
    return 1


# ---------------------------------------------------------------- 抓取
def _extract_round(page) -> list[dict]:
    """抽出當前 DOM 的貼文。

    ⚠️ 登入後 FB 桌面版：貼文正文是**最外層 `div[dir="auto"]` 且不在 `role="article"` 內**
    （留言才是 role="article"，會巢狀在貼文底下）。故主策略用 dir=auto；若為 0（未登入舊版
    DOM 把貼文包在 role="article"）則退回 role=article。permalink 盡力關聯（往上 8 層找貼文
    連結），但**去重是靠內文雜湊**（見 _key_of），permalink 只作來源連結，關聯不準也不影響去重。
    """
    js = r"""
    () => {
      const inArt = el => !!(el.closest && el.closest('div[role="article"]'));
      const txt = el => (el.innerText || '').trim();
      const MIN = 150;
      let cands = [...document.querySelectorAll('div[dir="auto"]')].filter(d => {
        if (inArt(d)) return false;              // 排除留言
        if (txt(d).length < MIN) return false;
        // 最外層：祖先中不能有另一個合格 dir=auto（否則是段落子區塊）
        let p = d.parentElement;
        while (p) {
          if (p.matches && p.matches('div[dir="auto"]') && !inArt(p) && txt(p).length >= MIN) return false;
          p = p.parentElement;
        }
        return true;
      });
      if (cands.length === 0) {                  // fallback：未登入/舊版 role=article
        cands = [...document.querySelectorAll('div[role="article"]')].filter(a => txt(a).length >= 40);
      }
      // 註：登入版桌面 feed 裡，往上找 permalink 會抓到鄰篇的連結（實測 mis-associated），
      // date 標籤同源也不可靠 → 一律不存，改由下游 LLM 從內文推斷日期。去重靠內文雜湊。
      return cands.map(d => ({permalink: null, dateLabel: null, text: txt(d), imgs: []}));
    }
    """
    try:
        return page.evaluate(js)
    except Exception as e:
        log.warning("extract round failed: %s", e)
        return []


def _dismiss_dialog(page) -> bool:
    """關掉 FB 登入/註冊彈窗（logged-out 揭露更多貼文；登入時清殘留對話框）。"""
    for sel in ('div[aria-label="關閉"]', 'div[aria-label="Close"]',
                '[aria-label="關閉"]', '[aria-label="Close"]'):
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click(timeout=800)
                page.wait_for_timeout(500)
                return True
        except Exception:
            pass
    return False


def _click_see_more(page) -> int:
    """展開貼文 see-more。用 JS click（非 Playwright .click()）——Playwright 點擊會自動把該
    元素捲進視窗，害我們離開頁面底部、觸發不了無限捲載入。JS click 原地展開不動視窗。"""
    js = r"""
    () => {
      const labels = ['查看更多','See more','See More','顯示更多','See More '];
      let n = 0;
      for (const b of document.querySelectorAll('div[role="button"], span[role="button"]')) {
        const t = (b.innerText || '').trim();
        if (!t || t.length > 8) continue;                 // see-more 文字短
        if (t.includes('留言') || t.toLowerCase().includes('comment')) continue;  // 排除展開留言
        if (labels.some(l => t === l || t === l.trim())) { try { b.click(); n++; } catch(e){} }
      }
      return n;
    }
    """
    try:
        return page.evaluate(js)
    except Exception:
        return 0


def _key_of(rec: dict) -> str:
    """以**內文雜湊**去重（貼文開頭穩定；permalink 關聯在登入版桌面 DOM 不可靠，
    用它當 key 會把不同貼文誤併）。正規化去空白後取前 120 字雜湊。"""
    body = re.sub(r"\s+", "", rec.get("text", ""))[:120]
    return "tx:" + hashlib.sha1(body.encode("utf-8", "replace")).hexdigest()[:16]


def do_scrape(headful: bool, logged_out: bool, max_stall: int, max_rounds: int,
              drop_corrupt: bool = False) -> int:
    from playwright.sync_api import sync_playwright

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # 先讀既有資料再開瀏覽器：SoT 有壞行時要在花任何抓取成本之前就擋下來。
    existing = _load_existing(drop_corrupt=drop_corrupt)
    log.info("既有 posts=%d", len(existing))

    with sync_playwright() as p:
        if logged_out:
            browser = p.chromium.launch(headless=not headful)
            ctx = browser.new_context(user_agent=UA, locale="zh-TW",
                                      viewport={"width": 1360, "height": 900})
        else:
            if not PROFILE_DIR.exists():
                log.error("找不到登入 profile。請先跑：python tools/fetch_baihua_fb.py --login")
                return 2
            ctx = p.chromium.launch_persistent_context(
                str(PROFILE_DIR), headless=not headful, user_agent=UA,
                locale="zh-TW", viewport={"width": 1360, "height": 900},
                args=["--disable-blink-features=AutomationControlled"])

        page = ctx.pages[0] if getattr(ctx, "pages", None) else ctx.new_page()
        page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(6000)

        if not logged_out and not _logged_in(ctx):
            log.error("profile 未登入（無 c_user cookie）。請先跑 --login。")
            ctx.close()
            return 2
        if logged_out:
            log.warning("logged-out 模式：預期只拿得到登入牆前 ~19 篇（僅供管線驗證）。")

        _dismiss_dialog(page)
        page.wait_for_timeout(1500)

        collected: dict[str, dict] = dict(existing)
        stall = 0
        prev_sh = 0
        truncated = False
        # 診斷用累計：`wall` 舊版只進 log 字串，迴圈一開始就只剩固定回 0 的路徑，
        # 於是 FB DOM 改版（選擇器全部落空）會顯示綠色「完成：新整理 0 篇」。
        wall_rounds = 0
        rounds_with_rows = 0
        rounds_done = 0
        for i in range(max_rounds):
            _dismiss_dialog(page)
            clicked = _click_see_more(page)
            if clicked:
                page.wait_for_timeout(600)
            rows = _extract_round(page)
            new = 0
            for r in rows:
                k = _key_of(r)
                prev = collected.get(k)
                # 保留最長 text（see-more 展開後會變長）
                if prev is None:
                    collected[k] = {"id": k, "permalink": r.get("permalink"),
                                    "date_label": r.get("dateLabel"),
                                    "text": r.get("text", ""), "imgs": r.get("imgs", [])}
                    if k not in existing:
                        new += 1
                else:
                    if len(r.get("text", "")) > len(prev.get("text", "")):
                        prev["text"] = r["text"]
                    if r.get("imgs"):
                        prev["imgs"] = sorted(set(prev.get("imgs", []) + r["imgs"]))
                    if not prev.get("date_label") and r.get("dateLabel"):
                        prev["date_label"] = r["dateLabel"]
            wall = bool(page.query_selector('input[name="email"]')) or (
                not logged_out and not _logged_in(ctx))
            rounds_done += 1
            wall_rounds += 1 if wall else 0
            rounds_with_rows += 1 if rows else 0
            log.info("round %2d: dom_rows=%d new=%d total=%d%s",
                     i, len(rows), new, len(collected), "  [WALL?]" if wall else "")
            # ⚠️ FB 虛擬化（DOM 只留數篇滑動視窗）+ 長文 see-more 展開後單篇極高。策略：
            #   - 未到底：逐視窗(0.85 高)往下步進，讓每篇在視窗停留夠久被 see-more 展開 + 抽取。
            #   - 已到底：硬跳到 scrollHeight 強制觸發下一批無限捲載入（溫和步進不會觸發）。
            # 結束條件＝已到底、硬跳後頁面不再增長、且無新貼文，連續 max_stall 輪。
            try:
                m0 = page.evaluate("() => ({sh: document.documentElement.scrollHeight,"
                                   " y: window.scrollY, ih: window.innerHeight})")
            except Exception:
                m0 = {"sh": prev_sh, "y": 0, "ih": 900}
            near_bottom = (m0["y"] + m0["ih"]) >= (m0["sh"] - 500)
            if near_bottom:
                page.evaluate("window.scrollTo(0, document.documentElement.scrollHeight)")
                page.wait_for_timeout(2600)
            else:
                page.evaluate("window.scrollBy(0, Math.round(window.innerHeight * 0.85))")
                page.wait_for_timeout(1500)
            try:
                m1 = page.evaluate("() => ({sh: document.documentElement.scrollHeight,"
                                   " y: window.scrollY, ih: window.innerHeight})")
            except Exception:
                m1 = m0
            grew = m1["sh"] > prev_sh + 300
            at_bottom = (m1["y"] + m1["ih"]) >= (m1["sh"] - 500)
            prev_sh = max(prev_sh, m1["sh"])
            progress = (new > 0) or grew or (not at_bottom)
            stall = 0 if progress else stall + 1
            if stall >= max_stall:
                log.info("已到頁面底部且停止增長 %d 輪 → 結束", max_stall)
                break
        else:
            # 迴圈跑完 max_rounds 卻沒觸發 stall 收尾 = 還沒捲到底就被上限截斷。
            # 舊版在這裡無聲落地並 return 0，與正常收工完全無法區分：實測全量需
            # 473 輪，而 App 按鈕吃預設 400，第 400 輪時只有 177/208 篇，畫面卻顯示
            # 綠色「完成」。截斷必須可辨識，故回 4 而非 0。
            truncated = True
            log.warning("達 --max-rounds 上限 %d 但頁面尚未捲到底（stall=%d < %d）"
                        " → 本輪為 **不完整** 抓取，已收 %d 篇；請提高 --max-rounds "
                        "後重跑（增量 merge，不會重抓已有的）",
                        max_rounds, stall, max_stall, len(collected))
        ctx.close()

    # 沒抽到任何貼文 = 選擇器全落空。這不是「沒有新文章」，而是抓取本身壞了：
    # FB DOM 改版、或整輪都撞在登入牆上。舊版這兩種情況都回 0，UI 顯示綠色「完成」。
    if rounds_done and rounds_with_rows == 0:
        reason = ("整輪都偵測到登入牆" if wall_rounds == rounds_done
                  else "DOM 選擇器全部落空（FB 版面可能改版）")
        log.error("抓取失敗：%d 輪內一篇貼文都沒抽到 —— %s。既有 %d 篇未被改動。",
                  rounds_done, reason, len(existing))
        return 6

    if wall_rounds:
        log.warning("有 %d/%d 輪偵測到登入牆跡象（仍抽到 %d 篇）—— 若篇數明顯偏少，"
                    "請重新登入後再抓一次", wall_rounds, rounds_done, len(collected))

    _save(collected)
    return 4 if truncated else 0


# ---------------------------------------------------------------- 落地
class CorruptSoTError(RuntimeError):
    """posts.jsonl 有無法解析的行，且接下來會整檔重寫。"""


def _load_existing(drop_corrupt: bool = False) -> dict[str, dict]:
    """讀既有 posts.jsonl。解析失敗**預設拒絕繼續**。

    這個檔案是「永久 SoT」而且被 gitignore（沒有 git 備份）。舊版對壞行
    `except Exception: pass`，不計數也不記錄，接著 `_save` 以 tmp+replace 把**整個**
    檔案重寫 —— 壞掉那行就此永久消失，原檔已被覆寫、事後無從還原
    （2026-08-02 code review）。

    `drop_corrupt=True`（CLI `--drop-corrupt-lines`）才是明示同意丟掉，且會先備份原檔。
    """
    out: dict[str, dict] = {}
    if not POSTS_JSONL.exists():
        return out
    bad = []
    for lineno, line in enumerate(POSTS_JSONL.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            out[rec["id"]] = rec
        except Exception as e:
            bad.append((lineno, f"{type(e).__name__}: {str(e)[:80]}"))
    if not bad:
        return out

    if not drop_corrupt:
        detail = "\n  ".join(f"line {n}: {msg}" for n, msg in bad[:10])
        raise CorruptSoTError(
            f"{POSTS_JSONL} 有 {len(bad)} 行無法解析。抓取會把整個檔案重寫，"
            f"照舊執行等於永久刪掉這些行（此檔為永久 SoT 且未入版控）。\n  "
            f"{detail}\n"
            f"請先人工修好那幾行；確定要捨棄請加 --drop-corrupt-lines（會先備份原檔）。")

    backup = POSTS_JSONL.with_suffix(".jsonl.bak")
    backup.write_bytes(POSTS_JSONL.read_bytes())
    log.warning("--drop-corrupt-lines：捨棄 %d 行無法解析的資料，原檔已備份到 %s",
                len(bad), backup)
    return out


def _save(collected: dict[str, dict]) -> None:
    # 依 date_label 無法穩定排序（相對時間），保留插入序即可，下游會正規化日期
    rows = list(collected.values())
    tmp = POSTS_JSONL.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.replace(POSTS_JSONL)

    manifest = {
        "count": len(rows),
        "with_permalink": sum(1 for r in rows if r.get("permalink")),
        "avg_text_len": round(sum(len(r.get("text", "")) for r in rows) / max(1, len(rows)), 1),
        "source": PAGE_URL,
    }
    MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("[OK] 寫入 %s (%d 篇) / manifest: %s", POSTS_JSONL, len(rows), manifest)


# ---------------------------------------------------------------- CLI
def main() -> int:
    ap = argparse.ArgumentParser(description="白話投資 FB 粉專抓取器")
    ap.add_argument("--login", action="store_true", help="開瀏覽器手動登入 FB（一次性）")
    ap.add_argument("--check-login", action="store_true", help="回報登入狀態（不開瀏覽器）")
    ap.add_argument("--import-cookies", metavar="FILE",
                    help="從已登入瀏覽器匯出的 cookie 檔匯入登入（遠端替代方案；支援 JSON / cookies.txt）")
    ap.add_argument("--scrape", action="store_true", help="抓取貼文（增量 merge）")
    ap.add_argument("--headful", action="store_true", help="顯示瀏覽器畫面（預設 headless）")
    ap.add_argument("--logged-out", action="store_true", help="不登入試抓（只拿牆前 ~19 篇）")
    ap.add_argument("--max-stall", type=int, default=6, help="連續幾輪無新貼文即停（預設 6）")
    ap.add_argument("--max-rounds", type=int, default=400, help="最多捲幾輪（安全上限）")
    ap.add_argument("--drop-corrupt-lines", action="store_true",
                    help="posts.jsonl 有無法解析的行時，明示同意捨棄（會先備份原檔）；"
                         "預設是拒絕執行，因為抓取會整檔重寫")
    args = ap.parse_args()

    if args.check_login:
        return check_login()
    if args.import_cookies:
        return import_cookies(args.import_cookies)
    if args.login:
        return do_login(headful=True)
    if args.scrape:
        try:
            return do_scrape(headful=args.headful, logged_out=args.logged_out,
                             max_stall=args.max_stall, max_rounds=args.max_rounds,
                             drop_corrupt=args.drop_corrupt_lines)
        except CorruptSoTError as e:
            log.error("%s", e)
            return 5
    ap.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
