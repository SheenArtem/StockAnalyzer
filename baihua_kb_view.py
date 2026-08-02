"""
白話投資 知識庫 view — 📚 知識庫 tab 內的「白話投資」來源 (2026-07-16)

- 內建「抓取新文章」按鈕：跑 tools/fetch_baihua_fb.py --scrape（增量 merge，跳過已抓 id）
  → tools/build_baihua_kb.py（增量，跳過已清洗 id）。重複點只處理真正新增的文章。
- 首次使用需登入 FB 一次：按鈕跑 --login 開瀏覽器，cookie 存 local/baihua_fb_profile/。
- 文章清單 + 唯讀檢視：讀 data/baihua/kb/*.md（frontmatter + 正文 + 原始擷取摺疊）。

抓取/整理是子行程（Playwright / claude CLI 不宜在 Streamlit ScriptRunner 執行緒內跑），
以 st.spinner 阻塞等待。純本地檔案讀取，無 API / 不碰 cache_manager / 不觸發大盤 banner。
"""
import json
import logging
import re
import subprocess
import sys
from pathlib import Path

import streamlit as st

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent
KB_DIR = REPO / 'data' / 'baihua' / 'kb'
RAW_JSONL = REPO / 'data' / 'baihua' / 'raw' / 'posts.jsonl'
MANIFEST = REPO / 'data' / 'baihua' / 'raw' / 'manifest.json'
PROFILE_DIR = REPO / 'local' / 'baihua_fb_profile'
FETCH = REPO / 'tools' / 'fetch_baihua_fb.py'
BUILD = REPO / 'tools' / 'build_baihua_kb.py'

_SKIP = {'INDEX.md', 'THEMES.md'}

# 子行程 timeout（秒）
_LOGIN_TIMEOUT = 660      # do_login 輪詢 c_user 最多 600s，須大於它
_SCRAPE_TIMEOUT = 1800    # 首次全量捲時間軸可能數分鐘
_BUILD_TIMEOUT = 3600     # 首次全量 LLM 清洗（每篇 Sonnet），增量則很快


# ====================================================================
#  子行程 runner
# ====================================================================

def _run(args: list, timeout: int) -> tuple[int, str]:
    """跑 tools 腳本，回 (returncode, stdout+stderr)。timeout 回 rc=-1。"""
    try:
        proc = subprocess.run(
            [sys.executable, *args], cwd=str(REPO),
            capture_output=True, text=True, encoding='utf-8', errors='replace',
            timeout=timeout,
        )
        return proc.returncode, (proc.stdout or '') + (proc.stderr or '')
    except subprocess.TimeoutExpired as e:
        return -1, f"逾時（{timeout}s）\n{e.stdout or ''}\n{e.stderr or ''}"
    except Exception as e:
        return -2, f"執行失敗：{e}"


def _has_login() -> bool:
    """讀 profile Cookies SQLite 判斷是否真的登入（c_user + xs 都在）。
    只看目錄非空會誤判——失敗的登入嘗試也會留下 pre-login cookie(datr/sb/fr…)。"""
    import sqlite3
    db = PROFILE_DIR / "Default" / "Network" / "Cookies"
    if not db.exists():
        return False
    uri = "file:" + db.as_posix() + "?mode=ro&immutable=1"
    try:
        con = sqlite3.connect(uri, uri=True)
        try:
            rows = con.execute(
                "SELECT name FROM cookies WHERE host_key LIKE '%facebook%'").fetchall()
        finally:
            con.close()
        names = {r[0] for r in rows}
        return "c_user" in names and "xs" in names
    except Exception:
        return False


def _kb_count() -> int:
    if not KB_DIR.exists():
        return 0
    return sum(1 for p in KB_DIR.glob('*.md') if p.name not in _SKIP)


def _raw_count() -> int:
    if MANIFEST.exists():
        try:
            return int(json.loads(MANIFEST.read_text(encoding='utf-8')).get('count', 0))
        except Exception:
            pass
    return 0


# ====================================================================
#  md 解析
# ====================================================================

def _parse_md(md: str) -> tuple[dict, str, str]:
    """回 (frontmatter dict, 主內文, 原始擷取 raw)。"""
    fm: dict = {}
    body = md
    m = re.match(r'^---\r?\n(.*?)\r?\n---\r?\n(.*)$', md, re.DOTALL)
    if m:
        for line in m.group(1).splitlines():
            if ':' in line:
                k, v = line.split(':', 1)
                fm[k.strip()] = v.strip()
        body = m.group(2)
    raw = ''
    idx = body.find('<details>')
    if idx != -1:
        main = body[:idx].rstrip()
        dm = re.search(r'````text\r?\n(.*?)\r?\n````', body[idx:], re.DOTALL)
        if dm:
            raw = dm.group(1)
    else:
        main = body
    return fm, main, raw


def _fm_list(fm: dict, key: str) -> list:
    """frontmatter 的 [a, b, c] 字串 → list。"""
    v = fm.get(key, '')
    v = v.strip().lstrip('[').rstrip(']').strip()
    return [x.strip() for x in v.split(',') if x.strip()] if v else []


def _list_articles() -> list:
    """回 [(path, fm)]，依 date_iso 新→舊（無日期用檔名序墊底）。"""
    KB_DIR.mkdir(parents=True, exist_ok=True)
    out = []
    for p in KB_DIR.glob('*.md'):
        if p.name in _SKIP:
            continue
        try:
            fm, _, _ = _parse_md(p.read_text(encoding='utf-8'))
        except Exception:
            fm = {}
        out.append((p, fm))
    # 檔名 NNNN_ 前綴＝抓取順序＝新→舊（FB feed 由上而下＝最新在前，已驗證）。升序=最新在上。
    out.sort(key=lambda t: t[0].name)
    return out


# ====================================================================
#  抓取動作
# ====================================================================

def _do_scrape_and_build() -> None:
    """抓取 + 建庫（增量）。結果寫入 session_state 供 rerun 後顯示。"""
    with st.spinner("抓取文章中…（首次或大量更新可能數分鐘，請勿關閉）"):
        # --max-rounds 必須明給：CLI 預設 400，但實測首次全量需 473 輪（第 400 輪
        # 時只有 177/208 篇），照預設會靜默少抓最舊的約 30 篇。實測每輪中位數 1.70s
        # ／最壞 3.57s，700 輪約 1300s，仍在 _SCRAPE_TIMEOUT=1800s 內。
        rc, out = _run([str(FETCH), '--scrape', '--max-stall', '8',
                        '--max-rounds', '700'], _SCRAPE_TIMEOUT)
    if rc == 2:
        st.session_state['baihua_msg'] = ('warn',
            "尚未登入 Facebook。請先點下方「登入 Facebook」完成一次性登入，再抓取。")
        return
    if rc == 4:
        # 撞到輪數上限：資料有落地但不完整，不可顯示綠色「完成」。
        st.session_state['baihua_msg'] = ('warn',
            "抓取**不完整**：撞到捲動輪數上限，最舊的文章可能沒抓到。"
            "已抓到的部分已存檔，再按一次「抓取新文章」可繼續往下捲（增量，不會重抓）。")
        return
    if rc == 6:
        # 一篇都沒抽到＝抓取本身壞了，不是「沒有新文章」。
        st.session_state['baihua_msg'] = ('error',
            "抓取失敗：**一篇貼文都沒抽到**。可能是 Facebook 版面改版讓選擇器失效，"
            "或登入已失效。既有文章未被改動。\n\n" + out[-1500:])
        return
    if rc != 0:
        st.session_state['baihua_msg'] = ('error', f"抓取失敗（rc={rc}）：\n{out[-1500:]}")
        return
    with st.spinner("整理知識庫中…（清洗新文章，跳過已整理的）"):
        rc2, out2 = _run([str(BUILD), '--concurrency', '4'], _BUILD_TIMEOUT)
    if rc2 == -1:
        # 逾時不等於白做工：build 現在逐篇原子寫 checkpoint，已清洗的不會重跑。
        # 實測換機首次全量約需 4,357s（206 篇 / conc 4）> _BUILD_TIMEOUT 3,600s，
        # 所以首次全量「按兩三次」是預期流程，不是故障。
        st.session_state['baihua_msg'] = ('warn',
            f"整理逾時（{_BUILD_TIMEOUT}s）—— **已清洗的文章都已存檔**，"
            f"再按一次「抓取新文章」會從中斷處繼續（首次全量約需按 2 次）。"
            f"目前知識庫共 {_kb_count()} 篇。")
        return
    if rc2 not in (0, 3):   # 3 = 部分失敗但有產出
        st.session_state['baihua_msg'] = ('error', f"整理失敗（rc={rc2}）：\n{out2[-1500:]}")
        return
    done = re.search(r'\[DONE\] ok=(\d+) fail=(\d+) .*?=(\d+)', out2)
    if done:
        ok, fail, total = done.groups()
        msg = f"完成：新整理 {ok} 篇" + (f"（{fail} 篇失敗）" if int(fail) else "") + f"，知識庫共 {total} 篇。"
    else:
        msg = f"完成。知識庫共 {_kb_count()} 篇。"
    st.session_state['baihua_msg'] = ('success', msg)


def _do_login() -> None:
    with st.spinner("已在**執行 App 這台機器**開啟瀏覽器，請在該視窗完成登入…（最多 10 分鐘）"):
        rc, out = _run([str(FETCH), '--login'], _LOGIN_TIMEOUT)
    if rc == 0:
        st.session_state['baihua_msg'] = ('success', "登入成功，cookie 已存本機。現在可以抓取文章了。")
    else:
        st.session_state['baihua_msg'] = ('error', f"登入未完成（rc={rc}）：\n{out[-1200:]}")


def _do_import_cookies(text: str) -> None:
    """把貼上的 cookie 內容寫本機暫存檔 → 匯入 profile → 立即刪暫存檔（token 不留多餘副本）。"""
    text = (text or "").strip()
    if not text:
        st.session_state['baihua_msg'] = ('warn', "請先貼上 cookie 內容。")
        return
    tmp = REPO / 'local' / '_fb_cookie_import.tmp'
    tmp.parent.mkdir(parents=True, exist_ok=True)
    unlink_error = None
    try:
        tmp.write_text(text, encoding='utf-8')
        with st.spinner("匯入 cookie 中…"):
            rc, out = _run([str(FETCH), '--import-cookies', str(tmp)], 180)
    finally:
        # 「即用即刪」是對使用者的安全承諾（docstring 與 UI caption 都這樣寫）。
        # 舊版 `except Exception: pass` 會讓它靜默變成假承諾 —— cookie 檔留在磁碟上
        # 而畫面照樣說已刪除。`local/` 有被 gitignore 所以不會外洩到 repo，但仍要講。
        try:
            tmp.unlink(missing_ok=True)
        except Exception as e:
            unlink_error = f"{type(e).__name__}: {e}"
            logger.warning("cookie 暫存檔刪除失敗 %s: %s", tmp, e)
    if unlink_error:
        st.session_state['baihua_msg'] = ('warn',
            f"cookie 已匯入，但**暫存檔刪不掉**：`{tmp}`（{unlink_error}）。"
            "該檔含登入 token，請自行手動刪除。")
        return
    if rc == 0:
        st.session_state['baihua_msg'] = ('success', "cookie 匯入成功，已登入。現在可以抓取文章了。")
    else:
        st.session_state['baihua_msg'] = ('error', f"匯入失敗（rc={rc}）：\n{out[-1200:]}")


# ====================================================================
#  UI
# ====================================================================

def render_baihua_kb():
    st.caption("來源：Facebook 粉專「白話投資」。文章原文與整理僅存本機（不入版控）。")

    # --- 上一次動作的結果訊息 ---
    msg = st.session_state.pop('baihua_msg', None)
    if msg:
        {'success': st.success, 'warn': st.warning, 'error': st.error}.get(msg[0], st.info)(msg[1])

    # --- 抓取列 ---
    c1, c2, c3 = st.columns([2, 2, 3])
    if c1.button("🔄 抓取新文章（跳過已抓）", key='baihua_fetch', type='primary', width='stretch'):
        _do_scrape_and_build()
        st.rerun()
    if not _has_login():
        if c2.button("🔓 登入 Facebook（開瀏覽器）", key='baihua_login', width='stretch'):
            _do_login()
            st.rerun()
    else:
        if c2.button("✅ 已登入（點此重新登入）", key='baihua_relogin', width='stretch',
                     help="cookie 已存；若失效可再登入一次"):
            _do_login()
            st.rerun()
    c3.caption(f"📚 知識庫 {_kb_count()} 篇　·　📥 已擷取 {_raw_count()} 篇原文")

    if not _has_login():
        st.info("首次使用請先登入一次。登入視窗會開在**執行 App 的那台機器**（server 端），"
                "不是你現在瀏覽的電腦。若你是遠端連線，請改用下方「貼 cookie 登入」。")

    # 遠端登入替代方案：貼上已登入瀏覽器匯出的 cookie（未登入時預設展開，方便發現）
    with st.expander("🔑 用 cookie 登入（遠端／不想在 server 開視窗時用）",
                     expanded=not _has_login()):
        st.caption(
            "在你**已登入 Facebook** 的瀏覽器取得 cookie 貼到下方，三種格式皆可：\n"
            "1. **開發者工具（免裝擴充）**：F12 → Network 分頁 → 重整頁面 → 點任一 "
            "`facebook.com` 請求 → Headers → Request Headers → 複製 **Cookie:** 整串貼上。\n"
            "2. Cookie-Editor 擴充 → Export as JSON。\n"
            "3. 『Get cookies.txt LOCALLY』擴充匯出的 cookies.txt。\n"
            "⚠️ 不要用 Console 的 `document.cookie`——登入必需的 `xs` 是 HttpOnly，那樣讀不到。\n"
            "cookie 是你自己的 session，只寫入本機 profile、可隨時在 FB 登出撤銷；貼上即用即刪，不留存於此欄。")
        cookie_text = st.text_area("貼上 cookie", height=120,
                                   key='baihua_cookie_paste', label_visibility='collapsed',
                                   placeholder='貼 Cookie 標頭字串（c_user=...; xs=...; datr=...）或 JSON 陣列或 cookies.txt')
        if st.button("匯入並登入", key='baihua_cookie_import'):
            _do_import_cookies(cookie_text)
            st.session_state.pop('baihua_cookie_paste', None)
            st.rerun()

    st.markdown("---")

    articles = _list_articles()
    if not articles:
        st.info("尚無文章。登入後點「🔄 抓取新文章」即可建立知識庫。")
        return

    # --- 主題篩選 ---
    all_themes = sorted({t for _, fm in articles for t in _fm_list(fm, 'themes')})
    picked = st.multiselect("依主題篩選", options=all_themes, default=[],
                            key='baihua_theme_filter', placeholder="（全部主題）")
    if picked:
        articles = [(p, fm) for p, fm in articles
                    if set(_fm_list(fm, 'themes')) & set(picked)]

    col_list, col_main = st.columns([1, 3])

    with col_list:
        query = st.text_input("搜尋文章", key='baihua_search',
                              placeholder="🔍 搜尋標題...", label_visibility='collapsed')
        rows = articles
        if query:
            q = query.lower()
            rows = [(p, fm) for p, fm in rows if q in (fm.get('title', p.stem)).lower()]
        labels, path_by_label = [], {}
        for p, fm in rows:
            d = fm.get('date_iso') or fm.get('date_label') or ''
            title = fm.get('title') or p.stem
            lbl = f"{d}｜{title}" if d else title
            # radio 選項需唯一
            if lbl in path_by_label:
                lbl = f"{lbl}（{p.stem[:4]}）"
            labels.append(lbl)
            path_by_label[lbl] = p
        if labels:
            sel = st.radio(f"文章（{len(labels)}）", options=labels, label_visibility='collapsed')
            sel_path = path_by_label.get(sel)
        else:
            st.caption("（沒有符合的文章）")
            sel_path = None

    with col_main:
        if sel_path is None:
            st.info("左側選一篇文章檢視。")
            return
        try:
            fm, main, raw = _parse_md(sel_path.read_text(encoding='utf-8'))
        except Exception as e:
            st.error(f"讀取失敗：{e}")
            return
        themes = _fm_list(fm, 'themes')
        tickers = _fm_list(fm, 'tickers')
        meta_bits = []
        if fm.get('date_iso') or fm.get('date_label'):
            meta_bits.append("🗓 " + (fm.get('date_iso') or fm.get('date_label')))
        if themes:
            meta_bits.append("🏷 " + " / ".join(themes))
        if tickers:
            meta_bits.append("📈 " + " ".join(tickers))
        if meta_bits:
            st.caption("　·　".join(meta_bits))
        # 拆出「## 原文」段：標題/摘要/重點直接顯示；原文與原始擷取各自摺疊
        head, _, body = main.partition('## 原文')
        head = re.sub(r'\r?\n##\s*$', '', head).rstrip()
        body = body.strip()
        if head:
            st.markdown(head)
        if body:
            with st.expander("📄 原文（清洗後）", expanded=False):
                # 還原段落換行：markdown 會吃掉單一 \n，補成空行避免糊成一整坨
                st.markdown(re.sub(r'\n(?!\n)', '\n\n', body))
        if raw:
            with st.expander("📄 原始擷取（未清洗，逐字保留）", expanded=True):
                st.text(raw)
        src = fm.get('source')
        if src:
            st.markdown(f"[🔗 FB 原文連結]({src})")
