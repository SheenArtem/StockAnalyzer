"""
筆記功能 view — 📒 筆記 tab (2026-06-12)

零資料庫設計：data/notes/*.md 一檔一筆記，檔名即標題，內容即 Markdown 本文。
列表排序用檔案 mtime（新→舊），改標題 = 寫新檔 + 刪舊檔（rename）。
純本地檔案 CRUD：無 API / 無 LLM / 不碰 cache_manager。
"""
import re
from datetime import datetime
from pathlib import Path

import streamlit as st

NOTES_DIR = Path(__file__).resolve().parent / 'data' / 'notes'

# Windows 檔名禁用字元
_FORBIDDEN_CHARS = r'[\\/:*?"<>|]'


# ====================================================================
#  儲存層 helpers（無 streamlit 依賴，可單元測試）
# ====================================================================

def _sanitize_filename(title: str) -> str:
    """過濾 Windows 禁用字元、收斂空白；空標題給預設名（未命名 + 時間戳）"""
    name = re.sub(_FORBIDDEN_CHARS, ' ', title or '')
    name = re.sub(r'\s+', ' ', name).strip()
    # 去尾端句點（Windows 不允許檔名結尾為 . ）
    name = name.rstrip('.')
    if not name:
        name = f"未命名 {datetime.now().strftime('%Y-%m-%d %H%M')}"
    return name


def _unique_path(title: str, exclude: Path = None) -> Path:
    """標題 → 不重複的檔案路徑；重名自動加 (2)(3)... 序號。

    exclude: 編輯既有筆記時傳原檔路徑 — 標題沒改時重存自己不算重名。
    """
    base = _sanitize_filename(title)
    path = NOTES_DIR / f"{base}.md"
    i = 2
    while path.exists() and path != exclude:
        path = NOTES_DIR / f"{base} ({i}).md"
        i += 1
    return path


def _list_notes() -> list:
    """所有筆記 Path，依修改時間新→舊"""
    NOTES_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(NOTES_DIR.glob('*.md'),
                  key=lambda p: p.stat().st_mtime, reverse=True)


def _read_note(path: Path) -> str:
    try:
        return path.read_text(encoding='utf-8')
    except Exception as e:  # 損毀/編碼問題不炸頁面，顯示錯誤讓用戶知道
        return f"（讀取失敗: {e}）"


def _save_note(title: str, content: str, old_path: Path = None) -> Path:
    """寫入筆記並回傳路徑；old_path 與新標題不同時 = rename（寫新刪舊）"""
    NOTES_DIR.mkdir(parents=True, exist_ok=True)
    new_path = _unique_path(title, exclude=old_path)
    new_path.write_text(content, encoding='utf-8')
    if old_path is not None and old_path != new_path and old_path.exists():
        old_path.unlink()
    return new_path


def _delete_note(path: Path) -> None:
    if path.exists():
        path.unlink()


# ====================================================================
#  UI
# ====================================================================

def _open_editor(target: str) -> None:
    """target: '__new__' 或既有筆記的檔名 stem"""
    st.session_state['notes_editing'] = target
    st.session_state['notes_delete_confirm'] = False


def render_notes():
    st.subheader("📒 筆記")

    col_list, col_main = st.columns([1, 3])

    # ---------------- 左欄：新增 + 搜尋 + 列表 ----------------
    with col_list:
        if st.button("➕ 新增筆記", key='notes_new_btn', width='stretch'):
            _open_editor('__new__')
            st.rerun()

        query = st.text_input("搜尋筆記", key='notes_search',
                              placeholder="🔍 搜尋標題或內文...",
                              label_visibility='collapsed')

        notes = _list_notes()
        if query:
            q = query.lower()
            notes = [p for p in notes
                     if q in p.stem.lower() or q in _read_note(p).lower()]

        stems = [p.stem for p in notes]
        selected = None
        if stems:
            # 儲存/刪除後指定要選中的筆記（radio 無 key，options 變動時吃 index 重設）
            pending = st.session_state.pop('notes_select_pending', None)
            idx = stems.index(pending) if pending in stems else 0
            selected = st.radio("筆記列表", options=stems, index=idx,
                                label_visibility='collapsed')
        else:
            st.caption("（沒有符合的筆記）" if query else "（還沒有筆記）")

    # ---------------- 右欄：編輯器 / 刪除確認 / 瀏覽 ----------------
    with col_main:
        editing = st.session_state.get('notes_editing')

        # --- 編輯 / 新增模式 ---
        if editing is not None:
            is_new = (editing == '__new__')
            if is_new:
                old_path, init_title, init_content = None, '', ''
            else:
                old_path = NOTES_DIR / f"{editing}.md"
                init_title = editing
                init_content = _read_note(old_path)

            st.markdown("##### ✏️ " + ("新增筆記" if is_new else "編輯筆記"))
            title = st.text_input("標題", value=init_title, key='notes_edit_title',
                                  placeholder="筆記標題（即檔名）")
            content = st.text_area("內容（Markdown）", value=init_content,
                                   key='notes_edit_content', height=500,
                                   placeholder="支援 Markdown 語法...")
            c1, c2, _ = st.columns([1, 1, 4])
            if c1.button("💾 儲存", key='notes_save_btn', type='primary'):
                new_path = _save_note(title, content, old_path=old_path)
                st.session_state['notes_editing'] = None
                st.session_state['notes_select_pending'] = new_path.stem
                st.rerun()
            if c2.button("取消", key='notes_cancel_btn'):
                st.session_state['notes_editing'] = None
                st.rerun()
            return

        # --- 無筆記提示 ---
        if selected is None:
            st.info("點左側「➕ 新增筆記」建立第一則筆記。")
            return

        note_path = NOTES_DIR / f"{selected}.md"

        # --- 刪除確認（兩段式防誤刪） ---
        if st.session_state.get('notes_delete_confirm'):
            st.error(f"確定刪除「{selected}」？此操作無法復原。")
            c1, c2, _ = st.columns([1, 1, 4])
            if c1.button("確定刪除", key='notes_del_yes', type='primary'):
                _delete_note(note_path)
                st.session_state['notes_delete_confirm'] = False
                st.rerun()
            if c2.button("取消", key='notes_del_no'):
                st.session_state['notes_delete_confirm'] = False
                st.rerun()
            return

        # --- 瀏覽模式 ---
        c1, c2, c3 = st.columns([1, 1, 4])
        if c1.button("✏️ 編輯", key='notes_edit_btn'):
            _open_editor(selected)
            st.rerun()
        if c2.button("🗑 刪除", key='notes_del_btn'):
            st.session_state['notes_delete_confirm'] = True
            st.rerun()
        if note_path.exists():
            mtime = datetime.fromtimestamp(note_path.stat().st_mtime)
            c3.caption(f"最後修改：{mtime.strftime('%Y-%m-%d %H:%M')}")

        st.markdown("---")
        st.markdown(_read_note(note_path))
