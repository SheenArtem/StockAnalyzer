"""notes_view 儲存層測試 — CRUD round-trip / rename / 重名序號 / sanitize / 空標題

UI (render_notes) 不在 cover 範圍（見 tests/README.md 設計原則 3），
只測會弄丟/蓋掉使用者筆記的純檔案邏輯。
"""
import notes_view as nv


def _patch_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(nv, 'NOTES_DIR', tmp_path)


def test_save_read_roundtrip(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    p = nv._save_note("測試筆記 A", "# hello\n中文內容")
    assert p.exists()
    assert p.stem == "測試筆記 A"
    assert nv._read_note(p) == "# hello\n中文內容"


def test_list_sorted_by_mtime_desc(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    import os
    p1 = nv._save_note("舊筆記", "old")
    p2 = nv._save_note("新筆記", "new")
    # 強制 mtime 順序（同一秒內寫入時 glob 順序不穩定）
    os.utime(p1, (1000000000, 1000000000))
    os.utime(p2, (2000000000, 2000000000))
    stems = [p.stem for p in nv._list_notes()]
    assert stems == ["新筆記", "舊筆記"]


def test_rename_via_save_deletes_old(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    p_old = nv._save_note("原標題", "內容")
    p_new = nv._save_note("新標題", "內容改", old_path=p_old)
    assert not p_old.exists()
    assert p_new.stem == "新標題"
    assert nv._read_note(p_new) == "內容改"


def test_resave_same_title_no_suffix(tmp_path, monkeypatch):
    """標題沒改重存自己，不可長出 (2) 序號"""
    _patch_dir(tmp_path, monkeypatch)
    p1 = nv._save_note("同標題", "v1")
    p2 = nv._save_note("同標題", "v2", old_path=p1)
    assert p2 == p1
    assert nv._read_note(p2) == "v2"
    assert len(nv._list_notes()) == 1


def test_duplicate_title_gets_suffix(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    p1 = nv._save_note("撞名", "a")
    p2 = nv._save_note("撞名", "b")
    p3 = nv._save_note("撞名", "c")
    assert p1.stem == "撞名"
    assert p2.stem == "撞名 (2)"
    assert p3.stem == "撞名 (3)"
    # 內容互不覆蓋
    assert nv._read_note(p1) == "a"
    assert nv._read_note(p2) == "b"


def test_sanitize_forbidden_chars(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    p = nv._save_note('a/b\\c:d*e?f"g<h>i|j', "x")
    assert p.exists()
    for ch in '\\/:*?"<>|':
        assert ch not in p.name.replace('.md', '')


def test_sanitize_trailing_dot_and_spaces(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    assert nv._sanitize_filename("  標題...  ") == "標題"
    assert nv._sanitize_filename("a    b") == "a b"


def test_empty_title_gets_default_name(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    p = nv._save_note("", "內容")
    assert p.exists()
    assert p.stem.startswith("未命名")
    p2 = nv._save_note("   ", "內容2")
    assert p2.exists()


def test_delete(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    p = nv._save_note("要刪的", "x")
    nv._delete_note(p)
    assert not p.exists()
    # 刪不存在的不噴錯
    nv._delete_note(p)
    assert nv._list_notes() == []
