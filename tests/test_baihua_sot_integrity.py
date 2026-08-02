"""白話投資 SoT 完整性：壞行不可靜默吞掉。

2026-08-02 code review 第五節。三處嚴重度不同，修法也不同：

- `fetch_baihua_fb._load_existing`：**破壞性** —— 讀完後 `_save` 會以 tmp+replace 把
  整個 `posts.jsonl` 重寫，壞行就此永久消失，而該檔是「永久 SoT」且被 gitignore
  （沒有 git 備份）。→ 預設拒絕執行。
- `build_baihua_kb.load_posts`：只讀不寫，但壞行 = **靜默漏掉一篇文章**。→ 計數並警告。
- `build_baihua_kb.load_state`：壞掉回 `{}` 會讓 206 篇全部重跑 Sonnet（真金白銀），
  且 :433 會用新 state 覆寫舊檔。→ raise。
"""
import json

import pytest

from tools import build_baihua_kb as BK
from tools import fetch_baihua_fb as FB


def _write_jsonl(path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def _good(pid, text='內文'):
    return json.dumps({'id': pid, 'text': text, 'permalink': None,
                       'date_label': None, 'imgs': []}, ensure_ascii=False)


# --------------------------------------------------------------------------- #
#  fetch_baihua_fb._load_existing —— 破壞性，預設拒絕
# --------------------------------------------------------------------------- #

def test_load_existing_refuses_when_a_line_is_unparseable(tmp_path, monkeypatch):
    posts = tmp_path / 'posts.jsonl'
    _write_jsonl(posts, [_good('a'), '{"id": "b", "text": ', _good('c')])
    monkeypatch.setattr(FB, 'POSTS_JSONL', posts)

    with pytest.raises(FB.CorruptSoTError) as exc:
        FB._load_existing()

    assert 'line 2' in str(exc.value)
    assert '--drop-corrupt-lines' in str(exc.value)


def test_load_existing_leaves_the_file_untouched_when_refusing(tmp_path, monkeypatch):
    posts = tmp_path / 'posts.jsonl'
    _write_jsonl(posts, [_good('a'), 'garbage{'])
    monkeypatch.setattr(FB, 'POSTS_JSONL', posts)
    before = posts.read_bytes()

    with pytest.raises(FB.CorruptSoTError):
        FB._load_existing()

    assert posts.read_bytes() == before, '拒絕時不得改動原檔'
    assert not posts.with_suffix('.jsonl.bak').exists(), '沒被要求捨棄就不該備份'


def test_drop_corrupt_lines_backs_up_before_discarding(tmp_path, monkeypatch):
    posts = tmp_path / 'posts.jsonl'
    _write_jsonl(posts, [_good('a'), 'garbage{', _good('c')])
    monkeypatch.setattr(FB, 'POSTS_JSONL', posts)
    original = posts.read_bytes()

    out = FB._load_existing(drop_corrupt=True)

    assert sorted(out) == ['a', 'c']
    backup = posts.with_suffix('.jsonl.bak')
    assert backup.exists() and backup.read_bytes() == original, \
        '明示捨棄前必須先備份，否則資料一樣是永久消失'


def test_load_existing_clean_file_needs_no_flag(tmp_path, monkeypatch):
    posts = tmp_path / 'posts.jsonl'
    _write_jsonl(posts, [_good('a'), '', '   ', _good('b')])
    monkeypatch.setattr(FB, 'POSTS_JSONL', posts)

    out = FB._load_existing()

    assert sorted(out) == ['a', 'b'], '空白行本來就該略過，不算壞行'


def test_missing_file_is_not_an_error(tmp_path, monkeypatch):
    monkeypatch.setattr(FB, 'POSTS_JSONL', tmp_path / 'absent.jsonl')
    assert FB._load_existing() == {}


def test_scrape_refuses_before_opening_a_browser(tmp_path, monkeypatch):
    """壞行必須在花掉任何抓取成本（開瀏覽器）之前就擋下。"""
    posts = tmp_path / 'posts.jsonl'
    _write_jsonl(posts, ['garbage{'])
    monkeypatch.setattr(FB, 'POSTS_JSONL', posts)
    monkeypatch.setattr(FB, 'OUT_DIR', tmp_path)

    with pytest.raises(FB.CorruptSoTError):
        # playwright 的 import 在函式內，若真的走到那步這裡會是別的錯誤
        FB.do_scrape(headful=False, logged_out=True, max_stall=1, max_rounds=1)


def test_scrape_cli_returns_5_on_corrupt_sot(tmp_path, monkeypatch, capsys):
    posts = tmp_path / 'posts.jsonl'
    _write_jsonl(posts, ['garbage{'])
    monkeypatch.setattr(FB, 'POSTS_JSONL', posts)
    monkeypatch.setattr(FB, 'OUT_DIR', tmp_path)
    monkeypatch.setattr(FB.sys, 'argv', ['fetch_baihua_fb.py', '--scrape'])

    assert FB.main() == 5, 'SoT 壞掉要有專屬 exit code，不可與正常完成同回 0'


# --------------------------------------------------------------------------- #
#  build_baihua_kb —— 漏文章要計數；壞 state 要 raise
# --------------------------------------------------------------------------- #

def test_load_posts_warns_about_skipped_articles(tmp_path, monkeypatch, caplog):
    raw = tmp_path / 'posts.jsonl'
    _write_jsonl(raw, [_good('a'), 'garbage{', _good('c')])
    monkeypatch.setattr(BK, 'RAW_JSONL', raw)

    with caplog.at_level('WARNING'):
        rows = BK.load_posts()

    assert [r['id'] for r in rows] == ['a', 'c']
    assert '1 行無法解析' in caplog.text
    assert 'line 2' in caplog.text


def test_load_posts_silent_when_clean(tmp_path, monkeypatch, caplog):
    raw = tmp_path / 'posts.jsonl'
    _write_jsonl(raw, [_good('a'), _good('b')])
    monkeypatch.setattr(BK, 'RAW_JSONL', raw)

    with caplog.at_level('WARNING'):
        rows = BK.load_posts()

    assert len(rows) == 2
    assert '無法解析' not in caplog.text


def test_load_state_raises_instead_of_pretending_nothing_processed(tmp_path, monkeypatch):
    state = tmp_path / '.processed.json'
    state.write_text('{"a": "0001_x.md", ', encoding='utf-8')   # 截斷
    monkeypatch.setattr(BK, 'STATE', state)

    with pytest.raises(RuntimeError, match='無法解析'):
        BK.load_state()


def test_load_state_absent_is_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(BK, 'STATE', tmp_path / 'absent.json')
    assert BK.load_state() == {}


def test_load_state_valid_roundtrip(tmp_path, monkeypatch):
    state = tmp_path / '.processed.json'
    state.write_text(json.dumps({'a': '0001_x.md'}), encoding='utf-8')
    monkeypatch.setattr(BK, 'STATE', state)

    assert BK.load_state() == {'a': '0001_x.md'}
