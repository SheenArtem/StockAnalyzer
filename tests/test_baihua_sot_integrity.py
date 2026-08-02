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


# --------------------------------------------------------------------------- #
#  逐篇 checkpoint —— 逾時不可讓整輪 LLM 成本歸零
# --------------------------------------------------------------------------- #

def _record(pid, seq, name):
    return {'seq': seq, 'id': pid, 'file': name, 'title': f't{seq}',
            'date_iso': '', 'date_label': None, 'themes': [], 'one_liner': '',
            'permalink': None, 'text_len': 100}


def test_save_state_is_atomic_and_roundtrips(tmp_path, monkeypatch):
    """寫到一半被殺會留下截斷 JSON，而 load_state 現在對壞檔會 raise ——
    那會讓下次直接卡死，所以必須 tmp + replace。"""
    state = tmp_path / '.processed.json'
    monkeypatch.setattr(BK, 'STATE', state)

    BK.save_state([_record('a', 1, '0001_a.md'), _record('b', 2, '0002_b.md')])

    assert not state.with_suffix('.json.tmp').exists(), 'tmp 檔應已被 replace 掉'
    out = BK.load_state()
    assert out['a'] == '0001_a.md' and out['b'] == '0002_b.md'
    assert len(out['__records__']) == 2


def test_save_state_dedupes_by_id_keeping_last(tmp_path, monkeypatch):
    monkeypatch.setattr(BK, 'STATE', tmp_path / '.processed.json')

    BK.save_state([_record('a', 1, 'old.md'), _record('a', 1, 'new.md')])

    out = BK.load_state()
    assert out['a'] == 'new.md'
    assert len(out['__records__']) == 1


def test_save_state_called_per_article_not_only_at_the_end(tmp_path, monkeypatch):
    """實測換機首次全量約需 4,357s（206 篇 / conc 4）> App build timeout 3,600s，
    所以「只在最後寫一次」必然踩到：md 都在、STATE 沒記錄 -> 下次全部重跑。
    """
    saves = []
    monkeypatch.setattr(BK, 'save_state', lambda recs: saves.append(len(recs)))
    monkeypatch.setattr(BK, 'build_index', lambda recs: None)
    monkeypatch.setattr(BK, 'load_posts', lambda: [
        {'id': 'a', 'text': 'x' * 50}, {'id': 'b', 'text': 'y' * 50},
        {'id': 'c', 'text': 'z' * 50}])
    monkeypatch.setattr(BK, 'load_state', lambda: {})
    monkeypatch.setattr(BK, 'process_one',
                        lambda seq, rec, dry: (_record(rec['id'], seq, f'{seq:04d}.md'), None))
    monkeypatch.setattr(BK.sys, 'argv',
                        ['build_baihua_kb.py', '--concurrency', '1'])

    assert BK.main() == 0
    # 3 篇 -> 逐篇 3 次 + 收尾 1 次
    assert len(saves) == 4, f'應逐篇 checkpoint，實際只存了 {len(saves)} 次'
    assert saves[:3] == [1, 2, 3], '每次都要把累積結果寫進去'


def test_dry_run_writes_nothing_at_all(tmp_path, monkeypatch):
    """--dry-run 搭 --rebuild 時 records 全是 stub，寫索引會把假標題蓋進 INDEX。"""
    calls = []
    monkeypatch.setattr(BK, 'save_state', lambda recs: calls.append('state'))
    monkeypatch.setattr(BK, 'build_index', lambda recs: calls.append('index'))
    monkeypatch.setattr(BK, 'load_posts', lambda: [{'id': 'a', 'text': 'x' * 50}])
    monkeypatch.setattr(BK, 'load_state', lambda: {})
    monkeypatch.setattr(BK.sys, 'argv',
                        ['build_baihua_kb.py', '--dry-run', '--rebuild'])

    assert BK.main() == 0
    assert calls == [], f'dry-run 不該寫任何檔案，實際呼叫了 {calls}'


def test_process_one_dry_run_does_not_touch_the_filesystem(tmp_path, monkeypatch):
    monkeypatch.setattr(BK, 'KB_DIR', tmp_path / 'kb')
    monkeypatch.setattr(BK, 'META_DIR', tmp_path / 'meta')
    monkeypatch.setattr(BK, 'write_md',
                        lambda *a: pytest.fail('dry-run 不該寫 md'))

    rec, err = BK.process_one(1, {'id': 'a', 'text': 'x' * 50}, True)

    assert err is None
    assert rec['file'].startswith('0001_'), '仍要回報預期檔名供索引預覽'
    assert not (tmp_path / 'meta').exists()
    assert not (tmp_path / 'kb').exists()


# --------------------------------------------------------------------------- #
#  抓取端：一篇都沒抽到 = 壞了，不是「沒有新文章」
# --------------------------------------------------------------------------- #

def test_scrape_exit_code_6_is_documented():
    """rc=6 必須與 0 區分 —— 舊版 DOM 改版時回 0，UI 顯示綠色「完成：新整理 0 篇」。"""
    assert '6' in FB.__doc__ and '一篇貼文都沒抽到' in FB.__doc__
