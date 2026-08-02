"""增量抓取不可破壞「最新在前」的排序。

2026-08-02 code review 第五節：位置語意是「越前面越新」（FB feed 由上而下＝最新在前），
但增量抓取的新貼文被 dict 插入序排到**尾端** -> 拿到最大 seq -> 在 INDEX 與 UI 清單沉到
最底，被當成最舊。加重因子：實測 208 篇中 `date_label` 非空 0 筆、`date_iso` 非空僅
4 篇，位置是唯一時序線索，人眼無法自我校正。

⚠️ 修法陷阱（報告特別警告）：**不可**把新貼文插到 JSONL 前端。
`build_baihua_kb.write_md` 開頭是 `for old in KB_DIR.glob(f"{seq:04d}_*.md"): old.unlink()`，
而 `_needs` 會跳過已處理貼文使舊檔不重新編號 —— 插前端會位移所有 seq，於是刪掉別篇
文章的檔案。正解是保留插入序（檔名穩定）+ 單調遞增的 `batch`。
"""
import json
from pathlib import Path

import pytest

from tools import build_baihua_kb as BK
from tools import fetch_baihua_fb as FB


def _post(pid, text='內文', batch=None):
    d = {'id': pid, 'permalink': None, 'date_label': None, 'text': text, 'imgs': []}
    if batch is not None:
        d['batch'] = batch
    return d


@pytest.fixture()
def paths(tmp_path, monkeypatch):
    posts = tmp_path / 'posts.jsonl'
    manifest = tmp_path / 'manifest.json'
    monkeypatch.setattr(FB, 'POSTS_JSONL', posts)
    monkeypatch.setattr(FB, 'MANIFEST', manifest)
    return posts, manifest


def _read(posts: Path) -> list:
    return [json.loads(x) for x in posts.read_text(encoding='utf-8').splitlines() if x.strip()]


# --------------------------------------------------------------------------- #
#  batch 標記
# --------------------------------------------------------------------------- #

def test_first_full_scrape_marks_everything_batch_1(paths):
    posts, _ = paths
    collected = {f'p{i}': _post(f'p{i}') for i in range(5)}

    FB._save(collected, existing={})

    assert [r['batch'] for r in _read(posts)] == [1] * 5


def test_incremental_scrape_gets_a_higher_batch(paths):
    posts, _ = paths
    first = {f'p{i}': _post(f'p{i}') for i in range(3)}
    FB._save(first, existing={})
    existing = {r['id']: r for r in _read(posts)}

    # 增量：2 篇新的（dict 插入序在尾端，正是原始缺陷的形狀）
    second = dict(existing)
    second['new1'] = _post('new1')
    second['new2'] = _post('new2')
    FB._save(second, existing=existing)

    rows = {r['id']: r['batch'] for r in _read(posts)}
    assert rows == {'p0': 1, 'p1': 1, 'p2': 1, 'new1': 2, 'new2': 2}


def test_existing_posts_keep_their_original_batch(paths):
    posts, _ = paths
    FB._save({'a': _post('a')}, existing={})
    existing = {r['id']: r for r in _read(posts)}
    FB._save({'a': _post('a'), 'b': _post('b')}, existing=existing)
    existing2 = {r['id']: r for r in _read(posts)}

    FB._save({'a': _post('a'), 'b': _post('b'), 'c': _post('c')}, existing=existing2)

    rows = {r['id']: r['batch'] for r in _read(posts)}
    assert rows == {'a': 1, 'b': 2, 'c': 3}, '既有貼文不可被重新標批次'


def test_jsonl_insertion_order_is_preserved(paths):
    """檔名穩定的前提 —— 不可為了排序把新貼文插到前端（會刪掉別篇的 md）。"""
    posts, _ = paths
    FB._save({'a': _post('a'), 'b': _post('b')}, existing={})
    existing = {r['id']: r for r in _read(posts)}

    FB._save({**existing, 'z': _post('z')}, existing=existing)

    assert [r['id'] for r in _read(posts)] == ['a', 'b', 'z'], \
        '新貼文必須留在尾端；排序改由 batch 處理'


def test_manifest_records_max_batch(paths):
    posts, manifest = paths
    FB._save({'a': _post('a')}, existing={})
    existing = {r['id']: r for r in _read(posts)}
    FB._save({**existing, 'b': _post('b')}, existing=existing)

    assert json.loads(manifest.read_text(encoding='utf-8'))['max_batch'] == 2


# --------------------------------------------------------------------------- #
#  索引排序
# --------------------------------------------------------------------------- #

def test_index_order_puts_newer_batch_first():
    """batch 2 的文章要排在 batch 1 之前，即使它的 seq 更大。"""
    recs = [
        {'seq': 0, 'batch': 1, 'id': 'old_newest'},
        {'seq': 1, 'batch': 1, 'id': 'old_older'},
        {'seq': 2, 'batch': 2, 'id': 'brand_new'},
    ]

    ordered = sorted(recs, key=BK._order_key)

    assert [r['id'] for r in ordered] == ['brand_new', 'old_newest', 'old_older']


def test_index_order_within_a_batch_is_seq_ascending():
    recs = [{'seq': 5, 'batch': 1}, {'seq': 2, 'batch': 1}, {'seq': 9, 'batch': 1}]

    assert [r['seq'] for r in sorted(recs, key=BK._order_key)] == [2, 5, 9]


def test_index_order_treats_missing_batch_as_oldest():
    """舊檔沒有 batch 欄位 -> 視為 0，排在有批次標記的之後。"""
    recs = [{'seq': 0}, {'seq': 7, 'batch': 1}]

    assert [r['seq'] for r in sorted(recs, key=BK._order_key)] == [7, 0]


def test_pure_seq_ordering_would_bury_the_new_post():
    """釘住原始缺陷：純 seq 升序會把最新的文章排到最底。"""
    recs = [{'seq': 0, 'batch': 1, 'id': 'old'},
            {'seq': 1, 'batch': 2, 'id': 'new'}]

    by_seq = [r['id'] for r in sorted(recs, key=lambda r: r['seq'])]
    by_batch = [r['id'] for r in sorted(recs, key=BK._order_key)]

    assert by_seq == ['old', 'new'], '這是舊行為'
    assert by_batch == ['new', 'old'], '這是修好後的行為'


# --------------------------------------------------------------------------- #
#  UI 清單排序
# --------------------------------------------------------------------------- #

def test_view_order_key_matches_the_index():
    import baihua_kb_view as V

    items = [
        (Path('0000_a.md'), {'batch': '1'}),
        (Path('0001_b.md'), {'batch': '1'}),
        (Path('0002_c.md'), {'batch': '2'}),
        (Path('0003_d.md'), {}),            # 舊檔無 batch
    ]

    ordered = sorted(items, key=lambda t: V._article_order_key(t[0], t[1]))

    assert [p.name for p, _ in ordered] == [
        '0002_c.md', '0000_a.md', '0001_b.md', '0003_d.md']


def test_view_order_key_tolerates_garbage_batch():
    import baihua_kb_view as V

    assert V._article_order_key(Path('0001_x.md'), {'batch': 'abc'}) == (0, '0001_x.md')
    assert V._article_order_key(Path('0001_x.md'), {'batch': ' 3 '}) == (-3, '0001_x.md')


# --------------------------------------------------------------------------- #
#  去重 key 碰撞：開頭相同的系列文不可被靜默合併
# --------------------------------------------------------------------------- #

def test_same_post_recognises_see_more_expansion():
    """see-more 只會往後接長，所以同一篇的舊文字必是新文字的前綴。"""
    short = '（四貸同堂）當股市槓桿取代房貸，誰在替誰的風險買單？'
    long = short + '這是展開後才看得到的後半段內容，很長很長。'

    assert FB._same_post(short, long)
    assert FB._same_post(long, short)
    assert FB._same_post(short, short)


def test_same_post_rejects_diverging_bodies():
    """共同開頭之後分岔 = 兩篇不同貼文（系列文），不是展開。"""
    common = '（四貸同堂）當股市槓桿取代房貸，' * 6      # 超過 120 字的共同開頭
    a = common + '第一集講的是融資餘額。'
    b = common + '第二集講的是券資比。'

    assert not FB._same_post(a, b)


def test_same_post_ignores_whitespace_differences():
    assert FB._same_post('開頭 內容 一樣', '開頭內容一樣')


def test_same_post_treats_empty_as_same():
    """空文字沒有可比的內容，不該被判成碰撞。"""
    assert FB._same_post('', 'anything')
    assert FB._same_post(None, 'anything')


def test_key_of_is_stable_across_see_more_expansion():
    short = 'x' * 200
    assert FB._key_of({'text': short}) == FB._key_of({'text': short + 'y' * 500}), \
        '前 120 字不變 -> key 必須不變，否則同一篇會變成兩筆'


def test_disambiguate_separates_colliding_posts():
    common = 'z' * 130
    k = FB._key_of({'text': common})
    ka = FB._disambiguate(k, common + 'AAA')
    kb = FB._disambiguate(k, common + 'BBB')

    assert ka != kb
    assert ka.startswith(k + '#') and kb.startswith(k + '#')
