"""mis.twse 每請求 ≤50 檔的硬規則要有測試背書。

`_MAX_BATCH = 50` 是實測出來的上限（market-pulse 實測 50 檔/請求 OK），而
`get_quotes` 是唯一會把多檔 pipe 併成一個 `ex_ch` 的路徑。超量會被 mis.twse 擋
（HTTP 非 200 或回非 JSON = 疑似被 ban），而那條路徑只 log warning 不 raise ——
也就是說一旦有人把批次調大或忘記切批，症狀是「報價整批靜默消失」而不是明顯錯誤。

2026-08-02 code review 第六節把這條列為測試缺口（`mis.twse` 每請求 ≤50 檔的硬規則
無測試背書）。相關背景見 Claude memory `reference_mis_twse_intraday`：單檔 / banner
可用，禁全市場掃描，有界批次 ex_ch ≤50/req OK。
"""
import pandas as pd
import pytest

import mis_twse_client
from mis_twse_client import MisTwseClient


def _elem(sid, prefix='tse'):
    """一個「有真實資料」的 msgArray element。"""
    return {'c': sid, 'ex': prefix, 'z': '100.0', 'v': '1234', 'y': '99.0',
            'o': '99.5', 'h': '101.0', 'l': '99.0', 'a': '100.1_', 'b': '99.9_',
            'n': f'N{sid}', 't': '13:30:00'}


@pytest.fixture()
def client(monkeypatch):
    c = MisTwseClient()
    monkeypatch.setattr(c, '_throttle', lambda *a, **k: None, raising=False)
    return c


def _capture(client, monkeypatch):
    """攔下 _fetch_many，記錄每次請求的 ex_ch，並回傳對應的假資料。"""
    calls = []

    def fake_fetch_many(ex_ch):
        calls.append(ex_ch)
        out = []
        for item in ex_ch.split('|'):
            # 'tse_2330.tw' -> ('tse', '2330')
            pref, rest = item.split('_', 1)
            out.append(_elem(rest.replace('.tw', ''), pref))
        return out

    monkeypatch.setattr(client, '_fetch_many', fake_fetch_many)
    return calls


def test_max_batch_constant_is_fifty():
    assert mis_twse_client._MAX_BATCH == 50, \
        '實測上限；改動前先確認 mis.twse 真的接受更多，否則會被擋成靜默無報價'


def test_single_request_stays_within_the_limit(client, monkeypatch):
    calls = _capture(client, monkeypatch)

    client.get_quotes([str(2000 + i) for i in range(50)])

    assert len(calls) == 1
    assert len(calls[0].split('|')) == 50


def test_oversized_input_is_split_into_bounded_chunks(client, monkeypatch):
    calls = _capture(client, monkeypatch)

    client.get_quotes([str(2000 + i) for i in range(120)])

    assert len(calls) == 3, '120 檔應切成 50 / 50 / 20'
    sizes = [len(c.split('|')) for c in calls]
    assert sizes == [50, 50, 20]
    assert all(s <= mis_twse_client._MAX_BATCH for s in sizes)


def test_every_chunk_is_bounded_for_many_sizes(client, monkeypatch):
    for n in (1, 49, 50, 51, 99, 100, 101, 250):
        calls = _capture(client, monkeypatch)
        client.get_quotes([str(3000 + i) for i in range(n)])
        sizes = [len(c.split('|')) for c in calls]
        assert sizes, f'n={n} 應至少發一個請求'
        assert all(s <= mis_twse_client._MAX_BATCH for s in sizes), \
            f'n={n} 出現超量批次 {sizes}'
        assert sum(sizes) == n, f'n={n} 檔數對不上：{sizes}'


def test_all_requested_ids_are_returned(client, monkeypatch):
    _capture(client, monkeypatch)
    ids = [str(2000 + i) for i in range(75)]

    out = client.get_quotes(ids)

    assert set(out) == set(ids), '切批不可漏掉任何代號'


def test_duplicate_ids_are_deduped_before_batching(client, monkeypatch):
    calls = _capture(client, monkeypatch)

    client.get_quotes(['2330'] * 60 + ['2317'] * 60)

    assert len(calls) == 1, '去重後只有 2 檔，不該切成多批'
    assert len(calls[0].split('|')) == 2


def test_suffixed_ids_are_normalised_then_batched(client, monkeypatch):
    calls = _capture(client, monkeypatch)

    out = client.get_quotes(['2330.TW', '6488.TWO', '2317'])

    assert len(calls) == 1
    assert len(calls[0].split('|')) == 3
    assert set(out) == {'2330.TW', '6488.TWO', '2317'}, '回傳的 key 是原輸入形式'


def test_empty_input_makes_no_request(client, monkeypatch):
    calls = _capture(client, monkeypatch)

    assert client.get_quotes([]) == {}
    assert calls == []


def test_misses_fall_through_to_the_otc_round_still_bounded(client, monkeypatch):
    """tse 輪查不到的代號會用 otc 再查一輪，那一輪同樣不得超量。"""
    calls = []

    def fake_fetch_many(ex_ch):
        calls.append(ex_ch)
        # 第一輪（tse）全部查無，第二輪（otc）才回資料
        if ex_ch.startswith('tse_'):
            return []
        return [_elem(i.split('_', 1)[1].replace('.tw', ''), 'otc')
                for i in ex_ch.split('|')]

    monkeypatch.setattr(client, '_fetch_many', fake_fetch_many)

    out = client.get_quotes([str(6000 + i) for i in range(70)])

    sizes = [len(c.split('|')) for c in calls]
    assert all(s <= mis_twse_client._MAX_BATCH for s in sizes), sizes
    assert len(out) == 70, 'otc 補查後應全數取得'
