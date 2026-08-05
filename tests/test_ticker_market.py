"""ticker_market 市場判定 + 各模組台股分流的回歸測試（2026-08-05）。

釘住的 bug：台股**主動型 ETF** 代號帶字母後綴（`00981A`），`'00981A'.isdigit()`
是 **False**，所以散落各處的「全數字＝台股」判定會把它送去美股路徑。實測後果：

- `load_and_resample` 用它選資料源 → yfinance 裸代號 404，`00981A_price.csv`
  從 2026-05-22 斷更到 08-05（兩個半月）
- `refresh_universe_prices` 用它挑要刷新的 CSV → 那檔從來不在夜間名單內
- 台股專屬區塊（法人 / 融資 / 集保 / 月營收 / 除權息）整段拿不到

每個 test 都附「舊寫法會得到什麼」的斷言，這樣萬一有人改回 isdigit()，
測試會明確指出是哪個判準退化了。
"""
import pytest

from ticker_market import (has_tw_suffix, is_tw, is_us, market_of, tw_core)

# 真實存在的台股主動型 ETF（2026-08-05 實測 FinMind 有資料）
ACTIVE_ETFS = ['00981A', '00982A', '00983A', '00991A']
# 槓桿 / 反向 / 外幣計價 ETF —— 同樣帶字母後綴
OTHER_LETTER_TW = ['00631L', '00632R', '00642U', '00625K']
PLAIN_TW = ['2330', '0050', '1101', '6488']
US = ['AAPL', 'MSFT', 'BRK-B', 'TSM', '^VIX', 'MSFU']


# ------------------------------------------------------------------ market_of

@pytest.mark.parametrize('ticker', ACTIVE_ETFS + OTHER_LETTER_TW + PLAIN_TW)
def test_letter_suffixed_tw_codes_are_tw(ticker):
    assert market_of(ticker) == 'tw'
    assert is_tw(ticker) is True
    assert is_us(ticker) is False


@pytest.mark.parametrize('ticker', US)
def test_us_tickers_are_us(ticker):
    assert market_of(ticker) == 'us'
    assert is_us(ticker) is True
    assert is_tw(ticker) is False


@pytest.mark.parametrize('ticker', ACTIVE_ETFS + OTHER_LETTER_TW)
def test_old_isdigit_rule_got_these_wrong(ticker):
    """釘住 bug 本身：舊判準對這些代號回「美股」，新判準必須回台股。

    這個 test 存在的意義是防止有人把判定改回 `isdigit()` —— 那會讓上面
    test_letter_suffixed_tw_codes_are_tw 紅掉，而這裡說明紅的原因。
    """
    assert ticker.isdigit() is False, '前提：這些代號本來就不是全數字'
    assert is_tw(ticker) is True, '但它們是台股'


@pytest.mark.parametrize('ticker,expected', [
    ('2330.TW', 'tw'), ('3324.TWO', 'tw'),
    ('00981A.TW', 'tw'), ('00981A.TWO', 'tw'),
    ('  2330  ', 'tw'), ('  00981A ', 'tw'),
])
def test_suffixes_and_whitespace(ticker, expected):
    assert market_of(ticker) == expected


def test_empty_and_none_default_to_us():
    """沿用既有行為：空值視為美股（呼叫點多半另有 `bool(ticker) and ...` 守衛）。"""
    for v in ['', '   ', None]:
        assert market_of(v) == 'us'
        assert is_tw(v) is False


# -------------------------------------------------------------------- tw_core

@pytest.mark.parametrize('ticker,core', [
    ('2330', '2330'),
    ('2330.TW', '2330'),
    ('3324.TWO', '3324'),          # ⚠️ .TWO 必須整段剝掉，不能只吃 '.TW' 留下 'O'
    ('00981A.TW', '00981A'),
    ('00981A.TWO', '00981A'),
    ('AAPL', 'AAPL'),
    ('  2330.TW  ', '2330'),
])
def test_tw_core_strips_suffix(ticker, core):
    assert tw_core(ticker) == core


def test_tw_core_does_not_repeat_the_get_path_bug():
    """`cache_manager._get_path` 先 replace('.TW') 再 replace('.TWO')，把
    `3324.TWO` 弄成 `3324O`（於是產生 99 個 `*O_price.csv` 重複檔）。
    tw_core 必須不犯同樣的順序錯誤。"""
    assert tw_core('3324.TWO') == '3324'
    assert not tw_core('3324.TWO').endswith('O')


@pytest.mark.parametrize('ticker,expected', [
    ('2330', False), ('2330.TW', True), ('3324.TWO', True),
    ('00981A', False), ('00981A.TW', True), ('AAPL', False),
])
def test_has_tw_suffix(ticker, expected):
    assert has_tw_suffix(ticker) is expected


# ------------------------------------------- 各模組的台股分流（回歸守衛）

def test_technical_analysis_market_of_delegates():
    import technical_analysis as ta
    for t in ACTIVE_ETFS + ['2330', '2330.TW']:
        assert ta._market_of(t) == 'tw'
    assert ta._market_of('AAPL') == 'us'


def test_analysis_engine_detect_us_stock():
    from analysis_engine import TechnicalAnalyzer
    for t in ACTIVE_ETFS + PLAIN_TW + ['2330.TW', '3324.TWO']:
        assert TechnicalAnalyzer._detect_us_stock(None, t) is False, t
    for t in ['AAPL', 'BRK-B', '^VIX']:
        assert TechnicalAnalyzer._detect_us_stock(None, t) is True, t


def test_portfolio_store_detect_market():
    """持有主動型 ETF 時，投組的幣別 / 報價來源不可判成美股。"""
    from portfolio_store import detect_market, normalize_ticker
    for t in ACTIVE_ETFS + OTHER_LETTER_TW:
        assert detect_market(t) == 'tw', t
    assert detect_market('AAPL') == 'us'
    assert normalize_ticker('00981A.TW') == '00981A'
    assert normalize_ticker('3324.TWO') == '3324'


def test_peer_comparison_detect_market():
    from peer_comparison import _detect_market
    assert _detect_market('00981A') == 'tw'
    assert _detect_market('AAPL') == 'us'


REJECT_MSG = "非台股代號，無法抓取籌碼數據"


def test_chip_analysis_rejects_us_ticker_without_network():
    """美股會在守衛處立刻回傳，不觸網 —— 可以安全放進 regression gate。"""
    from chip_analysis import ChipAnalyzer
    data, err = ChipAnalyzer().get_chip_data('AAPL', scan_mode=True)
    assert data is None and err == REJECT_MSG


@pytest.mark.parametrize('ticker', ['00981A', '3324.TWO', '2330', '2330.TW'])
def test_chip_analysis_does_not_reject_tw_tickers(ticker, monkeypatch):
    """`00981A`（主動型 ETF）與 `3324.TWO`（上櫃）原本都被回「非台股代號」
    而完全拿不到籌碼。

    ⚠️ 刻意不讓它真的去抓 —— 直接呼叫會打 FinMind，實測讓這支測試從 <1s 變成
    兩分鐘，還吃掉 600 req/hr 的額度。這裡把 CacheManager 換成會炸的替身：
    只要**通過**了守衛就會踩到替身，於是 RuntimeError 反而是「守衛放行」的證據；
    被守衛擋掉的話會拿到 REJECT_MSG 而不是例外。
    """
    import cache_manager

    class _Boom:
        def __init__(self, *a, **k):
            raise RuntimeError('__past_the_guard__')

    monkeypatch.setattr(cache_manager, 'CacheManager', _Boom)

    from chip_analysis import ChipAnalyzer
    try:
        data, err = ChipAnalyzer().get_chip_data(ticker, scan_mode=True)
    except RuntimeError as e:
        assert '__past_the_guard__' in str(e)
        return                      # 通過守衛 = 這個 ticker 被當成台股，正確
    assert err != REJECT_MSG, '%s 被誤判為非台股' % ticker


@pytest.mark.parametrize('mod_name', ['finviz_data', 'sec_edgar', 'us_stock_chip'])
def test_us_only_modules_guard_with_is_tw(mod_name):
    """美股專用模組原本用「.TW 後綴 or isdigit()」三條件擋台股，漏掉 `00981A`
    （無後綴且 isdigit() 為 False），會拿台股代號去打美股來源白查一趟。
    守衛必須是 is_tw()，不可退回三條件式。"""
    import importlib
    from pathlib import Path
    mod = importlib.import_module(mod_name)
    src = Path(mod.__file__).read_text(encoding='utf-8')
    assert 'is_tw(ticker)' in src, '%s 的台股守衛不是 is_tw()' % mod_name
    assert "ticker.isdigit()" not in src, \
        '%s 又出現 isdigit() 市場判定' % mod_name


def test_refresh_universe_prices_includes_active_etf():
    """夜間刷新名單必須含 `00981A`，且不可含 `.TWO` 被吃掉點號的重複 key。"""
    from pathlib import Path
    from tools import refresh_universe_prices as RUP

    fake = [Path('%s_price.csv' % s) for s in
            ['2330', '00981A', '3324', '3324O', 'AAPL', '2891A', '5483', '5483O']]
    keep, skipped = RUP._tw_cache_stems(fake)
    assert '00981A' in keep, '主動型 ETF 又被排除了'
    assert '2891A' in keep, '特別股也帶字母後綴'
    assert '2330' in keep and '3324' in keep
    assert 'AAPL' not in keep, '美股不該進台股刷新名單'
    assert '3324O' not in keep and '5483O' not in keep, '.TWO 壞 key 不該進名單'
    assert set(skipped) == {'3324O', '5483O'}


def test_refresh_universe_prices_keeps_mangled_key_without_sibling():
    """沒有全數字兄弟檔的 `*O` stem 不能當成壞 key 丟掉 —— 那可能是真代號。"""
    from pathlib import Path
    from tools import refresh_universe_prices as RUP

    keep, skipped = RUP._tw_cache_stems([Path('00631O_price.csv')])
    assert '00631O' in keep
    assert skipped == []
