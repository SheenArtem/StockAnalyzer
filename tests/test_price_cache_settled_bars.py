"""未完成 bar 不得落地 disk cache — 2026-08-04 迴歸釘子。

事故：投資組合 TWR 曲線的收盤價與官方不符，追出 data_cache/*_price.csv 存著
「盤中未完成 bar」。實測 MSFU 2026-08-03 cache 收 35.84 / 量 6.85M，
真實收盤 36.47 / 量 11.84M（-1.7%）；TSMX 07-28 -3.4% 已躺六天。

兩層根因，本檔各釘一組：
  A. 增量 / 全量兩條路徑都直接把 yfinance 回的當日 bar 寫進 cache
     （旁邊 _try_intraday_quote_as_today_bar 對台股 mis.twse 明確寫了
      「不該被寫入 disk cache」，但這條原則沒套到 yfinance 路徑）。
  B. 增量更新從 last_date + 1 天起算 -> 已落地的壞 bar 永遠在抓取範圍外，
     不會自己好。改成從 last_date 起算，讓真收盤覆寫它。

時間相依的測試一律注入 now 或改寫 _MARKET_SESSION 的收盤時間，
不依賴「跑測試的當下是不是盤中」。
"""
import datetime

import pandas as pd
import pytest

import technical_analysis as ta


def _ny_today():
    from zoneinfo import ZoneInfo
    return datetime.datetime.now(ZoneInfo('America/New_York')).date()


def _bar_df(dates, close=100.0, vol=1000):
    idx = pd.DatetimeIndex([pd.Timestamp(d) for d in dates])
    return pd.DataFrame({
        'Open': close, 'High': close, 'Low': close,
        'Close': close, 'Adj Close': close, 'Volume': vol,
    }, index=idx)


# ====================================================================
#  A-1. is_bar_settled：市場當地時間判定
# ====================================================================

@pytest.mark.parametrize('bar_date, now, expected', [
    # 過去的日期一律定案
    (datetime.date(2026, 8, 3), datetime.datetime(2026, 8, 4, 10, 32), True),
    # 當日盤中 -> 未定案
    (datetime.date(2026, 8, 4), datetime.datetime(2026, 8, 4, 9, 31), False),
    (datetime.date(2026, 8, 4), datetime.datetime(2026, 8, 4, 15, 59), False),
    # 收盤瞬間仍在結算緩衝內 -> 未定案
    (datetime.date(2026, 8, 4), datetime.datetime(2026, 8, 4, 16, 0), False),
    (datetime.date(2026, 8, 4), datetime.datetime(2026, 8, 4, 16, 14), False),
    # 過了緩衝 -> 定案
    (datetime.date(2026, 8, 4), datetime.datetime(2026, 8, 4, 16, 15), True),
    (datetime.date(2026, 8, 4), datetime.datetime(2026, 8, 4, 23, 59), True),
    # 時區錯位跑到未來 -> 保守當未定案
    (datetime.date(2026, 8, 5), datetime.datetime(2026, 8, 4, 20, 0), False),
])
def test_is_bar_settled_us(bar_date, now, expected):
    assert ta.is_bar_settled(bar_date, 'us', now=now) is expected


@pytest.mark.parametrize('now, expected', [
    (datetime.datetime(2026, 8, 4, 9, 0), False),    # 台股盤中
    (datetime.datetime(2026, 8, 4, 13, 29), False),  # 收盤前一分
    (datetime.datetime(2026, 8, 4, 13, 40), False),  # 緩衝內
    (datetime.datetime(2026, 8, 4, 13, 45), True),   # 13:30 + 15 分
    (datetime.datetime(2026, 8, 4, 22, 0), True),
])
def test_is_bar_settled_tw_uses_1330_close(now, expected):
    """台股收盤 13:30，與美股 16:00 分開判定。"""
    assert ta.is_bar_settled(datetime.date(2026, 8, 4), 'tw', now=now) is expected


def test_us_bar_not_settled_during_taipei_evening():
    """核心情境：台灣晚上 22:32 = 美東 10:32 盤中。

    這是實際踩到的坑 —— 用本機（台北）時鐘判斷會認為「早就過收盤了」，
    但美股正在盤中。判定必須以市場當地時間為準。
    """
    ny_now = datetime.datetime(2026, 8, 4, 10, 32)   # 美東盤中
    assert ta.is_bar_settled(datetime.date(2026, 8, 4), 'us', now=ny_now) is False


def test_is_bar_settled_none_date():
    assert ta.is_bar_settled(None, 'us') is False


def test_market_of():
    assert ta._market_of('2330') == 'tw'
    assert ta._market_of('2330.TW') == 'tw'
    assert ta._market_of('5483.TWO') == 'tw'
    assert ta._market_of('MSFU') == 'us'
    assert ta._market_of('BRK-B') == 'us'


@pytest.mark.parametrize('ticker', ['00981A', '00982A', '00981A.TW'])
def test_market_of_active_etf_is_tw(ticker):
    """台股主動型 ETF 代號帶字母後綴，`isdigit()` 會回 False。

    2026-08-05 實測踩到：用全數字判斷會把 `00981A` 當美股，於是台股 13:30 收盤後
    到美東 16:00 之間，已定案的 bar 被誤判成未收盤而不寫入 cache。
    """
    assert ta._market_of(ticker) == 'tw'


def test_active_etf_bar_settled_uses_tw_close():
    """回歸：`00981A` 在台股 14:00（已過 13:30+15min）必須算定案。"""
    now = datetime.datetime(2026, 8, 4, 14, 0)
    mkt = ta._market_of('00981A')
    assert ta.is_bar_settled(datetime.date(2026, 8, 4), mkt, now=now) is True


# ====================================================================
#  A-2. drop_unsettled_bars
# ====================================================================

def test_drop_unsettled_keeps_past_drops_today():
    now = datetime.datetime(2026, 8, 4, 10, 32)      # 美東盤中
    df = _bar_df(['2026-07-31', '2026-08-03', '2026-08-04'])
    kept, dropped = ta.drop_unsettled_bars(df, 'us', now=now)
    assert list(kept.index.strftime('%Y-%m-%d')) == ['2026-07-31', '2026-08-03']
    assert dropped == [datetime.date(2026, 8, 4)]


def test_drop_unsettled_keeps_all_after_close():
    now = datetime.datetime(2026, 8, 4, 16, 30)      # 美東收盤後
    df = _bar_df(['2026-08-03', '2026-08-04'])
    kept, dropped = ta.drop_unsettled_bars(df, 'us', now=now)
    assert len(kept) == 2
    assert dropped == []


def test_drop_unsettled_empty_and_none():
    assert ta.drop_unsettled_bars(pd.DataFrame(), 'us')[1] == []
    assert ta.drop_unsettled_bars(None, 'us') == (None, [])


def test_drop_unsettled_keeps_non_date_index_rows():
    """yfinance 多層 header 殘留的 'Ticker' 列不由本函式處理，須保留。"""
    df = _bar_df(['2026-07-31'])
    junk = pd.DataFrame({c: ['Ticker'] for c in df.columns}, index=['Ticker'])
    mixed = pd.concat([junk, df])
    kept, dropped = ta.drop_unsettled_bars(
        mixed, 'us', now=datetime.datetime(2026, 8, 4, 10, 32))
    assert 'Ticker' in [str(i) for i in kept.index]
    assert dropped == []


# ====================================================================
#  端到端：load_and_resample 增量路徑
# ====================================================================

class _FakeCM:
    """假 CacheManager：記錄 save_cache 實際落地的內容。"""

    def __init__(self, cached, last_date):
        self._cached = cached
        self._last_date = last_date
        self.saved = None
        self.save_calls = 0

    def load_cache(self, ticker, data_type, force_reload=False):
        return self._cached.copy(), 'partial', self._last_date

    def save_cache(self, ticker, df, data_type):
        self.saved = df.copy()
        self.save_calls += 1


@pytest.fixture
def intraday_env(monkeypatch):
    """把美股收盤時間挪到 23:59，讓「今天」在任何執行時刻都算未定案。"""
    monkeypatch.setitem(ta._MARKET_SESSION, 'us',
                        ('America/New_York', 23, 59))
    today = _ny_today()
    yday = today - datetime.timedelta(days=1)
    cached = _bar_df([str(yday)], close=50.0, vol=9999)
    fake = _FakeCM(cached, pd.Timestamp(yday))
    monkeypatch.setattr('cache_manager.CacheManager', lambda: fake)

    captured = {}

    def fake_download(ticker, start=None, **kwargs):
        captured['start'] = start
        # 資料源回「昨天的真收盤」+「今天的盤中未完成 bar」
        return _bar_df([str(yday), str(today)], close=77.0, vol=123)

    monkeypatch.setattr(ta.yf, 'download', fake_download)
    return fake, captured, today, yday


def test_intraday_bar_not_written_to_disk(intraday_env):
    """A 的釘子：盤中的當日 bar 進記憶體，但不得寫進 disk cache。"""
    fake, _captured, today, yday = intraday_env
    _name, df_day, _wk, _meta = ta.load_and_resample('MSFU')

    # 記憶體端：盤中仍看得到當日 bar
    assert pd.Timestamp(today) in df_day.index, "當日 bar 應保留在記憶體供盤中顯示"
    # disk 端：當日 bar 不落地
    assert fake.save_calls == 1
    assert pd.Timestamp(today) not in fake.saved.index, \
        "未收盤的當日 bar 落地了 —— 它會被永久釘住（本次修復的原始 bug）"
    assert pd.Timestamp(yday) in fake.saved.index


def test_incremental_refetches_last_date(intraday_env):
    """B 的釘子：起算日必須含 cache 最後一列，否則壞 bar 永遠修不掉。"""
    _fake, captured, _today, yday = intraday_env
    ta.load_and_resample('MSFU')
    assert captured['start'] == yday.strftime('%Y-%m-%d'), (
        f"起算日 {captured['start']} 應等於 last_date {yday}；"
        "從隔天起算會讓已落地的未完成 bar 永遠在抓取範圍外")


def test_last_date_row_overwritten_by_refetch(intraday_env):
    """重抓 last_date 那天，新的真收盤要覆寫 cache 裡的舊值。"""
    fake, _captured, _today, yday = intraday_env
    ta.load_and_resample('MSFU')
    # cached 是 50.0 / 量 9999，重抓回 77.0 / 量 123 -> 應以重抓值為準
    assert float(fake.saved.loc[pd.Timestamp(yday), 'Close']) == 77.0
    assert float(fake.saved.loc[pd.Timestamp(yday), 'Volume']) == 123


def test_stale_unsettled_row_in_cache_gets_purged(monkeypatch):
    """先前盤中誤寫進 cache 的當日壞 bar，這次存盤要被清掉（自我修復）。

    情境：cache 已含今天的未完成 bar，而資料源這次沒有新 bar 可給
    （美股在增量路徑常見）—— 存盤時仍應把那根未定案的列濾掉。
    """
    monkeypatch.setitem(ta._MARKET_SESSION, 'us', ('America/New_York', 23, 59))
    today = _ny_today()
    yday = today - datetime.timedelta(days=1)
    # cache 裡有「昨天的好資料」+「今天的壞 bar」
    cached = _bar_df([str(yday), str(today)], close=50.0, vol=9999)
    fake = _FakeCM(cached, pd.Timestamp(today))
    monkeypatch.setattr('cache_manager.CacheManager', lambda: fake)
    monkeypatch.setattr(ta.yf, 'download',
                        lambda *a, **k: _bar_df([str(today)], close=61.0, vol=222))

    ta.load_and_resample('MSFU')

    assert fake.save_calls == 1
    assert pd.Timestamp(today) not in fake.saved.index, \
        "cache 內既有的未定案列應被清掉，否則錯誤收盤價會一直留著"
    assert pd.Timestamp(yday) in fake.saved.index
