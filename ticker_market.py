"""ticker_market.py — TW / US 市場判定的唯一權威（零依賴，任何模組都可 import）

⚠️ **不要用 `ticker.isdigit()` 判市場。** 台股**主動型 ETF** 代號帶字母後綴
（`00981A` / `00982A` / `00983A` / `00991A`…），`'00981A'.isdigit()` 是 **False**，
用「全數字」判斷會把台股送去美股路徑。2026-08-05 實測到的後果：

- `load_and_resample` 用它**選資料源** → `00981A` 走 yfinance 裸代號 → Yahoo 回 404
  （`00981A.TW` 才抓得到）。`data_cache/00981A_price.csv` 因此從 2026-05-22 起
  斷更兩個半月，而增量更新只從 cache 最後一列往後抓，**不會自己好**。
- AI 報告 / 籌碼 / 因子的台股專屬區塊（法人 / 融資 / 集保 / 月營收）整段拿不到。

判準是「**數字開頭**」：台股代號一律數字開頭（`2330` / `0050` / `00981A` / `00631L`），
美股一律字母或符號開頭（`AAPL` / `BRK-B` / `^VIX`）。

⚠️ **別拿本模組去做「只要 4 碼普通股」的過濾** —— 那是另一件事。
`sid.isdigit() and len(sid) == 4 and not sid.startswith('0')` 這種寫法
（`momentum_screener` / `value_screener` / `screener_view`）是**刻意**要排除 ETF 與
權證的，維持原樣才對，不要誤當成本模組該接手的市場判定。

實測佐證（2026-08-05）：FinMind 有 `00981A`（291 列，到 2026-08-04）；
yfinance `00981A.TW` 可用、裸 `00981A` 404；mis.twse 即時報價**不支援**主動型 ETF（回 None）。
"""

# .TWO 必須排在 .TW 前面 —— 否則 '3324.TWO' 會先被 '.TW' 吃掉前三碼、剩下一個 'O'
# （`cache_manager._get_path` 就是踩到這個順序，才生出 `3324O_price.csv` 這種檔名）。
_TW_SUFFIXES = ('.TWO', '.TW')


def tw_core(ticker):
    """去掉 `.TW` / `.TWO` 後綴後的純代號：`'00981A.TW'` -> `'00981A'`。"""
    s = str(ticker or '').strip()
    up = s.upper()
    for suf in _TW_SUFFIXES:
        if up.endswith(suf):
            return s[:-len(suf)]
    return s


def has_tw_suffix(ticker):
    """是否已帶 `.TW` / `.TWO` 後綴（＝已是 yfinance 可直接用的台股符號）。"""
    return str(ticker or '').strip().upper().endswith(_TW_SUFFIXES)


def market_of(ticker):
    """`'tw'` / `'us'` —— 數字開頭即台股。空字串視為美股（沿用既有行為）。"""
    return 'tw' if tw_core(ticker)[:1].isdigit() else 'us'


def is_tw(ticker):
    """台股（含主動型 ETF、槓桿/反向 ETF、上櫃，帶不帶後綴都算）。"""
    return market_of(ticker) == 'tw'


def is_us(ticker):
    """美股。等價於 `not is_tw(...)`，另給一個名字讓呼叫點讀起來自然。"""
    return market_of(ticker) == 'us'
