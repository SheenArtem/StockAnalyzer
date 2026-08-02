# Data Sources — 抓取 / 快取 / fallback 準則

新增或修改任何資料抓取、快取、fallback，或不確定某資料該從哪個來源拿時必讀（route 自 `AGENTS.md`）。
All features MUST follow the same priority to avoid data drift。實作細節（函式參數 / SymbolID 格式 / endpoint）見對應 Claude memory。

## Data Source Priority (unified strategy)

| Data Type | Primary | Fallback | Notes |
|---|---|---|---|
| Institutional buy/sell | TWSE/TPEX official | FinMind | 統一在 ChipAnalyzer 層 |
| OHLCV daily（個股讀取）| Disk cache | FinMind → yfinance | `load_and_resample()` |
| OHLCV daily（TW 全市場日更寫入）| **最後一日：TWSE MI_INDEX + TPEX stk_quote_result 官方 EOD cross-section**；重疊歷史視窗：yfinance 批次 | 官方 lookback 7 天內找不到完整交易日 → 只用 yfinance | `tools/refresh_universe_prices.py`（2026-08-02 起）。被限流／不完整的 yfinance 批次**不得決定 production 的市場日期**；寫入前過 80% 有量覆蓋率健康度檢查並剔除不健康日期 |
| OHLCV intraday (today bar) | mis.twse 即時 JSON | FinMind/yfinance daily | TW only 9:00-13:30；單檔/banner 用，**禁批次掃**（5sec/3req 上限）|
| 美股個股即時報價 | Yahoo **v8** chart `range=1d&interval=1m&includePrePost=true` | 抓不到 → `get_current_prices` 補 EOD | `portfolio_pricing.py`。⚠️ **不要再試 v7 `/quote`**，本機實測 401（需 Cloudflare Worker IP）。常規盤取 `meta.regularMarketPrice`（lag 0~4 秒）；盤前/盤後取 1 分 K 最後一根非空收盤；前收取 `meta.previousClose`（**range 必須 1d**，5d 時 `chartPreviousClose` 會變成視窗基準日）|
| 大盤指數 (^TWII/^GSPC/^IXIC/^SOX) | yfinance（濾 NaN 尾列）| ^GSPC/^IXIC → FRED API → last-good 落盤；^TWII/^SOX → last-good | `market_banner._fetch_index_metrics`；失敗只短快取 5min |
| 美股波動率指數 (VIX/VIX3M/VVIX/SKEW/OVX) | CBOE 官方日線 CSV `cdn.cboe.com/api/global/us_indices/daily_prices/{NAME}_History.csv` | yfinance（僅補官方檔缺的早期歷史，`combine_first`）| `tools/fred_fetcher.py`；2026-08-01 起改官方 primary — Yahoo ^VIX3M 7/20 斷供只回 NaN 尾列，害 vol_complex 凍兩週 |
| ^MOVE 美債隱波 | yfinance（深度歷史 2002~）| Barchart EOD `$MOVE`（補近期，~800 交易日）| ICE 指數無官方免費源；`tools/fred_fetcher.py::fetch_barchart`，需先訪報價頁取 XSRF-TOKEN cookie；**每日 1 次呼叫，勿加頻**。⚠️ 別改用 Yahoo 小時線補日線——實測整條**落後一個交易日** |
| 台指期(全) 日盤+夜盤 | mis.taifex 即時 (`getQuoteDetail`) | dlFutDataDown EOD CSV | `taifex_data.get_full_session_quote`；banner TTL 15min |
| 期貨基差 (正逆價差) | mis.taifex 近月 tick − mis.twse 現貨 | dlFutDataDown 結算價（須濾時段=一般）| `taifex_data.get_futures_basis`；banner TTL 15min |
| Fundamentals (PE/PB) | yfinance + FinMind | TradingView fill | `get_fundamentals()` |
| Margin/ROE/ROA | TradingView Screener | — | TW + US unified |
| 融資融券 (margin trading) | TWSE MI_MARGN ALL + TPEX margin_bal_result.php（全市場整批 by-date）| FinMind per-stock (legacy) | `chip_history_dl.py::download_margin`；TPEX 2026-06-29 由 FinMind 改官方整批，1 call/日 |
| Day trade / holdings | FinMind | — | 無替代 (per-stock) |
| News | Google News RSS + udn money RSS | — | `news_fetcher.py` / `tools/news_theme_extract.py` |
| Analyst consensus | yfinance | — | Target price / Forward EPS / rating |
| Peer comparison | TWSE/TPEX PER + FinMind industry | — | `peer_comparison.py` |
| TV-show YT mentions | yt-dlp auto-sub + Claude Sonnet | — | → `data/sector_tags_dynamic.parquet` |
| Brokerage YT mentions | yt-dlp manual-sub + codex + Sonnet fallback | — | 獨立 pipeline，**不接 AI 報告**（合規）|

## Data Source Discovery SOP

宣告「No API」/ 走 LLM HTML parse / 自寫 scraper 前**必跑 3 步**：

1. **第三方逆推** — macromicro/cnyes/Goodinfo/TradingView 圖表標的「資料來源: X」就是真來源，繼續挖
2. **試檔案下載** — API 死掉試 `staticFiles/*.zip` / `download?type=` / `pdf/xlsx`；用 DevTools Network tab 看真 XHR
3. **猜 path pattern** — TWSE 慣例 `/staticFiles/.../{type}/{subtype}/YYYYMM_C{type}{subtype}.zip`

> 緣由（2026-05-10 TWSE PE 11 endpoint 全 404 教訓）+ endpoint cheat sheet 見 Claude memory `reference_twse_endpoints`。

## Cache Strategy

- 盤中 (09:00-13:30) TTL 5min / 盤後 TTL full day
- 籌碼：每日 21:30 後 refresh；cache dir `data_cache/` (CSV)
- TradingView / Google News：in-memory 30min~1hr
- `cache_manager.py` 有 `_cache_lock` 保執行緒安全

## TW vs US ticker detection

- 純數字或含 `.TW` → TW (FinMind + TWSE/TPEX + TradingView)
- 含字母 → US (Yahoo Finance + Finviz + TradingView)
