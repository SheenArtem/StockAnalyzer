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
| News (per-ticker) | Google News RSS | — | `news_fetcher.py` |
| News (題材批次) | cnyes JSON API + udn money RSS | cnyes 失敗有 graceful degradation state | `tools/news_theme_extract.py`；三層 storage / 22 欄 schema / dual-write backup 見 Claude memory `project_news_pipeline` 與 `feedback_keep_backup_dual_write` |
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

## ⚠️ 拿「請求的日期」當資料日期是錯的（2026-08-02 實測）

**TPEX 舊端點 `stk_quote_result.php` 完全無視 `d` 參數**：請求 `115/06/16`（6 週前）
回的是 `115/07/31` 的橫斷面，價格一字不差；請求週六 `115/08/01` 亦同。TWSE `MI_INDEX`
相反 —— 正確分辨每一天，非交易日直接回 `stat="很抱歉，沒有符合條件的資料!"`。

> ✅ **已治本（2026-08-02 第二輪）**：`get_market_daily_tpex` 已改打
> `www/zh-tw/afterTrading/dailyQuotes`（吃西元 `date=YYYY/MM/DD`），**正確認日期**，
> 且是同一份資料集（title 同為「上櫃股票行情」、19 欄含 `均價`、成交量同為含定價
> 口徑），所以欄位索引與舊版共用。**別再改回 `stk_quote_result.php`。**
> ⚠️ 也**不要**改用 `www/zh-tw/afterTrading/otc?type=EW` —— 它同樣認日期，但那是
> 「不含定價」口徑，成交量與現有 panel 不同調（876 檔上櫃股只有 31 檔相符，
> dailyQuotes 有 871 檔相符），混用會讓 panel 出現兩種量值定義。
> 回歸測試：`tests/test_tpex_daily_date_aware.py`。

所以**日期一律以 payload 自報值為準**：
- TWSE：頂層 `date`（西元 `20260731`）+ 表格 title（民國 `115年07月31日`）
- TPEX：頂層 `date`（西元）+ `tables[0].date`（民國 `115/07/31`）

`twse_api.get_market_daily_{twse,tpex,all}()` 已把它放進 **`data_date` 欄**，並有
`strict_date=True`（預設）—— 指定日期就只接受該日或回空。**新寫的呼叫端不要自己
拼日期，也不要關掉 strict_date。**

踩過的坑（都躲得過數值健康度檢查，因為每一欄都是正數的合理價格）：
- `refresh_universe_prices` 的官方 overlay 差點把「上一場」OHLCV 以錯誤日期寫進
  1900+ 支 CSV。
- `market_regime_logger` 每天 00:00 補「今天」時 TWSE 正確回空、TPEX 回上一場 →
  橫斷面變成純上櫃 → 等權均價被高價上櫃股抬成 **1.835 倍** → `regime_log.jsonl`
  的 `ret_20d` 飆到 +77%，週一~週四永久判成 volatile（有 7 個消費端，含
  `scanner_job` 的 REGIME FILTER）。細節見 `docs/code_review_2026-08-02.md` 第二輪新發現。

**另一個獨立教訓**：等權「均價」對成分極度敏感（上櫃有信驊 14,525、旺矽 5,280 這種
高價股）。任何「用 API 補齊 panel 落後日期」的程式，補值必須用**與 panel 相同的成分股
集合**並設覆蓋率下限，否則 `pct_change` 算出來的是成分變動而不是報酬。

## ⚠️ 交易日曆要問官方，不要用 `2330_price.csv`（2026-08-02 實測）

判斷「某天台股有沒有開盤」的權威是 **TWSE MI_INDEX**：非交易日回
`stat="很抱歉，沒有符合條件的資料!"`，交易日回 1,100~1,300 檔。

**`data_cache/2330_price.csv` 漏了 11 個真實交易日**，不可當交易日曆：
2016-01-30、2016-06-04、2016-09-10、2017-02-18、2017-06-03、2017-09-30、
2018-03-31、2018-12-22（皆為台股補行交易的週六）、2019-09-09、2021-04-06、
2025-08-01。官方端點證實這些日子都有成交（例：2025-08-01 有 1,302 檔），
是 yfinance 端缺資料 —— 而且**連 2330 / 2317 / 2454 / 1101 四大權值股一起缺**。

連帶影響（**已於 2026-08-02 回填修掉，以下為成因紀錄**）：`ohlcv_tw.parquet` 在那
11 天原本只存了 33~38% 的橫斷面，top300 等權代理從 294 檔掉到約 100~132 檔，
`2016-06-04` 因此產生 `|ret_20d| > 30%` 的假值。
`market_regime_logger._drop_thin_proxy_dates` 用「前後 21 個交易日成分數中位數的
80%」為門檻把這類日期剔除（用滾動中位數而非固定值，才不會誤殺 2006-2009 全市場本來
就只有數百檔的早期歷史）—— 那是**症狀處理**，根因是 panel 缺資料。

**✅ 已回填（2026-08-02）**：`tools/backfill_panel_gaps.py` 把這 11 天 + 2026-04
斷層 13 天共 **13,004 列**補進 per-stock CSV 並重建 panel（5,494,971 → 5,507,975 列）。

> 先前這裡寫「TPEX 不可行，上櫃股補不回來」是**錯的** —— 那結論建立在舊端點上，
> 改用 `dailyQuotes` 後兩市都補得回來。

回填後覆蓋率：2026-04 那 13 天 **99.9~100%**；11 個部分橫斷面日多數到 85~91%，
其中 10 天已超過 `_drop_thin_proxy_dates` 的 0.8 門檻**不再被剔除**，且放行後
top300 代理成分數回到 297~300 檔、`ret_20d` 全落在 −1.4%~+11.1%（原本 2016-06-04
會算出 >30% 的假值）。**根因已除，那個門檻退回成安全網。**

⚠️ **`2016-09-10` 仍只有 64.2%，刻意沒補滿**：它的 354 檔因前後還原係數不一致被
跳過，而查證後**不是除權息**（211 檔係數變大、144 檔變小，除權息會是單向的；對照組
2017-02-17→02-20 只有 0.2% 不一致）。

> **範圍已收斂（2026-08-03，`tools/reconcile_panel_vs_official.py`）**：不是「2016-09-12
> 這一天」，是 **2016-08-22 ~ 2016-10-03 共 30 個交易日**的窗口（佔全歷史 0.59%），
> 每天 22~26% 的股票與官方對不上，偏差中位 0.34~0.38%、尾巴到 19~21%。窗口外乾淨
> （跨 2015-2026 抽 27 天，中位 0.75%、零個超 5%）。根因未知，**未修**（修它等於覆寫
> 既有列，與「絕不覆寫」原則相反）。
>
> ⚠️ 對帳時**回填日不能當比對基準** —— 回填列是官方價推導的，比起來必然吻合，
> 會給出假乾淨。實例：09-12 的前一交易日回填後變成 09-10，對帳回報 0.09% 看似完美，
> 而同週未污染的四天全是 24~27%。工具已預設跳過這類日期。

**回填不可以直接塞官方原始價** —— CSV 存的是還原價，細節與作法見
`tools/backfill_panel_gaps.py` 檔頭的四點說明。

## Cache Strategy

- 盤中 (09:00-13:30) TTL 5min / 盤後 TTL full day
- 籌碼：每日 21:30 後 refresh；cache dir `data_cache/` (CSV)
- TradingView / Google News：in-memory 30min~1hr
- `cache_manager.py` 有 `_cache_lock` 保執行緒安全

## TW vs US ticker detection

- 純數字或含 `.TW` → TW (FinMind + TWSE/TPEX + TradingView)
- 含字母 → US (Yahoo Finance + Finviz + TradingView)
