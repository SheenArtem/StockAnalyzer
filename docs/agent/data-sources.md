# Data Sources — 抓取 / 快取 / fallback 準則

新增或修改任何資料抓取、快取、fallback，或不確定某資料該從哪個來源拿時必讀（route 自 `AGENTS.md`）。
All features MUST follow the same priority to avoid data drift。實作細節（函式參數 / SymbolID 格式 / endpoint）見對應 Claude memory。

## Data Source Priority (unified strategy)

| Data Type | Primary | Fallback | Notes |
|---|---|---|---|
| Institutional buy/sell | TWSE/TPEX official | FinMind | 統一在 ChipAnalyzer 層 |
| OHLCV daily（個股讀取）| Disk cache | FinMind → yfinance | `load_and_resample()` |
| OHLCV daily（TW 全市場日更寫入）| **最後一日：TWSE MI_INDEX + TPEX dailyQuotes 官方 EOD cross-section**；重疊歷史視窗：yfinance 批次 | 官方 lookback 7 天內找不到完整交易日 → 只用 yfinance | `tools/refresh_universe_prices.py`（2026-08-02 起）。被限流／不完整的 yfinance 批次**不得決定 production 的市場日期**；官方 payload 日期也是本輪寫入上限，Yahoo 晚於它的預載／錯標列一律剔除；寫入前過 80% 有量覆蓋率健康度檢查並剔除不健康日期 |
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
| 融券／借券賣出 (short sale / SBL) | TWSE TWT93U + TPEX margin_sbl_result.php（全市場整批 by-date） | —（FinMind 僅作跨源驗證，不在 runtime fallback） | `chip_history_dl.py::download_short_sale`；TPEX 2026-08-27 由 FinMind 逐檔改官方整批，809/809 檔六欄對帳一致，1 call/日 |
| Day trade / holdings | FinMind | — | 無替代 (per-stock) |
| News (per-ticker) | Google News RSS | — | `news_fetcher.py` |
| News (題材批次) | cnyes JSON API + udn money RSS | cnyes 失敗有 graceful degradation state | `tools/news_theme_extract.py`；三層 storage / 22 欄 schema / dual-write backup 見 Claude memory `project_news_pipeline` 與 `feedback_keep_backup_dual_write` |
| Analyst consensus | yfinance | — | Target price / Forward EPS / rating |
| Peer comparison | TWSE/TPEX PER + FinMind industry | — | `peer_comparison.py` |
| 公司行為（還原係數用）| 除權息／現金增資：FinMind `taiwan_stock_dividend_result`（逐檔）<br>減資：**TWSE `TWTAUU` + TPEX `bulletin/revivt` 官方**（全市場各 1 次呼叫）<br>面額變更：FinMind `taiwan_stock_split_price`（全市場 1 次呼叫）| 除權息 `reference_price` 壞掉時用 `stock_and_cache_dividend` 反推 | `tools/build_corporate_actions.py`。⚠️ **減資不可以用 FinMind `taiwan_stock_capital_reduction_reference_price`**（價格對、**日期會壞**）。細節見下方專節 |
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

## 公司行為事件表（還原係數的唯一來源，2026-08-10）

`tools/build_corporate_actions.py` → `data_cache/backtest/corporate_actions_tw.parquet`
（`data_cache/` 已 gitignore，不入版控）。

**為什麼是事件表而不是 `Adj Close` 欄**：把還原係數**存起來**的作法已經失敗過 ——
1,965 個台股 CSV 裡可判定的 1,034 檔只有 **262 檔（25.3%）健康**，772 檔的係數中途
往下掉（舊列停在舊基準、新列是新基準），成因是夜間增量只 append 新列，而資料源
每次除權息會回溯調整整段歷史。事件是不可變的歷史事實，**算出來的係數不會過期**。

### 四層來源與呼叫成本

| 層 | 來源 | 呼叫數 | 涵蓋起點 |
|---|---|---|---|
| 除權息／現金增資 | FinMind `taiwan_stock_dividend_result` | **每檔 1**（全市場 ≈1,965 ＝ 3.3h @600/hr）| 全歷史 |
| 減資（上市）| `https://www.twse.com.tw/rwd/zh/reducation/TWTAUU` | **1** | 2011-01-25 |
| 減資（上櫃）| `https://www.tpex.org.tw/www/zh-tw/bulletin/revivt` | **1** | 2013-01-16 |
| 面額變更 | FinMind `taiwan_stock_split_price` | **1** | 2019-09-09 |

後三層是**全市場一次抓完**，所以它們沒有、也不需要逐檔續跑狀態。

### 增量維護：`--sync-recent DAYS`（2026-08-11）

**要解的問題：續跑的完成判定是 per-STOCK 不是 per-DATE。** 逐檔層一旦把某檔標記為
「除權息已抓過」，它**之後新發生**的事件就再也補不到 —— 而 FinMind 當天入庫會落後，
所以夜間路徑必然錯過當天的事件。實證（2026-08-11）：官方行事曆顯示當天有 5 筆除權息，
**5 檔全部已在表中 ＝ 全部被續跑跳過**，那 5 筆除非重跑全庫（3.3h）否則永遠補不到。
**每一筆未來新發生的除權息都有同樣的問題**，所以它會隨時間累積。

| 市場 | 行事曆來源 | 呼叫數 | 拿得到價格？ |
|---|---|---|---|
| 上市 | `https://www.twse.com.tw/rwd/zh/exRight/TWT49U` | **1** | **是** —— 自帶前收盤價／參考價／權值+息值 |
| 上櫃 | `https://www.tpex.org.tw/openapi/v1/tpex_exright_prepost` | **1** | 否 —— 只有股利金額，要另外向 FinMind 拿 |

⇒ **上市的除權息零 FinMind 額度**；上櫃只對「表裡缺這一天」的那幾檔叫 FinMind
（實測每天 0~3 檔）。整體成本是 2 次官方呼叫 ＋ 極少量 FinMind。

⚠️ **`TWT49U` 的日期格式是第三種寫法**：`115年08月03日`，既不是減資表的 `111/10/31`
也不是 TPEX 的 `1150630`。同一個機構的兩張表用不同格式，照減資表的經驗猜會解析失敗。
`_roc_to_iso` 三種都吃，有測試釘住三者對同一天必須解出同一個值。

⚠️ **TPEX 那支端點不吃任何參數**，永遠回一個固定的近期窗口，所以**無法做區間回音
驗證** —— 這是它與其他三個官方 fetcher 的關鍵差異，不要照抄那套檢查。能驗的只有
「日期解析得出來」與「不是未來」。它是 pre+post（預告 ＋ 已實現），**未來日期的事件
價格還不存在**，一律剔除；payload 還混著日期為 `'0'` 的列，同樣剔除。

⚠️ **官方表的 source 標成 `twse_exright`，刻意不放進 `DIVIDEND_SOURCES`。**
那個集合是續跑的完成判據，而 `TWT49U` 只涵蓋查詢的那幾天、不是全歷史。標成
`reference` 會讓一檔從沒被逐檔抓過、卻剛好在增量窗口內配息的股票被永久標記完成，
它 2000~今天的歷史除權息就再也抓不到 —— **正是這個模式要修的病，只是換個方向重現**。

⚠️ **官方表含 ETF 與特別股**（實測 `00940` / `00946` / `00953B` / `00985B` /
`00400A` / `2348A`），會被宇宙過濾掉 —— 與既有減資層用**同一個**過濾器。宇宙是照
`data_cache/*_price.csv` 長出來的，沒有價格序列就沒有還原對象，所以這是預期行為
不是漏抓。**排查「某檔沒進表」時先確認它在不在宇宙內。**

⚠️ **兩個官方端點的日期參數格式不一樣，而且都要驗回音**：
TWSE 吃 `startDate=YYYYMMDD&endDate=YYYYMMDD`（**早於 2011-01-01 直接拒絕**，
回「查詢開始日期小於100年1月1日」）；TPEX **只認斜線** `startDate=YYYY/MM/DD`
（緊湊格式回「參數錯誤」），而裸 `date=` 參數**完全被忽略**、永遠回今天的窗口 ——
就是本文件前面那個「拿請求的日期當資料日期是錯的」同型陷阱。
兩支 fetcher 都會比對 payload 自報的區間，對不上就 raise。

### 還原係數公式

```
factor = reference_price / before_price                          # 主要（三種事件通用）
factor = (before_price - stock_and_cache_dividend) / before_price # 除權息層的 fallback
```

fallback 只在 `reference_price` 正好等於 `before_price`（＝ FinMind 沒填）
**且**確實有價值調整時啟用。**壞掉比率實測：上市 2.3% / 上櫃 7.5%**（上櫃是 3 倍）。
第二式在 `reference_price` 正常的事件上 132/133（99.2%）精確重現，
且正是官方 `TWT49U` 滿足的恆等式；**現金增資也適用**。

⚠️ **判準是 `stock_and_cache_dividend != 0`，不是 `> 0`**（2026-08-10 round-2 審查 F1）。
「權值+息值」**可以是負的** —— 認購價**高於**市價的現金增資就是，此時官方參考價
比前一日收盤**高**、係數 > 1。用 `> 0` 會讓這種事件靜默落回 factor 1.0 且
source 標成 `reference`：既沒還原，又假稱用了真實參考價，連拿 source 欄稽核都找不到。
官方 `TWT49U` 實查的三筆：`1312` 2026-06-11（13.20→13.40）、
`1735` 2013-01-07（12.60→12.73）、`1464` 2011-09-15（9.47→9.63）。

減資的 `factor > 1`（參考價高於前價），面額變更的 `factor` 可小到 0.1。
所以合法範圍是 **0.05 ~ 50**，不是直覺的 0.2 ~ 5 ——
實測極值：面額變更 10 元改 1 元 = 0.100（`6548`）、彌補虧損減資 = 40.03（`5364`）。

### ⚠️ 減資為什麼不用 FinMind（實測結論，別再試）

FinMind 有 `taiwan_stock_capital_reduction_reference_price`，抽 24 檔 / 41 筆事件
對官方逐筆比對：**價格 38/38 完全吻合，但日期會壞**，而壞日期是最致命的一種 ——
係數被套到錯的區間，且完全沒有跡象：

- `2327` 2022 年那次出現**兩列**（10/21 與 10/31，價格一字不差），官方只有 10/31。
  照單全收 = 1.2468 的係數乘兩次 = 憑空 **+55%**。
- `2412` 官方 2011-01-25、FinMind 記成 **2011-10-25**（差 9 個月）。
- `3018` 官方 2023-11-27、FinMind 記成 **2023-11-11**（**星期六**，不是交易日）。

官方兩張表是全市場各 1 次呼叫（vs 1,965 次）又是權威來源，沒有理由用它；
而且它在抽樣的 24 檔裡**沒有任何一筆早於官方視窗**，所以也補不了歷史深度缺口。

### 續跑語義（改這支工具前必讀）

- 合併是 **append-only**：既有列永遠獲勝（`keep='first'`），`--refetch-all`
  只能補「缺的事件」，**不能修既有列**。
  **唯一例外**：既有列的 `factor` 是 NaN 時會讓給新抓到的可用係數 ——
  NaN 不帶任何資訊，卻會永久佔住那個 key 且沒有修復路徑。
  三個來源層現在都會在寫入前丟掉算不出係數的列（原本只有減資層會）。
- 續跑的完成判定**只看除權息來源的列**（`reference` / `fallback` / `unusable`）。
  拿「股票在不在表裡」當判據會壞掉 —— 全市場層會讓沒抓過除權息的股票也出現在表中，
  那些股票的除權息就永遠補不到了。
- 全市場層用**完整 universe** 過濾，不是用續跑後的待辦清單（同一個理由）。
- **三個市場層的順序是刻意的**：面額變更（FinMind）排在逐檔除權息**之後**。
  FinMind 免費層對「不帶 `data_id` 的全市場查詢」回 400 `Your level is register`，
  SDK 變成 `KeyError('data')`，`cache_manager` 再一律當成額度爆掉並
  **negative-cache 300 秒（行程層級）**。市場層排前面的話，這個誤判會讓整輪除權息
  一筆都不跑卻回報「再跑一次即可續跑」，而且等多久都不會好。
- **`--limit N` 不會分頁**：切片在續跑過濾**之前**，永遠是同樣的前 N 檔。
  那 N 檔抓完後再跑只會得到 0 筆新事件。回補要跑全市場。
- Exit code：`0` 跑完 / `1` 拒絕寫入（除權息來源疑似整個壞掉，或既有表讀不出來
  且搬不走）/ **`3` 逐檔層被 FinMind 額度打斷**（已寫入的完整、但還沒抓完，
  再跑一次即續跑）。
  ⚠️ exit code 只描述**逐檔層**。三個市場層任一失敗都只 log.error 後繼續、
  **不改 exit code**（它們每跑一次都全量重抓，沒有續跑進度可言）——
  所以 exit 0 不等於三層都成功，要判斷得看 log。
- ⚠️ **官方減資表自己就有重複列**：`TWTAUU` 2011-01-01~2026-08-10 的 392 列裡有
  4 個 `(代號, 恢復買賣日期)` 重複、共 5 個多餘列。3 組只差公司名稱（無害），
  但 **`3536` 104/03/20 出現三次且不一致** —— 第一列的「除權參考價」是 `--`、
  另兩列是 13.06。交給 payload 順序決定會得到 2.02584 而不是正確的 1.98480。
  工具用明確偏好去重（有除權參考價的那列勝），不一致時 log.warning。

### 已宣告的涵蓋邊界（接 panel 前必讀）

1. **減資只回溯到 2011-01-25（上市）／2013-01-16（上櫃）**，官方端點給不出更早的。
   panel 歷史從 **2006** 開始 → 2006~2010（上市）／2006~2012（上櫃）的減資**沒涵蓋**，
   那段期間的還原序列在減資日仍會留著假跳空。
2. **面額變更只回溯到 2019-09-09**（FinMind 資料集起點，全市場合計 32 筆）。
3. **上櫃除權息的歷史深度沒有跨源驗過** —— TPEX 官方只給得出約兩週的窗口。
4. **還原後除權息當天仍有 +0.46% 的殘餘正向漂移**，無法用現有測試分辨是過度校正
   還是台股的填權息傾向。
5. ⚠️ **官方原始價序列有 `close == 0.00` 的停牌列**（`1529` / `1472` 各 4/40 列）。
   `raw × factor` 不會修掉它，消費端必須先剔除非正值收盤再算報酬。

## Cache Strategy

- 盤中 (09:00-13:30) TTL 5min / 盤後 TTL full day
- 籌碼：每日 21:30 後 refresh；cache dir `data_cache/` (CSV)
- TradingView / Google News：in-memory 30min~1hr
- `cache_manager.py` 有 `_cache_lock` 保執行緒安全

### ⚠️ 未收盤定案的 bar 一律不寫 disk cache（2026-08-04 治本，`e95745a`）

盤中抓到的當日 bar 是**未完成 bar**：Close 只是當下報價、Volume 只累積到當下。
它一旦落地就永久有效 —— 增量更新只從 cache 最後一列往後續抓，那根壞 bar
不會再被重抓修正。實例：`MSFU` 08-03 cache 收 35.84 / 量 6.85M，真實收盤
36.47 / 量 11.84M；`TSMX` 07-28 差 -3.4% 躺了六天。全庫實測 **475 檔 / 5,951 列**
受害，源頭是兩次在美股盤中跑的批次抓取。

寫任何「抓價 → 存 cache」的程式碼時：

1. **存盤前呼叫 `technical_analysis.drop_unsettled_bars(df, market)`**，當日未定案的列
   只留在記憶體（供盤中顯示），不落地。同源原則見 `_try_intraday_quote_as_today_bar`
   docstring。
2. **收盤判定一律用市場當地時間**（`is_bar_settled`：美東 16:00 / 台北 13:30 + 15 分
   結算緩衝）。**不可用本機時鐘** —— 台灣晚上正是美股盤中，本機日期還早一天。
   `refresh_universe_prices` 也有一套盤中剔除（`:362`），用 `<13:35` 是**正確的**
   （該工具只跑台股 `.TW`/`.TWO` 宇宙），但**不可照抄到含美股的路徑**。
3. **增量起算日含 cache 最後一列**（`last_date` 而非 `last_date + 1`），壞列才有機會
   被真收盤覆寫。順序有依賴：**第 1 點必須先成立，第 3 點才安全**，否則變成盤中值
   反覆覆寫已定案收盤價。
4. **量比價靈敏** —— 偵測未完成 bar 用成交量（只累積到抓取當下）比用收盤價可靠。
5. ⚠️ **「cache 與資料源不符」不等於未完成 bar** —— 分割 / 分拆會讓 yfinance
   **回溯調整整段歷史**，價量同時變，一樣觸發「量不足 + 價不符」。對分割股做
   **部分列覆寫會製造假跳空**（`DD` 一度憑空 +200%）；分割股只能整檔
   `force_update=True` 重抓。判別特徵：**連續多日同比例＝分割，單日隨機比例＝未完成 bar**。

**防回歸監控**：`tools/scan_unsettled_cache_bars.py` 已接夜間鏈
（`run_scanner.bat` 的 `US cache bar scan` stage，`--fail-on unsettled,split`），
上面第 5 點的分割判別已寫成程式碼護欄（`classify()`，`tests/test_unsettled_cache_scan.py`
釘住）。**夜間鏈刻意只報告不 `--apply`** —— 自動覆寫價格資料風險過高。
治本後這個 stage 應長期回 0；一旦有發現，就是有新的寫入路徑繞過了防護。
只掃美股，台股不掃的理由見該檔 docstring。

## TW vs US ticker detection

**唯一權威：`ticker_market.py`**（零依賴，任何模組都可 import）。

- `market_of(t)` / `is_tw(t)` / `is_us(t)` — 判準是「**數字開頭**」
  → TW (FinMind + TWSE/TPEX + TradingView) / US (Yahoo + Finviz + TradingView)
- `tw_core(t)` — 剝掉 `.TW` / `.TWO` 後綴（`.TWO` 先試，順序不可反）
- `has_tw_suffix(t)` — 是否已是 yfinance 可直接用的台股符號

**不要再手寫 `ticker.isdigit()` 判市場。** 台股**主動型 ETF**（`00981A` / `00982A` /
`00983A` / `00991A`）與槓桿反向 ETF（`00631L` / `00632R`）代號帶字母後綴，
`'00981A'.isdigit()` 是 **False**，會把台股整批送去美股路徑。

### 2026-08-05 已一次改齊（20 檔 / 89 處逐一分類）

實測資料（判別修法是否真的可行）：FinMind **有** `00981A`（291 列，到 2026-08-04）；
yfinance `00981A.TW` 可用、裸 `00981A` **404**；mis.twse 即時報價**不支援**主動型 ETF。

修掉的兩個真實斷更／缺資料路徑：

- `load_and_resample` 用它**選資料源** → `00981A` 走 yfinance 裸代號 404。
- `tools/refresh_universe_prices.py` 用 stem 的 `.isdigit()` **挑要刷新的 CSV**
  → 那檔**從來不在夜間名單內**。這才是斷更主因（`load_and_resample` 只在有人
  打開該股票時才跑）。已改為呼叫 `tools/tw_universe.is_tw_ticker()`，兩邊口徑統一。
  實測 `00981A_price.csv` 從 214 列 / 停在 2026-05-22 → 264 列 / 到 2026-08-04。

其餘改齊的分流點：`ai_report.py`（20 處）、`ai_report_pipeline.py`、`chip_analysis.py`
（原本連 `3324.TWO` 上櫃股都被回「非台股代號」）、`analysis_engine._detect_us_stock`、
`fundamental_analysis`、`individual_view`（5 處）、`peer_comparison`、`portfolio_store`
（持有主動型 ETF 時幣別/報價會走錯市場）、`news_fetcher`、`momentum_screener`、
`value_screener`、`finviz_data` / `sec_edgar` / `us_stock_chip`（美股專用模組原本沒攔住）。

### ⚠️ 刻意不改的三類（別「順手統一」）

1. **`sid.isdigit() and len(sid) == 4`** —— 那是「只要 4 碼普通股、排除 ETF 與權證」，
   不是市場判定（`momentum_screener` / `value_screener` / `screener_view`）。
2. **月營收 / F-Score / ETF 同步買超因子的 `isdigit()` 守衛** —— ETF 沒有營收也沒有
   F-Score，早退是對的（`addon_factors` ×2、`analysis_engine`、`position_monitor`）。
   改成 `is_tw()` 只會讓 ETF 通過守衛後白查一趟。
3. **`twse_api.py` 的 6 處批次報表列過濾** —— 那些餵大盤廣度／系統性籌碼**聚合**，
   放 ETF 進去會污染統計（2026-08-02 才修過美股混入台股廣度）。

### ⚠️ 未修的相鄰缺陷：`.TWO` cache key 被吃掉點號（99 檔，**有讀者、內容不重複**）

`cache_manager._get_path` 先 `replace('.TW','')` 再 `replace('.TWO','')`，所以
`3324.TWO` 會寫成 `3324O_price.csv`。實測 **99 個**這種檔，全部都有全數字兄弟檔。

⚠️ **2026-08-05 獨立審查訂正（finding F1）—— 本段初版寫的兩句話都是錯的**，
而且錯的方向會害人：照初版的說法會以為可以直接刪檔。實測結果：

- **不是「重複品」**：99 對裡 **85 個 `*O` 檔比全數字兄弟檔更長**、14 個更短、
  **0 個一樣長**（例：`1785O` 4,472 列 vs `1785` 2,410 列）。那是 `.TWO` 路徑抓到的
  另一份、通常更長的序列。
- **不是「沒有東西在讀」**：`tools/tw_universe.is_tw_ticker` 的 regex `^\d{4,6}[A-Z]?$`
  **會 match `1785O`**，所以 `tw_price_csvs(data_cache)` 回 **2,064 檔（含這 99 個）**，
  而 `data_cache/backtest/ohlcv_tw.parquet` 就把它們當**獨立 ticker** 收進去，貢獻
  **368,292 列**（2007-12-28 ~ 2026-04-08 凍結），且 99 個的全數字兄弟檔**全部也在**
  → **同一檔股票在 panel 裡被算兩次**。

這是**既有缺陷，不是市場判定改動造成的**（舊的 `stem.isdigit()` 同樣排除它們，
`_tw_cache_stems` 明確略過屬行為不變）。但動手前必須知道有讀者：**直接刪檔會讓
panel 少 368k 列**，改名成全數字 key 會和活著的兄弟檔對撞。
`ticker_market.tw_core()` 本身不犯這個順序錯誤（有測試釘住）。

驗證方式：`len(tw_price_csvs(data_cache))`、`ohlcv_tw.parquet` 裡結尾為 `O` 的
ticker 數與列數、以及逐對比較 `*O` 與兄弟檔的列數。
