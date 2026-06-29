# StockAnalyzer — TW/US Trading Analysis System

## ⚠️ LLM Usage Rules (mandatory)

Any code calling Claude CLI / LLM SDK MUST follow this table:

| Module | LLM | model flag | effort | extra flag | timeout |
|---|---|---|---|---|---|
| **AI Report** (`ai_report.py` / `ai_report_pipeline.py` / `strong_stocks_ai_analysis.py`) | Claude | `--model claude-opus-4-8[1m]` | `--effort max` | `--allowedTools "*"` | 7200s (2h) |
| **News / short-form / metadata extract** | Claude | `--model sonnet` | `--effort xhigh` | (optional) `--allowedTools` | 600s |
| **Calendar / structured table extract** | Claude | `--model haiku` | — (fast+cheap, no thinking) | — | 600s |
| **Sector tag extract (YT VTT / batch)** | Claude | `--model sonnet` | `--effort xhigh` | — | 600s |
| **Brokerage YT extract** (`tools/extract_yt_brokerage.py`) | codex GPT-5.5 (primary) + Claude Sonnet (fallback) | codex `-c model_reasoning_effort=medium` / claude `--model sonnet` | claude `--effort xhigh` | — | 600s |
| **Multi-agent debate / exploratory** + **AI Report 研究階段** (`report_web_research.py`) | Claude | `--model sonnet` | `--effort xhigh` | `--allowedTools "WebSearch,WebFetch"` | 600s |
| **Macro Compass 第二視角** (`tools/macro_compass_report.py`) | Claude | `--model sonnet`/`opus` | `--effort xhigh` | `--allowedTools "WebSearch,WebFetch"` | 7200s (2h) |
| **Theme curation** (`tools/curate_themes_pipeline.py`) | Claude | `--model sonnet` | `--effort xhigh` | `--allowedTools "WebSearch,WebFetch" --output-format json` | 420s/單題材 |

**⚠️ `--effort` 強制**：`claude -p` **不繼承** `~/.claude/settings.json` 的 `effortLevel`（即使設 max 也 0 reasoning tokens）— 必須 CLI 顯式帶 `--effort`。Haiku 例外（不開 thinking）。

**How to apply**:
- New call → pick from table；model + effort + timeout 必須照表
- Grep before changing — `claude.*-p` / `--model` / `--effort`
- AI Report 必須 Opus 4.8 1M + effort max + `--allowedTools "*"`
- No `timeout=None` — 一律明確秒數
- 新增 codex / OpenAI / 其他 provider → 先在此表加列 + 註明 fallback 順序

> 設計緣由（Opus/Sonnet/Haiku 分工、Gemini 撤除史、codex A/B 結果）見 memory `feedback_llm_usage_rules`。

---

## ⚠️ Core Principle: Robustness First (top priority, overrides others)

**This is a financial decision support tool. Errors directly mislead trading decisions — not "fix later".**
Before any commit (feature / fix / refactor / docs), do AT LEAST ONE of:

1. **Run before commit** — CLI / scheduled scripts 實跑 `python tools/xxx.py ...` 至少一次（lazy import / 動態簽章錯誤只在真呼叫時現形）
2. **Grep callers before changing** — 改 API 簽章 / 回傳 / 函式名前，grep 所有呼叫點對齊
3. **Run dry-run** — 有 `--dry-run` / no-LLM mode 的腳本 commit 前必跑
4. **Fail loud, no swallowing** — scheduler 吞 exit code / `try/except pass` / 缺 else 分支都是 SERIOUS bug

> 無法保證正確性的 POC / 重構中間態，commit message 必須標 **「未驗證」** 或 **「實驗」**，不得當正式變更合併。

---

## ⚠️ BAT files: ASCII-only hard rule

**所有 `.bat` 必須純 ASCII (0x00-0x7F)** — CP950 解析 UTF-8 BAT 會 silent scheduler failure。Chinese REM/echo/full-width 全禁。替代：`—`→`--` / `→`→`->` / `✓✗⚠`→`[OK][FAIL][WARN]`。

範圍：`run_*.bat` / `tools/*.bat` / `run_app.bat` 一律 ASCII，pre-commit hook 自動擋。

---

## Data Source Priority (unified strategy)

All features MUST follow the same priority to avoid data drift。實作細節（函式參數 / SymbolID 格式 / endpoint）見對應 memory。

| Data Type | Primary | Fallback | Notes |
|---|---|---|---|
| Institutional buy/sell | TWSE/TPEX official | FinMind | 統一在 ChipAnalyzer 層 |
| OHLCV daily | Disk cache | FinMind → yfinance | `load_and_resample()` |
| OHLCV intraday (today bar) | mis.twse 即時 JSON | FinMind/yfinance daily | TW only 9:00-13:30；單檔/banner 用，**禁批次掃**（5sec/3req 上限）|
| 大盤指數 (^TWII/^GSPC/^IXIC/^SOX) | yfinance（濾 NaN 尾列）| ^GSPC/^IXIC → FRED API → last-good 落盤；^TWII/^SOX → last-good | `market_banner._fetch_index_metrics`；失敗只短快取 5min |
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

### Data Source Discovery SOP
宣告「No API」/ 走 LLM HTML parse / 自寫 scraper 前**必跑 3 步**：
1. **第三方逆推** — macromicro/cnyes/Goodinfo/TradingView 圖表標的「資料來源: X」就是真來源，繼續挖
2. **試檔案下載** — API 死掉試 `staticFiles/*.zip` / `download?type=` / `pdf/xlsx`；用 DevTools Network tab 看真 XHR
3. **猜 path pattern** — TWSE 慣例 `/staticFiles/.../{type}/{subtype}/YYYYMM_C{type}{subtype}.zip`

> 緣由（2026-05-10 TWSE PE 11 endpoint 全 404 教訓）+ endpoint cheat sheet 見 memory `reference_twse_endpoints`。

---

## Development Rules

### Avoid rework & duplicate fetching (⚠️ MOST IMPORTANT)

實作 / 修改前**先查既有實作**，避免重工與浪費 API/網路。

1. **先讀架構** — `app.py` module map + 上方 Data Source Priority 表
2. **Grep 既有函式** — `load_and_resample` / `get_fundamentals` / `ChipAnalyzer.*` / `peer_comparison`
3. **確認資料路徑** — Scanner batch vs per-stock/AI per-ticker 不同（memory `reference_data_path_diff`）
4. **複用上游別重抓** — `report` / `chip_data` / `fund_data` / `df_day` 已存在就 NEVER 再呼叫 API
5. **先擴充既有 cache** — 加新 fetch 前先看 `cache_manager` 能否擴欄

**Forbidden**：同指標重算 / 同 ticker 重抓 / 新功能新開 API 不複用 session_state / 重寫既有 util（PE/PB/yield/ROE 已有）。
發現重複抓取或計算 → **先 refactor 統一，再建新功能**。

### Language
- Code 註解：繁中 + English 混用（**例外：`.bat` 純 ASCII**）
- Commit message：繁中為主，prefix English（feat/fix/refactor）
- UI text：繁中

### Versioning
- `app.py`：`st.caption("Version: vYYYY.MM.DD.序號")`；pre-commit hook 驗證 version bump

### Cache Strategy
- 盤中 (09:00-13:30) TTL 5min / 盤後 TTL full day
- 籌碼：每日 21:30 後 refresh；cache dir `data_cache/` (CSV)
- TradingView / Google News：in-memory 30min~1hr
- `cache_manager.py` 有 `_cache_lock` 保執行緒安全

### Chip Weights
`addon_factors.py::analyze_tw_chip_factors()` per C2-b IC validation：
**投信買超 / 融資 / 券資比 / 借券「high = penalty」是反直覺但 IC 驗證正確的答案（DO NOT split）**。
權重值 + IC 報告見原始碼 + `reports/chip_ic_matrix.csv`。

### TW vs US ticker detection
- 純數字或含 `.TW` → TW (FinMind + TWSE/TPEX + TradingView)
- 含字母 → US (Yahoo Finance + Finviz + TradingView)

---

## Notes
- **pytest** `tests/` 是 regression gate（`tools/snapshot_run_analysis.py` byte-for-byte）；新 case 接 `addon_factors` / `cache_manager`
- **FinMind free quota** 600 req/hr 易爆，法人已切 TWSE/TPEX primary
- **環境**：FinMind token 在 `local/.env`；Windows `.bat` launcher；Task Scheduler 主鏈見 memory `reference_scanner_all`
- **AI Report**：Claude CLI Team Plan quota（model 見頂部 LLM Rules）

## Tech debt resolved as "won't fix" (don't re-debate)
Won't fix：#3 SSL `verify=False` / #1c flat structure / #4b manual versioning / #5b requirements pinned。
Rationale：`docs/project_review_and_roadmap.md` Part 1。Already done：#1a/#1b/#2/#4a/#5a。
