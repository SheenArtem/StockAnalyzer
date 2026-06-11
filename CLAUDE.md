# StockAnalyzer — TW/US Trading Analysis System

## ⚠️ LLM Usage Rules (mandatory, locked 2026-05-01；Gemini 移除 2026-05-20；effort 加註 2026-05-21；codex fallback 加註 2026-05-21)

Any code calling Claude CLI / LLM SDK MUST follow:

| Module | LLM | model flag | effort | extra flag | timeout |
|---|---|---|---|---|---|
| **AI Report** (`ai_report.py` / `ai_report_pipeline.py` / `strong_stocks_ai_analysis.py`) | Claude | `--model opus` | `--effort xhigh` | `--allowedTools "*"` | 600s |
| **News / short-form / metadata extract** | Claude | `--model sonnet` | `--effort xhigh` | (optional) `--allowedTools` | 600s |
| **Calendar / structured table extract** | Claude | `--model haiku` | — (fast+cheap 不開 thinking) | — | 600s |
| **Sector tag extract (YT VTT / batch)** | Claude | `--model sonnet` | `--effort xhigh` | — | 600s |
| **Brokerage YT extract** (`tools/extract_yt_brokerage.py`) | **codex GPT-5.5 (primary)** + Claude Sonnet (fallback) | codex: `-c model_reasoning_effort=medium`<br>claude: `--model sonnet` | claude `--effort xhigh` | — | 600s |
| **Multi-agent debate / exploratory** | Claude | `--model sonnet` | `--effort xhigh` | `--allowedTools "WebSearch,WebFetch"` | 600s |
| **Macro Compass 第二視角** (`tools/macro_compass_report.py`) | Claude | `--model sonnet` 或 `opus` | `--effort xhigh` | `--allowedTools "WebSearch,WebFetch"` | 600s |

**Model choice rationale**: AI Report uses Opus (cost not a concern) / News uses Sonnet (balanced) / table extract uses Haiku (fast+cheap)。Gemini CLI 2026-05-20 暫停支援後全面撤除，所有節點改為 Claude 系列。Brokerage YT 2026-05-21 A/B 後改 codex GPT-5.5 primary（速度 4-6x，ticker code 較準，幻覺率 8% 可控）；Sonnet 當 codex quota / JSON 失敗時 fallback，必須帶 `--effort xhigh` 保證 fallback 品質。

**⚠️ `--effort` 強制規則 (2026-05-21)**: 實測 `claude -p` **不繼承 `~/.claude/settings.json` 的 `effortLevel`**（即使設 `max` 也 0 reasoning tokens）— 必須在 CLI 顯式帶 `--effort xhigh`。Haiku 例外（不開 thinking，保 fast+cheap 用意）。

**How to apply**:

- **New call → pick from table** — model + effort + timeout MUST follow above
- **Grep before changing** — `claude.*-p` / `--model` / `--effort`
- **AI Report MUST be Opus + effort xhigh** — `generate_report*` with `--model opus --effort xhigh --allowedTools "*"`
- **No timeout=None** — always specify explicit seconds
- **不要再加 Gemini 呼叫** — 2026-05-20 之後一律 Claude；若 Gemini CLI 復活想重啟，整批改前先在此 table 加回
- **新加 codex / OpenAI / 其他 LLM provider** — 必須先在此 table 加新列 + 註明 fallback 順序

---

## ⚠️ Core Principle: Robustness First (top priority, overrides others)

**This is a financial decision support tool. Errors are not "fix later when noticed" — they directly mislead trading decisions.**
Before any commit (new feature / bug fix / refactor / docs), do AT LEAST ONE of:

1. **Run before commit** — CLI / scheduled scripts run `python tools/xxx.py ...` at least once; lazy import / dynamic signature errors only surface on real call
2. **Grep callers before changing** — before changing API signature / return / function name, grep all call sites and align
3. **Run dry-run** — scripts with `--dry-run` / no-LLM-CLI mode MUST run before commit
4. **Fail loud, no swallowing** — schedulers swallowing exit codes / `try/except pass` / missing else branches are SERIOUS bugs

> Exploratory POCs or mid-refactor states that can't guarantee correctness MUST tag the commit message with **「未驗證」** or **「實驗」** — they cannot be merged as formal changes.

---

## ⚠️ BAT files: ASCII-only hard rule

**所有 `.bat` 必須純 ASCII (0x00-0x7F)** — CP950 codepage 解析 UTF-8 BAT 會 silent scheduler failure，Chinese REM / echo / full-width 全禁。

替代：`—`→`--` / `→`→`->` / `✓✗⚠`→`[OK][FAIL][WARN]` / Chinese REM/echo → English 或刪

範圍：scheduled `run_*.bat` / 工具 `tools/*.bat` / launcher `run_app.bat` 一律 ASCII 無例外，pre-commit hook 自動擋違規。

---

## Data Source Priority (unified strategy)

All features MUST follow the same priority to avoid data drift:

| Data Type | Primary | Fallback | Notes |
|---|---|---|---|
| Institutional buy/sell | TWSE/TPEX official | FinMind | Unified at ChipAnalyzer layer |
| OHLCV daily | Disk cache | FinMind → yfinance | `load_and_resample()` |
| OHLCV intraday (盤中 today bar) | mis.twse 即時 JSON | FinMind/yfinance daily fallback | TW only, 9:00-13:30；`mis_twse_client.get_quote()`；單檔/banner 用，**禁批次掃**（社群實測 5sec/3req 上限）|
| 大盤指數 (banner ^TWII/^GSPC/^IXIC/^SOX) | yfinance（NaN 尾列過濾）| ^GSPC/^IXIC → **FRED API close-only**（帶 key 0.5s；timeout 前科是無 key 的 fredgraph CSV 端點）→ last-good 落盤 stale；^TWII/^SOX → last-good | `market_banner._fetch_index_metrics`；FRED 路徑無 KD；失敗結果只短快取 5min |
| 台指期(全) 日盤+夜盤報價 | **mis.taifex 即時** (`getQuoteDetail`, SymbolID=`TXF{月碼A-L}{年尾數}-{F日盤/M盤後}`, CRefPrice=漲跌基準) | dlFutDataDown EOD CSV（只有已收盤時段） | `taifex_data.get_full_session_quote`；夜盤進行中回最新成交價；banner TTL 15min |
| Fundamentals (PE/PB) | yfinance + FinMind | TradingView fill | `get_fundamentals()` |
| Margin/ROE/ROA | TradingView Screener | — | TW + US unified |
| Margin trading / day trade / holdings | FinMind | — | No alternative |
| News | Google News RSS + udn money RSS | — | `news_fetcher.py` (per-ticker) / `tools/news_theme_extract.py` (theme batch) |
| Analyst consensus | yfinance | — | Target price / Forward EPS / rating |
| Peer comparison | TWSE/TPEX PER + FinMind industry tag | — | `peer_comparison.py` |
| TV-show YT mentions | yt-dlp auto-sub + Claude Sonnet | — | `fetch_yt_transcripts.py` → `extract_yt_sector_tags.py` → `data/sector_tags_dynamic.parquet` |
| Brokerage YT mentions | yt-dlp manual-sub + codex GPT-5.5 + Claude Sonnet fallback | — | 摩爾 8 分析師個人頻道 + 元大看盤室單頻道輪換 (BROKERAGES dict 加 `channel_type` 區分 personal/rotating_guest)；`fetch_yt_brokerage.py` → `extract_yt_brokerage.py` → `data/yt_brokerage_{mentions,videos}.parquet`；獨立 pipeline 不混 TV-show；**不接 AI 報告**（合規） |

## Data Source Discovery SOP — 不公開 / 改版後 endpoint 失蹤

**觸發**：宣告「No API」/ 走 LLM HTML parse / 自加權 proxy / 自寫 scraper 之前，**必跑 3 步**。

1. **第三方逆推（先做）** — macromicro / cnyes / Goodinfo / TradingView 圖表標「資料來源: X」就代表 X 真有，繼續挖；沒標或「整理計算」→ 第三方也 proxy
2. **試檔案下載** — JSON/CSV API 死掉後試 `staticFiles/*.zip` / `download?type=...` / `pdf/xlsx`；政府機構改版後新 URL 不在 sitemap / OpenAPI swagger / Google，用 DevTools Network tab 點 download 按鈕看真 XHR
3. **猜 path pattern** — TWSE 慣例 `/staticFiles/inspection/inspection/{type}/{subtype}/YYYYMM_C{type}{subtype}.zip`

**Why**: 2026-05-10 TWSE 大盤 PE 教訓 — 11 endpoint 全 404，逆推 macromicro 才找 ZIP path (`2cf8796`)。**Reference**: `memory/reference_twse_endpoints.md`

## Development Rules

### Avoid rework & duplicate fetching (⚠️ MOST IMPORTANT)

This project has many features. **Before implementing or modifying anything, check existing implementations** to avoid rework and wasted API/network calls.

**Pre-implementation checklist**:

1. **Read architecture first** — `app.py` module map + CLAUDE.md "Data Source Priority" table
2. **Grep existing functions** — `load_and_resample` / `get_fundamentals` / `ChipAnalyzer.*` / `peer_comparison`
3. **Confirm data path** — Scanner batch vs per-stock/AI per-ticker differ (memory `reference_data_path_diff`)
4. **Reuse upstream, don't refetch** — if `report` / `chip_data` / `fund_data` / `df_day` exists, NEVER recall API
5. **Extend existing cache first** — before adding new fetch, check if `cache_manager` can extend columns

**Forbidden**:

- ❌ **Recompute same indicator** — `technical_analysis` / `analysis_engine` / `ai_report` each computing once
- ❌ **Refetch same ticker** — multiple OHLCV/chip downloads for same stock in one flow
- ❌ **New API call for new feature** — not reusing session_state / upstream return values
- ❌ **Rewrite existing util** — PE/PB/yield/ROE already exist, don't rewrite

**If duplicate fetching/computation found, refactor to unify FIRST, then build new feature**.

### Language

- **Code comments**: Traditional Chinese + English mixed (**exception: `.bat` must be pure ASCII**)
- **Commit messages**: Traditional Chinese primarily, prefix in English (feat/fix/refactor)
- **UI text**: Traditional Chinese

### Versioning

- Version in `app.py`: `st.caption("Version: vYYYY.MM.DD.序號")`
- Git pre-commit hook validates version bump

### Cache Strategy

- Trading hours (09:00-13:30) TTL = 5min / after-hours TTL = full day
- Chip data: refresh after 21:30 daily / cache dir: `data_cache/` (CSV)
- TradingView / Google News: in-memory cache 30min~1hr
- `cache_manager.py` has `_cache_lock` for thread safety

### Chip Weights

`addon_factors.py::analyze_tw_chip_factors()` per C2-b IC validation (2026-04-16):
**Investment trust buy / margin / short-margin ratio / SBL "high = penalty" is the counter-intuitive correct answer** (IC validated, **DO NOT split**).
Weight values + IC report: see source + `reports/chip_ic_matrix.csv`.

### TW vs US ticker detection

- Pure digits or contains `.TW` → TW (FinMind + TWSE/TPEX + TradingView)
- Letters → US (Yahoo Finance + Finviz + TradingView)

## Notes

- **pytest** `tests/` 全綠（regression via `tools/snapshot_run_analysis.py` byte-for-byte）；新 case 接 `addon_factors` / `cache_manager`
- **FinMind free quota** 600 req/hr 易爆，法人已切 TWSE/TPEX primary
- **環境**：FinMind token 在 `local/.env`；Windows `.bat` launcher；Task Scheduler 主鏈見 `reference_scanner_all.md`
- **AI Report**：Claude CLI Team Plan quota（model 設定見頂部 LLM Rules）

## Tech debt resolved as "won't fix" (don't re-debate)

Won't fix: #3 SSL `verify=False` / #1c flat structure / #4b manual versioning / #5b requirements pinned.
Rationale: `docs/project_review_and_roadmap.md` Part 1 (2026-04-30 review). Already done: #1a/#1b/#2/#4a/#5a.
