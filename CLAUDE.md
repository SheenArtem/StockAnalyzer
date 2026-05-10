# StockAnalyzer — TW/US Trading Analysis System

## ⚠️ LLM Usage Rules (mandatory, locked 2026-05-01)

Any code calling Claude CLI / Gemini CLI / LLM SDK MUST follow:

| Module | LLM | model flag | extra flag | timeout |
|---|---|---|---|---|
| **AI Report** (`ai_report.py` / `ai_report_pipeline.py`) | Claude | `--model opus` | `--allowedTools "*"` | 600s |
| **News / short-form / metadata extract** | Claude | `--model sonnet` | (optional) `--allowedTools` | 600s |
| **Calendar / structured table extract** | Claude | `--model haiku` | — | 600s |
| **Sector tag extract (YT VTT / batch)** | Claude(primary) / Gemini(backup) | `sonnet` / `gemini-3.1-pro-preview` | — | 600s / 900s |
| **Multi-agent debate / exploratory** | Claude | `--model sonnet` | `--allowedTools "WebSearch,WebFetch"` | 600s |
| **Any Gemini call** | Gemini | `gemini-3.1-pro-preview` | — | 900s |

**Model choice rationale**: AI Report uses Opus (cost not a concern) / News uses Sonnet (balanced) / table extract uses Haiku (fast+cheap) / Gemini always preview.

**How to apply**:

- **New call → pick from table** — model + timeout MUST follow above
- **Grep before changing** — `claude.*-p` / `--model` / `gemini.*-p`
- **AI Report MUST be Opus** — `generate_report*` with `--model opus --allowedTools "*"`
- **No timeout=None** — always specify explicit seconds

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

**All `.bat` files MUST contain only ASCII (0x00-0x7F). Chinese REM, echo, full-width chars all forbidden.**
(CP950 codepage misparses UTF-8 BAT → silent scheduler failures; see memory for past incidents)

### Substitutions

`—` → `--` / `→` → `->` / `✓✗⚠` → `[OK][FAIL][WARN]` / Chinese REM/echo → English or delete

### Exemptions: NONE

All **scheduled BATs (`run_*.bat`)**, **tool BATs (`tools/*.bat`)**, **manual launcher BATs (`run_app.bat`)** are pure ASCII. pre-commit hook blocks violations.

Manual check:

```bash
PYTHONIOENCODING=utf-8 python -c "
import glob
for p in glob.glob('**/*.bat', recursive=True):
    n = sum(1 for b in open(p,'rb').read() if b > 127)
    if n > 0: print(f'{p}: {n} non-ASCII')"
```

---

## Data Source Priority (unified strategy)

All features MUST follow the same priority to avoid data drift:

| Data Type | Primary | Fallback | Notes |
|---|---|---|---|
| Institutional buy/sell | TWSE/TPEX official | FinMind | Unified at ChipAnalyzer layer |
| OHLCV daily | Disk cache | FinMind → yfinance | `load_and_resample()` |
| OHLCV intraday (盤中 today bar) | mis.twse 即時 JSON | FinMind/yfinance daily fallback | TW only, 9:00-13:30；`mis_twse_client.get_quote()`；單檔/banner 用，**禁批次掃**（社群實測 5sec/3req 上限）|
| Fundamentals (PE/PB) | yfinance + FinMind | TradingView fill | `get_fundamentals()` |
| Margin/ROE/ROA | TradingView Screener | — | TW + US unified |
| Margin trading / day trade / holdings | FinMind | — | No alternative |
| News | Google News RSS + udn money RSS | — | `news_fetcher.py` (per-ticker) / `tools/news_theme_extract.py` (theme batch) |
| Analyst consensus | yfinance | — | Target price / Forward EPS / rating |
| Peer comparison | TWSE/TPEX PER + FinMind industry tag | — | `peer_comparison.py` |

## Data Source Discovery SOP — 不公開 / 改版後 endpoint 失蹤

**觸發**：宣告「No API available」/ 走 LLM HTML parse / 自加權 proxy / 自寫 scraper 之前，**必先跑下列 3 步**。

### Step 1 — 第三方逆推（最便宜，先做）

去 **macromicro / cnyes / Goodinfo / TradingView** 等台股入口找同樣 chart/數據：

- 圖表底下標「資料來源: 官方機構X」→ 證明 X 真有，繼續挖
- 沒標 source / 標「整理計算」→ X 可能也是 proxy，回頭考慮 LLM/proxy 路線

### Step 2 — JSON API 死掉試檔案下載

JSON / CSV API 全 404 後**必試**（順序）：

1. `staticFiles/.../*.zip`（Excel / HTML zipped reports）
2. `download?type=...&subType=...&date=...`（download button 觸發 XHR）
3. `pdf/...` / `xlsx/...`（PDF / Excel reports）

政府機構改版後新 URL 通常**不在** sitemap / OpenAPI swagger / robots.txt / Google index。用 browser DevTools Network tab 點 download 按鈕看真實 XHR。

### Step 3 — 猜 path pattern

TWSE 統計類報告慣例：

```
/staticFiles/inspection/inspection/{type}/{subtype}/YYYYMM_C{type}{subtype}.zip
```

`type` / `subtype` 在 statisticsList?type=XX&subType=YY URL 可看出。

### Why（2026-05-10 TWSE 大盤 PE 教訓）

前兩輪 agent 試 11 個 JSON endpoint 全 404 / per-stock，差點走 LLM HTML parse 或 4hr 自加權 proxy。第三輪逆推 macromicro 才找到隱藏 ZIP path（commit `2cf8796` 落地 196 月）。

**Reference**: `memory/reference_twse_endpoints.md`（具體 URL / xlrd 解析範例 / SSL note）

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

- **pytest** — `tests/` 66 tests all green (piotroski 20 / pattern_detection 15 / scenario_engine 18 / post_validate_numbers 13), 0.7s. Next batch: cover `addon_factors` / `cache_manager` pure functions, see `tests/README.md`
- **analysis_engine.py** — 2026-04-23 M2 refactor 2281→937 lines (-59%), regression via `tools/snapshot_run_analysis.py` byte-for-byte
- **FinMind free quota** — 600 req/hr, easy to exceed. Institutional already switched to TWSE/TPEX primary
- **No .env** — FinMind token in `local/.env`, other settings hardcoded + session state + JSON
- **Windows platform** — `.bat` launchers, path handling needs Windows compatibility
- **Task Scheduler** — main chain `run_scanner.bat` (TUE-SAT 00:00) + bulk revenue / TDCC / C1 / MOPS probe; see memory `reference_scanner_all.md`
- **AI Report** — Claude CLI `claude -p --model opus --allowedTools "*"` (Team Plan quota, see LLM Rules at top)

## Tech debt resolved as "won't fix" (don't re-debate)

Won't fix: #3 SSL `verify=False` / #1c flat structure / #4b manual versioning / #5b requirements pinned.
Rationale: `docs/project_review_and_roadmap.md` Part 1 (2026-04-30 review). Already done: #1a/#1b/#2/#4a/#5a.
