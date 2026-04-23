# Robustness Scan — 2026-04-23

**Trigger**: 2026-04-22 auto_ai_reports 三連 bug 事件後，user 要求全 codebase scan
**Method**: 3 parallel Explore agents，12 向度（Python/Streamlit 版本）
**Consolidator verification**: 每個 🔴 High finding 已人工 grep 驗證，noise 已剔除

---

## 📊 Executive Summary

| 層級 | 統合後 findings | 重疊度 |
|---|---|---|
| 🔴 High（影響正確性 / 靜默失敗） | **8** | 2 group 都提到 5 個 |
| 🟡 Med（可能誤導 / 結構負債） | **12** | 各 group 各別發現為主 |
| 🟢 Low 計數 | ~50 處（print 當 log / 資源未 close / 型別寬鬆） | — |
| ❌ 偽陽性 / 過時 / CLAUDE.md 已註記 | 8 | 排除列表在最後 |

**結論**：今日事件**不是孤立事件**。Codebase 有 3 類系統性漏洞：
1. **靜默失敗機制**（吞 exit code、except pass、缺 log）— 最危險，直接對應昨日事件
2. **Lazy import + tuple unpack 型別鬆散**— 同樣模式在其他 14 處存在
3. **Code duplication**— auto_ai_reports/_ai_report_worker、momentum/value screener、chip fetch 三套重複

---

## 🔴 High Findings（必修 / 優先修）

### H1. 排程腳本吞 exit code（今日事件根因）
- **File**: `run_scanner.bat:87-89`
- **問題**: `python tools\auto_ai_reports.py --n 3 --format md >> scanner.log 2>&1` 執行後不檢查 exit code，直接進下一行。註解明文寫「report failures are non-critical」— 這個設計假設已被昨日事件反證
- **影響**: 靜默失敗整晚，隔日早上才發現
- **Fix**: `if errorlevel 1 (python -c "from scanner_job import send_alert_notification; send_alert_notification(...)")`，或最低限度把 exit code 寫到 scanner.log 末尾 + Task Scheduler trigger 失敗警示

### H2. auto_ai_reports.py lazy imports 在 runtime 才爆
- **File**: `tools/auto_ai_reports.py:40-43`（fn `_run_one` 內）
- **問題**: 4 個 `from X import Y` 都在 function body 內。lazy import 本身不違法，但搭配 H1 的 exit-code-swallow = 定時炸彈
- **影響**: 今日三連 bug 全部是此模式（module rename / API signature drift 在排程才爆）
- **Fix**:
  - 短期：保留 lazy import，但加 module-level `_smoke_imports()` 在 main() 開頭呼叫一次（import 驗證）
  - 長期：配合 H3 根因剷除

### H3. auto_ai_reports 是 `_ai_report_worker` 的重複實作（code duplication drift）
- **File**: `tools/auto_ai_reports.py:_run_one` vs `app.py:_ai_report_worker`
- **問題**: 兩份幾乎一樣的 pipeline（load 價量 → load 籌碼 → load 基本面 → run analyzer → call Claude CLI → save）。`_ai_report_worker` 每天被 UI 用，錯誤會立刻被看到；`auto_ai_reports` 只有 22:00 排程跑，錯誤會躲過
- **影響**: 任何一邊被 refactor，另一邊會走樣。今日就是這樣炸的
- **Fix**（Option H 之前 Discord 提過）：抽出 `ai_report_pipeline.py` 模組，兩者共用同一 `generate_one_report(ticker, fmt, progress_cb=None)` 函式；UI 傳 `progress_cb` 寫 session_state，CLI 傳 `progress_cb=logger.info`

### H4. `_ai_report_worker` thread-unsafe dict mutation
- **File**: `app.py:244-329`
- **驗證**: `grep threading.Lock app.py` = 0（已確認）
- **問題**: 背景 thread 對 `job` dict 寫入（`job['progress'].append`, `job['status'] = ...`），主 UI thread 從 `session_state` 讀同一 dict（`app.py:1925-1950`）。Python GIL 讓單次 append/assign 是 atomic，但**迭代 `job['progress']` 時若 worker 同時 append → `RuntimeError: list changed during iteration`**
- **影響**: UI 偶發 race crash（使用者看到 exception page）
- **Fix**: 加 `_job_lock = threading.Lock()` module-level；worker 所有 `job[...]` 寫入包 `with _job_lock:`；UI 讀取也同樣包。或改用 `queue.Queue` 傳 progress

### H5. `ChipAnalyzer.get_chip_data()` 回傳 tuple，14 caller 都靠紀律 unpack
- **File**: `chip_analysis.py:13`（return type `(dict, str|None)`）
- **驗證**: grep 結果 14 個 caller，1 個今日漏（已修）
- **問題**: 新加 caller 第 15 個時，沒讀過 source 就會再漏一次。tuple-return 在大多數內部 API 是 anti-pattern
- **Fix Options**:
  - A) 改 raise exception on error（Pythonic 做法）
  - B) 回傳 dataclass `ChipResult(data: dict, error: str | None)`，存取 `.data` 不會 unpack 錯
  - C) 保持 tuple 但加 type stub（`Tuple[Dict, Optional[str]]`）+ pre-commit mypy check

### H6. 靜默 `except Exception: pass`（共 100+ 處）
- **File**: 散落各處（`ai_report.py:306, 1120`, `app.py` 11 處, `dividend_revenue.py` 5 處, `momentum_screener.py` 12 處）
- **問題**: 違反 Robustness First 原則 4「靜默失敗視為嚴重 bug」。尤其 `ai_report.py:306` 是今日 TDCC 事件的 pattern
- **分類**:
  - 🔴 不該吞的（如 ai_report 主 pipeline 內）: 預估 20 處
  - 🟡 probe 性質合理吞（tdcc optional feature）: 預估 30 處
  - 🟢 single-stock fail 在 batch scan 內吞，合理但缺 log: 預估 50 處
- **Fix**:
  - 第 1 類：改 raise 或 `logger.error` + `return None`
  - 第 2 類：改 `except Exception as e: logger.debug(...)` 保留優雅失敗但可診斷
  - 第 3 類：改 `except Exception as e: logger.warning("Skip %s: %s", stock_id, e); failures.append(stock_id)`

### H7. 籌碼抓取邏輯 3 處各寫一份
- **File**:
  - `chip_analysis.py:13-357`（canonical ChipAnalyzer）
  - `value_screener.py:1285-1297`（手寫 batch → fallback 邏輯）
  - `momentum_screener.py:985-1001`（類似但不同）
- **問題**: 同 H3 的 code duplication drift 風險；違反 CLAUDE.md「避免重工 & 重複抓取」明文禁令
- **影響**: 若 `ChipAnalyzer.get_chip_data` 改 signature，兩個 screener 都要改，容易漏
- **Fix**: 抽 `chip_fetcher.py` 單一 function `fetch_chip_for_scan(stock_id, market, batch_cache=None)` 三個 caller 共用

### H8. 新聞抓取的 stock_id/name 未 escape 進 prompt
- **File**: `ai_report.py:798-801`（assemble_prompt 對 WebSearch hint 插值）
- **問題**: 金融工具 prompt injection 後果嚴重（誤導分析 → 誤導交易）。雖然目前 `validate_ticker()` 限制 ticker 格式，但 stock_name 從 fund_data 撈，若 FinMind 回傳奇怪名稱可破壞 prompt 結構
- **影響**: 低機率高影響
- **Fix**: 對 stock_name 強制 `repr()` 或移除 `\n"` `{` `}`；prompt 內特殊字元用 base64 包

---

## 🟡 Medium Findings（建議修，非緊急）

| # | File:Line | 類別 | 摘要 |
|---|---|---|---|
| M1 | `finviz_data.py:66`, `etf_signal.py:29`, `twse_api.py:51`, `taifex_data.py:97`, `sec_edgar.py:67`, `mops_fetcher.py:198` | Resource leak | `requests.Session()` 無 `.close()` — class 層級 session，實際洩漏量 bounded（每類 1-2 個），不致命但應補 `__del__` |
| M2 | `analysis_engine.py`（67KB, 2281 lines, 25 methods） | God class | TechnicalAnalyzer 混合技術面 + 籌碼 + 情緒 + 營收 + ETF 訊號 + pattern。拆分需要大 refactor，放 backlog |
| M3 | `technical_analysis.py:660` return 5-tuple | API design | `plot_dual_timeframe()` 回傳 `(figures, errors, df_week, df_day, meta)`，26 個 caller 都手寫 unpack；改 order 會爆 |
| M4 | `chip_analysis.py:48,57,77` + `dividend_revenue.py`（30+ 處） | Logging | `print()` 當 logger 用，違反 user global CLAUDE.md GL2。Streamlit 環境下 print 也不 thread-safe |
| M5 | `analysis_engine.py` 多處 `.iloc[-1]` | Type safety | 已 grep：19 處，~14 處有上游 `df.empty` 或 `len(df) >= N` 保護，~5 處沒明顯保護（line 913 `df.iloc[-1]['Close'] if not df.empty else 0` 有，其他要逐個驗證） |
| M6 | `analysis_engine.py:359`, `momentum_screener.py:544` | Type safety | `float(pe_str)` 沒 try/except — 若 FinMind 回 'TBD' / 'N/A' 會 crash |
| M7 | `cache_manager.py` FinMind loader | Error surfacing | 失敗只 log warning，caller 無法區分「無資料」vs「網路掛了」 |
| M8 | `scanner_job.py:47-52` | Error handling | `_load_latest_regime()` silent fail on JSON decode error |
| M9 | `ai_report.py` _build_* functions | Prompt assembly | 10 個 builder function 用 f-string 拼文字，未來加欄位易失控 |
| M10 | `momentum_screener.py`/`value_screener.py` 多處 iterrows | Performance | DataFrame iterrows 比 vectorized 慢 100x，scan 時間多 60-120 秒 |
| M11 | `run_scanner.bat` auto_ai_reports 的失敗無 Discord ping | Observability | 已有 `scanner_job.send_discord_notification` helper，卻沒被 auto_ai_reports 使用 |
| M12 | 所有 `from X import Y` in function body 共 30+ 處 | Import hygiene | lazy import 過量，靜態分析看不出 signature drift；建議列白名單（確實需 lazy 的才保留） |

---

## ❌ 偽陽性 / 過時 / 已註記（透明 disclosure）

| Agent report 項 | 狀態 | 原因 |
|---|---|---|
| Group C#10 `backtest_engine.py` dead code | CLAUDE.md 已明文「閒置模組」 | 非 bug |
| Group C#11 `strategy_manager.py` dead code | 同上 | 非 bug |
| Group C#12 `ml_signal.py` dead code | 同上 | 非 bug |
| Group C#8 auto_ai_reports import 未檢查 | 今日已修（commit 318a561） | 過時 |
| Group C#14 run_scanner.bat exit code | = H1，合併 | 重複 |
| Group C#9 auto_ai_reports exit code 未傳 | = H1 | 重複 |
| Group B#1 generate_report_html vs generate_report 回傳元組不一致 | 不同 entry point（html vs md 模式）by design | 非 bug |
| Group C#5 momentum 1500-ticker scan O(N) 慢 | 無 vectorized 可用替代方案（per-stock 決策） | 架構限制非 bug |

---

## 🎯 建議行動計畫（按 ROI 排序）

### Phase 1（今天/明天內，1-3 小時）
1. **H1 修 `run_scanner.bat` exit code handling**（20 分）
2. **H2 加 auto_ai_reports 的 `_smoke_imports()` pre-flight check**（10 分）
3. **H6 掃 top 10 最危險的 `except Exception: pass`（ai_report / scanner_job）補 logger.warning**（30 分）

### Phase 2（本週內，3-6 小時）
4. **H3 重構 auto_ai_reports → 共用 `_ai_report_worker`** 核心（抽 `ai_report_pipeline.py`）
5. **H4 加 `_job_lock` 保護 `_ai_report_worker`**（30 分）
6. **H7 合併 3 處 chip 抓取邏輯到 `chip_fetcher.py`**（1-2 小時）

### Phase 3（排 backlog）
7. **H5 ChipAnalyzer 改 raise/dataclass（大範圍變更，14 caller 要同步）**
8. **M2 analysis_engine.py 拆分（需要先寫 regression test）**
9. **M4 print → logger 批次替代**
10. **M10 iterrows → vectorized（performance pass）**

### 不做（報酬率低 / 風險高）
- 閒置模組歸檔（CLAUDE.md 已標註，留作未來探索）
- Resource leak 加 `__del__`（實際影響 bounded，暫不動）

---

**Scan completed**: 2026-04-23
**Verified by**: 手動 grep + log 對照源碼（遵守 user global CLAUDE.md feedback_audit_verification 規則）
