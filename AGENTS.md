# StockAnalyzer — TW/US Trading Analysis System

跨 agent 共用的 repo 規則權威。工具轉接檔（`CLAUDE.md` 等）只指向本檔，不自帶規則。
**開工前先讀 `.ai/HANDOFF.md`**（本機交接檔，不入版控）了解進行中 / 未 commit 的工作。

## Routing

| Read | When |
|---|---|
| `docs/agent/llm-usage.md` | **任何呼叫 Claude CLI / LLM SDK 的程式碼**（新增、改 model / effort / timeout、換 provider）|
| `docs/agent/data-sources.md` | 新增或修改任何資料抓取 / 快取 / fallback，或不確定某資料該從哪個來源拿 |
| `docs/project_review_and_roadmap.md` Part 1 | 想重啟 tech debt 議題前（won't-fix 理由都在裡面）|
| `.ai/HANDOFF.md` → `WORKLOG.md` / `DECISIONS.md` | 交接、續作、查決策脈絡（本機檔，不入版控）|

---

## ⚠️ LLM Usage Rules (mandatory)

寫任何呼叫 Claude CLI / LLM SDK 的程式碼前，**先讀 `docs/agent/llm-usage.md`** — 每個模組的 model / effort / timeout 都是規定值，不可憑直覺挑。改 model 或 effort 前先 grep `claude.*-p` / `--model` / `--effort` 對齊所有呼叫點。

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

## ⚠️ BAT files: ASCII-only + CRLF hard rule

**所有 `.bat` 必須純 ASCII (0x00-0x7F)** — CP950 解析 UTF-8 BAT 會 silent scheduler failure。Chinese REM/echo/full-width 全禁。替代：`—`→`--` / `→`→`->` / `✓✗⚠`→`[OK][FAIL][WARN]`。

**且必須 CRLF 行尾** — cmd.exe 用 byte-offset 追蹤 batch 執行位置且假設 CRLF；**LF-only 檔會讓 `goto` 跳錯行**（byte 累計偏移），停用區塊的 `goto skip_*` 失效→誤跑 dead code（2026-06-29 run_scanner_weekly 因此誤觸 `git push`）。詳見 Claude memory `project_bat_crlf_goto_corruption`。

範圍：`run_*.bat` / `tools/*.bat` / `run_app.bat` 一律 ASCII + CRLF，pre-commit hook 自動擋（含 lone-LF 偵測）。

## ⚠️ Python 檔：UTF-8 + 可編譯（pre-commit 自動擋）

**批次改含中文的檔案，一律用 `read_bytes` / `write_bytes`，不要讓中文經過 cp950 邊界。**
`pathlib.write_text` 在 Windows 還會把 LF 全轉 CRLF，造成整檔假 diff。

2026-08-02 全 `tools/` 掃描發現 **5 支已追蹤 `.py` 從進版控起就是非法 UTF-8**
（`ab_test_codex_vs_sonnet` / `dcf_ic_analyze` / `test_brokerage_yt` /
`vf_chip_dual_inst_signal` / `_regime_gate`），Python 直接拒絕解析，逐版追查確認
歷來沒有任何版本編譯得過 —— 寫入當下就以有損編碼落盤，一路 commit 進來無人攔截。
程式碼是 ASCII 沒壞，壞的是中文註解、docstring 與**功能性字串**（頁面比對字串、
DataFrame 中文欄位名都受損）。已於 `137eae3` 全數修復。

pre-commit hook 現在會對 staged 的 `.py` 檢查「能以 UTF-8 解碼」且「能通過
`py_compile`」，任一不過即擋下 commit。

---

## Development Rules

### Avoid rework & duplicate fetching (⚠️ MOST IMPORTANT)

實作 / 修改前**先查既有實作**，避免重工與浪費 API/網路。

1. **先讀架構** — `app.py` module map + `docs/agent/data-sources.md` 的 Data Source Priority 表
2. **Grep 既有函式** — `load_and_resample` / `get_fundamentals` / `ChipAnalyzer.*` / `peer_comparison`
3. **確認資料路徑** — Scanner batch vs per-stock/AI per-ticker 不同（Claude memory `reference_data_access`）
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

### Chip Weights
`addon_factors.py::analyze_tw_chip_factors()` per C2-b IC validation：
**投信買超 / 融資 / 券資比 / 借券「high = penalty」是反直覺但 IC 驗證正確的答案（DO NOT split）**。
權重值 + IC 報告見原始碼 + `reports/chip_ic_matrix.csv`。

---

## Notes
- **pytest** `tests/` 是 regression gate（`tools/snapshot_run_analysis.py` byte-for-byte）；新 case 接 `addon_factors` / `cache_manager`
- **FinMind free quota** 600 req/hr 易爆，法人已切 TWSE/TPEX primary
- **環境**：FinMind token 在 `local/.env`；Windows `.bat` launcher；Task Scheduler 主鏈見 Claude memory `reference_scanner_all`
- **AI Report**：Claude CLI Team Plan quota（model / effort / timeout 見 `docs/agent/llm-usage.md`）

## Tech debt resolved as "won't fix" (don't re-debate)
Won't fix：#3 SSL `verify=False` / #1c flat structure / #4b manual versioning / #5b requirements pinned。
Rationale：`docs/project_review_and_roadmap.md` Part 1。Already done：#1a/#1b/#2/#4a/#5a。
