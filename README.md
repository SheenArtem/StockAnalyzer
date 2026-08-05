# StockAnalyzer

台股 / 美股 交易分析系統。Streamlit UI + 排程 scanner + AI 報告。

---

## Quickstart（5 分鐘跑起來）

### 1. 環境

- **Python 3.13+**
- **Windows 10/11**（排程依賴 Task Scheduler；`.bat` 啟動器；其他平台需自行調整）
- **Claude CLI**（AI 報告必須）— 安裝方式見 https://code.claude.com/docs

### 2. 安裝依賴

```bash
git clone https://github.com/SheenArtem/StockAnalyzer.git
cd StockAnalyzer

pip install -r requirements.txt
playwright install chromium      # PDF 印出 + UI 測試用
```

### 3. 建立 `local/.env`

`local/` 已是 git-ignored 目錄。**必須**建立 `local/.env` 並填入：

```bash
# FinMind API Token (台股財報 / 籌碼 / 月營收)
# 申請: https://finmindtrade.com/  (免費 600 req/hr)
FINMIND_API_TOKEN=your_finmind_token_here

```

> Discord webhook 已於 2026-07-06 全數退役（`git grep DISCORD_WEBHOOK -- '*.py'` 零命中），
> 排程結果與警報一律印進 `scanner.log` 等排程 log。**不要再設 `DISCORD_WEBHOOK_*`，
> 也不要新寫 webhook 呼叫。**

> ⚠️ 不要 commit `local/.env`。專案 `.gitignore` 已涵蓋 `local/`。

### 4. 啟動 UI

```bash
run_app.bat              # Windows
# or:
python -m streamlit run app.py
```

預設 `http://localhost:8501`。Sidebar 輸入股票代號（台股 4 碼 / 美股代號）即可分析。

---

## 主要功能模組

「狀態」欄是**現況**，不是設計意圖：`排程` = 有 Windows 工作排程實際在跑；
`手動` = 程式碼在、只在人工觸發時跑；`停用` = 刻意關閉但程式碼保留（重啟方式寫在對應
bat 或 `app.py` 的註解裡）。

| 功能 | 入口 | 狀態 | 說明 |
|---|---|---|---|
| 個股分析 UI | `app.py` | 排程（開機自啟） | 技術 / 基本面 / 籌碼 / 同業 / AI 報告 |
| 總經大盤風向 | `app.py` → 🧭 | 排程（面板日更） | 7 區塊；資料由 `run_macro_panels_*.bat` 產 |
| 市場掃描 | `app.py` → 📡 | 排程 | 法人週榜 / 題材動能 / 新聞流量異常 |
| AI 報告 | `app.py` → 📝 / `tools/auto_ai_reports.py` | 手動 | Claude Opus 深度分析；儀表板在 📝 tab（daily 排程已停用）|
| 投資組合 | `app.py` → 💼 | 手動 | 手輸交易紀錄 → 持股 / 損益 / TWR；純本地不入版控 |
| 知識庫 | `app.py` → 📚 | 手動 | 本地筆記（`data/notes/*.md`，零 DB）|
| 題材策展 | `app.py` → 🎨 | 手動 | TW + US 多市場題材 |
| 新聞題材 | `tools/news_theme_extract.py` | 排程 | UDN + cnyes RSS → Claude Sonnet 萃取 |
| 籌碼面 | `chip_analysis.py` / `chip_history_dl.py` | 排程 | 三大法人 / 融資融券 / 借券 / 當沖 / TDCC |
| QM 品質選股 | `scanner_job.py --mode qm` | 停用 | F-Score 50% + 體質 30% + 趨勢 20%；2026-05-23 起 informational |
| Value 選股 | `scanner_job.py --mode value` | 停用 | 5 因子組合 |
| Momentum 選股 | `scanner_job.py --mode momentum` | 停用 | 全市場掃描 trigger_score |
| 強勢股報告 PDF | `tools/strong_stocks_daily.py` (日) / `_weekly_screener.py` (週) | 停用 | 日報與週報**皆已停用**（2026-05-21，2026-06-16 billing 顧慮解除後仍確認維持停用）|
| Step-A / Paper trade | `tools/step_a_engine.py` / `paper_trade_engine.py` | 停用 | 2026-05-23 起 `goto skip_mode_d` |
| 投顧 YT 抓取 | `tools/fetch_yt_brokerage.py` 等 3 支 | 手動 | 2026-05-22 起退出主排程鏈（`goto skip_brokerage_yt`），改跑 `run_yt_brokerage_sync.bat` |
| 投顧追蹤 tab | `app.py` → 📺 | 停用 | 已從 `_mode_options` 移除（handler 保留為死代碼）；合規考量不接 AI 報告 |

詳細架構與開發規範：見 [`AGENTS.md`](AGENTS.md)（跨 agent 規則權威），細節路由到
[`docs/agent/data-sources.md`](docs/agent/data-sources.md)（資料源優先順序）與
[`docs/agent/llm-usage.md`](docs/agent/llm-usage.md)（LLM 規範）。

---

## Windows Task Scheduler 排程

`run_scanner.bat` 是主排程鏈：**Daily 00:00**（每天午夜跑，盤後資料齊全）。

### 設定步驟

1. `Win+R` → `taskschd.msc`
2. 建立基本工作 → 名稱 `StockAnalyzer Scanner`
3. **觸發**：每天 00:00
4. **動作**：啟動程式
   - 程式：`C:\GIT\StockAnalyzer\run_scanner.bat`
   - 起始位置：`C:\GIT\StockAnalyzer`
5. **條件**：取消「只在 AC 電源時執行」
6. **設定**：勾「錯過後盡快執行」

### 主排程鏈內容

實際會跑的 stage（依 `run_scanner.bat` 執行順序）：

```
Scanner started
→ YT 影片同步 (fetch → extract → panel)
→ News 題材萃取 → News 流量異常 → 題材動能
→ 量價情緒指標 (ATM PUT 權利金 / 小台多空比 / 選擇權法人)
→ RF-1 cache consistency check → Market regime 紀錄
→ Universe 價格更新 → TW breadth panel → Refresh backtest panels
→ 價格離群掃描 (panel 毀損偵測)
→ 美股 cache 未完成 bar 掃描 (防回歸；盤中快照被當收盤)
→ 籌碼歷史 resume → 法說會行事曆 fetch
→ Scanner finished
→ verify_scan_stages 驗證
```

同一支 bat 裡有 **6 個以 `goto skip_*` 停用的區塊**，程式碼保留但不會執行：
投顧 YT 同步、QM + Value 選股、Step-A + Paper trade、強勢股 Stage 1 enrich、
強勢股 Stage 2/3 AI + render、Auto AI reports。重啟方式寫在各區塊上方的 `REM`。

> 「哪些 stage 必須真的跑完」的唯一權威是
> [`tools/verify_scan_stages.py`](tools/verify_scan_stages.py) 的 `REQUIRED_STAGES`
> （目前 17 個），它每晚比對 `scanner.log`。**改動排程鏈時要同步那份清單**，
> 否則 stage 靜默失敗不會被任何後檢查發現。
> `tests/test_scanner_fail_loud.py` 會對這份清單逐項 `parametrize`，驗證每個 marker
> 都真的存在於 `run_scanner.bat` —— 漏接會直接紅在 pytest，不必靠人工對照。

### 其他排程 BAT

下表的排程欄位以 **Windows 工作排程器實際登記狀態**為準
（`Get-ScheduledTask | ? { $_.Actions.Execute -like "*StockAnalyzer*" }`），
不是以 bat 存在為準 —— 兩者曾經長期不一致。

| BAT | 排程 | 用途 |
|---|---|---|
| `run_scanner.bat` | **每天 00:00** | 主排程鏈（見上）|
| `run_app_startup.bat` | 登入時自啟 | 背景起 Streamlit UI。工作名 `StockAnalyzer App Autostart`，實際執行的是 `wscript.exe run_app_startup.vbs`（無視窗包裝，避免登入時閃黑框），由它再叫這支 bat |
| `run_macro_panels_dawn.bat` | **TUE-SAT 07:00** | ETF flows / CNN FGI / 市值 / systemic chip / FRED / 領頭羊 |
| `run_macro_panels_evening.bat` | **TUE-SAT 17:30** | 法人合計 / 期貨法人 / AAII / TW LEI / 估值 / 當日 breadth |
| `run_taifex_signals_afterclose.bat` | **TUE-SAT 16:30** | 期交所盤後訊號。**刻意只有這一個 trigger**（2026-08-02 拍板，原 14:35 + 15:30 + 16:30 三重容錯已簡化掉）。已知代價：archiver 失敗當天不重試，靠當晚 00:00 的 scanner 當隔日 backup |
| `run_tdcc_weekly.bat` | **週六 08:00** | TDCC 集保 + 籌碼 margin/short_sale 補抓 |
| `run_bulk_revenue_monthly.bat` | **每月 11 日 00:30** | 月營收下載（10 號公布，11 號抓才拿得到當月）|
| `run_app.bat` | 手動 | 啟動 Streamlit UI（有視窗，開發用）|
| `run_yt_sync.bat` / `run_yt_brokerage_sync.bat` | 手動 | YT 影片 / 投顧頻道同步 |
| `run_news_intraday.bat` | 手動 | 盤中新聞監控（Intraday Disable，等需要再啟）|
| `run_mops_probe.bat` | 手動 | MOPS 探測（`USE_MOPS` 預設 false）|
| `run_c1_monthly.bat` | **未登記＋內部停用** | C1 regime tilt 拐點偵測；2026-05-30 排程工作已 Unregister，bat 第 64 行也加了 `goto skip_c1`。手動仍可跑 `python tools\compute_c1_tilt.py` |
| `run_scanner_weekly.bat` | **未登記＋內部停用** | 強勢股週報；第 62 行無條件 `goto skip_weekly_all`，即使觸發也是 no-op |

> ⚠️ 所有 `.bat` 必須 **pure ASCII + CRLF**（CP950/UTF-8 衝突會讓排程靜默失敗；LF-only 會讓 `goto` 以 byte-offset 跳錯行、誤跑 dead code）。pre-commit hook 會擋 CJK 與 lone-LF。

---

## 開發約定

- **Robustness First**：commit 前必須 end-to-end 跑過、grep caller 確認、fail loud（不要 try/except pass）
- **資料源優先順序**：見 `docs/agent/data-sources.md` Data Source Priority 表（避免重複拉同一資料）
- **LLM 規範**：每個模組的 model / effort / timeout 是規定值，見 `docs/agent/llm-usage.md`（唯一權威，勿在他處另抄一份）
- **代碼註解**：繁中 + 英文混用（`.bat` 例外，必須 ASCII + CRLF）
- **Commit 訊息**：英文 prefix（feat/fix/refactor）+ 繁中正文

詳細：見 `AGENTS.md`。

---

## 強勢股報告範例 (日報 + 週報)

> ⛔ **本節整套已停用，以下是報告格式與 scoring 設計的參考，不是現行行為。**
> 2026-05-21 日報與週報一併停用（2026-06-16 billing 顧慮解除後仍確認維持停用）：
> - `run_scanner.bat` 有 `goto skip_strong_stocks_all` 與 `goto skip_strong_stocks_ai`
> - `run_scanner_weekly.bat` 未登記進排程器，且第 62 行無條件 `goto skip_weekly_all`
> - `app.py` 的 `_mode_options` 已移除 `'strong_stocks'`，UI 沒有這個 tab
>
> **只有下方「手動產出」那幾道命令仍可跑。** 底下的「每天 00:00 自動產出」／
> 「週日 12:00 自動產出」／「Streamlit UI 切 🌟 強勢股報告 mode」都已不成立。

### 日報 (原每天 00:00 自動產出，現已停用)

```
data/strong_stocks_reports/YYYY-MM-DD.html   # YYYY-MM-DD = 資料日 (ref_date), 不是 scan run 日
data/strong_stocks_reports/YYYY-MM-DD.pdf
```

> 嚴格日期對齊（schema v2）：所有欄位（價量 / 法人 / 融資 / 當沖 / 借券）都對齊到 OHLCV cache 共識最新日 (`ref_date`)，避免「price 是 D-1 + 籌碼是 D」混錯一天。OHLCV cache 不一致 → fail loud 拒絕產出。

12 欄表格（代號 / 名稱 / 族群 / 收盤 / 漲幅 / 量比 / 5日漲 / 法人 / 融資 / 當沖% / 借券賣 / 評分）+ AI 五段論述（資金熱點 / 族群行情 / 追高警告 / 潛力觀察 / 整體風險）。

族群 3 層 fallback：manual themes → YT dynamic tags → TradingView industry。

### 週報 (原週日 12:00 自動產出, 2026-05-14 新增，現已停用)

```
data/strong_stocks_reports/YYYY-Www.html     # ISO 週次, 例 2026-W20
data/strong_stocks_reports/YYYY-Www.pdf
```

13 欄表格（代號 / 名稱 / 族群 / 週收 / 週漲幅 / 5週量比 / 13週累積 / 52週新高 / 站MA20W / 5日法人 / 5日融資 / 5日借券賣 / 週評分）+ AI 週度 5 段論述。

⚠️ **週度 scoring informational tier**: 未經 IC 驗證，僅供盤勢回顧，**不接 paper_trade / 出場邏輯**。Universe scoring 公式：週漲幅 30% + 5週量比 20% + 13週累積 20% + 52週新高 15% + 站MA20W 15%。

### 網頁查看（已無此入口）

原本：Streamlit UI sidebar → 切「🌟 強勢股報告」mode → 上方 **📅 日報 / 📊 週報** radio 切換 → 日期/週次下拉選歷史報告 + 直接 inline 渲染 + 一鍵下載 PDF。
現在 `_mode_options` 沒有 `'strong_stocks'`，render handler 保留為死代碼；恢復方式見 `app.py` 該處註解。既有 HTML/PDF 仍在 `data/strong_stocks_reports/`，可直接開檔。

### 手動產出

```bash
# 日報
python tools/strong_stocks_daily.py        # enrich + bucket
python tools/strong_stocks_ai_analysis.py  # Opus 5 段論述 + 5d 本地新聞 + WebSearch
python tools/strong_stocks_render.py       # HTML + PDF

# 週報
python tools/strong_stocks_weekly_screener.py       # 週度 5-signal scoring
python tools/strong_stocks_ai_analysis.py --weekly  # Opus 5 段 + 14d 新聞
python tools/strong_stocks_render.py --weekly       # HTML + PDF
```

---

## Disclaimer

本系統純屬投資研究 / 個人決策輔助工具，**不構成投資建議**。台股 / 美股市場有風險，下單請自行判斷。

LLM 輸出（Sonnet / Opus 報告）有事實錯誤可能，所有數字 / 名稱請與表格 / 原始資料對照。
