# StockAnalyzer — 台股/美股右側交易分析系統

## ⚠️ 核心原則：Robustness First（最高優先，凌駕其他規範）

**本專案要求 robustness — 一切修改必須保證正確性。**

作金融決策輔助工具，錯誤不是「之後發現再改」，是「直接誤導交易決策」。任何
修改（新功能、bug fix、refactor、文件）commit 前必須做到以下任一：

1. **手動跑過一次 end-to-end**（最低門檻）— 特別是 CLI 工具 / 排程腳本 /
   「set-and-forget」類型的東西，commit 前至少 `python tools/xxx.py ...` 實跑
   一次確認不炸。lazy import / 動態 signature 的錯誤只有真的呼叫到才會浮出。
2. **影響面 grep**（跨模組必做）— 改 API signature、return 型別、函式名稱時，
   先 grep 所有 caller，確認每個呼叫點都對齊（今天 ChipAnalyzer tuple unpack
   就是 14 caller 中 1 個新加的漏掉）。
3. **Dry-run / smoke test**（有的話優先用）— 若腳本有 `--dry-run` / 省略 Claude
   CLI / 只跑資料裝配的模式，commit 前跑一次。
4. **靜默失敗視為嚴重 bug**（不只是「non-critical」）— 排程吞 exit code、
   try/except pass、`if x:` 缺 else 分支 ⋯⋯ 都可能讓 bug 多活一晚才被發現。
   新寫程式優先 fail loud，不要 fail silent。

**反例（2026-04-22 auto_ai_reports 事件教訓）**：昨天下午新增的 CLI 腳本，
沒有本地實跑，直接進 22:00 排程，三個 bug（import 路徑錯、建構子誤用、tuple
未 unpack）一次爆光，還因為 `run_scanner.bat` 把 exit code 吞掉，隔天早上才
發現。**整條失效鏈每一站都違反上面 4 條之一**。

> 若修改無法保證正確性（例如只是探索性 POC、大型 refactor 中間狀態），
> 必須在 commit message 或 PR description 明確標示「**未驗證**」或「**實驗**」，
> 不能當成正式變更混入主分支。

---

## 概述
基於 Streamlit 的股票分析工具，結合技術面、籌碼面、基本面與 AI 觸發分數，輔助右側交易決策。主要針對台股（FinMind + TWSE/TPEX + TradingView），兼容美股（Yahoo Finance + Finviz + TradingView）。

## 技術棧
- **Python 3.14** / **Streamlit 1.52**
- 數據源：`yfinance`、`FinMind`、`TWSE/TPEX 官方 API`、`TradingView Screener`
- 技術分析：`ta`、`mplfinance`
- 圖表：`plotly`
- 網頁爬蟲：`beautifulsoup4`、`curl_cffi`
- AI：Claude CLI（`claude -p --allowedTools "WebSearch,WebFetch"`）

## 啟動方式
```bash
# 安裝依賴 + 啟動
run_app.bat
# 或直接
pip install -r requirements.txt && streamlit run app.py
```

## 三大功能模式

```
📈 個股分析 — 6 tabs (週K/日K/籌碼面/基本面/情緒期權/除息營收)
🔍 自動選股 — 5 tabs (動能台股/動能美股/價值台股/價值美股/績效追蹤)
📝 AI 報告 — 2 tabs (生成報告/報告庫)
```

## 模組架構

```
app.py (Streamlit UI 入口, 3 模式)
  │
  ├─ 個股分析 ─────────────────────────────────────────────
  │  ├→ technical_analysis.py   — 技術指標計算 + 互動圖表
  │  │     含: MA, BB, ATR, RSI, KD, MACD, OBV, DMI, EFI, TD Sequential,
  │  │         VWAP, Supertrend, RVOL, Squeeze Momentum
  │  ├→ analysis_engine.py      — AI 觸發分數計算（最大模組）
  │  │     ├→ chip_analysis.py       — 台股籌碼（TWSE/TPEX優先 → FinMind fallback）
  │  │     ├→ us_stock_chip.py       — 美股籌碼（機構持股/ETF/空單/內部交易）
  │  │     └→ pattern_recognition.py — K線型態辨識
  │  ├→ fundamental_analysis.py — 基本面（yfinance + FinMind + TradingView overlay）
  │  ├→ taifex_data.py          — TAIFEX 期貨選擇權 + 恐懼貪婪指數
  │  ├→ dividend_revenue.py     — 除權息行事曆 + 月營收追蹤
  │  └→ cnn_fear_greed.py       — CNN Fear & Greed Index（美股情緒）
  │
  ├─ 自動選股 ─────────────────────────────────────────────
  │  ├→ momentum_screener.py    — 右側動能選股（Stage 1 初篩 + Stage 2 觸發分數）
  │  ├→ value_screener.py       — 左側價值選股（估值+體質+營收+技術轉折+聰明錢）
  │  ├→ scanner_job.py          — CLI 入口（--mode momentum/value/both --market tw/us/all）
  │  └→ scan_tracker.py         — 績效追蹤（追蹤 picks 的 5/10/20 日報酬+勝率）
  │
  ├─ AI 報告 ──────────────────────────────────────────────
  │  ├→ ai_report.py            — 14 區塊 prompt 組裝 + Claude CLI 呼叫 + 報告庫
  │  │     ├→ news_fetcher.py        — Google News RSS 新聞搜尋 + 法人目標價提取
  │  │     └→ peer_comparison.py     — 同業 PE/PB/殖利率比較
  │  └→ prompts/stock_analysis_system.md — 系統 prompt 模板
  │
  ├─ 共用模組 ─────────────────────────────────────────────
  │  ├→ cache_manager.py        — 本地 CSV 快取（智慧 TTL）+ FinMind loader
  │  ├→ twse_api.py             — TWSE/TPEX 官方 API（法人/融資/PE/全市場行情）
  │  ├→ piotroski.py            — Piotroski F-Score + Altman Z-Score + ROIC/FCF
  │  ├→ etf_signal.py           — 主動型 ETF 同步買賣超（TWActiveETFCrawler）
  │  ├→ sec_edgar.py            — SEC EDGAR（13F/Form 4）
  │  └→ finviz_data.py          — Finviz 美股快照（估值/技術/分析師目標價）
  │
  └─ 閒置模組（保留未使用）────────────────────────────────
     ├→ backtest_engine.py      — 回測引擎（Walk-Forward/Monte Carlo/Pyramiding）
     ├→ strategy_manager.py     — 買賣閾值管理
     └→ ml_signal.py            — XGBoost 信號分類器
```

## 資料源優先順序（統一策略）

所有功能必須遵循同一優先順序，避免資料不同步：


| 資料類型       | 優先                           | Fallback           | 說明                  |
| ---------- | ---------------------------- | ------------------ | ------------------- |
| 法人買賣超      | TWSE/TPEX 官方                 | FinMind            | ChipAnalyzer 底層已統一  |
| 價量日線       | 磁碟快取                         | FinMind → yfinance | load_and_resample() |
| 基本面(PE/PB) | yfinance + FinMind           | TradingView 補缺     | get_fundamentals()  |
| 三率/ROE/ROA | TradingView Screener         | —                  | 台股美股統一              |
| 融資融券/當沖/持股 | FinMind                      | —                  | 無替代                 |
| 新聞         | Google News RSS              | —                  | news_fetcher.py     |
| 分析師共識      | yfinance                     | —                  | 目標價/Forward EPS/評級  |
| 同業比較       | TWSE/TPEX PER + FinMind 產業分類 | —                  | peer_comparison.py  |


## 開發規範

### 避免重工 & 重複抓取（⚠️ 最重要）

本專案功能繁多（個股分析 / 自動選股 / AI 報告三大模式 + 共用模組），**實作或修改任何功能前必須先檢查既有實作**，避免重工與重複 API/網路請求浪費資源。

**實作前檢查清單**：

1. **先讀 `app.py` 模組架構圖 + CLAUDE.md「資料源優先順序」表**，確認要抓的資料是否已有現成函式
2. **Grep 既有函式名稱**（如 `load_and_resample`、`get_fundamentals`、`ChipAnalyzer.`*、`peer_comparison`）— 同樣的資料優先復用，不要重寫
3. **確認資料流路徑** — 參考 memory 的 `reference_data_path_diff`（Scanner batch vs 個股/AI 逐檔路徑不同，別混用）
4. **AI 報告 / 儀表板等整合型功能**：資料應從上游算好的物件（`report`、`chip_data`、`fund_data`、`df_day`）撈，**禁止再重新呼叫 API**
5. **若真的需要新抓資料**：先確認 `cache_manager` 是否已有快取欄位可加，優先擴充既有快取而非開新檔

**禁止行為**：

- ❌ 同一個指標/資料在 technical_analysis、analysis_engine、ai_report 各算一次
- ❌ 同一檔股票的價量/籌碼在一次生成流程內重複下載
- ❌ 為了新功能另開 API 呼叫，而不是從既有的 session_state / 上游回傳復用
- ❌ 未檢查既有 util 就新寫重複邏輯（PE/PB/殖利率/ROE 這類基本面已經算好）

**若發現重複抓取或重複計算，應先重構統一，再做新功能**。

### 語言

- **程式碼註解**：繁體中文 + 英文混用
- **Commit 訊息**：繁體中文為主，前綴用英文（feat/fix/refactor）
- **UI 文字**：繁體中文

### 版本管理

- 版本號在 `app.py` 中：`st.caption("Version: vYYYY.MM.DD.序號")`
- Git pre-commit hook 會驗證版本更新

### 快取策略

- 交易時段（09:00-13:30）：TTL = 5 分鐘
- 收盤後：TTL = 整日
- 籌碼數據：每日 21:30 後更新
- 快取目錄：`data_cache/`（CSV 格式）
- TradingView / Google News：記憶體快取 30 分鐘 ~ 1 小時
- `cache_manager.py` 有 `_cache_lock` 確保執行緒安全

### 籌碼面評分（C2-b IC 驗證版）

`analysis_engine.py` `_analyze_chip_factors()` 的方向依據 C2-b 截面 IC 驗證（2026-04-16）。
核心原則：**「籌碼乾淨 = 好」**（法人不追、散戶不擠的股票未來表現更好）。

| 因子 | 方向 | IC IR | 說明 |
|------|------|-------|------|
| 外資買賣超 | 微正（+0.3） | +0.06（不顯著） | 保守給小分 |
| 投信買賣超 | **反轉**（買超 -0.5） | **-0.32** | 過熱逆向指標 |
| 融資使用率/增量 | 高=減分 | -0.24 | 散戶追漲 |
| 券資比 | 高=減分（**-0.6**） | **-0.57** | 空方正確看空，最強因子 |
| 借券增減 | 增=減分 | -0.33 | 法人放空 |

Cap: ±2.0（regime 乘數調整）。籌碼歷史資料在 `data_cache/chip_history/`（5 年 parquet）。
IC 驗證報告在 `reports/chip_ic_matrix.csv`、組合驗證在 `reports/chip_combo_ic.csv`。

### 台股/美股判斷

- 純數字或含 `.TW` → 台股（使用 FinMind + TWSE/TPEX + TradingView）
- 英文字母 → 美股（使用 Yahoo Finance + Finviz + TradingView）

## 注意事項

- **無正式測試套件** — `tools/` 下有手動驗證腳本（verify_/debug_/test_），但無 pytest
- **analysis_engine.py** 是最大且最複雜的模組（~67KB），修改時注意影響範圍
- **FinMind 免費額度** — 600 req/hr，容易爆。法人已改 TWSE/TPEX 優先
- **無 .env** — FinMind token 在 `local/.env`，其他設定以硬編碼 + session state + JSON 為主
- **Windows 平台** — `.bat` 啟動腳本、路徑處理需注意 Windows 相容性
- **Scanner 排程** — `run_scanner.bat` 每日 22:00 via Windows Task Scheduler
- **AI 報告** — 使用 Claude CLI `claude -p --allowedTools "WebSearch,WebFetch"`，Team Plan 額度

