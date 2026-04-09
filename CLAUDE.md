# StockAnalyzer — 台股/美股右側交易分析系統

## 概述
基於 Streamlit 的股票分析工具，結合技術面、籌碼面、基本面與 AI 觸發分數，輔助右側交易決策。主要針對台股（FinMind + Yahoo Finance），兼容美股（Yahoo Finance）。

## 技術棧
- **Python 3.14** / **Streamlit 1.52**
- 數據源：`yfinance`、`FinMind`
- 技術分析：`ta`、`mplfinance`
- 圖表：`plotly`
- 網頁爬蟲：`beautifulsoup4`、`curl_cffi`

## 啟動方式
```bash
# 安裝依賴 + 啟動
run_app.bat
# 或直接
pip install -r requirements.txt && streamlit run app.py
```

## 模組架構

```
app.py (Streamlit UI 入口)
  ├→ technical_analysis.py   — 技術指標計算 + 互動圖表
  ├→ analysis_engine.py      — AI 觸發分數計算（最大模組）
  │     ├→ chip_analysis.py       — 台股籌碼（三大法人/融資融券/當沖/持股）
  │     ├→ us_stock_chip.py       — 美股籌碼（機構持股/ETF/空單/內部交易）
  │     ├→ pattern_recognition.py — K線型態辨識
  │     └→ strategy_manager.py    — 買賣閾值管理（讀寫 strategy_config.json）
  ├→ fundamental_analysis.py — 基本面（本益比/ROE/殖利率/財報）
  ├→ backtest_engine.py      — 歷史回測引擎
  └→ cache_manager.py        — 本地 CSV 快取（智慧 TTL）
```

## 開發規範

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
- `cache_manager.py` 有 `_cache_lock` 確保執行緒安全

### 台股/美股判斷
- 純數字或含 `.TW` → 台股（使用 FinMind + Yahoo Finance）
- 英文字母 → 美股（使用 Yahoo Finance）

## 注意事項

- **無正式測試套件** — `tools/` 下有手動驗證腳本（verify_/debug_/test_），但無 pytest
- **strategy_config.json** 存放每檔股票的買賣閾值，`StrategyManager` 讀寫此檔
- **analysis_engine.py** 是最大且最複雜的模組（~67KB），修改時注意影響範圍
- **無 .env** — 設定值以硬編碼預設值 + Streamlit session state + JSON 檔為主
- **Windows 平台** — `.bat` 啟動腳本、路徑處理需注意 Windows 相容性
