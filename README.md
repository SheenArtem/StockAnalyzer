# StockAnalyzer

台股 / 美股 交易分析系統。Streamlit UI + 排程 scanner + AI 報告 + 強勢股日報 PDF。

---

## Quickstart（5 分鐘跑起來）

### 1. 環境

- **Python 3.13+**
- **Windows 10/11**（排程依賴 Task Scheduler；`.bat` 啟動器；其他平台需自行調整）
- **Claude CLI**（AI 報告必須）— 從 https://docs.anthropic.com/claude/docs/cli 安裝

### 2. 安裝依賴

```bash
git clone https://github.com/SheenArtem/StockAnalyzer.git
cd StockAnalyzer

pip install -r requirements.txt
playwright install chromium      # 強勢股日報 PDF 印出用
```

### 3. 建立 `local/.env`

`local/` 已是 git-ignored 目錄。**必須**建立 `local/.env` 並填入：

```bash
# FinMind API Token (台股財報 / 籌碼 / 月營收)
# 申請: https://finmindtrade.com/  (免費 600 req/hr)
FINMIND_API_TOKEN=your_finmind_token_here

# Discord Webhook (optional, 排程結果推播)
# 建立: Discord 頻道 → 整合 → Webhooks → 新增 Webhook
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

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

| 功能 | 入口 | 說明 |
|---|---|---|
| 個股分析 UI | `app.py` | 技術 / 基本面 / 籌碼 / 同業 / AI 報告 |
| QM 品質選股 | `scanner_job.py --mode qm` | F-Score 50% + 體質 30% + 趨勢 20% |
| Value 選股 | `scanner_job.py --mode value` | 5 因子組合 |
| Momentum 選股 | `scanner_job.py --mode momentum` | 全市場掃描 trigger_score |
| AI 報告 | `tools/auto_ai_reports.py` | Claude Opus 深度分析 |
| 強勢股日報 PDF | `tools/strong_stocks_daily.py` + `_ai_analysis.py` + `_render.py` | 仿 LINE 群分發格式，12 欄含籌碼 + AI 五段論述 |
| 新聞題材 | `tools/news_theme_extract.py` | UDN + cnyes RSS → Claude Sonnet 萃取 |
| 籌碼面 | `chip_analysis.py` / `chip_history_dl.py` | 三大法人 / 融資融券 / 借券 / 當沖 / TDCC |

詳細架構：見 [`CLAUDE.md`](CLAUDE.md)（核心規範 + 資料源優先順序 + LLM 規範）。

---

## Windows Task Scheduler 排程

`run_scanner.bat` 是主排程鏈：**TUE-SAT 00:00**（盤後資料齊全）。

### 設定步驟

1. `Win+R` → `taskschd.msc`
2. 建立基本工作 → 名稱 `StockAnalyzer Scanner`
3. **觸發**：每週 TUE-SAT, 00:00
4. **動作**：啟動程式
   - 程式：`C:\GIT\StockAnalyzer\run_scanner.bat`
   - 起始位置：`C:\GIT\StockAnalyzer`
5. **條件**：取消「只在 AC 電源時執行」
6. **設定**：勾「錯過後盡快執行」

### 主排程鏈內容

```
YT 影片同步 → News 題材萃取 → 量價情緒指標 (PUT/小台/期權)
→ Cache consistency check → Market regime 紀錄
→ QM 選股 → Value 選股
→ Step-A engine → Paper trade engine
→ 強勢股日報 (enrich + AI Sonnet + HTML+PDF)
→ Substack 同步 → 籌碼歷史 resume
→ 法說會行事曆 fetch
→ verify_scan_stages 驗證
```

### 其他排程 BAT

| BAT | 排程 | 用途 |
|---|---|---|
| `run_app.bat` | 手動 | 啟動 Streamlit UI |
| `run_bulk_revenue_monthly.bat` | 月初 | 月營收下載 |
| `run_c1_monthly.bat` | 月初 | C1 regime tilt 拐點偵測 |
| `run_tdcc_weekly.bat` | 週六 08:00 | TDCC 集保 + 籌碼 margin/short_sale 補抓 |
| `run_taifex_signals_afterclose.bat` | TUE-SAT 14:30 | 期交所盤後訊號 |

> ⚠️ 所有 `.bat` 必須 **pure ASCII**（CP950/UTF-8 衝突會讓排程靜默失敗）。pre-commit hook 會擋 CJK 字元。

---

## 開發約定

- **Robustness First**：commit 前必須 end-to-end 跑過、grep caller 確認、fail loud（不要 try/except pass）
- **資料源優先順序**：見 `CLAUDE.md` Data Source Priority 表（避免重複拉同一資料）
- **LLM 規範（鎖定 2026-05-01）**：
  - AI Report → Claude Opus + `--allowedTools "*"` + 600s
  - News / 分析 → Claude Sonnet + 600s
  - Calendar / 表格萃取 → Claude Haiku
  - Gemini → `gemini-3.1-pro-preview` + 900s
- **代碼註解**：繁中 + 英文混用（`.bat` 例外，必須 ASCII）
- **Commit 訊息**：英文 prefix（feat/fix/refactor）+ 繁中正文

詳細：見 `CLAUDE.md`。

---

## 強勢股日報範例

每天排程跑完後產出：

```
data/strong_stocks_reports/YYYY-MM-DD.html
data/strong_stocks_reports/YYYY-MM-DD.pdf
```

12 欄表格（代號 / 名稱 / 族群 / 收盤 / 漲幅 / 量比 / 5日漲 / 法人 / 融資 / 當沖% / 借券賣 / 評分）+ AI 五段論述（資金熱點 / 族群行情 / 追高警告 / 潛力觀察 / 整體風險）。

族群 3 層 fallback：manual themes → YT dynamic tags → TradingView industry。

**網頁查看**：Streamlit UI sidebar → 切「📰 強勢股日報」mode → 日期下拉選歷史報告 + 直接 inline 渲染 + 一鍵下載 PDF。

**手動產出**：
```bash
python tools/strong_stocks_daily.py        # enrich + bucket
python tools/strong_stocks_ai_analysis.py  # Sonnet 5 段論述
python tools/strong_stocks_render.py       # HTML + PDF
```

---

## Disclaimer

本系統純屬投資研究 / 個人決策輔助工具，**不構成投資建議**。台股 / 美股市場有風險，下單請自行判斷。

LLM 輸出（Sonnet / Opus 報告）有事實錯誤可能，所有數字 / 名稱請與表格 / 原始資料對照。
