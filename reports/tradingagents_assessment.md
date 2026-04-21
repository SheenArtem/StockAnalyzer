# TradingAgents 整合評估

**評估日期**: 2026-04-19
**評估者**: Claude Opus 4.7 (深度代碼審查, A 階段)
**Repo**: TauricResearch/TradingAgents v0.2.3（51.6k⭐）
**本地位置**: `third_party/TradingAgents/`

---

## TL;DR

- **架構成熟**：LangGraph + LangChain，12 agents 分 5 層（Analyst → Researcher → Trader → Risk → PM）
- **Claude 原生支援**：`langchain-anthropic` + `anthropic_client.py` 內建
- **單檔決策成本**：12–27 次 LLM call（vs 我們現在 1 次）
- **🔴 台股整合要改 dataflows**：yfinance 可打 2330.TW，但 news/fundamentals/insider 需補台灣源
- **記憶機制 BM25 無持久化**：重啟程式全部歸零，需要自己加存檔
- **建議路線**：**抽架構自製 (C 方案)** — 借辯論流程 + 我們自己的 prompts/資料源，**不要**直接 fork

---

## 1. 架構拆解

### 1.1 執行流程（`graph/setup.py`）

```
START
 │
 ├→ [Market Analyst]      ┐
 ├→ [Social Analyst]      │ 4 analysts 串行，各自可 tool_call 2-3 輪
 ├→ [News Analyst]        │
 └→ [Fundamentals Analyst]┘
      ↓
 [Bull Researcher] ⇄ [Bear Researcher]   × max_debate_rounds (預設 1)
      ↓
 [Research Manager]   ← 裁判決定 Buy/Sell/Hold
      ↓
 [Trader]             ← 生成交易計畫
      ↓
 [Aggressive] ⇄ [Conservative] ⇄ [Neutral]  × max_risk_discuss_rounds (預設 1)
      ↓
 [Portfolio Manager]  ← 最終決策
      ↓
 END
```

### 1.2 Agent 清單（12 個）

| 層級 | Agent | 職責 | LLM 深淺 |
|------|-------|------|---------|
| Analyst | Market / Social / News / Fundamentals | 蒐集資料 + 寫報告 | quick |
| Researcher | Bull / Bear | 多空辯論 | quick |
| Manager | Research Manager | 裁判辯論、Buy/Sell/Hold | **deep** |
| Trader | Trader | 生交易計畫 | quick |
| Risk | Aggressive / Conservative / Neutral | 三角風控辯論 | quick |
| Manager | Portfolio Manager | 最終 Buy/Overweight/Hold/Underweight/Sell | **deep** |

TradingAgents 設計了 **quick_think_llm + deep_think_llm 兩層**（`default_config.py`）— 成本優化 pattern，辯論過程用快/便宜模型，最終決策用貴的深度模型。

---

## 2. LLM 調用成本

### 2.1 單次 `propagate("2330.TW", "2026-04-18")` 估算

| 階段 | 調用數 | LLM |
|------|--------|-----|
| 4 analysts × 1-3 tool 迭代 | 4-12 | quick |
| Bull + Bear × 1 輪 | 2 | quick |
| Research Manager | 1 | **deep** |
| Trader | 1 | quick |
| 3 Risk debators × 1 輪 | 3 | quick |
| Portfolio Manager | 1 | **deep** |
| **小計（不含反思）** | **12–20** | |
| reflect_and_remember（可選） | +5 | quick |

### 2.2 成本對比（⚠️ 2026-04-19 修正）

**關鍵事實**：我們現有 `ai_report.py` 用 **Claude CLI subprocess**（`claude -p --allowedTools "WebSearch,WebFetch"`）走 **Team Plan quota**，不是 API key 按量計費。

TradingAgents 底層是 `langchain-anthropic.ChatAnthropic` → **只接受 ANTHROPIC_API_KEY**，**不支援 Claude CLI**。要整合代表要分叉：

| 維度 | 現有 ai_report.py (CLI) | TradingAgents 原生 (API key) |
|------|------------------------|-----------------------------|
| 計費模式 | Team Plan quota | 按 token 付費 |
| 單檔「成本」 | 🟢 Team Plan 內免費 | 🔴 $1.5-5 USD 真金白銀 |
| LLM call | 1 | 12-20 |
| 時間 | 30-60s | 3-8 min（API）/ 8-20 min（若改 CLI wrap） |
| Quota 壓力 | 低（1 次 CLI） | ⚠️ 無（走 API key） |
| 併發 | 單一 CLI blocking | ✅ API 可併發 |

**真正的 tradeoff**：
- 走 TradingAgents 原生 → 要**付 Anthropic API 錢**，每日 scanner 30 檔 = $45-150/day
- C 方案若堅持走 Claude CLI → 12-20 次 sequential subprocess，**8-20 分鐘等待**而且 Team Plan quota 會快速耗盡
- 折衷：用 **Claude Agent SDK** 或「Haiku 辯論層走 API、Sonnet 決策層走 CLI」混搭

---

## 3. 資料源 & 台股支援

### 3.1 dataflows/ 盤點

已有：
- `y_finance.py` — yfinance OHLCV
- `yfinance_news.py` — yfinance 新聞（美股充足，台股貧乏）
- `alpha_vantage_*.py` — 美股 only，台股無覆蓋
- `interface.py` — 資料層抽象介面

### 3.2 2330.TW 支援度

| 資料 | yfinance | Alpha Vantage | 備註 |
|------|---------|---------------|------|
| OHLCV | ✅ 2330.TW 可打 | ❌ | |
| 新聞 | ⚠️ 稀疏 | ❌ | 要串 CNYES/Google News |
| 基本面 | ⚠️ 部分欄位缺 | ❌ | 要串 FinMind/TWSE |
| 內部交易 | ❌ 台股無 | ❌ | 要串 TWSE 申報轉讓 |
| 技術指標 | ✅（stockstats 計算） | ❌ | 本地算 |

### 3.3 台股整合工作量

| 檔案 | 改動 | 時數 |
|------|-----|-----|
| `dataflows/y_finance.py` | 新增 TW 後綴處理 | 2h |
| `dataflows/yfinance_news.py` | Fork → 整合 Google News RSS (台灣關鍵字) | 6h |
| 新寫 `dataflows/finmind_data.py` | 基本面 + 月營收 + 籌碼 | 16h |
| 新寫 `dataflows/twse_insider.py` | 申報轉讓爬蟲（or 直接捨棄此 agent） | 8h |
| `default_config.py` | market toggle | 1h |
| 4 analysts prompt | 刪除硬編碼美國指標（e.g. SPY, DXY） | 2h |
| **合計** | | **~35h / 1 週全力** |

**🔴 關鍵**：Alpha Vantage 台股完全無效，若走 Alpha Vantage 路徑要全部 disable。

---

## 4. 記憶 & 反思（🔴 修正 Explore agent 說法）

讀過 `agents/utils/memory.py`（145 行），實際狀況：

| 項目 | 實際 |
|------|------|
| 儲存 | **純 in-memory list**（documents + recommendations） |
| 檢索 | BM25Okapi（`rank_bm25`） |
| 持久化 | **完全沒有**—沒有 save/load/pickle |
| 向量 DB | 無 |
| Redis | pyproject 列依賴但實際沒用 |

`reflect_and_remember(returns)` 把歷史決策餵回 memory，但**程式關掉全丟**。要做持久化要自己加 `pickle.dump` / SQLite。

---

## 5. 對比現有 ai_report.py

| 維度 | ai_report.py | TradingAgents | 勝方 |
|------|-------------|---------------|------|
| LLM call | 1 | 12-20 | ai_report |
| 成本（單檔） | $0.05 | $1.5-5 | ai_report |
| 台股支援 | 完整（FinMind/TWSE） | 需改 35h | ai_report |
| 可解釋性 | 黑盒 prompt | 完整辯論 log | TradingAgents |
| 反向證據 | 靠 prompt 技巧 | 機制性保證（Bear/Bear Research） | TradingAgents |
| 風控審核 | 無 | 三角辯論 | TradingAgents |
| 記憶學習 | 無 | BM25（需補持久化） | TradingAgents |
| 即時性 | 30-60s | 3-8 min | ai_report |

---

## 6. 三種整合路線（⚠️ 2026-04-19 修正: Claude CLI 約束）

### A. 直接 fork 套用（langchain + API key）
- 工作量：⭐（3 天跑美股 POC）
- 成本：🔴 **要付 API 真錢** $1.5-5/檔，每日 scanner 30 檔 = $45-150/day
- 台股：❌ 要再 35h
- **適用**：只在「特定股票深度研究」單次觸發 且 用戶接受 API 成本

### B. 改寫整合（Fork 後改 dataflows + prompts）
- 工作量：⭐⭐⭐⭐（3-4 週）
- 維護：🔴 和 upstream 分叉，後續升級困難
- **不建議**

### C. **抽架構自製（推薦，但要決定 LLM backend）**
- 借「多 agent 辯論 + 風控 + PM 最終決策」概念
- **自己實作 5 層流程**，不用 LangGraph

**C 方案 LLM backend 三選一**：

| 子方案 | LLM backend | 成本 | 時間 | 併發 |
|------|------------|-----|-----|-----|
| **C1: 純 Claude CLI** | 12-20× `subprocess claude -p` | 🟢 Team Plan 內 | 🔴 8-20 min | 🔴 sequential |
| **C2: 純 API key** | 12-20× Anthropic API | 🔴 $1.5-5/檔 | 🟢 3-8 min | ✅ 併發 |
| **C3: 混搭** | 辯論層(Haiku API) + 決策層(CLI) | 🟡 ~$0.3/檔 | 🟡 5-10 min | 部分 |

工作量：⭐⭐⭐（2-3 週，但全盤可控）
**適用場景**：AI 報告 v2，只在用戶明確觸發「深度研究」按鈕跑（不是 default）

**推薦 C3 混搭**：
- Bull/Bear/3 Risk debators = 5 次 Haiku API call（便宜快），辯論真值很多時候 Haiku 夠用
- Research Manager + Trader + Portfolio Manager = 3 次 Sonnet CLI call（走 Team Plan）
- 單檔總成本：~$0.3 API + 3 次 Team Plan quota，4-6 分鐘

---

## 7. 紅旗 & 決策點

### 🔴 硬風險
1. **成本爆炸**：12-20× LLM call 撐不住每日 scanner 量
2. **LangGraph 鎖死**：整個執行流程綁在 LangGraph state，抽出來很難
3. **台股資料 gap**：news/insider/基本面都要重寫
4. **記憶無持久化**：reflect_and_remember 重啟即消失

### 🟡 軟風險
5. Claude extended thinking token 爆量（`anthropic_effort` 預設 None 還好）
6. Prompts 內嵌美國指標（SPY/DXY/美國國債），台股版要改
7. 依賴鏈重：langchain-core + langgraph + langchain-anthropic 三個包

### 綠燈
1. ✅ Anthropic client 內建，可直接用 Claude
2. ✅ yfinance 模式對台股 OHLCV 可用
3. ✅ 12 agents 代碼結構乾淨（analysts/ researchers/ managers/ 分離好）
4. ✅ BM25 memory 簡單可理解

---

## 8. POC 建議範圍（B 階段，⚠️ 需要 API key）

**限制**：TradingAgents 原生不支援 Claude CLI，POC 必須用 API key。
若用戶沒有 Anthropic API key → **跳過 POC-1，直接走 C3 自製路線**（用 CLI + Haiku API 混搭）。

若要繼續跑 POC，**建議不要用 2330.TW，先用 NVDA**（英文新聞多、資料齊全、最接近 repo 預設場景）：

### POC-1: 原生體驗（半小時，需 API key，$2-8 USD）
```
pip install -r third_party/TradingAgents/requirements.txt
配 .env ANTHROPIC_API_KEY
改 default_config.py → llm_provider=anthropic, deep=sonnet-4-6, quick=haiku-4-5
跑 main.py 預設 NVDA 2024-05-10
```

**觀察重點**：
- 實際 token 消耗
- 辯論品質（Bull/Bear 有沒有真的反駁，還是各說各話）
- 最終 PM 決策是否比單一 prompt 有 insight

### POC-2: Claude vs GPT 比較（1 小時，可選）
- 同一檔同一天，跑 Claude 版 + OpenAI 版
- 對比輸出品質 / 速度 / 成本

### POC-3: 台股嘗試（2 小時，可選）
- 直接打 `ta.propagate("2330.TW", ...)`，看會爛在哪一個 agent
- 記錄 error trace，估計改寫點

---

## 9. 最終建議

| 方案 | 做 or 不做 |
|------|----------|
| 直接 fork 整合到 Streamlit | **不做**（成本、維護、分叉三重風險） |
| 抽架構自製 AI 報告 v2 | **做**（2-3 週，完全自控） |
| 先跑 POC-1 驗證效果 | **做**（半小時，成本 $1-5） |

**建議工作流**：
1. ✅ **先做 POC-1**（B 階段）：NVDA 原生跑一次，看**多 agent 辯論 vs 單一 prompt** 的實際輸出品質差距有多大
2. 若差距顯著 → 進入 C 方案：自製 AI 報告 v2（5 層簡化版 + Claude SDK 直呼 + 我們的資料源）
3. 若差距不大 → 結案，繼續強化現有 ai_report.py 的 prompt（例如加「反方檢查」區塊）

---

## Appendix: 關鍵檔案清單

| 檔案 | 行數 | 重要性 |
|------|-----|--------|
| `graph/trading_graph.py` | 287 | 🔴 核心 orchestrator |
| `graph/setup.py` | 201 | 🔴 agent 連線圖 |
| `graph/conditional_logic.py` | 67 | 🟠 辯論分岔邏輯 |
| `agents/utils/memory.py` | 145 | 🟡 BM25 記憶 |
| `agents/analysts/market_analyst.py` | 89 | 🟠 prompt 範例（~3KB） |
| `llm_clients/anthropic_client.py` | 48 | 🟢 Claude adapter |
| `default_config.py` | 38 | 🟢 config 入口 |
| `dataflows/y_finance.py` | — | 🟠 台股改寫點 |

---

*本文件由代碼審查 + Explore agent 協助 + 關鍵點驗證產出，部分 Explore agent 的斷言經核對修正（如記憶持久化）。*
