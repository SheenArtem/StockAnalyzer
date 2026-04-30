# StockAnalyzer 專案檢視與未來發展藍圖

根據對 `StockAnalyzer` 專案的全面掃描與架構檢視，本專案具備相當豐富且強大的量化分析功能，但在軟體工程實踐與架構設計上累積了不少「技術債」。以下是專案目前的狀態檢視，以及由工程與產品雙視角所規劃的未來發展藍圖。

## 第一部分：專案現況檢視 (Current State Review)

> **2026-04-30 更新**：原列 9 項技術債逐項 pre-flight 評估後，5 項已動（✅）、
> 4 項判定不動（❌，附理由）。狀態詳見每項標注；翻盤理由總結同步到
> `CLAUDE.md` 的「已評估後不動」section。

### 1. 架構與結構問題 (巨石架構)

* ✅ **`app.py` 負載過重**（2026-04-29 完成）：原 4243 行，拆解後 **319 行**（-92.5%）。
  Streamlit UI / 背景執行緒 / 緩存 / 業務邏輯分離到 `*_view.py` 等模組。
* ✅ **`analysis_engine.py` 上帝類別**（2026-04-23 M2 完成）：原 2281 行，重構
  後 **933 行**（-59%）。形態偵測 / add-on 因子 / 劇本計畫拆到
  `pattern_detection.py` / `addon_factors.py` / `scenario_engine.py`。
* ❌ **扁平化的專案結構**：app.py 主痛點已解（-92.5%），剩 root 平鋪 .py 是
  ergonomic 問題，不是 critical bug。動則 80+ import path 全改、Streamlit
  run path 變、pytest fixtures 對齊、一 typo 整個 app boot 失敗。風險/ROI
  不對等。**等真要 packaging (`pip install -e .`) 才動**。

### 2. 缺乏自動化測試 (Testing) ✅（2026-04-30 起步）

* ✅ `tests/` + `pytest.ini` + `conftest.py` 框架建好，`pytest` 一行跑全部
* ✅ 首批 cover `piotroski.py` 純函式 **20 tests** 全綠
* 下批排程：`pattern_detection / scenario_engine / addon_factors /
  cache_manager`，見 `tests/README.md`
* 歷史手寫驗證腳本歸檔到 `tools/_archive/{manual_tests,verify}/`

### 3. 安全性隱患 (Security Risks) ❌（評估後不動）

* **關閉 SSL 驗證**：23 處集中在 `mops_fetcher / twse_api / taifex_data /
  money_supply / mops_bulk_fetcher / chip_history_dl` 等 7-8 個 data fetcher。
  **不是程式問題，是台灣公部門/金融機構憑證本身爛**（mops_fetcher 註解寫
  「MOPS 憑證缺 Subject Key Identifier」，伺服器壞 client 無解）。
  抓的全是公開 market data，無 auth/secret/credential 上傳，MITM 最壞=拿到
  錯股價，沒攻擊誘因。改的代價：scanner 整鏈 + 個股分析 + Discord summary
  都掛這些 module，verify=True 一改不對全鏈炸。**保留 verify=False 為
  documented decision，不動**。

### 4. 程式碼品質與反模式 (Code Quality)

* ✅ **異常處理過於寬鬆**：`except Exception: pass` / `except: pass` grep 結果
  **0 個**（CLAUDE.md「靜默失敗視為嚴重 bug」規範實施後清乾淨）。
* ❌ **手動版本管理**：pre-commit hook 已驗證更新（半自動化）。手動填號
  force 開發者想「這次改有沒有 user-facing 變化」這個 friction 是有功能的，
  完全自動化邊際效益低。**現狀 sweet spot 不動**。

### 5. 雜亂的工具與依賴

* ✅ **`tools/` 目錄未清理**（2026-04-30 完成）：187 → **113 active scripts**
  (-40%)。74 檔搬到 `tools/_archive/{vf,manual_tests,verify,ui_tests}/`，
  分類 README 索引：52 個 vf 驗證 study / 11 個 manual test / 7 個 verify /
  4 個 UI test。
* ❌ **`requirements.txt` 版本風險**：原 review 點誤判。85 行全 `==` pinned
  是 reproducible build best practice，不是「鎖定不夠嚴謹」。`yfinance==1.0`
  是真實版本（最新 1.3.0，落後 3 minor），但 yfinance 有名 API 不穩，盲升
  風險 > 收益。**不動**。

---

## 第二部分：未來發展藍圖 (Future Roadmap)

為了引導專案邁向下一階段，除了底層架構與個別功能的升級外，我們更確立了一個核心的產品願景：將現有的掃描與報告產線，升級為企業級的「全自動買方（Buy-Side）分析師」。

### 👑 旗艦級應用：全自動 Buy-Side 投資備忘錄產線 (Flagship Feature: Autonomous Buy-Side AI Analyst)
**目標**：不依賴使用者手動輸入代號，系統每日盤後全自動掃描全市場，篩選出具備「極致安全邊際與強勁動能」的 Top Picks，並透過多個 AI Agent 進行多空辯論，最終產出媲美避險基金水準的「Buy-Side 投資備忘錄 (Investment Memo)」。

* **Phase 1: 升級自動篩選引擎 (Screener 2.0)**
  * 改造現有的 `run_scanner.bat` 與 `qm_office_picks` 邏輯。
  * 候選標的不僅需具備技術與籌碼動能，更強制要求「現價必須低於 DCF 絕對估值 20% (安全邊際)」。系統只針對真正具備價值與爆發力的少數標的啟動高耗能的 AI 分析。
* **Phase 2: 導入 ai-hedge-fund 級別的多智能體協作架構 (LangGraph Multi-Agent Orchestration)**
  * 參考 `virattt/ai-hedge-fund` 的開源架構，將目前的單向 Prompt 升級為基於狀態機 (如 LangGraph) 的多代理人管線。
  * **分析代理 (Analytical Agents)**：將現有 `analysis_engine.py` 包裝為專屬 Tool Agents (例如：Valuation Agent 專責 DCF 計算、Sentiment Agent 專責新聞情緒、Technicals Agent 專責型態辨識)，讓 AI 能自主查詢所需數據。
  * **投資大師代理 (Persona Agents)**：建立多個具備強烈風格的子模型來進行平行辯論。例如：
    * **Warren Buffett Agent (價值/看空方)**：尋找安全邊際，挑剔高槓桿與財報瑕疵，撰寫 Bear Case。
    * **Cathie Wood Agent (成長/看多方)**：關注動能突破與產業資金流入，尋找催化劑，撰寫 Bull Case。
  * **管理代理 (Management Agents)**：
    * **Risk Manager (風控經理)**：驗證投資大師的提案是否符合 Phase 1 的 ATR 與投資組合風險上限。
    * **Portfolio Manager (投資組合經理)**：最終的「CEO」，負責匯總多空辯論結果，進行嚴格的「事前驗屍 (Pre-mortem)」，並給出最終的 **Investment Thesis (投資論點)** 與建議部位大小。
* **Phase 3: 自動化交付與推播 (Automated Delivery)**
  * 將每日生成的 Top 3 Buy-Side 備忘錄，自動渲染為高互動性的 HTML Dashboard，並透過 Discord / Slack 推播核心摘要。讓使用者每日晨間只需花 5 分鐘，即可掌握經過 AI 深度辯論與風險控管的極致選股。

---

我們從以下七個專業維度，規劃了支撐此旗艦應用的底層發展路線：

### 🛠️ 視角一：工程與軟體架構 (Software Architecture)
目標：償還技術債，提升系統的可維護性與可擴展性。

* **Phase 1: 單體拆解 (Monolith Breakdown)**
  * 將目前的四種模式（選股、分析、報告、掃描）從 `app.py` 中拆分為獨立的 Streamlit View 模組與 Controller。
  * 建立共用的 UI 元件庫（UI Components），確保展示層與背景資料處理層完全脫鉤。
* **Phase 2: 核心邏輯解耦 (Logic Decoupling)**
  * 重構 `analysis_engine.py` 這個「上帝類別 (God Class)」。
  * 將技術、籌碼、營收分析分離成獨立的微服務或模組。對分析引擎進行「純函數化 (Pure Functions)」改造，確保引擎只負責接收數據並返回結果，消除其內部潛藏的網路呼叫或 UI 狀態依賴。
* **Phase 3: 自動化測試防護網 (Test Automation)**
  * 全面引入 `pytest` 框架，取代 `tools/` 底下上百個需要人工檢視的臨時腳本。
  * 針對核心的技術指標計算、訊號觸發邏輯建立單元測試 (Unit Tests)，並使用 Mock 資料進行回歸測試，確保未來加入新模型時不會破壞既有勝率。
* **Phase 4: 架構現代化 (Architecture Modernization)**
  * 導入標準的 Python Package 專案結構（如劃分 `src/`, `tests/`）。
  * 引入 Pydantic 進行資料庫/API的結構與型別驗證，並建立更安全的配置管理（Configuration Management）系統，消滅程式碼中的硬編碼 (Hardcoding) 與魔術數字。

### 📈 視角二：產品與量化功能 (Product & Quant Features)
目標：將大量的離線回測成果產品化，升級為全方位的智能投資平台。

* **Phase 1: 預測模型強化 (Predictive Models Enhancement)**
  * **引入非線性樹狀模型**：將技術與籌碼指標作為特徵，引入 XGBoost 或 LightGBM 預測勝率，並透過 SHAP 值讓 AI 能解讀特徵重要性。
  * **動態狀態權重自適應 (Adaptive Regime Allocation)**：讓模型根據 HMM Regime（多空狀態切換）自動學習在不同市況下，哪些因子的權重應該提升。
* **Phase 2: 擴充新興與另類數據源 (Alternative Data Integration)**
  * **社群與新聞情緒串流**：接入 PTT 股版、StockTwits 等社群熱度與 NLP 情緒分數，做為預警訊號。
  * **產業鏈資金輪動矩陣**：將目前的靜態 Sector Tag 升級為動態關聯圖譜，自動追蹤上下游的資金流入狀態。
* **Phase 3: AI 報告管線升級 2.0 (AI Pipeline 2.0)**
  * **多智能體辯論 (Multi-Agent Debate)**：讓一個 Agent 扮演「價值挑剔者」，另一個扮演「動能投機者」進行交互辯論，最後總結出最客觀的「牛熊觀點 (Bull vs Bear Case)」。
  * **事件驅動與對話式分析 (Chat with Data)**：支援盤中特定事件觸發的短訊推播；並在 UI 中加入自然語言對話框，讓使用者可以直接問 AI 財務估值問題。
* **Phase 4: 投資組合與風險管理 (Portfolio & Risk Management)**
  * **動態部位縮放 (Dynamic Position Sizing)**：結合預期回撤或凱利公式，給出具體的「建議資金佔比」。
  * **多空組合壓力測試**：讓使用者能預覽目前的選股組合，在極端情境下的預期最大回撤 (Max Drawdown)。

### 🧐 視角三：專業價值投資人 (Professional Value Investor)
目標：深化護城河與估值模型，避免價值陷阱，符合避險基金標準。

* **Phase 1: 導入絕對估值模型 (Two-Stage DCF)**
  * 實作基於 WACC (加權平均資本成本) 的兩階段自由現金流折現模型。
  * 算出絕對公允價值，並強制輸出**安全邊際 (Margin of Safety) 百分比**，解決 DDM 無法適用於成長型價值股 (GARP) 的問題。
* **Phase 2: 杜邦分析拆解模組 (DuPont Analysis)**
  * 將 ROE 拆解為三大引擎：淨利率、總資產周轉率、權益乘數。
  * 建立高風險槓桿預警機制，若 ROE 高於 15% 且驅動力超過 60% 來自「高負債」，則予以扣分。
* **Phase 3: 景氣循環調整本益比 (Shiller P/E / CAPE)**
  * 計算過去 5-7 年的平均 EPS 來取代 TTM EPS。
  * 針對電子零組件、航運等特定 Sector，強制使用 CAPE 進行篩選，避免在景氣高峰時誤判為「低 PE 價值股」。
* **Phase 4: 營運資金與現金轉換循環 (CCC)**
  * 計算應收帳款收現天數 + 存貨周轉天數 - 應付帳款付款天數 (CCC)。
  * 若 CCC 連續三季拉長，觸發「塞貨/庫存跌價損失風險」扣分機制。
* **Phase 5: 升級為總股東回報率 (Shareholder Yield)**
  * 將股息殖利率擴展為：股息殖利率 + 庫藏股殖利率 + 還債殖利率，更全面涵蓋資本配置優秀的企業。

### 🚀 視角四：專業量化與動能交易者 (Professional Quant & Momentum Trader)
目標：升級動能因子、引入投資組合回測與波動率控管，符合機構級量化標準。

* **Phase 1: 動能因子升級 (Momentum Factor Enhancement)**
  * **實作多週期動能**：新增 1M, 3M, 6M 的 Rate of Change (ROC)。
  * **相對強度百分位 (RS Rating)**：計算個股相對於全市場大盤的 RS 分數 (1-99分)，剔除弱勢股。
  * **趨勢平滑度過濾 ($R^2$)**：對過去 90 天價格進行線性迴歸計算 $R^2$，剔除單日暴漲但走勢極度震盪的假動能股。
* **Phase 2: 投資組合級回測引擎 (Cross-Sectional Portfolio Backtester)**
  * 重構 `backtest_engine.py` 支援 Panel Data 輸入，實作**定期調倉 (Rebalance)** 機制，從「單一標的回測」升級為「投資組合回測」。
  * 引入**大盤濾網 (Market Regime Filter)**，當大盤跌破關鍵均線或處於熊市時自動降低總曝險。
* **Phase 3: 風險平價與部位管理 (Risk-Based Position Sizing)**
  * **波動率倒數配置**：引入基於 ATR (真實波動幅度) 的部位控管公式 (`買入股數 = 總資金風險上限 / ATR`)，讓不同波動率標的的總資金回檔風險一致化。
* **Phase 4: 自適應演算法與信號對齊 (Adaptive Logic & Signal Consistency)**
  * 將型態偵測 (`pattern_detection.py`) 參數改為基於波動率自適應 (Adaptive) 的動態窗口。
  * 徹底消除回測與實盤「信號偏移 (Signal Drift)」，確保回測引擎直接讀取每日歷史截面的真實分析報告 (Point-in-Time Data)。

---

### 🛡️ 視角五：專業風險控管專家 (Professional Risk Manager)
目標：建立投資組合層級的防護網，確保在極端市況與黑天鵝事件下的資金生存率。

* **Phase 1: 波動度調整資金控管 (Volatility-Adjusted Sizing)**
  * 導入「固定風險比例 (Fixed Fractional Position Sizing)」，依據個股 ATR 動態計算買入股數，確保每筆交易的最大潛在虧損鎖定在總資金的 1%~2%。
* **Phase 2: 投資組合層級熔斷與限制 (Portfolio-Level Risk Controls)**
  * 實作產業集中度上限 (如單一產業不超過 30%) 與相關性過濾 (拒絕加入與現有持股高度相關的標的)。
  * 建立投資組合 VaR 熔斷機制，若總淨值短期回檔過大，強制暫停所有新進場買訊。
* **Phase 3: 防甩轎漏洞修補與時間停損 (Advanced Exits & Whipsaw Fixes)**
  * 修補「無量緩跌」導致停損失效的漏洞，加入無視量能的「絕對硬停損 (Absolute Hard Stop)」。
  * 導入時間停損 (Time Stop)，釋放長時間盤整未發動的部位資金。
* **Phase 4: 總體市場寬度與現金水位控制 (Macro Regime & Market Breadth)**
  * 以市場寬度 (如站上月線的股票家數比例) 定義大盤 Regime。
  * 動態調整總體現金水位：多頭滿倉，震盪半倉，空頭保留 80% 以上現金或僅限避險操作。

### 🧠 視角六：AI 與機器學習工程師 (AI & ML Engineer)
目標：建立現代化 MLOps 流程，提升預測模型魯棒性，並導入進階的生成式 AI 架構。

* **Phase 1: 建立 MLOps 基礎設施 (MLOps Infrastructure)**
  * 導入 MLflow 或 W&B 進行模型註冊與實驗追蹤，取代手動儲存 `.joblib`。
  * 建立輕量級 Feature Store，確保訓練與推論的特徵邏輯一致。
  * 導入 LangSmith 監控 AI 報告的 Token 消耗、延遲與輸出穩定性。
* **Phase 2: 提升模型魯棒性 (Model Robustness)**
  * 採用「三重屏障標籤法 (Triple-Barrier Method)」取代固定時間窗的標籤，提升模型預測的真實交易意義。
  * 實作「淨化交叉驗證 (Purged CV)」防止時間序列的前瞻偏誤。
  * 導入 Meta-Labeling：以規則引擎產生訊號，ML 模型預測該訊號的勝率，決定部位大小。
* **Phase 3: 進階 AI 能力擴充 (Advanced AI Capabilities)**
  * 建立 RAG (檢索增強生成) 智能研報系統，整合法說會逐字稿與財報。
  * 訓練輕量級 CNN/ViT 將 K 線圖轉為圖像，取代現有寫死的型態辨識規則。
  * 微調金融專用 NLP 模型 (如 FinBERT/TAIDE)，自動計算論壇與新聞的情緒分數作為新特徵。

### 🗄️ 視角七：資料工程師與架構師 (Data Engineer & Architect)
目標：提升資料管線的可靠性、擴展性，建立企業級的反爬蟲與資料治理機制。

* **Phase 1: 強化排程與 Orchestration (提升可靠性)**
  * 將脆弱的 Windows Bat Script 遷移至 Apache Airflow 或 Dagster。
  * 建立清晰的任務依賴 DAG，並設定自動重試與即時告警 (Slack/Discord)。
* **Phase 2: 快取與儲存的分散式重構 (提升擴展性)**
  * 導入 Redis 實作分散式快取與鎖 (Redlock)，取代本地的 `threading.Lock`，為未來容器化與多機併發抓取鋪路。
  * 將核心與籌碼資料遷移至關聯式時序資料庫 (如 PostgreSQL + TimescaleDB)。
* **Phase 3: 資料擷取層與代理池 (解決 WAF 阻擋)**
  * 整合商業 Proxy API 或自動輪替 IP Pool，徹底解決 MOPS 等網站的反爬蟲與 IP Ban 問題。
  * 使用標準化 Resiliency 函式庫 (如 `tenacity`) 處理 Exponential Backoff。
* **Phase 4: 資料治理與品質監控 (Data Governance)**
  * 建立資料契約 (Data Contracts)，在寫入前透過 Pandera 進行欄位型別與 Null 值驗證。
  * 不合規資料放入 Dead Letter Queue (DLQ)，實現 Fail-fast，防止髒資料污染回測系統。

---

### 💡 執行策略建議
建議採取**並行推進**的策略：在進行 Phase 1 的工程重構（拆解 `app.py`、建立測試、導入 Airflow 與 MLOps）時，順勢將產品與專業投資人所需的初期強化功能（如樹狀模型、DCF 估值、多週期動能）整合進新的架構中。這樣既能解決技術債，也能不斷交付具有極高實戰價值與商業潛力的新功能。