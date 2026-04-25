---
name: songfen
description: Use this skill when the user types `/songfen <ticker>` or explicitly asks for 「宋分視角 / 宋分分析師 / 美股送分題 / re-rate 分析」 for a specific stock. Produces an analyst-lens deep dive applying the Songfen framework (re-rate signals, 5-layer P&L decomposition, timing discipline, regime context). Do not auto-invoke on regular AI reports.
---

# Songfen Analyst Lens

用「宋分 / 美股送分題」分析師的底層框架為指定股票做獨立深度分析。**此 skill 為指定觸發**，不影響既有 AI 報告 tab 的 prompt。

## 觸發條件

- 使用者明確輸入 `/songfen <ticker>`（例：`/songfen 2330`, `/songfen NVDA`）
- 或使用者語意上要求「宋分視角 / 宋分分析師 / 機構分析師 / re-rate 拆解」並指定標的

若使用者只是請求一般 AI 報告、不提及宋分/送分題關鍵字，**不要啟動此 skill**。

## 執行步驟

### Step 1 — 讀框架與當期主題

依序讀下列兩份檔案做為分析骨架：

1. `prompts/songfen_framework.md` — 永久性方法論（re-rate 訊號、5-layer 損益表、擇時紀律、行為對比）
2. `knowledge/songfen/INDEX.md` — 最新文章索引（特別是 **Current Themes** 區塊，有時效性）

### Step 2 — 收集股票數據

若使用者未同時提供數據，主動從本 repo 取用：

- 個股技術/籌碼：`analysis_engine.TechnicalAnalyzer` 產出的 report 物件
- 基本面：`fundamental_analysis.get_fundamentals(ticker)` 拿 PE/PB/ROIC/毛利/營收
- Piotroski：`piotroski.calculate_fscore(ticker)` 拿 F-Score
- 同業比較：`peer_comparison`

**禁止重複呼叫 API** — 先從 session_state / 既有 cache 撈。若皆無，才抓新資料並 log 來源。

### Step 3 — 套用四道視角（必須全做）

#### 視角 A：re-rate 訊號檢核

- 收入可預測性是否提升？（recurring / subscription 佔比、backlog）
- 現金流結構是否改變？（CCC 縮短、FCF 穩定度）
- 是否變產業基建？（客戶 design-in、不可替代性）
- 競爭格局改變？（寡佔 vs 廝殺）
- 管理層資本配置是否被信任？（回購 / CAPEX 歷史）
- **判斷現在位於定價三階段的哪一段**（好消息大漲 / 不動 / 下跌）

若判斷 re-rate 可能發生，指出是三等級的哪一種（假反彈 / 節奏型 / 結構性），以及還需看到什麼訊號才確認。

#### 視角 B：5-layer 損益表拆解

1. **營收**：成長來自 🥇 recurring / 🥈 結構性 / 🥉 週期性？比例與趨勢
2. **毛利**：邊際改善還是水準？原因（product mix、規模、降價、庫存減損？）
3. **費用**：分好費用（投資）vs 壞費用（燒錢）；檢查 SBC 稀釋速度
4. **營業利益**：是否跨越營業槓桿臨界點？
5. **EPS**：剝掉業外/回購/稅率雜訊後的「標準化獲利」趨勢

**特別留意 ROIC vs EPS 背離**：若 ROIC 正在上升但 EPS 還醜，可能是 Amazon 式「藏投資在費用」的階段。

若為資本密集產業（記憶體、類比、重工業），明確說明應該用 PE 還是 PB。

#### 視角 C：擇時與紀律

- **thesis 寫下**：買進理由是什麼？預期什麼訊號驗證？沒發生要怎麼辦？
- **停損條件**：根據 thesis 判斷何時算「邏輯壞掉」（非價格）
- **當前加碼訊號**：利空不再破底？強勢股撐住？市場分化？→ 對應 10-20% / 30-50% / 60-80% 倉位
- **獲利了結節奏**：若已有倉位，套三段式減碼框架
- **大盤對照**：反彈三風險指標（HY spread / VIX3M-VIX / 10Y）是否不再惡化？

#### 視角 D：regime 與當期 theme 對照

從 INDEX.md 的 **Current Themes** 中挑出與此股票最相關的 1-3 項主題，說明：
- 該 theme 是 time-sensitive（3 個月內有效）
- 此股票是否站在 theme 的「正確邊」（受惠 vs 受害 vs 無關）
- 該 theme 若失效，這筆交易會怎樣

### Step 4 — 反面論點（必須）

宋分框架核心：**「永遠準備反面論點。找『為什麼我會看錯』比『為什麼我是對的』更有價值。」**

至少列 3 個「看空」或「thesis 可能被推翻」的情境，具體到訊號層級，不是空泛的「宏觀風險」。

### Step 5 — 輸出格式

繁體中文，以 Markdown 呈現，結構如下：

```
# 宋分視角：<ticker> <公司名>

## 核心結論
（3-5 點；紅色 <span style="color:red">偏多</span> / 綠色 <span style="color:green">偏空</span> / 中性）

## 視角 A：re-rate 訊號
| 訊號 | 當前狀態 | 判斷 |

（文字總結 — 現在在三階段哪一階段？re-rate 若發生是哪一等級？）

## 視角 B：5-layer 損益表拆解
| Layer | 訊號 | 方向 |

（文字總結 — ROIC 趨勢 / 營業槓桿位置 / 標準化 EPS）

## 視角 C：擇時與紀律
- Thesis（買進理由）：...
- 看錯條件（stop-loss logic）：...
- 當前加碼訊號與建議倉位：...
- 若已持有：獲利了結節奏建議

## 視角 D：當期 theme 對照
（列相關 time-sensitive themes 與 exposure 判斷）

## 反面論點（3+）
1. 看空情境一：...（可觀察訊號：...）
2. 看空情境二：...
3. 看空情境三：...

## 最後一句
一句話總結：此股票在宋分框架下處於什麼位置、值不值得機構型投資人現在動手。
```

## 限制

- **不要**把框架原文照搬到結論裡 — 要針對這檔股票輸出結論，框架是思考工具
- **不要**用「可能」「或許」這類模糊語氣掩蓋不確定性 — 直接說「數據不足」
- **不要**引用超過 3 個月以上的備忘錄 time-sensitive 觀點 — 會過時
- **不要**動到 QM / Value Screener 選股分數 — 此 skill 純粹是 ad-hoc 深度分析工具，不影響量化層

## 典型呼叫範例

```
/songfen 2330
/songfen NVDA
請用宋分視角看一下 0050
```

## 相關檔案

- `prompts/songfen_framework.md` — 方法論精華（必讀）
- `knowledge/songfen/INDEX.md` — 文章索引 + 當期 themes（必讀）
- `C:\ClaudeCode\Normal\substack_posts\` — 原文（如需查詢細節才讀）
- `tools/sync_substack.py` — 更新索引
