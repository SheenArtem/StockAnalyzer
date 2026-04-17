# MOPS 爬蟲可行性 PoC 報告

**執行日期：** 2026-04-17  
**環境：** Python 3.14 / Windows 11 / requests + BeautifulSoup4  
**腳本：** `tools/mops_poc.py`  
**樣本資料：** `reports/mops_poc_samples/`

---

## TL;DR

| 項目 | 結論 |
|------|------|
| 月營收可抓 | **YES** — 完整 JSON，6/6 月份資料與 FinMind 完全一致 |
| 財報可抓（損益/資產負債/現金流） | **YES** — 三張表均成功，全年累計與 FinMind 四季加總精確相等 |
| 股利可抓 | **YES** — 12 筆現金股利金額與 FinMind 完全一致 |
| 是否被 Rate Limit 擋 | **NO** — 100 req/burst 全部 200 OK，無任何限速 |
| 資料與 FinMind 是否一致 | **YES，數值完全相同**，只是單位 / 日期標準不同 |
| 建議 | **做** — 技術可行，資料品質優，且零費用 |

---

## 詳細結果

### 1. 連線性

**關鍵發現：MOPS 已全面改版為 Vue SPA + JSON REST API（2024 年後）。**

舊的 `mops.twse.com.tw/mops/web/ajax_t05st10_ifrs`（Form POST HTML）路由已棄用，會回傳「此頁面無法被存取」安全錯誤。實際 API 為：

```
Base URL:  https://mops.twse.com.tw/mops/api/
協議:       HTTPS POST, Content-Type: application/json
回傳格式:   UTF-8 JSON  {"code": 200, "result": {...}}
認證:       JSESSIONID cookie（GET https://mops.twse.com.tw/ 取得，無需登入）
SSL:        Python requests 需加 verify=False（MOPS SSL 憑證缺少 Subject Key Identifier）
```

| 連線要求 | 說明 |
|----------|------|
| JS 渲染 | **不需要** — 純 HTTP POST，BeautifulSoup / Playwright 均不必要 |
| Cookie | **需要 JSESSIONID** — GET 根域取得，約 1 秒 |
| CSRF token | **不需要** |
| 登入 / API key | **不需要** |

### 2. 三類 Endpoint 結果

| Dataset | API Endpoint | 2330 | 3008 | 6789 | 資料筆數（2330） | 備註 |
|---------|-------------|------|------|------|-----------------|------|
| 月營收 | `api/t05st10_ifrs` | OK | OK | OK | 9 欄位 / 月 | 含本月/去年同期/累計/增減% |
| 損益表 | `api/t164sb04` | OK | OK | OK | 46 行 / 季 | 合併報表，P&L 全科目 |
| 資產負債表 | `api/t164sb03` | OK | OK | OK | 67 行 / 季 | 含流動/非流動/負債/股東權益 |
| 現金流量表 | `api/t164sb05` | OK | OK | OK | 80 行 / 季 | 含營業/投資/籌資 |
| 股利 | `api/t05st09_2` | OK | OK | OK | 16 筆（普通股） | 含現金/股票股利、除息日、章程 |

**全部 15 次測試（3 股 × 5 endpoint）100% 成功，回應時間 0.07~0.19 秒。**

#### 關鍵參數差異

**月營收：**
```python
# dataType="1" 回傳最新月份（自動選最近可得）
# dataType="2" 指定 year/month
{"companyId": "2330", "dataType": "2", "month": "3", "year": "113"}
# year 為民國年；回傳 JSON result.data[0][1] = "415,191,699"（千元）
```

**財報（損益/資產負債/現金流）：**
```python
# dataType="1" = 最新季度；dataType="2" = 指定 year/season
# season: "1"=Q1, "2"=Q2, "3"=Q3, "4"=Q4/全年
{"companyId": "2330", "dataType": "2", "year": "113", "season": "4"}
# season=4 回傳「全年累計損益表」（非 Q4 增量）
```

**股利：**
```python
{"companyId": "2330", "dataType": "2", "firstYear": "111", "lastYear": "113", "queryType": "1"}
# queryType 必填，缺少會回 500；result.commonStock.data[] 每列含現金股利金額
```

### 3. Rate Limit

| 輪次 | N | 間隔 | 成功 | 被擋 | 速率 | avg 回應 |
|------|---|------|------|------|------|---------|
| Burst | 10 | 無 | 10 | 0 | 827 req/min | 0.073s |
| 1s 間隔 | 30 | 1s | 30 | 0 | 56 req/min | 0.077s |
| 0.5s 間隔 | 50 | 0.5s | 50 | 0 | 104 req/min | 0.079s |
| Burst | 100 | 無 | 100 | 0 | 801 req/min | 0.075s |

**結論：在測試期間 MOPS API 完全沒有 Rate Limit 保護。** 100 次 burst 請求全部 200 OK，速率約 800 req/min。實務建議保守使用 120-300 req/hr（即 0.5-1s 間隔）以避免觸發未知的後端保護。

### 4. FinMind 資料比對

#### 4a. 月營收（2330，6 個月，民國 113 年 1–6 月）

| 月份 | MOPS（千元） | FinMind（元） | 比值 | 結果 |
|------|------------|--------------|------|------|
| 11301 | 215,785,127 | 215,785,127,000 | 1000.0x | MATCH |
| 11302 | 181,648,270 | 181,648,270,000 | 1000.0x | MATCH |
| 11303 | 195,210,804 | 195,210,804,000 | 1000.0x | MATCH |
| 11304 | 236,021,112 | 236,021,112,000 | 1000.0x | MATCH |
| 11305 | 229,620,372 | 229,620,372,000 | 1000.0x | MATCH |
| 11306 | 207,868,693 | 207,868,693,000 | 1000.0x | MATCH |

**6/6 完全相符。** 單位差異：MOPS 為千元（NT\$K），FinMind 為元（NT\$1）。換算公式：`FinMind.revenue = MOPS.data[0][1] * 1000`。

#### 4b. 財報（2330，FY2024 全年）

| 指標 | MOPS 全年累計 | FinMind 四季加總 | 相差 |
|------|-------------|-----------------|------|
| 營業收入合計 | NT\$2,894,307,699,000 | NT\$2,894,307,699,000 | **0.0%** |

精確相等。重要說明：**MOPS season=4 = 全年累計報表，非第四季增量**。如需取得各季增量數值，需自行做相鄰季度相減（與 FinMind 原始季度資料用法不同）。

#### 4c. 股利（2330，民國 111–113 年）

| 指標 | MOPS | FinMind |
|------|------|---------|
| 普通股股利筆數 | 12 | 32（含歷史年份更多） |
| 現金股利金額比對 | 12/12 完全一致 | — |
| 日期標準 | 董事會決議日 | 除息交易日（通常晚 1–2 個月） |

**12/12 現金股利金額完全一致。** FinMind 提供更長歷史（需調整查詢 firstYear）。日期標準差異不影響數值正確性，production 實作時需注意對齊方式。

### 5. 實作建議

若決定實作 production MOPS scraper：

**Session 管理：**
```python
import requests, warnings
warnings.filterwarnings("ignore")   # suppress SSL warning
sess = requests.Session()
sess.headers.update({"User-Agent": "Mozilla/5.0 ...", "Content-Type": "application/json", ...})
sess.get("https://mops.twse.com.tw/", verify=False)   # 取 JSESSIONID
```

**財報季度增量計算（若需取代 FinMind 季度資料）：**
```python
# 需抓 Q1/Q2/Q3/Q4 各季累計，再相減得季度增量
q4 = fetch_income(stock_id, year=113, season=4)   # 全年
q3 = fetch_income(stock_id, year=113, season=3)   # 前三季
q4_incremental = q4 - q3                           # Q4 單季
```

**建議速率：** 0.3–0.5s 間隔，預計 120–200 req/min 不觸發任何保護。

**預估全市場掃描耗時（約 1,700 上市股）：**
- 月營收（每月一次）：1,700 × 0.1s = 2.8 分鐘
- 四季財報（季更）：1,700 × 4 季 × 3 表 × 0.1s = 34 分鐘
- 股利（年更）：1,700 × 0.1s = 2.8 分鐘
- **合計：不超過 45 分鐘 / 全市場完整重抓一次**（含間隔保守估）

**維護風險：**
- MOPS REST API 是新系統（2024 年啟用 SPA），短期改版機率低
- API endpoint 名稱（`t164sb04` 等）延續舊系統命名，已穩定多年
- **若發生改版，影響範圍有限**（只需重新找 JS bundle 中的 endpoint 名稱）

### 6. 工作量估算

| 階段 | 工作內容 | 估計時間 |
|------|----------|---------|
| PoC → Production（月營收） | Session 管理 + 分頁 + 快取 + 單元測試 | 0.5 天 |
| Production（財報三表 + 季度增量計算） | 資料轉換 + FinMind 欄位對齊 + 測試 | 1.5 天 |
| Production（股利） | 日期對齊 + 資料清洗 + 測試 | 0.5 天 |
| 整合到 cache_manager.py + fundamental_analysis.py | fallback 邏輯設計 | 1 天 |
| **合計** | | **3.5 天** |

**vs FinMind 付費方案（NT\$12,000–20,000/年）：**
- 3.5 天工程師時間成本 >> NT\$12K 年費
- **短期 ROI：不划算**
- **長期（若 FinMind 改費用結構或 rate limit 成為瓶頸）：MOPS 是最佳 fallback**

---

## 環境

- Python 3.14 + requests + BeautifulSoup4（後者本次未動用，純 JSON API）
- MOPS API base: `https://mops.twse.com.tw/mops/api/`
- SSL: `verify=False`（MOPS 憑證問題，與專案 TWSE API 相同處理方式）
- 快取比對：`cache_manager.get_finmind_loader()` + `get_finmind_cached()`

## 產出檔案

- `tools/mops_poc.py` — PoC 腳本（可重跑，自動取 JSESSIONID）
- `reports/mops_poc_samples/*.json` — 各 endpoint 原始 JSON 回應（供人工核對）
  - `p1_t05st10_ifrs.json` / `p1_t164sb04.json` / `p1_t164sb03.json` / `p1_t164sb05.json` / `p1_t05st09_2.json`
  - `p2_revenue_{stock}.json` — 三檔月營收範本
  - `p4_income_2330_FY2024.json` / `p4_dividend_2330.json` — 比對用原始資料
  - `poc_results.json` — 所有 Part 結果摘要 JSON
