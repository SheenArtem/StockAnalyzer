# 完整 Code Review — 未提交工作區變更（2026-08-02）

## 一句話結論

這批變更（主力選股全移除 + 投資組合報價改寫 + 月營收與行情管線加固 + 白話投資知識庫新功能）**加固方向正確、機械檢查全過**（pytest 354、BAT 規則、LLM 規範、無吞例外的新增碼），但審查挖出 **4 件會產生錯誤數字或造成外洩的問題**，其中兩件的錯誤**已經落盤到 production 資料檔**：

1. 金融族群的 `quality_score` 被系統性誤扣約 22 分，錯誤結果已寫進 `quality_scores.parquet`，且線上選股路徑正在讀它。
2. 投資組合的 NAV / CAGR / Sharpe 會被「單日加碼」灌爆（實測單日 +50%）。
3. 兩份針對**具名真實人物**的負面評價報告沒有被 gitignore，`git add` 會推上公開 GitHub。
4. `portfolio_store.py` 的說明文件錯誤宣稱個人真實交易紀錄是「git 追蹤」。

另外，**現在直接 commit 會被 pre-commit hook 擋下**（版本號不是今天日期），而且 `AGENTS.md` 還沒 `git add`，漏掉的話整套專案規則會消失。

---

## 修復進度（2026-08-02）

全套測試 **384 passed**（原 354，+30 為本次新增的回歸測試）。工作區已全部 commit
（本 session 共 16 個 commit，`2d965b5`..`e2c4f85`，未 push）。

| 項目 | 狀態 | 驗證方式 |
|---|---|---|
| P0-3 `.gitignore` 排除 `reports/yt_analyst_*` | ✅ `2d965b5` | `git check-ignore` 兩檔皆命中；同目錄週報仍可加入；無已追蹤檔被誤蓋 |
| P0-4 `portfolio_store.py:5` docstring | ✅ `3291d50` | 改為 local-only 敘述並移除誤導的 memory 引用 |
| P0-1 `normalize_financial_wide` 缺值政策 | ✅ `3291d50` | 建欄留 NaN + `compute_zscore_row` 必要欄位 guard；`quality_scores.parquet` 已重產 |
| P1-4 測試把缺陷寫成規格 | ✅ `3291d50` | 3 條錯誤斷言改寫，新增 5 條涵蓋 NaN 保留 / Z 回 None / 中性計分 / guard 未誤殺 / F-Score 略過 |
| P0-2 `build_nav_series` TWR 歸因 | ✅ `3291d50` | 當日淨增部位改以成交成本計價；4 條回歸測試經 HEAD 對照確認能抓到原錯誤 |
| P1-6 `test_bulk_revenue_safety` 時間炸彈 | ✅ `b2a6fbb` | patch `expected_revenue_period` + 斷言 merge 恰呼叫 1 次；已證明舊寫法在 8/10 後 merge 呼叫 0 次 |
| P1-1 `app.py` 版本號 | ✅ `ff80466` | 改為 `v2026.08.02.1`，pre-commit 版本檢查 PASS |
| P1-2 `AGENTS.md` 入版控 | ✅ `59f3633` | 與 `CLAUDE.md`/`README.md` 同 commit；另 `3876349` 把 22 處註解指標改指 AGENTS.md / docs/agent |
| P1-3 三個新模組與 5 支新測試入版控 | ✅ `aee14b7` `b2a6fbb` | 與 `notes_view.py` 同 commit |
| P2 TW breadth 混入美股 | ✅ `40a96dd` | 抽出 `tools/tw_universe` 共用判別；重建後移除 164 個日期（161 個非台股交易日），共同日期約 2,400 列數值修正 |
| P2 4 天過期門檻春節假 FAIL | ✅ `e2c4f85` | 改用官方交易日判準；20.6 年真實日曆模擬：舊判準 132 個假 FAIL 夜、新判準 0 次 |
| P2 盤中執行必 RuntimeError | ✅ `e2c4f85` | 剔除今日 bar 的順序移到健康度計算之前 |
| P2 單檔 merge 失敗殺全批 | ✅ `e2c4f85` | 容忍 max(5, 1%)，覆蓋率檢查仍是安全網；`os.replace` 加 PermissionError 重試 |
| P3 `.gitignore:21` 註解列已刪的 `ledger_append` | ✅ `ff80466` | |
| P3 資料源表未反映官方 EOD overlay | ✅ `61900ee` `3503e56` | 另補美股個股報價列（v8 chart、禁 v7） |
| P3 LLM 表未列 `build_baihua_kb` | ✅ `c01eb92` | |

**P0-1 的實際效果**：6,486 列（7.7%）分數變動；4,455 列 / 848 檔的 `z_score` 由「有限低值」回到 NaN，平均 quality_score **+15.1**；富邦金 2881 由 5 → 30。對 `value_screener.py:576` 大型股通道的影響：22 檔金融股的 955 個季列中，`quality_score >= 50` 的列數由 **0 → 275**。

**P0-1 的意外收穫**：對照 HEAD 逐欄比對時發現 FinMind 主要拼法是 `NoncurrentLiabilities`（小寫 c），佔 76,367 / 83,934 列。HEAD 只找 `NonCurrentLiabilities`，導致 **F5 長期負債比與 ROIC 兩個分支對 93% 的股票從未生效**。這次的別名合併順帶修好了它 —— 這也是修正後與 HEAD 差異達 40% 的原因，方向正確。同時已把別名表拆成 `_SPELLING_ALIASES`（同義字，安全）與 `_PROXY_ALIASES`（代理值，仍待處理），拆分經驗證為行為中性。

**仍未處理**：
- 第四節剩下三項：官方 EOD overlay 以「請求日期」而非 payload 自報日期蓋章（需先讓 `twse_api.get_market_daily_all` 回傳資料自報日期）、FinMind 額度封鎖錨在本 process 第一筆請求（最長鎖 3605 秒）、`_PROXY_ALIASES` 的代理值無旗標。
- 第五節白話投資各項（STATE checkpoint、400 輪截斷、登入牆靜默成功、seq 排序、SoT 靜默截短、cookie 刪除失敗吞例外）。
- 第六節文件衛生剩餘項（`notes_view.py:4` docstring、`test_scanner_fail_loud` 手抄 marker、breadth stage 無 verifier marker、`expected_stock_count` 用檔案數、缺 newest-first 回歸測試、殘留 Whale 文字 4 處、持股表「名稱」欄）。
- 第九節既有問題（見下方新增發現）。

**修復過程中新發現（不在原報告內）**：全 `tools/` 做完整 `py_compile` 掃描後，**編譯失敗的不只 `dcf_ic_analyze.py` 一支，而是 5 支已追蹤檔**：`tools/ab_test_codex_vs_sonnet.py`、`tools/dcf_ic_analyze.py`、`tools/test_brokerage_yt.py`、`tools/vf_chip_dual_inst_regime_gate.py`、`tools/vf_chip_dual_inst_signal.py`（另有 16 支未追蹤的本機 UI 測試腳本同樣壞掉）。原報告只掃了「本次變更的檔案」所以只發現 1 支。逐版追查 git 歷史後確認：**這 5 支歷來沒有任何一個版本編譯得過**，是寫入當下就以有損編碼落盤（AGENTS.md 警告的「CJK 過 native pipe」事故）。損壞本質是**非法 UTF-8 位元組**（Python 直接拒絕解析），程式碼本身是 ASCII 未受影響，壞的只有註解與 docstring 文字（每檔 7~61 個字元被截斷）；`dcf_ic_analyze.py` 另有 1 處換行被吞掉（第 76 行把 `agg = ...` 併進註解）。修復方式是把檔案轉成合法 UTF-8（受損字元以 `?` 標記）並補回被吞的換行 —— 程式碼可完整還原，但原註解文字已永久遺失。這 5 支都不是 production 路徑、也從未被執行過，因此沒有任何功能因此退化。

---

## 審查範圍與方法

**對象**：`git status` 的未提交工作區變更 —— 42 個變更的程式/設定檔 + 3 個新增未追蹤模組（`baihua_kb_view.py`、`tools/fetch_baihua_fb.py`、`tools/build_baihua_kb.py`）+ 5 個新增未追蹤測試檔 + 審查期間落地的 AI 文件重構（`AGENTS.md`、`docs/agent/`）。資料檔（`data/**.parquet`）的內容變更不在範圍。

**方法**：8 個平行審查代理依元件分組，每個中高風險發現再交由獨立代理做**對抗式驗證**（任務是「試圖推翻它」）。最終 **32 個代理全數完成、零失敗**，產出 45 條發現、24 條經對抗式驗證，其中 **22 條確認成立、2 條被駁回**，另有多條在驗證階段被修正嚴重度或修法。

**證據等級**：本報告的 high 級發現全部有實跑重現（重放真實 parquet、重現 TWR 數列、實打 Yahoo API、模擬 20.5 年交易日曆），不是純讀碼推論。

---

## ⚠️ 先更正我在第一版報告寫錯的兩件事

補跑完整審查後，第一版報告有兩處結論被推翻，先講清楚以免誤導後續工作：

**更正一：版本號不是「通過」，而是「會擋下 commit」。**
第一版我寫「`app.py` 版本號已從 v2026.07.06.1 升至 v2026.07.16.1，pre-commit hook 不會擋」。這是錯的——我只比對了它與 HEAD 不同，沒有讀 hook 實際的判斷條件。`.git/hooks/pre-commit:20-24` 比對的是**今天日期**：`CURRENT=2026.07.16.1` vs `TODAY=2026.08.02` 不符 → 走警告分支 → `read -r response`，而 git 給 hook 的 stdin 是 `/dev/null`，讀到 EOF 使 response 為空 → `exit 1`，**commit 被擋**。commit 當天必須把版本號改成當日日期。

**更正二：「美股前收價缺測試守衛」被實測駁回。**
第一版列為 P1-4 的「改回 `range=5d` 會讓當日漲跌全算錯」不成立。驗證代理實打 Yahoo v8（AAPL / SPY / CRMG 三檔）證明：被視窗污染的只有 `chartPreviousClose`，而 `previousClose` 與 range 無關，range=1d 或 5d 都回正確昨收；`_prev_close`（`portfolio_pricing.py:84`）的 fallback 順序是 `previousClose` 排第一，**這個順序本身就是既有的守衛**。真正的載重參數是 `interval=1m`（range=1d + interval=1d 實測 `previousClose=None`），不是 range。此外被刪的 3 個舊測試釘的是已移除的 `_prev_close_from_chart()`，在新的 1 分 K 設計下語意不成立，刪除正確；測試數還從 3 增加到 17。此項降為 info，不必修。

---

## 一、必須先修（P0）

### P0-1　缺值一律補 0，讓整個金融族群的 `quality_score` 被誤扣約 22 分（錯誤已落盤）

- **位置**：`tools/compute_historical_fscore.py:68`（本次新增的 `normalize_financial_wide`）
- **機制**：第 68-72 行對 16 個欄位無條件 `fillna(0.0)`。金控／銀行／保險的資產負債表不分流動與非流動，FinMind 根本沒有 `CurrentAssets` / `CurrentLiabilities` 這兩個 type。補 0 後 `compute_zscore_row` 的 `x1 = (ca - cl) / ta` 變成 0，Altman Z 從「NaN（無法計算）」變成「有限但無意義的 0.1 左右」，必然 `< 1.81` → `score -= 20`。
- **關鍵反駁點（驗證階段確認）**：函式 docstring 自稱只是「把既有的 missing=0 政策寫明白」，看似無行為變更 —— **這句話是錯的**。舊路徑 `curr.get('CurrentAssets', 0)` 的預設值 0 永遠不會生效，因為 `_pivot_by_type` 只要全市場有任一檔有該 type 就會建出欄位，金融股是「欄位在、值為 NaN」。所以本次是**實質改變政策**（NaN → 0），不是澄清。
- **實跑證據**：
  - 合併後 wide frame 86,334 列中，`CurrentAssets` 為 NaN 的有 **1,820 列 / 104 檔**（不只金融股，還有 1262、1507、2066、3202 等資料不全的個股）。
  - 舊行為：1,820 列全部 NaN、finite 0 列；新行為：1,648 列變 finite，其中 **100% 落在 < 1.81**。
  - 隔離實驗（只切換 fillna、28xx 金融股 1,524 季列）：`changed=1498, mean=-22.11, median=-25.0`。
  - 我本人查磁碟：`data_cache/backtest/quality_scores.parquet`（mtime **2026-07-14 20:38**）中 2897 各季 `z_score` 全部是 0.12~0.13、`quality_score` 被壓到 0~33。**錯誤結果已經在檔案裡了。**
- **影響（驗證階段修正，比初判更嚴重）**：這個檔案至少有 6 個 active 消費者，**包含線上生產路徑**：
  - `value_screener.py:576-598` 大型股 Graham 例外通道，門檻是 `f_score >= 5` 且 `quality_score >= 50` —— 金融股被壓到 5~33 後**永久出局**。
  - `tools/value_historical_simulator.py:85`（Quality 佔 25%）、`tools/qm_historical_simulator.py:546`、`position_monitor.py`。
- **修法**：不要用單一 `fillna(0.0)` 打平所有欄位。分兩類：真正「沒有就是 0」的（如 `AmortizationExpense`）才補 0；分母／結構性欄位（`TotalAssets`、`CurrentAssets`、`CurrentLiabilities`、`Equity`、`Liabilities`、`Revenue`）維持 NaN，並在 `compute_zscore_row` 開頭加必要欄位檢查（任一為 NaN 就 return None），讓「無法評分」與「評分很差」保持可區分。**修完必須重跑一次 `quality_scores.parquet`**，磁碟上那份是污染的。
- **信心**：已確認（真實 parquet 全量重放 + 我本人查驗磁碟結果）。

### P0-2　投資組合 NAV / CAGR / Sharpe 會被單日加碼灌爆

- **位置**：`portfolio_store.py:495`（`build_nav_series`）
- **機制**：日報酬算式是 `r = (mv_d - flow_d - prev_mv) / prev_mv`。但買進在第 470 行用 `shares.index >= ed`（**含成交當日**），代表當日買進的股票當日就以收盤價計入 `mv_d`；分母 `prev_mv` 卻只有昨日的舊部位。於是「新錢的當日帳面損益 =（當日收盤 − 成交價）× 股數」被整包除在舊部位的小分母上。這與第 434 行 docstring 宣稱的「現金流視為當日末發生，不賺當日報酬」直接矛盾。
- **我本人重現**：6/01 買 AAA 1000 股 @20；6/02 買 BBB 1000 股 @1000、當日收 1010 → `ret = [0.0, 0.5]`、`nav = 1.5`。**單日 +50%**，正解（期初現金流慣例）是 `10000 / (20000+1000000) = +0.98%`。
- **驗證階段補充的三個案例**：只要成交價 ≠ 當日收盤就一定偏（同檔加碼 10.5 買、收 11，算出 +15%、正解 +10%）；沒有既有部位的當沖損益在績效曲線**完全蒸發**（`prev_mv=0` 走 else 分支回 0）；有既有部位時當沖被算成 +25%。
- **這是既有缺陷，不是本次引入**：`portfolio_store.py` 本次 diff 只改了 2 行註解。但 `portfolio_view.py` 這次把績效指標改成內聯計算，`total_return` / `CAGR` / `Sharpe` / `MDD` / `win_rate` 六個頭條數字全部從 `nav_df['ret']` 導出，所以投組 tab 顯示的就是這組被污染的數字。
- **修法（注意：原始 finding 給的公式是錯的，驗證階段已更正）**：期初現金流慣例的標準式是 `r = mv_d / (prev_mv + flow_d) - 1`（**分子不可再扣 flow**，扣了等於把本金當虧損）。原 finding 寫的 `(mv_d - flow_d)/(prev_mv + flow_d) - 1` 代入案例會得到 −97%，照做會更糟。若要改走期末慣例，必須成套：成交當日 mv 以**成交成本**計入新部位，隔日起才改市價 —— 只把股數延後、flow 仍記當日，會算出 −4900%。最穩的選擇是 Modified Dietz。
- **信心**：已確認（我本人重現數列 + 驗證代理 4 個案例實跑）。

### P0-3　兩份具名真人負評報告未被 gitignore，會推上公開 GitHub

- **位置**：`reports/yt_analyst_xie_chenyan_profile.md`、`reports/yt_analyst_xie_chenyan_appendix.md`（未追蹤新檔，共 111 KB）
- **機制**：我實跑 `git check-ignore -v reports/yt_analyst_xie_chenyan_profile.md` → **rc=1（未被任何規則排除）**。`.gitignore` 對 `reports/` 只排 `*.parquet` / `macro_report_*.json` / `claude_[0-9]*` / 部分 screenshots。本次同目錄還有 5 支 `weekly_chip_report_*.md` 要進版控，用 `git add -A` 或 `git add reports/` 收攏時它們會一起進 commit。`git remote -v` → `https://github.com/SheenArtem/StockAnalyzer.git`（公開）。
- **內容性質**：對一位可辨識的在職投顧分析師的評價性調查，含「學歷疑點…台灣博碩士論文系統查無其博士論文」「話術結構…系統性兩面話」「已踩合規紅線」等段落。
- **影響**：把針對真實個人的負面評價公開發佈，屬名譽權與合規風險，且 GitHub 公開後難以真正撤回（fork / cache）。這與專案既有立場矛盾——投顧 YT pipeline 刻意不接 AI 報告就是合規考量，`data/baihua/` 這次也正是為了「不推入公開 repo」才新增排除。
- **修法**：在 `.gitignore` 加 `reports/yt_analyst_*`（或整批移到已排除的 `data/` 路徑下），commit 前用 `git status --short reports/` 覆核。
- **信心**：已確認（我本人跑 check-ignore + 讀檔案內容）。

### P0-4　`portfolio_store.py` 說明文件錯誤宣稱個人交易紀錄「git 追蹤」

- **位置**：`portfolio_store.py:5`
- **機制**：本次 diff 改寫了這一句，把主力選股的交叉引用拿掉，卻**保留了錯誤的「git 追蹤」四個字**：「儲存：data/manual_trades/transactions.json（git 追蹤的累積型 state；見 memory project_daily_outputs_untracked）」。
- **實測反證（我本人驗證）**：`git check-ignore -v data/manual_trades/transactions.json` → 命中 `data/.gitignore:13`；`git ls-files data/manual_trades/` → 空。`data/.gitignore:11-13` 原文寫「真實持股與損益 = 敏感財務資料；本 repo 會 push GitHub，比照 positions.json local only, never push」。
- **加重因素**：docstring 引用的那份 memory **整份沒有提到 manual_trades**（寫於 2026-05-21，早於本功能 2026-07-01），而且它的規則主張「累積型 state 要 tracked」。有人循線查證後會得到「docstring 是對的、`.gitignore` 才是漏設」的反向錯誤結論，可能直接去刪掉 `.gitignore:13`。
- **修法**：改成與 `.gitignore` 一致的敘述（本機 only、永不入版控），並**連同那個 memory 引用一起拿掉**。
- **信心**：已確認。

---

## 二、Commit 前必須處理（P1-blocking）

### P1-1　現在直接 commit 會被 pre-commit hook 擋下

`app.py:70` 的版本號停在 `v2026.07.16.1`，hook 比對今天日期 `2026.08.02` 不符，非互動環境下 `read -r response` 讀到 EOF → `exit 1`。commit 當天把 caption 改成當日日期即可。（詳見上方「更正一」。）

### P1-2　`AGENTS.md` 仍是未追蹤檔，但 `CLAUDE.md` / `README.md` 已改成指向它

`git ls-files AGENTS.md` 回空、`git check-ignore` rc=1（純粹沒 add）。`CLAUDE.md` 已從 135 行規則本體改成 9 行轉接檔（`@AGENTS.md`），`README.md:66/123` 也改指它。若只 commit 已修改的 tracked 檔，任何 clone / CI / 其他 agent 拿到的規則就只剩一個指向不存在檔案的轉接檔，**等同全部專案規則消失**，README 在 GitHub 上的連結也會 404。必須把 `AGENTS.md` 與 `CLAUDE.md` / `README.md` 放同一個 commit。

### P1-3　三個新模組與 5 支新測試檔必須與 `notes_view.py` 同一個 commit

`notes_view.py:105` 已 import `baihua_kb_view`，但 `baihua_kb_view.py`、`tools/fetch_baihua_fb.py`、`tools/build_baihua_kb.py` 都還是 `??`。漏 add 的話其他機器 pull 後「白話投資」來源整個不可用（會被 try/except 接住顯示錯誤，不炸頁）。

---

## 三、應盡快修（P1）

| # | 位置 | 問題 | 驗證後嚴重度 |
|---|---|---|---|
| P1-4 | `tests/test_historical_fscore_schema.py:37` | 新測試把 P0-1 的缺陷**寫成規格**：第 35-37 行斷言金融股補 0 後 `CurrentAssets == 0.0` 且 `compute_zscore_row` 必須 finite。修好 P0-1 之後這三行必紅，很容易被誤判成「改壞了」而回退。同時檔名叫 `_schema` 卻沒有任何一項驗證輸出欄位集合，而 `position_monitor.py:345` 讀取失敗是 try/except 吞掉回空 DataFrame —— 欄位改名不是 crash，是**靜默停用 F-Score 掉分警報** | medium 已確認 |
| P1-5 | `tools/vfvc_backfill_monthly_rev.py:221` | 新的加固版 merge 搬進 vfvc，但 `mops_bulk_fetcher.py:127/204` 的舊版與其 CLI 原封不動留著，是一條繞過全部新驗證（原子寫、橫斷面、新舊 gate）直接寫進同一個 production cache 的路徑 | medium 已確認 |
| P1-6 | `tests/test_bulk_revenue_safety.py:135` | fixture 期別硬編碼 202606 且未 patch `expected_revenue_period`。我實算邊界：8/09 → 202606（通過），**8/10 → 202607**，此後新舊 gate 會先拋錯，測試只斷言回傳 False 所以照樣綠燈，但 merge / sync / aggregate 三個哨兵永遠走不到。驗證階段補充：**這是永久性失效**，不只 8 月（12/01 實測 expected=202610） | medium 已確認 |
| P1-7 | `tools/aggregate_fundamental_cache.py:90` | 單檔 parquet 讀取失敗只記 warning，只要還剩一檔成功就把殘缺結果原子覆蓋掉 `financials_*.parquet`，全程無列數／檔數比對。寫入端確實有非原子寫（`fetch_financial_history.py:135` 等 5 處），半寫檔不是假設。**觸發路徑是每日排程**：`run_scanner.bat:191` 每天跑 `rf1_cache_consistency_check.py --fix`。加重因子：`.bak` 每次執行都覆寫，連跑兩次殘缺聚合會毀掉最後一份好備份 | medium 已確認 |
| P1-8 | `tools/build_baihua_kb.py:433` | STATE 只在整批結束時寫入、`process_one` 不重用 `_meta` 快取。實測 `_build.log`：206 篇 @concurrency 6 花 **2904 秒**，換算 UI 用的 concurrency 4 約 **4357 秒**，穩定超過 3600 秒 timeout（約在第 170 篇被砍）。換機或新 clone 就回到首次全量情境，按鈕永遠收斂不了、每次重燒 LLM 額度。註：md 檔已逐篇落盤，損失的是額度與索引，不是資料 | medium 已確認 |

---

## 四、排程與資料鏈的穩健性問題（P2）

這些多半是本次新增的加固碼引入的**過嚴門檻**，方向對但邊界沒調好。

- **`tools/refresh_universe_prices.py:276` 的 4 天硬性過期門檻，春節必然連續多夜假 FAIL。** 驗證代理用磁碟上 `2330_price.csv` 的真實交易日曆（2006-2026、5043 筆）模擬：休市間隔 ≥6 個日曆天的事件 24 次，共 **132 個假 FAIL 夜**，平均 6.4 夜／年；2026 春節（最後交易日 2/11 → 下一個交易日 2/23）會**連續 7 夜 FAIL（2/17~2/23）**。每次 FAIL 都讓 `run_scanner.bat` `goto skip_market_panels`，連 breadth 與 backtest panels 一起跳過。修法：門檻改用「距離上一個實際交易日的交易日數」，或找不到任何交易日時 log warning 後以 exit 0 結束（no-op）。

- **`refresh_universe_prices.py:275` 盤中手動執行必以 RuntimeError 收場。** 健康度在剔除今日未完成 bar「之前」計算 → `healthy_date` 被判成今天 → 剔除今日後 `healthy_written` 停在 0 → `RuntimeError: healthy market date written for only 0/1964 stocks`。錯誤訊息完全誤導（CSV 其實都正確寫好了），而且 bat 會因此跳過整段面板。觸發窗是 09:00~13:35（判斷式是 `now < 13:35`，13:30 收盤到 13:35 之間最穩定觸發）。修法：把剔除今日 bar 的區塊移到健康度計算之前。

- **`refresh_universe_prices.py:226` 的 `if fail: raise` 是全有全無閘門。** 1964 檔只要有 1 檔 merge 失敗就整支非 0 結束，即使 `healthy_written=1963` 遠高於門檻 1572。這與同檔 merge 迴圈註解「單檔失敗不可中斷整批」的設計意圖自相矛盾。驗證代理**實地重現**了新增的 `os.replace` 在 Windows 上的失敗面：`PermissionError [WinError 5]`（原 finding 猜的是 ERROR_SHARING_VIOLATION，實測是 ACCESS_DENIED），而常駐的 Streamlit App Autostart 正好會讀同一批 CSV。修法：改成容忍度門檻（fail 佔比 >1%，或讓 healthy_written 掉到 required 以下才 raise）。

- **`refresh_universe_prices.py:136` 官方 EOD overlay 用「請求的日期」蓋章，而 TPEX 端點實測會忽略 date 參數。** 目前唯一的保護是列數門檻（TPEX 單獨 888 檔、TWSE 單獨 1093 檔都跨不過 1572），屬巧合式保護而非日期驗證。scanner.log 實證：2026-08-01 是週六，TPEX 仍回了 888 檔。一旦檔數比例改變或有人用 `--limit`，就會把「上一場」的 OHLCV 蓋上錯誤日期寫進 1900+ 支 CSV，而且所有欄位都是正數、健康度檢查抓不到。修法：overlay 只接受「payload 日期 == 請求日期」的 cross-section。

- **`cache_manager.py:547` 的額度封鎖時間錨在本 process 自己的第一筆請求。** `_reserve_request` 在打 API 之前就把 `now` 塞進 deque，所以剛啟動的 process 撞到 server 端額度時，`_seconds_until_window_reset()` 回 ≈3605 秒，直接鎖滿一小時；而且 `_set_quota_block` 只用 `max()`，沒有任何成功後解除或提前重探的路徑。驗證代理實跑重現：全新 tracker 第一筆就爆 → 鎖 3605 秒。真正受害的是長時間多次呼叫者（`chip_history_dl.py` 的 per-stock 迴圈、backfill、scanner、常駐 Streamlit）。修法：server 端額度改走固定上限的指數退避（300s 起、上限 900s）+ 解除後重探。

- **`tools/compute_historical_fscore.py:33` 的 `_CANONICAL_ALIASES` 把不同會計科目當同義字互填。** `NetInterestIncome → Revenue`（569 列 / 14 檔）、`PreTaxIncome → OperatingIncome`（1,406 列 / 35 檔），沒有留下任何代理值旗標。驗證階段的重要修正：**Z-Score 的 −20 懲罰要歸咎於 P0-1 的 fillna(0)，不是別名**（有無別名，z < 1.81 的比例都是 99.4%）；別名真正的後果是 `f_score` 平均 +0.21（全部來自 `Revenue ← NetInterestIncome` 經 F9 資產週轉率），以及落盤的 `z_score` 數值本身被污染。同一張表裡另外四組（`NoncurrentLiabilities`、`IncomeAfterTax`、`EquityAttributableToOwnersOfParent`、`NetCashInflowFromOperatingActivities`）確實只是拼寫差異，屬正確修正 —— 兩類混在同一張表沒有區隔才是問題。

- **`tools/build_tw_breadth.py:36`（既有問題）把約 500 檔美股一起算進台股廣度面板。** `CACHE.glob('*_price.csv')` 沒有台股代號過濾（對照 `refresh_backtest_panels.py:115` 有 `_TW_TICKER_RE`）。驗證代理在已發佈的面板裡找到**硬證據**：2026-02-17~20（農曆年）、2026-04-06、2026-05-01 六列是「台股休市、美股開盤」的純美股廣度列，已寫進 parquet，近 500 列窗內有 34 個此類日期。這些列同時進入 ADL 累加與 McClellan EMA，往後污染真正的台股日期；`macro_dashboard.py:426` 取 `df.iloc[-1]`，**連假期間會把美股盤勢當台股盤勢顯示**，而 `ai_report.py:1994` 也把 `pct_above_50/200dma` 寫進 AI 報告 prompt。污染還會再生：`cache_manager.py:783` 用通用命名寫入同一個 `data_cache`，使用者在 App 分析任何美股都會刷新美股 CSV。

- **月營收分數的可用日訂在法定截止日當天（10 號），但唯一的產出排程是 11 日 00:30。** 回測看得到 7/10 那一列，實盤在 7/11 00:30 前拿不到 —— 1 天的 look-ahead。驗證階段修正了三點：實際頻率是 33/1059 週（3.12%）落在 10 號、不限星期五；`compute_historical_fscore` 的季報日永遠不會踩到 10 號所以曝險為 0，真正受影響的只有 `value_historical_simulator.py` 的週度換股；而且**本次變更的方向是對的** —— HEAD 版誤判原始 date 語意，把可用日壓晚了整整一個月，這次是拉回法定截止日，殘留缺陷只有 1 天。

---

## 五、白話投資知識庫（P2 / P3）

功能本身安全性做得好（無帳密入碼、cookie 只寫 gitignored 的 `local/` 且 finally 即刪、`data/baihua/` 已排除、LLM 呼叫合規、檔案 I/O 全帶 utf-8）。以下是穩健性缺口：

- **登入牆與 DOM 改版靜默回報成功**（`fetch_baihua_fb.py:446`，已確認、降為 low）：`wall` 變數算出來後全檔只用在 log 字串，迴圈一旦開始就只剩固定回 0 的路徑；UI 顯示綠色「完成：新整理 0 篇」。驗證階段修正了兩點：cookie 被清掉的情境其實已被 `:410` 的 `_logged_in` 檢查接住（回 2 → 黃色警告），**真正無守門的是 FB DOM 改版導致選擇器落空**；另外子行程 stdout 只在 `rc != 0` 時才貼到畫面，所以 `[WALL?]` 這唯一的診斷資訊在成功路徑上被整包丟棄。

- **App 按鈕會在第 400 輪靜默截斷**（驗證階段新發現）：`baihua_kb_view.py:149` 只傳 `--max-stall 8`、沒傳 `--max-rounds`，吃預設 400；但實測需要 **473 輪**才捲到底（第 400 輪時只有 177/208 篇）。所以「首次全量用 App 按鈕抓」會少抓最舊的約 30 篇。

- **增量抓取破壞排序**（`fetch_baihua_fb.py:417`，已確認 medium）：新貼文被 dict 插入序排到尾端 → 拿最大 seq → 在 INDEX 與 UI 清單沉到最底（位置語意是「最舊」）。加重因子：實測 208 篇中 `date_label` 非空 0 筆、`date_iso` 非空僅 4 篇，位置是唯一時序線索，人眼無法自我校正。目前 `posts.jsonl` 是單次全量抓取，尚未跑過增量，所以現況順序仍正確。
  > **修法有陷阱**：不要照直覺把新貼文插到 JSONL 前端。`write_md`（build:205-209）開頭是 `for old in KB_DIR.glob(f"{seq:04d}_*.md"): old.unlink()`，而 `_needs` 會跳過已處理貼文使舊檔不重編號 —— 插前端會**刪掉別篇文章的檔案**。正解：在 fetch 落地時寫入單調遞增的 batch 欄位（每次 scrape +1），build/view 改以 `(batch desc, seq asc)` 排序。另外 `baihua_kb_view.py:126` 的 docstring 也錯寫成「依 date_iso 新→舊」，實際只依檔名排序，修註解時兩處都要改。

- **原文 SoT 靜默截短**（`fetch_baihua_fb.py:494`，已確認 low）：`_load_existing()` 對 JSON 解析失敗 `except Exception: pass`，不計數不記錄；接著 `_save` 以 tmp+replace 把**整個** `posts.jsonl` 重寫 —— 壞掉那行就此永久消失，且原檔已被覆寫、事後無從還原。`build_baihua_kb.py:335` 有同樣寫法。

- **cookie 暫存檔刪除失敗被吞掉**（`baihua_kb_view.py:195`，已確認 low）：`finally` 是 `try: tmp.unlink() except Exception: pass`，沒有 log 也沒有 UI 提示，但 docstring 與 UI caption 都對使用者保證「即用即刪、不留存」。`local/` 有被 gitignore 所以不會外洩到 repo，但這是一個**會靜默為假的安全承諾**。

- 其餘 low：RATE_LIMIT 偵測後不 graceful stop（失敗篇不進 STATE，下次會自動 resume，影響比初判小）；`--dry-run` 會把 stub 寫進 STATE 與 `_meta`，之後正式建庫全部靜默跳過；死碼與文件漂移（`SEE_MORE` / `PERMALINK_RE` / `done_ids` 未使用，`permalink` / `dateLabel` 結構性恆空使「FB 原文連結」永不觸發）。

---

## 六、文件與測試衛生（P3）

- `notes_view.py:4` docstring 寫「分頁結構（st.tabs）」，與同檔 96-97 行「禁用 st.tabs（2026-07-16 UI 測試實證 CRUD 按鈕會失效）」直接矛盾，實作用的是 radio。
- `tests/test_scanner_fail_loud.py:11` 的 `SUCCESS_MARKERS` 是手抄的 `REQUIRED_STAGES` 副本，驗的是 verifier 對自己清單的一致性；只有 2/14 個 marker 真的用 `batch.index()` 釘到 bat 文字。改成從 bat 解析 `call :log` 字串並直接引用 `verifier.REQUIRED_STAGES` 才能守住 marker 漂移。
- `run_scanner.bat:336` TW breadth 失敗只印 `[WARN]`，而 `verify_scan_stages.REQUIRED_STAGES` 沒有對應 marker → 持續失敗不會被任何後檢查發現。同一輪 diff 已替另外兩個 stage 加了 `(exit=0)` marker，唯獨 breadth 沒加。
- `docs/agent/data-sources.md:11` 的資料源優先序表未反映本次讓官方 TWSE/TPEX EOD 成為「最後一日」權威來源的改動，仍寫 `Disk cache | FinMind → yfinance`。表格開頭明文要求「All features MUST follow the same priority to avoid data drift」。
- `tools/refresh_backtest_panels.py:87` 的 `expected_stock_count` 用 CSV 檔案數（2064，含已下市永久保留的）而非在市檔數（1964），門檻算出 1652 但實際每日有量只有 1920~1951，餘裕只剩 15%，且分母會隨下市 CSV 累積單向變大 —— 是會自己走向硬失敗的設計。兩支工具對「全市場多大」的定義也不一致（一支用 regex、一支用 `.isdigit()`）。
- `tools/compute_historical_fscore.py:352` 舊的 `compute_revenue_score` 已無呼叫點但保留，且仍是 look-ahead 版本（次月 1 日 vs 新版的次月 10 日，提前 9 天）。應刪除或標 DEPRECATED。
- 測試缺口：缺「FinMind 新→舊排序輸入」的回歸測試（本 repo 踩過最大的坑，實作目前正確）；`mis.twse` 每請求 ≤50 檔的硬規則無測試背書。
- 移除不徹底的文字殘留 4 處：`reports/rvol_atr_factor_validation.md:14` 與 `:209`、`docs/research/technical_analysis_first_principles_2026-06-07.md:178`、`twse_api.py:1420`。
- `.gitignore:21` 註解仍把已刪除的 `tools/test_ledger_append.py` 列為「仍追蹤」的正式測試。
- `portfolio_view.py:238` 持股表改版時「名稱」欄消失，commit 說明未提及 —— 請確認是否刻意（`mis.twse` 批次報價本來就不回股名）。
- `portfolio_pricing.py:175` Yahoo 被擋（401/429）時三處 `continue` 都沒有 log，且 UI 標題仍寫「即時」。
- `portfolio_view.py:108` 年化 CAGR / 日勝率把「尚未建倉」的 0 報酬日一起算（8 天 1.95% 報酬被年化成 +83.7%）。與被刪的舊實作逐值等價，是忠實沿用而非本次引入 —— 但疊加 P0-2 之後，這個 tab 的頭條數字整體不可信。
- `tools/fetch_baihua_fb.py:377` 去重 key 只取正規化內文前 120 字，開頭相同的系列文可能被靜默合併。

---

## 七、已被駁回的疑慮（別重複調查）

**一、「美股前收價缺 `range=1d` 測試守衛」—— 駁回。** 見上方「更正二」。實打 Yahoo v8 三檔證明 `previousClose` 與 range 無關，fallback 順序本身就是守衛；被刪的舊測試釘的是已移除的函式。

**二、「白話投資抓取沒有 checkpoint，1800 秒 timeout 會丟失整輪進度，且孤兒 chromium 會鎖住 profile」—— 駁回。** 真實 `_scrape.log` 顯示全量抓取 474 輪只花 **876.9 秒**（每輪中位數 1.70 秒、最壞 3.57 秒），且 App 路徑被預設 400 輪封頂（約 720 秒），兩層上界都在 1800 秒內。孤兒 chromium 也不成立：Playwright driver 在 stdin 關閉時走 graceful exit（30 秒硬底線），Windows 上用 `taskkill /pid /T /F` 殺整棵瀏覽器樹，profile 佔用是 ProcessSingleton mutex、行程死即釋放。（但驗證過程順帶發現了真正的問題：400 輪截斷，見第五節。）

---

## 八、機械檢查結果

| 檢查 | 結果 |
|---|---|
| `pytest tests/` 全套 | **354 passed**（2.63 秒） |
| Python 編譯（27 個變更/新增檔） | 26 通過；`tools/dcf_ic_analyze.py` 失敗（既有毀損，見第九節） |
| BAT 純 ASCII + 純 CRLF | 全 15~16 支 bat 皆 0 個非 ASCII 行、0 個 lone-LF |
| `run_scanner.bat` goto/label | 8 label / 16 goto，零懸空，全部前向跳 |
| `run_scanner.bat` exit code 傳遞 | 兩個關鍵 stage 都是 `set X=%ERRORLEVEL%` 緊接 python 呼叫、只在 `PY_EXIT` 仍為 0 時提升、末尾 `exit /b %PY_EXIT%`；無吞 exit code |
| `verify_scan_stages` 與 bat 對齊 | 14/14 marker 都能對到 `call :log`；新增的兩個 `(exit=0)` marker 正確 |
| LLM 使用規範 | 全數合規：AI 報告 `claude-opus-4-8[1m]` + `--effort max` + `--allowedTools "*"` + 7200s；`build_baihua_kb.py` sonnet + xhigh + 600s。無 `timeout=None` |
| 新增 except 區塊 | 全部有 log 或往上冒泡，**無吞例外**（但既有碼有兩處 `except: pass`，見第五節） |
| 檔案寫入 encoding | 新增/修改的文字檔寫入全部顯式 `utf-8` |
| 公開 repo 洩漏檢查 | `data/baihua/`、`data/manual_trades/`、`local/` 全部確實被忽略且無已追蹤檔；**但 `reports/yt_analyst_*` 沒有**（見 P0-3） |
| 移除完整性 grep | `whale_picks` / `pit_universe` / `line4_flow` / `trend_dmi_sar` / `trade_ledger` 等 12 個關鍵字在活程式碼中零殘留 |
| 工作排程器 | 7 個 StockAnalyzer 工作，無一指向已刪除的 bat 或腳本 |
| `app.py` 版本號 | **不通過** —— `v2026.07.16.1` vs 今天 `2026.08.02`，pre-commit hook 會擋（見 P1-1） |

---

## 九、既有問題（非本次引入，值得另外排程）

- **`tools/dcf_ic_analyze.py` 無法編譯**：第 76 行 UTF-8 位元組毀損，把 `agg = long.groupby(...)` 吞進註解，造成 `SyntaxError: unmatched ')'`。`git show HEAD` 版本同樣壞掉。27 個變更/新增的 Python 檔中只有這一支編不過。
- **`tools/vfvc_backfill_monthly_rev.py:506`**：per-stock FinMind 路徑全數失敗時仍 exit 0，且 sync 呼叫未帶 `raise_on_error`。HEAD 版相同，且只有 `--bulk-update` 路徑接排程。
- **`get_finmind_cached` 額度失敗回空 DataFrame 而非過期快取**：錯誤訊息自稱「callers fall back to stale cache」，但實際流程是「磁碟快取過期 → 去抓 → 失敗 → 回空 frame」，磁碟上那份過期資料並沒有被使用。此行為在 HEAD 已存在。
- **`build_tw_breadth.py` 混入美股**（見第四節）與 **`rf1_cache_consistency_check.py:74-77` 對讀取失敗 `except Exception: continue`**（使被丟掉的股票連 drift 都報不出來，破洞自我隱藏）也都是既有問題。

---

## 十、建議動工順序

1. **`.gitignore` 加 `reports/yt_analyst_*`（P0-3）** —— 一行，但擋的是對真實個人的公開發佈。做完再考慮任何 `git add`。
2. **`portfolio_store.py:5` docstring（P0-4）** —— 一行，擋的是真實持股外洩；連同誤導性的 memory 引用一起拿掉。
3. **`normalize_financial_wide` 的 fillna 政策（P0-1）+ 同步修 `tests/test_historical_fscore_schema.py`（P1-4）+ 重跑 `quality_scores.parquet`** —— 這三件是一個包，分開做會被測試擋回來。這是唯一「已經在算錯數字且線上路徑正在讀」的問題。
4. **`build_nav_series` 的 TWR 現金流歸因（P0-2）** —— 用 `r = mv_d / (prev_mv + flow_d) - 1` 或 Modified Dietz，**不要照原始 finding 的公式**。修完投組 tab 的六個頭條數字才可信。
5. **Commit 前置**：更新 `app.py` 版本號（P1-1）、`git add AGENTS.md`（P1-2）、`git add` 三個新模組與 5 支新測試（P1-3）。
6. **時效項（8 天內）**：修 `test_merge_error_blocks_sync_and_aggregate` 的日期依賴（P1-6）。
7. **universe 排程**：把 `tdcc_universe_download.py` 掛回 `run_tdcc_weekly.bat`，只捨棄 `build_pit_universe` 那段。注意排程已註銷，revert bat 檔無效。驗證階段的修正：真正會造成資料實質錯誤（新上市股被剔除）的只有 `market_scan_view.py:79-80` 的週成交榜，其餘三處是股名顯示缺失。
8. **P1-5 / P1-7 / P1-8**：統一 merge 路徑；補聚合覆蓋率 guard；白話投資 STATE checkpoint。
9. **P2 的門檻調整**：4 天過期門檻改用交易日、盤中順序缺陷、`if fail: raise` 容忍度、官方 overlay 日期驗證、FinMind 封鎖退避。
10. P3 依餘裕處理。既有問題（`dcf_ic_analyze.py` 毀損、TW breadth 混入美股）可獨立排程。

---

## 十一、未評估與殘餘風險

- **未實跑的驗證**：Streamlit App 端對端（UI regression）、Facebook 實際抓取（DOM 選擇器對當前版面的命中率）、盤中時段實跑 `refresh_universe_prices.py`、`data/**.parquet` 的內容數值稽核。
- **未執行第三方 API 的完整驗證**：美股盤前/盤後分支只做到程式邏輯與合成 fixture 測試（不過 `previousClose` 的 range 無關性已由三檔實打證實）。
- **本次審查已無代理覆蓋缺口**：第一版報告中「快取與基本面」「排程鏈」「專案規則遵循」三組未完成的問題已補齊，且正是這三組貢獻了 P0-1、P0-3 與多數 P2 發現 —— 補跑是必要的。
- **審查期間 repo 有並行變更**：AI 文件重構（`AGENTS.md`、`docs/agent/`）在第一輪審查啟動後才落地，已納入第三輪的規則遵循組審查（P1-2 即由此而來）。
