# Phase C 驗證稽核 — 三格價量訊號 D 判定的偽陰性審查

> 稽核員：IC Validator (量化因子驗證)
> 日期：2026-06-08
> 任務：三格 backtest 全判 D。確認這些 D **不是 bug / over-cleaning 造成的偽陰性**（誤殺真 edge），而非再證偽。
> 方法：① 逐項套 `validation_audit_checklist.md` ② 獨立重寫複現「判生死」數字（不 import 原函式）③ 偽陰性決定性檢驗。

---

## TL;DR 裁決表

| 格 | 訊號 | 原 verdict | 稽核裁決 | 偽陰性風險 |
|---|---|---|---|---|
| **格1** | `sig_lambda_diverge` / level / signed | D / reject | **D confirmed robust** | 無。正交化後負 IC 更強，spread 正是 tail 假象 |
| **格2** | `interact_*` / `volconf_mom_*` | D / reject | **D confirmed robust** | 無。對角 LS≈0/負 bit 級複現，無交互結構 |
| **格3 Part A** | turnover×volatile | informational_only | **D confirmed robust** | 無。IC 歸零複現，spread 是 3 melt-up 年假象 |
| **格3 Part B** | aggregate-illiq regime gate | informational_only | **D confirmed robust**（措辭需微調） | 無。SOP-12 FAIL 獨立否決；gate 是 coincident risk-off 非 leading |

**核心結論：三格的 D 全部站得住，沒有任何一格的 D 是 bug / over-cleaning 造成、底下藏真訊號。** 三支腳本 look-ahead / V=0 剔除 / 成本 / 多重檢定全部乾淨。唯一需修正的是格3 Part B 報告一句**措辭**（「純 cash-drag」應為「coincident risk-off 的 MDD 縮減 + SOP-12 FAIL」）、以及一個**研究紀律披露**（格1 報告數字來自 SAMPLE 300，非 full；格2 cross-regime 段讀了污染 jsonl，但都不影響 verdict）。

---

## 1. 通用區稽核（G1-G13，三支共用）

| 項目 | 檢查 | 結果 |
|---|---|---|
| **G1 fwd shift 方向** | 三支 grep `shift(-` | ✅ 全為 `pct_change(h, fill_method=None).shift(-h)`，標準未來報酬構造；`fill_method=None` 防 ffill 跨缺口造假。特徵端無 `shift(-n)`。 |
| **G5 Close<=0 剔除** | loader 是否先剔 | ✅ 三支都剔。實測 panel `Close<=0 = 0 列`（毒列已被前期 commit 清淨），fwd return 有 `replace(inf)` + `(−0.95, 5.0)` clipping。 |
| **G6 V=0 凍結列在量分母前剔** | 三支核心地雷 | ✅ **全部正確**。實測 V<=0 = 112,473 列（全 ==0 無負值），其中 **99.0% 為 High==Low 凍結列、Close 100% 合法** → 確認是停牌參考價填充列，**剔除是對的、非誤殺正常列**。格1/格3 在 loader 剔（算 λ/turnover 前）；格2 `_drop_frozen_volume()` 在 `compute_factors()` **之前**呼叫（main line 340-341）。剔除筆數均揭露。 |
| **G8 多重檢定 N_trials** | DSR 的 N 是否涵蓋全變體 | ✅ 格1 N=72（3 score×3 H×4 tier×2 dir）；格2 N=9（4 focus+2 turn+3 baseline）。無低估。格1/格2 DSR 全 FAIL（最嚴），N 偏估也不改結論。 |
| **G9 成本 2 腿** | long-short 收 2×cost | ✅ 三支 `net = gross − 2×cost`，COST=[0.0025, 0.0035]；格3 gating sim 對 state 切換收 round-trip。 |
| **G10 IC-spread 一致性** | 核心教訓 | ✅ 工具有 `ic_spread_sign_ok` 欄主動標反號。格1 三訊號全 `sign_ok=False`（reverse-artifact）→ 正確判 D。 |
| **G4 Survivor 披露** | 量化方向 | ✅ 三份報告都有 survivor caveat（panel ~2064 檔現存、~46% PIT 缺價 backlog、下市股排除→偏樂觀）。 |

---

## 2. 獨立複現對照表（不 import 原函式，自寫最小版，FULL universe）

> 原報告：格1 = SAMPLE 300；格2/格3 = FULL。我一律用 FULL（2054 檔，剔 V=0 後 5,306,886 列）複現，更嚴格。

| 格 | 判生死指標 | 原 backtest | **我獨立複現 (FULL)** | 一致？ |
|---|---|---|---|---|
| **格1** | `diverge` IC @liq_50m h=20 | −0.0272 (sample) | **−0.0179** (t=−9.19, n=4964) | ✅ 同號顯著負 |
| **格1** | `diverge` decile D10−D1 | +0.703% | **+0.628%** | ✅ 同號正（但 tail 假象，見下） |
| **格1** | `diverge` mono rho | +0.745 | **+0.867** | ✅ |
| **格1** | `level` rank-IC（winsor-invariant） | +0.0327 | **+0.0293** (t=+13.7) | ✅ |
| **格2** | 交互 LS 對角 (all) | +0.000157 | **+0.000157** | ✅ **bit 級吻合** |
| **格2** | 交互 LS 對角 (liq_50m) | −0.004703 | **−0.004703** | ✅ **bit 級吻合** |
| **格3A** | turnover×volatile IC | +0.0064 (IR +0.033) | **+0.0003** (IR +0.014, t=0.48) | ✅ 同為噪音 (t<2) |
| **格3B** | SOP-12 composite vs best | 0.423 << 1.119 FAIL | **0.423 << 1.119 FAIL** | ✅ |
| **格3B** | SOP-13 peak xcorr lag | −1d coincident | **−1d coincident** | ✅ |
| **格3B** | gate_illiq Sharpe / cash | 1.119 / 29.6% | **1.119 / 29.6%** | ✅ |

**所有判生死數字方向與量級一致。** 格2 對角 bit 級吻合（確認無隱性差異）；格1/格3A 我用 FULL 複現量級略小於 sample（合理，更多薄量股稀釋），但**符號與顯著性結論完全相同**。

---

## 3. 逐格偽陰性決定性檢驗（重點：D 底下有沒有藏真訊號？）

### 格1 — `sig_lambda_diverge`：D confirmed robust，**無偽陰性**

最可疑處：IC 負（−0.018）但 decile D10−D1 正（+0.628%），mono +0.867。表面像「rank-IC 低估了一個正 spread alpha」。

**決定性檢驗 — 增量 IC（正交化短期反轉 prior20 + RVOL）**（prior_art §1/§4 指令）：

| | IC | t |
|---|---|---|
| raw | −0.0178 | −9.18 |
| **正交化 prior20+RVOL 後** | **−0.0285** | **−21.25** |

→ 正交化後負 IC **不但沒消失，反而更強更顯著**。徹底排除「diverge 底下藏正 alpha」。

**那 +0.628% 正 spread 是什麼？** 看 decile 數列（FULL，×100）：
`D1=0.99 D2=0.94 D3=1.04 D4=1.02 D5=1.09 D6=1.11 D7=1.08 D8=1.07 D9=1.23 D10=1.62`
→ D1-D9 平坦（0.94~1.23），**只有 D10 單格跳到 1.62**。spread 完全由 D10 右尾撐起，**整體 rank 關係是負的**。這是 RVOL/ATR% 教訓的同型：**rank-IC（中位數驅動）與 equal-weight 籃子 mean（右尾驅動）反向，籃子 spread 是 non-tradeable 的 tail 假象**。`ic_spread_sign_ok=False` 正確捕捉。

**over-cleaning 排除**：level 訊號用 spearman rank-IC，winsorize 1/99 是單調變換**不改 rank**，故 over-cleaning 數學上不可能是 level 的死因。diverge 的 z-score 雖受 winsor 影響，但正交化檢驗已直接證負 IC 是訊號本質非 clipping artifact。

**成本真實性補充**：FULL gross +0.628%，扣 2 腿 0.5% → net +0.13%；扣 0.7% → **net −0.07%（翻負）**。報告 sample 版 net@.35% = +0.003%（≈0）。無論哪版，正 spread 在 0.35% 成本下蒸發。

**off-by-one / slope 構造檢查**（L1-3）：price_slope 與 lambda_slope 都用 `_rolling_slope(window=20)` 後望窗，z-score by date 截面（非全期）。我自寫的 slope 完全複現 IC，**無 off-by-one 把正訊號翻負**。負 IC 是真的。

### 格2 — 交互動量：D confirmed robust，**無偽陰性**

對角 LS bit 級複現（all +0.000157 / liq_50m −0.004703）。**無交互結構**是真的，非 binning artifact。

額外確認（liq_50m 對角四格）：`hi_prior_hi_vol=0.0153 < hi_prior_lo_vol=0.0177` —— 可交易池裡高動能**低量**反而比高量賺，與 LMSW「高量續勢」**完全相反**，與 prior_art §2「台股續勢只在大型股+法人流」一致。量對贏家/輸家**等量加分** = 純 RVOL 加法 tilt，非條件化。

gauntlet 佐證：所有 interact/volconf 在 liq_50m 的 `net_vs_rvol` ≈ 0 或負（interact_5d +0.001%、interact_20d −0.0034%），**交互版不贏純 RVOL**（L2-1 hard gate FAIL）。純動量 mom_only 反而 net 最高（+0.0027~+0.0062%），但那是動量本身非「量條件化」貢獻。

### 格3 Part A — turnover×volatile：D confirmed robust，**無偽陰性**

IC 複現歸零（+0.0003，t=0.48）。舊 +0.71 是 survivor 假象。

**spread +32.5% ann 的 outlier 集中**（CSV `partA_ls_portfolio` 逐年）：
- net_monthly Sharpe pooled 0.95，但 **2018/2022（volatile-DOWN 熊年）翻負**（−0.91 / −0.13）。
- 2020/2023/2026 三 melt-up 年 net_monthly ann_ls = 141%/180%/212%，**剔掉後其餘 9 年平均掉到 ~12%**（~14x 集中）。
- net_daily Sharpe **全 14 年皆負**（每日全換成本殺死）。

→ 'volatile' regime 把 melt-up/melt-down 混為一談，turnover×volatile 實為**偽裝的 high-beta/動量 tilt**，牛市波動賺、熊市波動賠。非可交易 alpha。

### 格3 Part B — aggregate-illiq regime gate：D confirmed robust，但報告**措辭需微調**

三個 SOP gate 數字全複現吻合。**唯一看似有 edge 處：gate_illiq Sharpe 1.119 > BH 1.000**（MDD −60.8%→−36.3%，cash 29.6%）。

**決定性檢驗 — illiq gate vs 隨機同比例 cash gate（200 次）**：

| | Sharpe | MDD |
|---|---|---|
| BH momentum | 1.000 | −60.8% |
| illiq gate | 1.119 | −36.3% |
| **隨機 30% cash gate (200x)** | mean **−0.372**, 95pct −0.18, max −0.01 | — |

→ illiq gate 打敗 **100%** 的隨機 gate。**這是反向發現，需謹慎解讀：illiq gate 的出場時點並非隨機等價，它確實把現金擺在危機期。**

**但這不構成偽陰性、不推翻 D**，三個獨立理由：
1. **SOP-13 已證 illiq 是 coincident（lag −1d）非 leading**。gate 打敗隨機是因 illiq 與 drawdown **同期相關**（市場已在跌、流動性已乾才出場 = 跟跌 risk-off），不是**預測**。隨機對照只能排除「純運氣」，無法把 coincident 升級成 predictive。
2. **SOP-12 FAIL 是更硬的獨立否決**：composite 0.423 << best-single 1.119。專案 SOP 規定 composite 必須 > best-single 否則 reject，與 cash-drag 無關。
3. **Sharpe 提升全來自分母（波動/MDD 降），CAGR 反而較低**（22.7% < 24.3%）。對 100% Whale production 而言，「降報酬靠同期風險指標減回撤」屬 risk-off informational tier。

→ 最終 verdict `informational_only / reject as gate` **精準正確**。

**🚩 措辭精度（非 bug，需修正報告一句）**：line3 報告寫「Sharpe 微升純 cash-drag」。更精確應為「**Sharpe 微升來自 coincident risk-off 的 MDD 縮減（非隨機等價，但 SOP-13 證非 leading），且 SOP-12 FAIL 獨立否決**」。「純 cash-drag」字面會讓人以為隨機 cash 也能達標（我的檢驗證明不能），削弱了「為何仍 reject」的真正理由（coincident + SOP-12）。建議改字，但**verdict 不變**。

---

## 4. 發現的 🚩（全部為紀律/披露層，無一推翻 verdict）

| # | 🚩 | 嚴重度 | 影響 verdict？ |
|---|---|---|---|
| 1 | **格1 報告數字來自 SAMPLE 300，報告頭已標 `SAMPLE 300` 但 TL;DR 未強調** | 低 | ❌ 不影響。我 FULL 複現符號/顯著性全一致；最終數字建議補跑 full（量級會略變但結論不變）。 |
| 2 | **格2 cross-regime 段（robust D）讀了污染 jsonl**（line 466 `read_json(REGIME_LOG)`）。稽核清單 L3-3/G6 明令「不信任 jsonl」（2026-04-28+ ret_20d 110-180% 污染） | 低 | ❌ 不影響主 verdict。主 verdict 來自 double-sort + IC（用 clean panel），cross-regime 只是 robust 補充。但格2 與格3 處理不一致（格3 自算 regime，格2 讀 jsonl），建議格2 也改自算以求一致。 |
| 3 | **格3 Part B「純 cash-drag」措辭不精確**（見上 §3） | 低 | ❌ verdict 正確，僅措辭。 |
| 4 | 格3 Part B SOP-14 = `eligible`（50 episodes / 20 strict-fire），但報告仍判 informational | — | ✅ **正確的保守**。SOP-12/13 FAIL 時，SOP-14 eligible 不足以救活，報告判 informational 是對的。 |

**沒有發現任何 over-cleaning（winsorize/流動性過濾砍 tail）造成的偽陰性。** 反而格1 winsorize 對 rank-IC 數學上無影響、正交化後負 IC 更強，徹底排除此風險。

---

## 5. 結論

**三格 D 全部 confirmed robust，無偽陰性。** 偽陽方向（誤判有 edge）與偽陰方向（誤殺真 edge）兩者皆已排除：

- **格1**：負 rank-IC 是訊號本質（正交化後更負），+0.628% spread 是 D10 單格右尾的 non-tradeable 假象，成本 0.35% 即蒸發。與 RVOL/ATR% 教訓同型，亦吻合 prior_art VPIN「控制波動後消失」。
- **格2**：交互 LS 對角 bit 級複現 ≈0/負，無條件化結構，可交易池高動能低量反勝（與 LMSW 相反、與 prior_art 台股結論一致）。
- **格3A**：turnover×volatile IC 複現歸零，正 spread 是 3 個 melt-up 年 + survivor 假象，熊市翻負。
- **格3B**：gate 雖打敗隨機（非純運氣），但 SOP-13 證其為 coincident risk-off 非 leading，且 SOP-12 FAIL 獨立否決，最終 informational_only 正確。

三支腳本資料清洗（Close<=0 + V=0 凍結列）、look-ahead 防護（shift 方向 + fill_method=None + by-date 截面 z）、成本 2 腿、多重檢定 N_trials 全部乾淨。可放心將三格歸 D / informational-only 級，**不存在被誤殺的真 edge**。
