# 因子驗證 Bug 稽核清單 — line1/line2/line3 三支驗證腳本

> **用途**：逐項勾選稽核 `tools/line1_lambda_validate.py`（格1 量價彈性 λ）/ `tools/line2_vol_conditioned_validate.py`（格2 量條件化動量）/ `tools/line3_liquidity_regime.py`（格3 流動性 regime gate）。
> **來源**：SOP 1-14（`memory/project_validation_bias_warning.md`）+ RVOL/ATR% 驗屍（`reports/rvol_atr_factor_validation.md`）+ 4-blocker audit（Sharpe 1.70→1.01）+ eps_yoy sign-flip + V=0 凍結列 + whale_picks spec。
> **正面範本**：`tools/rvol_atr_validate.py`（三支都 import / mirror 它的 harness）、`tools/indicator_ic_analysis.py`。
> **稽核方法**：每項 = ❓檢查問句 + 📍在哪看（code 段 / 輸出 CSV 欄）+ 🚩紅旗門檻（命中即 FAIL / 降 informational / 退回重跑）。
> **驗證執行紀律**：每項都要「對著 code grep + 對著輸出數字看」，不接受「設計上應該對」的口頭保證（4-blocker 教訓：v13 backtest 跑 13 輪 iter 沒人發現 look-ahead，因為數字「看起來合理」）。

---

## 使用方式

1. 三支跑完後，先過「通用區」13 項（每支都要過）。
2. 再過該支的「格N 專屬」區。
3. 任一 🚩 命中 → 在報告 verdict block 明確標 FAIL / MARGINAL / informational-only，**不可**靜默放行。
4. 報告 TL;DR 必須有一張 verdict 表（仿 rvol_atr 報告），逐 horizon × liquidity tier 給 PASS/MARGINAL/FAIL + 一句話理由。

---

# 通用區（三支全部適用）

## G1. Look-ahead — fwd return shift 方向

❓ `fwd_Nd` 是否真的「未來 N 日報酬」、有沒有 `.shift(-h)`？特徵全部用 `t` 或更早資料算，**沒有**用到 `t+1..t+h` 的任何值？
📍 看 `add_fwd_returns()`：rvol_atr 範本是 `groupby[...].pct_change(h, fill_method=None).shift(-h)`。對照每支自己的 `add_fwd_returns` / fwd 計算。逐一檢查 score 欄（lambda_smooth / 交互分數 / turnover / aggregate λ）的 rolling/EWMA window 是否只看後望（pandas `.rolling()`/`.ewm()` 預設後望，正確；但 `.shift(-n)` 出現在特徵端就是 leak）。
🚩 特徵端出現 `shift(-n)`（n>0）/ score 與同期 fwd 用同一根 bar 算 / `pct_change(h)` 沒接 `.shift(-h)`（變成「過去 N 日」當未來）/ fill_method 預設 ffill 跨缺口製造假報酬（範本用 `fill_method=None`）。

## G2. Look-ahead — rebalance / 持有期時點對齊

❓ decile 分組與 fwd return 是否同日對齊（t 日 score → t 日進場 → t+h 出場）？有沒有「用 t+h 才知道的分組邊界」回頭套到 t？
📍 看 `compute_decile_returns()`（範本：`groupby('date')` 內 `qcut(rank)`，分組只用當日截面，正確）。看 walk-forward 切片是否 train/test 之間留 embargo（fwd_60d 需 ≥60d embargo，否則 train 尾與 test 頭重疊洩漏）。
🚩 qcut 邊界跨日計算（全期統一 binning 而非逐日截面）/ walk-forward train 末期與 OOS 首期間隔 < max(horizon)（embargo 缺失）。

## G3. Look-ahead — lead-time anchor 不可 forward-looking（SOP-11，格3 重點）

❓ 任何「領先 N 天」宣稱，anchor 是否為 forward-looking 構造（如 `peak = forward window 極大值`）？只認 strict-preceding（fire 在 event 啟動**之前**）的 case？
📍 格3 Part B：看 regime/gate event 的 `event_start_date` 怎麼定義。SOP-11 hard rule：`strict_lead_d = event_start_date - first_fire_date`，且要求 `strict_lead_d ≥ 5 AND forward_5d_ret(first_fire) < -1%`。檢查有沒有 `lead_d`（event_window_avg）混充 strict lead。
🚩 lead time 用 forward window 極值當 anchor（crash predictor R3 的 smoking gun：6/8 fire 卡 ~58-60d = window 寬度本身被當 lead，strict-preceding 真實只 17%）/ 宣稱「預警/領先」但 xcorr lag < 3d（見 G11）。

## G4. Survivor bias — universe 含下市股 / PIT 完整性

❓ panel 是否 survivor-only？缺價 backlog 比例多少？報告**有沒有量化披露**方向性影響？
📍 三支都讀 `data_cache/backtest/ohlcv_tw.parquet`（已知 survivor-only，~2064 檔；對照 `universe_tw_pit.parquet` 3621 檔 → 1660 檔（46%）PIT 已知 ticker 完全沒價格）。看報告有沒有「最後一根 bar 落在 panel 末日的檔數佔比」「0 檔在末日前 90 天以上下市」這類量化證據（rvol_atr 範本作法）。格1/格3 docstring 已聲明 survivor caveat — 驗證它在報告 verdict 旁**真的出現**且**標明方向**。
🚩 報告沒有 survivor 段 / 有段但沒量化（只寫「可能有偏差」）/ 沒講清楚偏差對該因子是利多還是利空方向（rvol_atr 範本：ATR% 是「致命方向更致命」、RVOL 是「薄 edge 更薄」）/ 對 survivor-favorable 因子在 survivor panel 達 PASS 卻沒掛「PIT 回補重驗為上 production 硬前置」trigger。

## G5. 資料污染 — Close<=0 剔除

❓ 計算 fwd return / 任何以 Close 為分母的量之前，有沒有剔 `Close<=0`？
📍 看 loader（範本 `load_panel`：`n_bad=(df["Close"]<=0).sum(); df=df[df["Close"]>0]`）。格2 直接 import 範本 `load_panel`（繼承此清洗）；格1/格3 自帶 loader，**逐一確認**有同等剔除。檢查 fwd return 殘餘 inf 有沒有 `.replace([inf,-inf], nan)` + `|20d ret| ∈ (−95%, +500%)` 護欄。
🚩 loader 沒剔 Close<=0（清洗前 fwd_20d max 曾達 +621,818%）/ 沒 inf 護欄 / 沒 ret clipping → 單一 reprice 列拉爆 decile mean（ATR% D1 +28%/60d 假象的根因）。

## G6. 資料污染 — V=0 凍結列在「量分母」前剔除（三支全中的核心地雷）

❓ **任何以 Volume / 成交額為分母**的量（格1 λ=|ret|/(Close·Vol)、格2 RVOL=Vol/rolling_mean、格3 turnover=Vol/shares + Amihud），計算前有沒有先剔 `Volume<=0`？
📍 panel 有 ~112,473 列 Volume<=0（其中 110,749 為停牌參考價填充凍結列，價格合法但 V=0）。
- 格1：docstring 聲明「計算 lambda 前必先剔除 Volume<=0」→ 看 loader/因子建構是否真的剔，且**揭露筆數**。
- 格2：看 `_drop_frozen_volume()`（已定義，剔 `Volume<=0` + log 筆數）有沒有在算 RVOL **之前**被呼叫。
- 格3：docstring 聲明「計算前一律剔除 Volume<=0，揭露筆數」+ 「regime 自算時聚合前剔 Volume<=0」→ 兩處都要驗。
🚩 任一以量為分母的計算在剔除前執行（V=0 → 除零 inf / RVOL log clip 到 -4.6 假極低量 / 恢復交易日 rolling_mean 被 0 拉低 → RVOL 爆高）/ 剔了但沒揭露筆數 / 只在某一條 path 剔、另一條漏（格3 兩處：因子分母 + regime 聚合）。
⚠️ 注意凍結列**不等於** Close<=0 毒列（V=0 凍結列價格是合法的）——兩個 filter 都要有，不能用一個代替。

## G7. 資料污染 — 單日尖刺 / 單位錯置

❓ 有沒有殘留「單日暴漲次日反向回落」的單位錯置列（元/分混用、100x 位移）？fwd return 護欄擋得住嗎？
📍 panel 層已有 spike-and-revert 濾網（fdeb991：單日 >5x 且次日反向 >5x），但驗證腳本仍應靠 `|h-day ret|` clipping 兜底。看 fwd return 上界（範本 `< 5.0`=+500%）。格1 lambda_raw 跨股極右偏 → 看有沒有 cross-section winsorize 1%/99%（docstring 聲明有）。
🚩 fwd return 無上界 clipping / lambda 沒 winsorize 就 rank（極端值主導 rank）/ 看到某 decile mean 量級異常（如 >+20%/20d）但沒進 forensic 追根因（rvol_atr 範本對 ATR% D1 做了 forensic CSV）。

## G8. 多重檢定 — N trials + deflated Sharpe haircut

❓ 總共測了幾個變體（signal × horizon × liquidity tier × direction × 子訊號）？deflated Sharpe（Bailey/López de Prado）算了沒、N_trials 報了沒？
📍 格1 gauntlet 第 8 項明列 `deflated Sharpe (N=測過變體數)` → 看 `line1_lambda_deflated_sharpe.csv` 的 N 是否涵蓋**所有**跑過的變體（含兩方向、3 horizon、4 liquidity tier、A/B/C 三子訊號），不是只數「最後勝出那條」。格2 `line2_volcond_robust.csv`、格3 同理。
🚩 deflated Sharpe 的 N 低估（只數報告裡列出的，沒數丟棄的變體）/ 完全沒算 deflated Sharpe（格1/格2 docstring 都聲明要算）/ 用 t-stat 顯著就宣稱 PASS 但沒做多重比較校正（mean-reversion 教訓：t=+9.68 仍是 D 級多重比較陷阱）。

## G9. 成本真實性 — round-trip × 2 腿 × 換手頻率

❓ 成本是否用 0.25% / 0.35% round-trip？long-short 有沒有算 **2 腿**？換手頻率對不對（h=20 ≈ 每 20 交易日換一次）？淨 spread 是否仍正？
📍 看淨 spread 計算（範本 `net = gross - 2 * cost`，COST_ROUNDTRIP_TIERS=[0.0025,0.0035]）。格1 `line1_lambda_net_spread.csv` / 格2 `line2_volcond_gauntlet.csv` 的 net 欄 / 格3 Part B `COST_ROUNDTRIP=0.0025` 在 gating sim 內每次 rebal 收費。
🚩 long-short 只收 1 腿成本（少算一半）/ 換手頻率與 horizon 不符（h=60 卻按 h=20 收費）/ gross 正但 net 負卻仍給 PASS（RVOL 教訓：liq_50m gross +0.44% 成本後 −0.06%→只能 MARGINAL）/ 格3 gating sim 沒對「進出 state 的換手」收費（cash↔invested 切換是真實成本）。

## G10. IC-decile 一致性 — spread sign == IC sign + 單調 ≥ +0.5

❓ decile spread 的 sign 是否與 IC sign 一致？decile 單調性 Spearman rho ≥ +0.5？
📍 看 `decile_spread()`（範本：dir=top→D10−D1，dir=bot→D1−D10）+ `monotonic_score()`（decile index vs mean_ret 的 Spearman）。格1 gauntlet 第 2/3 項、格2 `line2_volcond_gauntlet.csv` mono 欄、格3 Part A decile。
🚩 **IC 與 spread 反號**（ATR% 教訓：rank-IC −0.044 看似低波動異象，但 equal-weight 籃子 LS spread 16/18 年為負——rank-IC 由中位數決定、籃子 mean 由右尾決定，兩者可反向）→ 直接降 D / 倒 U 或 U 形（mono rho 負，如 combo_rvol_lowatr −0.79）→ 「組合 IC > 單因子」是 non-tradeable rank 假象 → reject。
⚠️ 核心教訓：**rank-IC 是 non-tradeable 統計量，IC 顯著 ≠ equal-weight 籃子賺錢**（SOP「IC 全綠不能上線」）。可交易結論看 spread/portfolio，不看 IC。

## G11. 單年/離群依賴 — LOYO + 抽 2020 COVID

❓ Leave-one-year-out 後 edge 是否塌？特別抽掉 2020 COVID 後 edge 是否歸零？walk-forward 是否 ≥5 OOS 年且夠多年同號？
📍 格1 `line1_lambda_loyo.csv`（LOYO + ex-2020）+ `line1_lambda_walkforward_annual.csv`。格2 `line2_volcond_robust.csv`（LOYO/ex-2020）+ `line2_volcond_walkforward.csv`。格3 Part B `line3_liqregime_partB_loyo.csv`。
🚩 edge 高度依賴單一極端年（COVID 2020 / 2008 / 2022）抽掉後歸零（mean-reversion 教訓：去掉 2020 後 +1.4% → +0.2%）/ walk-forward < 5 OOS 年就宣稱穩（4 windows 不夠）/ 多頭年偏差未標（2021-2025 樣本 4/5 多頭，結論最多 A-，需跨 bull-bear cycle）/ 2026 部分年（~80 交易日）極端值混入穩定窗統計。

## G12. Regime mixing / 小樣本（SOP-13/14，格3 重點）

❓ regime episode 有幾個？strict fire 幾次？cross-regime 是否只有單一 cell 正（conditioning 假設破功）？cash_pct 是否 >30%（偽裝 MDD 改善）？
📍 格1 `line1_lambda_regime.csv`（cross-regime by vol percentile）。格3 Part B `line3_liqregime_partB_episodes.csv`（episode/strict-fire count）+ gating sim 的 cash_pct 欄。
🚩 N_events < 30 或 strict_fire ≤ 5 卻沒標 `informational_only`（SOP-14 hard rule：自動 gate，禁 banner/rebalance/position_size）/ cross-regime 多個 cell 都正→真正 conditioning 變數可能搞錯（mean-reversion 教訓：以為 bear 其實是 vol_high；bull+vol_high 也正 = 假設破功）/ cash_pct > 30% 沒標 `low_exposure_artifact`（cash drag 偽裝成 MDD 改善）/ regime 跨 5 個不同 crisis（dotcom/2008/2015/2020/2022）疊加當「統計顯著」。

## G13. portfolio vs proxy — gating 類訊號必跑真 daily-allocation sim（SOP-10/12，格3 強制）

❓ gating / regime / 開關類訊號是否跑了**真 portfolio daily-allocation sim**（不是 sample mean proxy）？有沒有 B&H + best-single + composite 三欄齊全？composite 是否贏 best-single？
📍 格3 Part B `line3_liqregime_partB_gating.csv`：docstring 聲明「B&H + best-single + composite 三欄 CAGR/Sharpe/MDD」。SOP-10：AUC/IC/lead-time 全綠**不能**上線，必跑 portfolio sim。SOP-12：composite portfolio Sharpe 必須 > 任一 single-factor Sharpe。
🚩 用 fwd_N sample mean 當 portfolio path return（Dual cf1e2e0 教訓：proxy 認定 4 條改善 3 條方向相反）/ 三欄不齊（缺 B&H baseline 或 best-single）/ composite 輸 best-single 卻仍 promote（crash predictor 教訓：composite Sharpe 0.81 輸 rv10-only 0.98）/ portfolio P&L < B&H 但因 IC 全綠就放行 → SOP-10 強制砍。
⚠️ 這是專案踩過 **2 次** 的坑（2026-04-29 Dual + 2026-05-08 crash predictor），對格3 是 hard gate，沒這欄的 verdict 標 `INVALID — missing portfolio gate`。

---

# 格1 專屬 — line1_lambda_validate.py（量價彈性 / Amihud-Kyle λ 背離）

## L1-1. λ 除零 — Volume<=0 必須在算 λ 之前剔

❓ `lambda_raw = |ret| / (Close·Volume)` 的分母 `Close·Volume`，Volume=0 時是否已被剔（否則 inf）？
📍 看 loader / λ 因子建構順序：剔 Volume<=0 必須在 `lambda_raw` 計算**之前**。docstring 明列此為「本檔額外處理，rvol_atr 沒處理因其指標不以 V 為分母」。
🚩 先算 λ 再剔列（inf 已污染 EWMA/rank）/ 用 Close<=0 filter 代替 Volume<=0 filter（兩者不同：凍結列 Close 合法但 V=0）。

## L1-2. λ 右偏 — winsorize 在 rank 之前

❓ `lambda_raw` 跨股極右偏，有沒有 cross-section winsorize 1%/99% **再** rank？
📍 看 WINSOR_LO/HI=0.01/0.99 的套用點 + `sig_lambda_level = -rank(lambda_smooth)`。
🚩 沒 winsorize 直接 rank（少數巨值主導排序）/ winsorize 在時間序列方向而非截面方向（應 by date 截面）。

## L1-3. 背離訊號雙腿時點 — price_slope 與 lambda_slope 同窗對齊、皆後望

❓ `sig_lambda_diverge = z(price_slope_20d) − z(lambda_slope_20d)`，兩個 slope 是否都用 `t-20..t` 後望窗（SLOPE_WINDOW=20）、沒用未來？z-score 是 cross-section by date 還是時序？
📍 看 price_slope / lambda_slope 的 rolling 方向 + z() 的 groupby 維度。
🚩 slope 用 forward window / 兩腿窗口不一致（一個 20d 一個別的）/ z-score 用全期 mean-std（含未來資訊）而非逐日截面。

## L1-4. 帶符號背離（訊號 C）— 只在 B「有脈搏」才做，避免無效變體灌大 N

❓ 訊號 C（CLV/sign-weighted λ）是否**僅當訊號 B 顯示有 edge 時才測**？若 C 也測了，有沒有計入 deflated Sharpe 的 N_trials？
📍 docstring：「訊號 C（帶符號背離，僅 B 有脈搏才做）」。看 main flow 有沒有條件 gate；若無條件全跑，N_trials 必須 +1 含 C。
🚩 C 無條件跑卻沒計入 N（低估多重比較）/ C 跑出顯著但 B 不顯著卻挑 C 報（cherry-pick 變體）。

## L1-5. 背離方向語意自洽 — 「價漲+λ走低=吸籌(+)」的 sign 是否與實證 spread 同向

❓ 背離訊號的預設語意方向（吸籌=+），實證 decile spread / IC 是否真的同向？反向時有沒有當 reverse-artifact 處理（不是偷偷翻 sign 救活）？
📍 看 FOCUS_SCORES 的方向標註 vs `line1_lambda_ic_matrix.csv` / decile spread 實際 sign。gauntlet 第 2 項：sign 必與 IC 同號，否則 reverse-artifact → D。
🚩 預設方向與實證反向卻事後翻 sign 宣稱 alpha（事後 sign-flip = 過擬合）/ level 訊號兩方向都測但只報賺錢那向沒計 N。

## L1-6 (格1 最可能藏 leak 處). EWMA span 與 fwd horizon 的隱性重疊

❓ `lambda_smooth = EWMA(span=20)` 與 h=10 fwd 是否有資訊重疊？EWMA 後望正確，但 score 在 t 日的值是否只含 ≤t 的 λ？
📍 看 `.ewm(span=20).mean()` 是否 `adjust` 預設後望、有無 `.shift()` 錯置。
🚩 EWMA 用了 center=True（雙向，洩漏未來）/ smooth 後沒對齊到 t（off-by-one 把 t+1 的 λ 算進 t 的 score）。

---

# 格2 專屬 — line2_vol_conditioned_validate.py（量條件化動量）

## L2-1 (格2 最關鍵 gate). 交互版必須「顯著贏」純 RVOL baseline，不是只贏一點

❓ 量條件化交互（prior_ret × RVOL）的成本後淨 spread，是否**顯著**贏過純 RVOL baseline（已驗 MARGINAL，liq_50m 成本後 ~0）？「顯著」有沒有量化（差距 / t-stat / deflated）？
📍 docstring：「交互版淨 spread 必須顯著贏純 RVOL 才上線」。看 `line2_volcond_gauntlet.csv` 有沒有 `vs RVOL baseline` 對照欄（docstring 聲明有）。對照 `reports/rvol_atr_net_spread.csv` 的 RVOL liq_50m net（−0.06~−0.26%）。
🚩 沒有 vs-RVOL 對照欄 / 交互版只贏純 RVOL 一點點（差距在噪音內）卻宣稱 PASS / 在 `all` universe 贏但 `liq_50m`（可交易池）不贏（RVOL 教訓：可交易池才算數）/ 贏的是 in-sample 而 walk-forward 不贏。

## L2-2. 5x5 雙重排序 — 每 cell 樣本數足夠，邊角 cell 不是噪音

❓ 5x5（prior_ret × RVOL）double-sort 每個 cell 每日樣本數是否 ≥ 合理門檻？極端 cell（高 prior×高 RVOL）會不會樣本太少 = 噪音？
📍 看 `line2_volcond_double_sort.csv` 各 cell 的 n / N_SORT=5 的 qcut 是否 by date 截面。MIN_CROSS_SECTION=50 在 5x5 下每 cell 僅 ~2 檔/日（50/25），可能過稀。
🚩 5x5 在 liq_50m 過濾後某 cell 每日 < 5 檔卻當結論 / 邊角 cell 報酬極端但 n 極小（單一 cell outlier，mean-reversion 教訓）/ qcut 用全期 binning 而非逐日。

## L2-3. prior_ret 與 fwd_ret 不可重疊（動量訊號的經典 leak）

❓ `prior_ret`（5d/20d 前段報酬，PRIOR_WINDOWS）是否用 `t-W..t` 過去窗、`fwd_Nd` 是否用 `t..t+N` 未來窗、**兩窗不重疊**？
📍 看 prior_ret 計算（應 `pct_change(W)` 不 shift，代表「截至 t 的過去 W 日」）vs fwd（`.shift(-N)`）。檢查 prior 窗末端 = t，fwd 窗始端 = t，無 overlap。
🚩 prior_ret 含到 t+1（用了未來）/ prior 與 fwd 窗重疊（如 prior_20d 實際算到 t+something）/ prior_ret 沒剔 Volume<=0 凍結列導致跨停牌期失真。

## L2-4. RVOL 沿用既有定義 — 沒重新發明、log clip 一致

❓ RVOL 是否真的 import 自 `_compute_one_ticker` 的 `sig_rvol_log = log(clip(Vol/Vol.rolling(20).mean(), 0.01))`，沒重寫成不同公式？
📍 docstring：「RVOL 計算沿用 tools/indicator_ic_analysis._compute_one_ticker」。grep import 確認；RVOL_WINDOW=20 與既有一致。
🚩 自己重寫 RVOL（與 baseline 不同公式 → vs-RVOL 對照失效）/ rolling 窗含凍結列把均量拉低。

## L2-5. 交互分數建構 — 不是事後挑「最賺的 cell」當訊號

❓ 「交互分數」（連續 score）是 pre-registered 的函數（如 sign(prior)×RVOL_rank），還是事後從 5x5 表挑出最賺的對角線拼出來的？
📍 看交互分數定義 vs 5x5 表的關係。Lee & Swaminathan 假設是 pre-registered（帶量續勢 / 量縮反轉），方向應預先 sign。
🚩 交互分數 = 從 double-sort 結果反推最佳組合（in-sample fitting，whale_picks v4 教訓：IS composite Sharpe 1.98 → WF 0.49）/ 方向事後決定。

---

# 格3 專屬 — line3_liquidity_regime.py（流動性 regime gate）

## L3-1 (格3 Part A 最關鍵). turnover×volatile IR +0.71 是否為舊盤 survivor 假象

❓ 把 `vf_turnover_summary.md` 的 turnover×volatile（IR +0.71 / D10-D1 +128%）丟回 clean 全市場 ohlcv_tw panel 重跑後，數字是否還在？舊盤是 survivor-biased 的 `trade_journal_*`（QM 已選股池）。
📍 看 `line3_liqregime_partA_ic.csv`（含 volatile IR vs 舊 +0.71 對照）+ `line3_liqregime_partA_decile.csv`。舊 +128% spread 來自「已篩過的 QM 池 + survivor」雙重放大。
🚩 clean panel 重跑後 IR 大幅縮水卻仍引用舊 +0.71（必須 mutation 舊結論，如 RVOL 推翻 SW-1 Sharpe 9.50 的作法）/ volatile cell 仍強但其他 regime 也強（conditioning 假設破功，G12）/ 沒對照舊盤 vs clean panel 的差異量級。

## L3-2 (格3 Part B 最關鍵). regime gate 必過完整 SOP-10~14（hard gate）

❓ 「市場 aggregate Amihud λ 當 state gate 動量」是否跑完整 portfolio gating（G13）+ episode count（G12）+ xcorr lag（G3/G11）？這是 gating 類訊號，**不是** cross-sectional 因子。
📍 docstring 明列 SOP-10（portfolio sim 三欄）/ SOP-12（composite > single）/ SOP-13（xcorr lag + cash_pct）/ SOP-14（episode/strict fire <30 或 ≤5 → informational）/ LOYO+COVID+WF。逐 CSV 驗：`partB_gating` / `partB_xcorr` / `partB_episodes` / `partB_loyo`。
🚩 缺任一 SOP gate（尤其 portfolio sim 三欄）/ regime episode < 30 卻給 banner-tier verdict / xcorr lag < 3d 卻宣稱「流動性領先」（coincident 不是預測）/ cash_pct > 30% 沒標 artifact。

## L3-3. regime 自算 — 不信任污染的 jsonl、聚合前剔 V=0

❓ regime 是否用 clean panel 自算（不讀污染的 `regime_log.jsonl`）？自算時聚合（top300 equal-weight）**之前**是否剔 Volume<=0？自算規則是否與 `market_regime_logger` 一致（可重現）？
📍 docstring：「不信任 jsonl，用 clean panel 自算 regime（同 market_regime_logger 規則，top300 equal-weight，但聚合前剔 Volume<=0）」。`regime_log.jsonl` 2026-04-28+ 受凍結/尖刺價污染（ret_20d 110-180% 不可能值）。看有沒有誤讀 jsonl 的 code path。
🚩 讀了 jsonl（吃進 110-180% 污染報酬）/ 自算 regime 聚合前沒剔 V=0（凍結列污染 top300 日報酬）/ 自算規則與 logger 不一致（不可重現，無法對照）。

## L3-4. turnover 分母 — 流通股數 PIT 對齊 + 時間範圍限制

❓ `turnover = 20d 均量 / 流通股數`，流通股數（financials_balance OrdinaryShare/10）是否 PIT 對齊（+45d 公告延遲，TW_FIN_LAG_DAYS=45）？turnover 結論是否限定 2015-03+（股數資料起點）？
📍 docstring：「流通股數只回溯到 2015-03 → turnover 受限 2015+；Amihud 純 OHLCV 可回溯 2006+」。看 balance merge 有沒有 `merge_asof` + 45d lag（4-blocker 教訓：merge 用 quarter_end 會 3/31 就用 5/15 才公開的報）。
🚩 流通股數沒加 +45d publication delay（fundamental look-ahead leak）/ turnover 結論宣稱涵蓋 2006-2026 但股數只有 2015+（前段 turnover 全 NaN 或用錯股數）/ Amihud 與 turnover 兩因子時間範圍混用未分開報。

## L3-5. Part A vs Part B 因子方向不可混 — turnover 高賺 vs Amihud 高賺各自實證

❓ Part A 兩因子（turnover「高周轉賺」dir=top / Amihud「高 illiq 賺」流動性溢酬 dir=top）方向是否各自由 decile spread 實證決定，不是預設？兩者相關性查了沒（會不會其實是同一因子）？
📍 docstring：「方向以 decile spread 實證為準，這裡先標預期方向」。turnover vs Amihud 應查 Spearman rho（vf_turnover 範本查過 turnover vs RVOL rho 確認不重疊）。
🚩 方向預設死沒看實證 spread / turnover 與 Amihud rho 高（>0.7）卻當兩個獨立因子報（重複計算）/ Amihud illiquidity（高=illiquid）與 turnover（高=liquid）方向上應反相關，若實證同向需解釋。

---

# 報告交付物自檢（三支共用，仿 rvol_atr 報告結構）

❓ 報告是否具備以下，缺一即退回：
- [ ] TL;DR verdict 表：逐因子 × (horizon × liquidity tier) 給 PASS/MARGINAL/FAIL + 一句話理由 + tier（production / informational / D 級歸檔）。
- [ ] 「判準從嚴」表：① 流動性過濾後單調存活 ② walk-forward ≥70% 年同號 ③ 成本後 spread 仍正 ④ rank-IC 與 spread 同號（可交易）——四者全過才 PASS。
- [ ] 紅旗 forensic 段：對任何「異常漂亮」的 cell（如某 decile mean 量級突出）追根因到資料層（rvol_atr 對 ATR% D1 +28% 追到 Close<=0 + 流動性殭屍）。
- [ ] 資料品質註記：明列剔除筆數（Close<=0 / Volume<=0 凍結列）、無 AdjClose 的除息 gap 方向性偏差、2026 部分年警語、winsorize/clipping 參數。
- [ ] Survivor 披露段：量化（佔比 + 方向 + 是否掛 PIT 回補 trigger）。
- [ ] 連帶推翻的舊結論：若數字推翻既有 memory（如格3 Part A 推翻 turnover×volatile +0.71），明確標「不可再引用」並指向 mutation 位置（不是新增「之前說 X 現在 Y」）。
- [ ] 輸出檔案清單表：每個 CSV 的內容一句話。
🚩 報告只有結論沒有 verdict 表 / 漂亮數字沒 forensic 追根因 / 推翻舊結論卻沒同步 memory。
