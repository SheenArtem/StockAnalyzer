# 三格價量訊號驗證 — 最終定論 (2026-06-08)

> 入口文件。完整管線:prior-art 先驗 → 三條線 SOP-14 backtest → Phase C 偽陰性稽核。
> 起因:用戶問「能不能自己發現一個有用的價量技術分析訊號?」

## TL;DR

**三格全部 D + 第四格(大型股法人流續勢)SUBSUMED。Phase C 確認無 bug 偽陰性。台股自撿價量/籌碼 alpha 四度落空 —— Whale Picks 已吃掉唯一的 edge,外面沒有容易撿的。**

| 格 | 訊號 | Verdict | 判生死 |
|---|---|---|---|
| 1 | λ (Amihud/Kyle) 量價彈性背離 | **D** | 背離方向不自洽 + 成本後歸零 + deflated Sharpe FAIL;level = 非單調 illiquidity premium 假象 |
| 2 | 量條件化反轉/續勢 (LMSW) | **D** | 雙重排序交互 LS ≈ 0,量對贏家/輸家等量加分 = 純 RVOL tilt 無交互結構 |
| 3 | 流動性 (turnover/Amihud) regime gate | **D / informational** | turnover×volatile +0.71 是 survivor+outlier 假象;illiq gate SOP-12/13 FAIL (coincident 非 leading) |

## 管線與檔案

1. **Phase B 先驗** `prior_art_three_cells.md` — 三格文獻校準,**預登記**全部失效模式 + kill-test。三發全中。
2. **Phase A backtest** `line1_lambda_divergence.md` / `line2_vol_conditioned.md` / `line3_liquidity_regime.md` + `line{1,2,3}_*.csv` — full-run gauntlet。
3. **稽核武器** `validation_audit_checklist.md`。
4. **Phase C 偽陰性稽核** `phase_c_audit.md` — 獨立重寫複現 (FULL universe),三格 D confirmed robust,4 個 🚩 全屬披露層不推翻 verdict。

## 方法論結論 (可複用)

**「盡量燒 token」花在 rigor 而非 variant**:先做 prior-art 預登記失效模式 + kill-test,再看數字 → 在相信任何漂亮 backtest 前就知道往哪開刀。三格的漂亮數字 (格1 level IC +0.32 / 格3 +0.71/+128%) 全是預言中的假象。列入新訊號驗證 SOP。

## 殘留 / 後續

- **格4 大型股×法人流續勢 (已測 SUBSUMED)**:Hsieh-Hu 大型股續勢 clean panel 不複現 (IC +0.003 t=1.3);對 Whale 正交化後殘差 IC **-0.011 (t=-4.04)**,法人腿與 Whale foreign_pct/total_pct 相關 +0.48/+0.53 = 已在 Whale 籌碼欄。**不開第四條線。** 見 `line4_flow_continuation_overlap.md`。
- **披露層 🚩 (不影響 verdict)**:格1 已補跑 full 定稿 (2054 檔, 9 artifacts, line1_lambda_divergence.md 全 full-run, 🚩 解除, verdict 不變);格2 cross-regime 補充段誤讀污染 jsonl (主 double-sort verdict 不受影響,建議改自算 regime);格3B「純 cash-drag」措辭改為「coincident risk-off 的 MDD 縮減 + SOP-12 FAIL」。
