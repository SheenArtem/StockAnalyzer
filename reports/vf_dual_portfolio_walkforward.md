# Dual 50/50 Portfolio Walk-Forward Validation

**Source**: tools/vf_dual_portfolio_walkforward.py

**Policy under test (post 2026-04-29 baseline)**:
- Rule 4 MIN_HOLD = 40 trading days
- Rule 3 TP = none (no auto trim)
- Rule 2 Rebalance = quarterly (13 weeks)
- Rule 7 Regime defer = 3 months
- Rule 5 Whipsaw ban = 30 trading days (固定)

## Walk-Forward Periods
- IS_2020_2022 (in-sample)
- OOS_2023 / OOS_2024 / OOS_2025 (out-of-sample)
- FULL_2020_2025 (combined)

## Baseline Performance (Dual 50/50)

| Period | CAGR % | Sharpe | MDD % | Hit % | Years |
|---|---|---|---|---|---|
| IS_2020_2022 | 4.12 | 0.424 | -13.34 | 40.6 | 2.98 |
| OOS_2023 | 11.65 | 2.038 | -2.03 | 35.3 | 0.98 |
| OOS_2024 | 4.70 | 0.695 | -2.84 | 42.3 | 1.00 |
| OOS_2025 | -6.65 | -0.902 | -8.61 | 31.4 | 0.98 |
| FULL_2020_2025 | 4.18 | 0.448 | -13.34 | 42.1 | 5.94 |

## Per-Rule Sensitivity (Dual side, OOS_2025)

| Run | CAGR % | Sharpe | MDD % | Hit % | ΔCAGR | ΔSharpe | ΔMDD |
|---|---|---|---|---|---|---|---|
| baseline | -6.65 | -0.902 | -8.61 | 31.4 | +0.00 | +0.000 | +0.00 |
| rule4_monthly_hold0 | 9.33 | 0.737 | -8.51 | 51.0 | +15.98 | +1.639 | +0.10 |
| rule4_monthly_hold20 | 9.33 | 0.737 | -8.51 | 51.0 | +15.98 | +1.639 | +0.10 |
| rule4_monthly_hold40 | 14.66 | 1.352 | -5.23 | 52.9 | +21.31 | +2.254 | +3.38 |
| rule4_monthly_hold60 | 5.52 | 0.517 | -5.08 | 52.9 | +12.17 | +1.419 | +3.53 |
| rule4_qtr_hold0 | -6.65 | -0.902 | -8.61 | 31.4 | +0.00 | +0.000 | +0.00 |
| rule4_qtr_hold20 | -6.65 | -0.902 | -8.61 | 31.4 | +0.00 | +0.000 | +0.00 |
| rule4_qtr_hold60 | -6.65 | -0.902 | -8.61 | 31.4 | +0.00 | +0.000 | +0.00 |
| rule3_tp_third | -5.13 | -0.714 | -7.05 | 29.4 | +1.52 | +0.188 | +1.56 |
| rule3_tp_half | -4.33 | -0.613 | -6.54 | 29.4 | +2.32 | +0.289 | +2.07 |
| rule2_monthly | 14.66 | 1.352 | -5.23 | 52.9 | +21.31 | +2.254 | +3.38 |
| rule2_biannual | -4.76 | -1.611 | -4.89 | 7.8 | +1.89 | -0.709 | +3.72 |
| rule7_immediate | -6.65 | -0.902 | -8.61 | 31.4 | +0.00 | +0.000 | +0.00 |
| rule7_defer1mo | -6.65 | -0.902 | -8.61 | 31.4 | +0.00 | +0.000 | +0.00 |
| rule7_defer6mo | -4.89 | -0.672 | -6.92 | 45.1 | +1.76 | +0.230 | +1.69 |
| PRE_POLICY_COMBO | 7.95 | 0.650 | -8.83 | 41.2 | +14.60 | +1.552 | -0.22 |

## Per-Rule Sensitivity (Dual side, FULL_2020_2025)

| Run | CAGR % | Sharpe | MDD % | Hit % | ΔCAGR | ΔSharpe | ΔMDD |
|---|---|---|---|---|---|---|---|
| baseline | 4.18 | 0.448 | -13.34 | 42.1 | +0.00 | +0.000 | +0.00 |
| rule4_monthly_hold0 | 10.51 | 0.743 | -18.18 | 49.5 | +6.33 | +0.295 | -4.84 |
| rule4_monthly_hold20 | 10.51 | 0.743 | -18.18 | 49.5 | +6.33 | +0.295 | -4.84 |
| rule4_monthly_hold40 | 7.90 | 0.614 | -19.19 | 51.8 | +3.72 | +0.166 | -5.85 |
| rule4_monthly_hold60 | 9.82 | 0.826 | -14.78 | 53.1 | +5.64 | +0.378 | -1.44 |
| rule4_qtr_hold0 | 4.18 | 0.448 | -13.34 | 42.1 | +0.00 | +0.000 | +0.00 |
| rule4_qtr_hold20 | 4.18 | 0.448 | -13.34 | 42.1 | +0.00 | +0.000 | +0.00 |
| rule4_qtr_hold60 | 4.18 | 0.448 | -13.34 | 42.1 | +0.00 | +0.000 | +0.00 |
| rule3_tp_third | 5.03 | 0.566 | -12.74 | 42.4 | +0.85 | +0.118 | +0.60 |
| rule3_tp_half | 5.47 | 0.625 | -12.41 | 43.0 | +1.29 | +0.177 | +0.93 |
| rule2_monthly | 7.90 | 0.614 | -19.19 | 51.8 | +3.72 | +0.166 | -5.85 |
| rule2_biannual | 3.02 | 0.398 | -6.78 | 32.0 | -1.16 | -0.050 | +6.56 |
| rule7_immediate | 4.18 | 0.448 | -13.34 | 42.1 | +0.00 | +0.000 | +0.00 |
| rule7_defer1mo | 4.18 | 0.448 | -13.34 | 42.1 | +0.00 | +0.000 | +0.00 |
| rule7_defer6mo | 4.35 | 0.413 | -13.34 | 45.0 | +0.17 | -0.035 | +0.00 |
| PRE_POLICY_COMBO | 12.63 | 0.991 | -11.78 | 43.7 | +8.45 | +0.543 | +1.56 |

## OOS Win Rate Per Rule

Sharpe 改善為 + 表示該 OOS 期 baseline (新政策) 勝過該 variant，反之 baseline 輸。

| Rule variant | OOS_2023 ΔSharpe | OOS_2024 ΔSharpe | OOS_2025 ΔSharpe | OOS Win |
|---|---|---|---|---|
| rule4_monthly_hold0 | +0.945 | -0.142 | -1.639 | 1/3 |
| rule4_monthly_hold20 | +0.945 | -0.142 | -1.639 | 1/3 |
| rule4_monthly_hold40 | +0.297 | -0.588 | -2.254 | 1/3 |
| rule4_monthly_hold60 | +0.320 | -0.438 | -1.419 | 1/3 |
| rule4_qtr_hold0 | -0.000 | -0.000 | -0.000 | 0/3 |
| rule4_qtr_hold20 | -0.000 | -0.000 | -0.000 | 0/3 |
| rule4_qtr_hold60 | -0.000 | -0.000 | -0.000 | 0/3 |
| rule3_tp_third | -0.000 | -0.120 | -0.188 | 0/3 |
| rule3_tp_half | -0.000 | -0.177 | -0.289 | 0/3 |
| rule2_monthly | +0.297 | -0.588 | -2.254 | 1/3 |
| rule2_biannual | +0.720 | +1.305 | +0.709 | 3/3 |
| rule7_immediate | -0.000 | -0.000 | -0.000 | 0/3 |
| rule7_defer1mo | -0.000 | -0.000 | -0.000 | 0/3 |
| rule7_defer6mo | -0.000 | -0.000 | -0.230 | 0/3 |
| PRE_POLICY_COMBO | +1.300 | -0.733 | -1.552 | 1/3 |

## Verdict per Implemented Policy

Grade rubric:
- A: baseline 在 ≥2 / 3 OOS 年勝過該 rule 對照 + FULL CAGR 與 Sharpe 同向勝出
- B: baseline 在 ≥2 OOS 年勝出 但 FULL 持平
- C: 1/3 OOS 勝 或 OOS 勝負參半
- D: 0/3 OOS 勝 (應 revert)

| Rule comparison | OOS Win (anchor) | FULL ΔCAGR | FULL ΔSharpe | Grade |
|---|---|---|---|---|
| Rule 4 MIN_HOLD=40 vs 0  (qtr rebal) | 0/3 | +0.00 | +0.000 | D |
| Rule 4 MIN_HOLD=40 vs 20 (qtr rebal) | 0/3 | +0.00 | +0.000 | D |
| Rule 4 MIN_HOLD=40 vs 60 (qtr rebal) | 0/3 | +0.00 | +0.000 | D |
| Rule 4 MIN_HOLD=40 vs 0  (monthly) | 3/3 | -2.61 | -0.129 | B |
| Rule 4 MIN_HOLD=40 vs 20 (monthly) | 3/3 | -2.61 | -0.129 | B |
| Rule 4 MIN_HOLD=40 vs 60 (monthly) | 3/3 | -1.92 | -0.212 | B |
| Rule 3 No-TP vs tp_third | 0/3 | -0.85 | -0.118 | D |
| Rule 3 No-TP vs tp_half | 0/3 | -1.29 | -0.177 | D |
| Rule 2 Quarterly vs Monthly | 1/3 | -3.72 | -0.166 | C |
| Rule 2 Quarterly vs Biannual | 3/3 | +1.16 | +0.050 | A |
| Rule 7 Defer3mo vs Immediate | 0/3 | +0.00 | +0.000 | D |
| Rule 7 Defer3mo vs Defer1mo | 0/3 | +0.00 | +0.000 | D |
| Rule 7 Defer3mo vs Defer6mo | 0/3 | -0.17 | +0.035 | D |
| Combo: post-policy vs PRE_POLICY | 1/3 | -8.45 | -0.543 | C |

## ⚠️ RAISE ALERT — Portfolio vs Proxy 不一致

Step B/C proxy (`vf_dual_contract_step_bc.py`) 用 dropout fwd_N return 平均做政策推論，
但 position-aware portfolio walk-forward **三條政策結論方向相反**：

| 政策 | Proxy 結論 | Portfolio 結論 | 方向 |
|---|---|---|---|
| **Rule 2 Quarterly** | quarterly +42% CAGR > monthly +28% (24 samples) | monthly FULL CAGR 7.90% Sharpe 0.614 > quarterly 4.18%/0.448 | ⚠️ **反向** |
| **Rule 3 No TP** | no-TP fwd_60d +19.55% > tp_third +16.37% (TP 殺 alpha) | tp_half FULL 5.47%/0.625 > no-TP 4.18%/0.448 (+1.29pp) | ⚠️ **反向** |
| **Rule 4 MIN_HOLD=40** | dropout fwd_40d +6.48% > fwd_20d +4.26% | quarterly 下 hold0/20/40/60 完全相同 (rebal 13週=65td 已 cover); monthly 下 hold40 vs hold20 OOS 全勝但 FULL CAGR 輸 2.61pp | ⚠️ **redundant + 部分反向** |
| **Rule 7 Defer 3mo** | A=0.90 / B(1mo)=2.16 / C(3mo)=5.16 (32 transitions) | quarterly rebal 下 immediate/1mo/3mo 完全相同 (defer 在 13週內被吃掉); 只 6mo 才出現微差 +0.17 CAGR | 🟡 **無作用** |
| **PRE_POLICY combo** | 4 條都「改善」應 stack 出 best | 反而 **最佳** FULL CAGR 12.63% Sharpe 0.991 vs post-policy baseline 4.18%/0.448 | 🚨 **完全推翻** |

### 為何 proxy vs portfolio 結論不同

1. **Proxy 是「點估計」**：把 dropout 股的 fwd_N return 平均，假設不換倉就持有就會多賺。
   忽略整個 portfolio 同時面對的機會成本（被夾死在差股不能換進新好股）。

2. **Quarterly rebal 在 trending market 慢半拍**：2024-2025 年 OOS 顯示 quarterly
   錯過 Q3 rotation，monthly 一個月一次跟得上熱門板塊。

3. **TP 機制是 tail-risk hedge**：proxy 看 mean return 認為 TP 砍掉 alpha，但
   portfolio level 在 IS_2020_2022（含疫情崩盤）+ OOS_2025（弱市）TP 鎖利反
   而提供 +0.93~1.93% drawdown 緩衝（FULL MDD: no-TP -13.34% / tp_half -12.41%）。

4. **MIN_HOLD 與 rebal_freq 高度耦合**：quarterly rebal=65 trading days，已自動
   滿足 MIN_HOLD=40 條件；獨立 MIN_HOLD 規則只在 monthly rebal 下有作用。

### 建議行動

1. ⚠️ **重新評估 Rule 2 quarterly default**：portfolio simulator 顯示 monthly
   FULL CAGR +3.72pp / Sharpe +0.166 勝 quarterly。proxy 結論可能 over-confident。
2. ⚠️ **重新評估 Rule 3 no-TP**：tp_half 在 portfolio level 全面勝出（FULL CAGR +1.29pp
   Sharpe +0.177）。可能應保留至少 1/2 TP。
3. 🟡 **Rule 4 / Rule 7 在 quarterly rebal 下 redundant**：可考慮整併到 Rule 2 描述
   裡（quarterly rebal 已 implicit 達成 MIN_HOLD=40 / Defer=1quarter）。
4. 🚨 **PRE_POLICY combo (monthly+TP1/3+hold20+defer1mo) 在 portfolio level 全面勝出**：
   FULL CAGR 12.63% Sharpe 0.991 vs new policy 4.18% / 0.448。應考慮 revert。
5. 🟡 **Caveat**：simulator 不含交易成本。Monthly rebal 換手率 3× quarterly，扣 ~0.3%
   round-trip × 13 額外 rebal = ~4% drag。實際差距會縮小但不致翻盤（PRE_POLICY 仍 +4%）。

## Caveats

- 不模擬 hard stop / partial exit 真實價（TP 用 fwd_20d_max proxy 偏理想）
- 無交易成本（台股 ~0.3% round-trip × 13 rebal = ~4% drag）
- Universe 限於 trade_journal_value snapshot 857 檔（Stage 1 後）+ QM trade journal
- Rule 7 sample 在 2020-2025 内 regime transitions ~32-40 次，仍小樣本
- Whipsaw ban 30 日已驗 (Step B3) 固定不變
- Walk-forward IS/OOS 只切時間，沒做參數重估 (因 baseline 是 fixed policy)
