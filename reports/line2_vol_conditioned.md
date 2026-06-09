# 格2 — 量條件化反轉 vs 續勢 驗證報告

> Verdict: **D (reject)**。2026-06-08 full-run。
> backtest agent 完成 compute + 寫出 5 個 CSV,但被切斷未寫本報告;本檔由主控依 full-run CSV 鎖定。
> Panel: `data_cache/backtest/ohlcv_tw.parquet` (clean) / all ~4,400 檔、liq_50m ~2,600 檔 / 2008-2026。

## 假設
LMSW (2002) C2:同樣的前段價格移動,**高量=資訊=續勢、低量=流動性=反轉**。測 prior_ret(5d/20d) × {RVOL, turnover} 交互。

## 決定性證據

### 1. 雙重排序 — 沒有交互結構(殺手)
`reports/line2_volcond_double_sort.csv`,prior_window=5:

| | all | liq_50m |
|---|---|---|
| 高 prior:高量−低量 | +0.63% | -0.23% |
| 低 prior:高量−低量 | +0.62% | +0.24% |
| **交互 LS 對角** | **+0.00016 ≈ 0** | **-0.0047 (負)** |

量對「贏家」與「輸家」**等量加分** → 這只是 **RVOL 的加法 tilt**,不是 prior×量 的條件化。LMSW 的「高量續勢 / 低量反轉」在台股**不成立**。

### 2. 交互分數 IC 全負
`reports/line2_volcond_gauntlet.csv`:interact_5d / volconf_mom_5d / interact_20d / interact_turn_* 在 all / liq_50m / liq_100m / ex_bottom20pct **全部** ic_mean、ic_ir 為負 (ic_ir -0.13 ~ -0.17,t 達 -11)。direction=top → **做多高交互分數是虧的**。

### 3. 輸給(本就 marginal 的)純 RVOL
`net_vs_rvol` 欄多數為負。對照 rvol_only:all ic_ir ~0.03-0.07、**liq_50m ic_ir ~0.01(≈0)**,且成本後 net_spread 多數轉負 — 與既有 RVOL=MARGINAL 結論一致。交互版不僅沒新增,反而稀釋。

### 4. Walk-forward 不穩
interact_5d 年度 ic_ir 2008-2024 幾乎全負(2022 達 -0.59),僅 2025-26 翻正 — 非穩定可交易訊號。

## 對照 prior-art
完全吻合 `reports/prior_art_three_cells.md` §2:Avramov-Chordia-Goyal (2006)「not possible to profit」;LMSW 交互被 RVOL/動量 subsume;符號不穩。

## 唯一未測的 pivot(follow-up)
prior-art 指出台股若有 informed-flow 續勢,只在**大型股 × 外資/投信流量**(Hsieh-Hu 2010),非本次的全市場 RVOL/turnover 通用交互。**此變體未測**(需 join 籌碼流量),為唯一存活假設,記入 Phase D follow-up。

## 結論
通用「量條件化動量交互」= **D**。volume conditioning 在台股不提供可利用結構,只是稀釋本就 marginal 的 RVOL。不上線。
