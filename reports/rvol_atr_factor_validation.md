# RVOL / ATR% 因子正式驗證報告

> 對 `docs/research/technical_analysis_first_principles_2026-06-07.md` 附錄兩條「未驗證延伸」給出正式 verdict。
> 跑於 2026-06-07，全量 2,054 檔台股日線（2006-01-02 ~ 2026-06-05，5,419,390 列，已清洗）。
> 驗證腳本：`tools/rvol_atr_validate.py`（**沿用** `tools/indicator_ic_analysis.py::_compute_one_ticker` /
> `indicator_combo_analysis.py::{rank_normalize_signals, build_combos}` 的既有訊號定義，未重新發明）。

---

## TL;DR — Verdict Block

| 因子 | Verdict | Tier | 一句話理由 |
|---|---|---|---|
| **log(RVOL)** | **MARGINAL** | **informational / ranking-tilt only**（**不**進 Whale composite 為 standalone LS book） | 唯一 rank-IC 與 decile spread **同號**、單調、流動性過濾後存活、walk-forward 18/18 年正的訊號；但 edge 太薄（20d LS spread 僅 +0.4~0.7%），成本後 LS 淨值在可交易池歸零/轉負。只能當「已持有組合內的排序加分」，不能當獨立多空策略。 |
| **ATR% 反向（低波動）** | **FAIL** | **D 級歸檔（untradeable artifact + 方向相反）** | 兩重死因：(1) 原 h=60 D1 斷崖 / bot-10 +28% 是 **stale-price reprice artifact**，清洗 Close≤0 + 流動性過濾後完全消失；(2) 即使在乾淨資料，equal-weight 低波動籃子的 LS spread **16/18 年為負**——高波動股**平均報酬更高**，低波動異象在本panel/horizon方向相反。 |
| `combo_rvol_lowatr` | **FAIL（作為組合無增量價值）** | informational only | −ATR% leg 拖累；LS spread 6/18 年正、成本後深度負；組合「tradeable」表現**比 RVOL 單因子更差**，未通過「組合 IC > max(單因子)」的可交易性檢驗。 |

判準（從嚴，四者全過才 PASS）達成情況：

| 因子 | ① 流動性過濾後單調存活 | ② walk-forward ≥70% 年正 | ③ 成本後 spread 仍正 | ④ rank-IC 與 spread 同號（可交易） | 結論 |
|---|---|---|---|---|---|
| log(RVOL) | ✅（mono +0.99，ex_bottom20pct +0.52%） | ✅（all 18/18；liq_50m 13/18≈72%）| ❌（liq_50m 淨 −0.06~−0.26%）| ✅ | **3/4 → MARGINAL** |
| ATR% 反向 | ❌（spread 為負且方向相反）| ❌（LS spread 4/18 年正）| ❌（淨 −1.5~−1.8%）| ❌（IC 負但 spread 也負）| **0/4 → FAIL** |

---

## 紅旗 Forensic（mandate 指定優先項）

### 紅旗 1：ATR% 最低波動 D1 斷崖 + 不可能的 +28%/60d — 確認為 **stale-price artifact**

**Mandate 觀察（既有 h=60，`reports/quantile_decile_returns.csv`）**：
- ATR% D1 mean **+5.87%/60d**（win 81.4%）vs D2 +2.88% — 斷崖式落差
- top-N bot（最低 ATR% 10 檔）mean **+28.07%/60d**（win 67.2%，sharpe_proxy 6.9）

**Forensic 結論：確認為 artifact，三條獨立證據**

**(a) 資料層面找到根因 — 12,823 列 Close ≤ 0。** 當前 panel 有 12,589+ 列收盤價 ≤ 0（penny / 停牌 / 還原失敗）。
這些列經 `pct_change` 產生 inf 或極端報酬：清洗前全 panel `fwd_20d` 最大值 **+621,818%**（max 6219x）、
8,505 列 inf、222 列 >+500%。近零價股票的 ATR%（ATR/Close，分母趨 0 但分子也小）量測出**假性低波動**→
落進 ATR% D1，而它們一次 reprice 就貢獻天文數字報酬，把 D1 *mean* 拉爆。h=60 視窗更長→更容易吃到 reprice→
斷崖在 h=60 最嚴重。

**(b) 清洗 + 縮 horizon 後斷崖消失。** 本報告剔除 Close≤0、`pct_change(fill_method=None)` 不跨缺口、
並把 |20d 報酬| 限制在 (−95%, +500%) 視為 artifact 剔除後，乾淨 h=20 的 ATR% decile：

```
ATR% (raw rank, 低值=低波動) | h=20 | universe=all  —— 平滑、無斷崖
  D1:+0.67  D2:+0.75  D3:+0.92  D4:+1.05  D5:+1.10  D6:+1.09  D7:+1.31  D8:+1.45  D9:+1.75  D10:+1.79  (%/20d)
```

D1→D2 從 +0.67%→+0.75%（原 h=60 是 +5.87%→+2.88%）。**斷崖不存在。** 乾淨 bot-20 低波動籃子報酬從
**+28%/60d 崩到 +0.81%/20d**（all）。

**(c) D1 是流動性殭屍。** ATR% D1 成分股 20 日均成交額**中位數 = 0.023 億 NTD（230 萬）**，
當日成交額百分位中位數 0.26（位於市場最低 1/4）。加 `liq_50m`（≥5000 萬）過濾後 D1 成交額中位數跳到 1.86 億——
**過濾直接換掉了一批股票**。

**極端報酬其實在高波動端，不在低波動端**（推翻「低波動股不可能有高報酬」的錯誤直覺其實反過來）：

```
ATR% forensic | universe=all | >30%/20d 報酬佔比 + p99
  D1(低波動)  >30%frac 0.58%  p99 +21.7%   median +0.00%   amt_med 0.023億
  D10(高波動) >30%frac 6.74%  p99 +68.7%   median -1.11%   amt_med 0.891億
```
**右尾（暴漲）集中在高波動 D10（6.74% 的股票 20d 漲 >30%），低波動 D1 幾乎沒有右尾（0.58%）。**

> **裁決：mandate 的「加流動性過濾看 D1 異常是否消失」→ 消失了 → verdict 直接 FAIL（untradeable artifact）。** 與 mandate 預設一致。

---

### 紅旗 2：RVOL 在 h=60 方向反轉 — 確認「短 horizon 有效、不單調延伸到長 horizon」

**Mandate 觀察**：h=60 top-N bot（低量）贏 top（高量），與 10-20d 正向 IC 矛盾，疑 U 型。

**Forensic 結論：屬實但無害。** RVOL 的資訊半衰期短——在 IC 最強的 **h=10/20** 它**單調且方向正確**：

```
log(RVOL) | h=20 | universe=all   mono_rho = +0.988（近完美單調遞增）
  D1:+0.85  D2:+1.00  D3:+1.06 ... D8:+1.36  D9:+1.42  D10:+1.59  (%/20d)
高量 Top-20 +1.63%/20d  vs  低量 Bot-20 +0.83%/20d  →  高量勝（與 10-20d IC 一致）
```

h=60 的反轉是「量爆量後 1 季均值回歸」的已知現象，**不影響 h=10/20 的 production 用途**（RVOL 本來就是短 horizon 訊號）。
本驗證全部聚焦在 h=10/20，h=60 反轉僅作背景說明。

---

## 必跑項目結果

### 1. Decile + Top-N（h=10 / h=20，IC 最強 horizon；既有只有 h=60）

完整 CSV：`reports/rvol_atr_decile_returns.csv`、`reports/rvol_atr_topn_portfolio.csv`

**Decile（h=20，跨流動性檔位）—— D1 / D10 / 單調性**

| 因子（方向） | 流動性 | D1 %/20d | D10 %/20d | D10−D1 | mono_rho | D1 win% |
|---|---|---|---|---|---|---|
| **log(RVOL)**（多頭=高分） | all | +0.85 | +1.59 | **+0.74** | **+0.99** | 59.3 |
| | liq_50m | +0.99 | +1.43 | +0.44 | +0.99 | 57.5 |
| | ex_bottom20pct | +0.91 | +1.44 | +0.52 | **+1.00** | 59.1 |
| **ATR%**（多頭=低分/低波動，LS=D1−D10） | all | +0.67 | +1.79 | **−1.12** | +0.99 | 63.8 |
| | liq_50m | +0.84 | +1.83 | −0.99 | +0.99 | 63.0 |
| | ex_bottom20pct | +0.63 | +1.71 | −1.07 | +0.99 | 61.5 |
| **combo_rvol_lowatr**（多頭=高分） | all | +1.39 | +0.94 | **−0.45** | −0.79 | 59.7 |
| | liq_50m | +1.29 | +0.84 | −0.45 | −0.81 | 57.3 |

注意 mono_rho 對 RVOL 與 ATR% 都是 +0.99（單調遞增），**但 ATR% 的單調方向是「高波動→高報酬」**，
對「低波動異象」假說是**反向**——所以低波動 LS spread（D1−D10）為負。RVOL 是「高量→高報酬」，方向正確。

**Top-N（long-only，h=20）—— RVOL 的真實用法（排序加分，不付多空兩腿成本）**

| 流動性 | Top10 | Top20 | Top50 | win%(Top20) | sharpe_proxy(Top20) |
|---|---|---|---|---|---|
| all | +1.675 | +1.633 | +1.531 | 60.2 | +3.60 |
| liq_50m | +1.452 | +1.396 | +1.413 | 59.9 | +2.93 |
| ex_bottom20pct | +1.462 | +1.407 | +1.351 | 59.3 | +3.03 |

> 警語：上述 mean 含 2008-2025 台股整體正漂移（同期所有 decile 皆正）。真正的 edge 是**相對**：
> RVOL 高量 Top-20 +1.63% vs 低量 Bot-20 +0.83% = **+0.80%/20d 相對差**，這才是 alpha，量級偏薄。
> sharpe_proxy 是 h-day 報酬 ÷ 跨日 std × √252 的**量化比較指標**，非實盤可實現 Sharpe（重疊持有期未調整），勿直接當操盤 Sharpe 解讀。

### 2. 流動性過濾敏感度（全 universe vs 過濾後 ≥2 檔位）

四檔位：`all` / `liq_50m`（≥5000 萬 NTD）/ `liq_100m`（≥1 億）/ `ex_bottom20pct`（剔最低 20% 成交額）。

- **ATR% D1 異常**：`all` 下 D1 是 230 萬成交額殭屍 → 過濾後成交額中位數跳 80x，但 LS spread **始終為負**（−0.99 ~ −1.12%）。異常（原 h=60 斷崖）來自 reprice，已被清洗消除；剩下的負 spread 是真實的「高波動溢酬」結構，與低波動假說相反。
- **RVOL**：過濾後 spread 從 +0.74%（all）降到 +0.44%（liq_50m）/+0.52%（ex_bottom20pct）——**衰減但存活、仍單調正**。RVOL 不是流動性 artifact。
- **combo**：所有檔位 LS spread 皆負（−0.45 ~ −0.56%），被 ATR% leg 拖死。

### 3. Walk-Forward 年度切片（20d IC + D10−D1 LS spread，2008-2025 穩定窗）

完整 CSV：`reports/rvol_atr_walkforward_annual.csv`（含 2006-2026 全年；2026 為 80 交易日**部分年**，極端值已剔除於下表統計）。

| 因子 | 流動性 | 年均 rank-IC | signed-IC 為正年數 | **LS spread 為正年數** | 年均 LS | 最差年 LS |
|---|---|---|---|---|---|---|
| **log(RVOL)** | all | +0.0144 | **18/18** | **17/18** | +0.7% | −0.2% |
| | liq_50m | +0.0128 | 13/18 (72%) | 14/18 (78%) | +0.5% | −0.7% |
| **ATR% 反向** | all | −0.0436 | 16/18（IC 負=有利） | **4/18（22%）** | −0.9% | −6.7% |
| | liq_50m | −0.0466 | 15/18 | 5/18（28%） | −0.8% | −3.8% |
| combo | all | +0.0418 | 17/18 | 6/18（33%） | −0.2% | −2.1% |

**關鍵矛盾的解釋（ATR% 的核心 forensic）**：ATR% 的 rank-IC 為負（16-17/18 年），看似「低波動有利」；
**但 equal-weight 低波動籃子的 LS spread 卻 16/18 年為負**。兩者不矛盾——
**rank-IC（Spearman）由中位數/主體決定，而籃子 mean 報酬由右尾決定**。高波動股主體偏弱（中位數 −1.11%，
故 rank 上排序低 → 負 IC，呈現「低波動異象」假象），但少數高波動股的暴漲右尾（6.7% 漲 >30%）把**平均**拉高。
**可交易的 equal-weight 組合吃的是 mean，不是 rank-IC** → 低波動多空實際**賠錢**。
> 換言之：附錄引用的「ATR% 20d IC −0.079＝低波動異象」是一個 **non-tradeable rank 統計量**，
> 一旦做成 equal-weight 籃子持有就反號，正是被高波動肥尾翻盤。

RVOL 則 rank-IC 與 spread 同號（皆正、18/18 年），是全表唯一「rank 結論 = 可交易結論」的訊號。

### 4. 成本後淨 spread（h=20 月度再平衡 ≈ 年換手 12 次）

台股單邊摩擦：手續費（折扣後）+ 賣出證交稅 0.15%；採 round-trip **0.25% / 0.35%** 兩檔。
Long-short = 兩條腿，每腿每次再平衡一次 round-trip → 每期成本 = 2 × round-trip。
完整 CSV：`reports/rvol_atr_net_spread.csv`

| 因子 | 流動性 | gross LS %/20d | cost 0.25% → net | cost 0.35% → net |
|---|---|---|---|---|
| **log(RVOL)** | all | +0.74 | **+0.24** | +0.04 |
| | liq_50m | +0.44 | **−0.06** | −0.26 |
| | ex_bottom20pct | +0.52 | +0.02 | −0.18 |
| ATR% 反向 | all | −1.12 | −1.62 | −1.82 |
| | liq_50m | −0.99 | −1.49 | −1.69 |
| combo_rvol_lowatr | all | −0.45 | −0.95 | −1.15 |
| | liq_50m | −0.45 | −0.95 | −1.15 |

> **RVOL 的多空 LS book 在可交易池（liq_50m）成本後歸零/轉負**（−0.06 ~ −0.26%）。
> 這就是為什麼 RVOL 只能當「long-only 排序加分」而非獨立多空策略——long-only 排序 tilt 不額外付兩腿換手成本
> （股票本來就要持有），但純多空建倉付不起。

### 5. Survivorship 披露（量化方向性影響）

**當前 `data_cache/backtest/ohlcv_tw.parquet` 是 survivor-only panel。** 量化證據：
- panel 2,064 檔，其中 **1,890 檔（91.6%）最後一根 bar 正好落在 panel 末日**；**0 檔**在末日前 90 天以上下市。一個含下市股的真 PIT panel 不可能有「零 90 天前死亡」。
- 對照 `data_cache/backtest/universe_tw_pit.parquet`：PIT universe 認得 **3,621** 檔（status 含 正常 2,438 / 下市櫃・暫停・終止買賣 約 1,183），**價格 panel 只覆蓋 2,064 檔 → 1,660 檔（46%）PIT 已知 ticker 在價格 panel 中完全沒有資料**，絕大多數是下市/終止名單。panel 由 `refresh_universe_prices.py` 只抓現役 ticker 建成。

**方向性影響（量化推估）**：
- **對 ATR% 反向因子（高估方向，但結論不變）**：下市股多為低波動殭屍（停牌前成交枯竭→ATR% 假低→歸 D1）最終下市清算（報酬 ≈ −100%）。survivor panel 把它們剔除 → **低波動 D1 報酬被系統性高估**。但即便在這個對「低波動有利」**最有利**的偏差下，乾淨 h=20 的低波動 LS spread 仍 16/18 年為負。**真值只會更負** → FAIL 結論在 survivor-corrected panel 下**只會更強**，不會翻盤。
- **對 RVOL（影響小）**：RVOL 是橫斷面當下相對量，與「最終是否下市」關聯弱；下市前夕常伴隨異常爆量（恐慌/處置），若納入可能小幅**降低**高量端報酬。方向性影響有限，但 RVOL 的薄 edge 本就脆弱，survivor-corrected 後可能再縮。
- 結論：survivorship 讓**兩個因子的呈現都偏樂觀**；對 ATR% 是「致命方向更致命」，對 RVOL 是「薄 edge 可能更薄」。**建議若要把 RVOL 正式上 production，必須在 PIT panel（含下市，`universe_tw_pit` + 重抓下市股價）重跑確認 edge 不消失。**

---

## 各因子完整 Verdict

### log(RVOL) — MARGINAL / informational ranking-tilt（**不**作 standalone composite weight）

```
結論：log(RVOL) 是全表唯一 rank-IC 與 decile spread 同號、單調 (+0.99)、流動性過濾後存活、
      walk-forward 18/18 年(all)/13-14/18(liq_50m≈72-78%) 正向的技術訊號。但 edge 太薄：
      20d 多空 LS spread 僅 +0.4~0.7% gross，成本後在可交易池(liq_50m)歸零/轉負(−0.06~−0.26%)。
      → MARGINAL：可作「已持有組合內的短 horizon 排序加分」(long-only Top-N tilt 不付兩腿成本)，
        不可作獨立多空策略。tier = informational / ranking-tilt only。

關鍵數字 (h=20):
  - rank-IC 年均 +0.013, IR(年度) ~1.1, signed-IC 18/18 年正(all)
  - decile 單調 mono_rho +0.99; D1 +0.85% → D10 +1.59% (all)
  - 過濾敏感度: spread all +0.74% → liq_50m +0.44% → ex_bottom20pct +0.52% (衰減但存活)
  - Top-20 long-only: +1.63%/20d (all) / +1.40% (liq_50m), win ~60%; 相對差(top−bot) +0.80%/20d
  - 成本後多空: all +0.24%(0.25%cost) / liq_50m −0.06% ← 多空建倉付不起
```

**為何只給 MARGINAL 不給 PASS**：未過判準③（成本後 spread 在可交易池為負）。多空淨值歸零、絕對 edge < ATR_Stop 等
已上線風控工具的價值密度，且 survivor panel 下真值可能更薄。

**Next step（僅在進一步驗證後才考慮，這輪不跑增量）**：
1. **必要前置**：在 PIT panel（`universe_tw_pit` 含下市股，需 `market-data-rd` 重抓 1,660 檔下市股歷史價）重跑 RVOL h=10/20，確認 Top-20 相對差 +0.80% 不因 survivorship 消失。
2. 若存活：以 **long-only 排序加分**形式測試對 Whale composite 的**增量** IC（mandate 規定 standalone PASS 後才測；RVOL 是 MARGINAL 非 PASS，故增量測試需與用戶確認是否值得）。RVOL 與 Whale 的籌碼/質量因子相關性預期低（不同資訊維度），理論上可補位，但 +0.013 IC 量級下增量可能不顯著。
3. 不建議作 standalone timing / 多空 book。

### ATR% 反向（低波動）— FAIL / D 級歸檔

```
結論：FAIL。兩重死因——
  (1) 原始證據(h=60 D1 +5.87% / bot-10 +28%/60d) 是 stale-price reprice artifact:
      根因 12,823 列 Close≤0 + 近零價股 ATR% 假低。清洗 + 流動性過濾後 D1 斷崖完全消失
      (D1 mean +0.67%/median +0.00% @h=20), bot-20 籃子報酬從 +28%/60d 崩到 +0.81%/20d。
  (2) 即使乾淨資料，方向也相反: equal-weight 低波動籃子 LS spread 16/18 年為負, 高波動股平均
      報酬更高(D10 +1.79% > D1 +0.67%)。所謂「20d IC −0.079 低波動異象」是 non-tradeable rank 統計量,
      做成籃子持有就被高波動肥尾(6.7%股票漲>30%)翻號。
  → 不可上線任何形式(filter / weight / signal)。tier = D 級歸檔。

關鍵數字 (h=20):
  - rank-IC −0.044 (16-17/18 年負) BUT LS spread (D1−D10) 4/18 年正 (22%) ← 矛盾的解, 見上
  - 成本後多空 −1.5 ~ −1.8% (深度負)
  - 低波動 bot-20 +0.81%/20d < 高量 RVOL top-20 +1.63% (低波動是錯方向)
  - survivorship: 下市低波動殭屍被剔 → D1 已被高估, 真值只會更負, FAIL 更強
```

**作為負向 filter 的可能性也否決**：mandate 附錄提「驗低波動篩選作為負向 filter」。資料顯示
**高 ATR% 反而對應較高平均報酬**，用 ATR% 低當 filter = 篩掉較高報酬的高波動股 = 反向傷害。
（ATR% 的合法身分仍是附錄第 5 層講的「波動 regime 描述 + ATR_Stop 風控」，**不是橫斷面選股因子**。）

### combo_rvol_lowatr — FAIL（無組合增量價值）

```
結論：FAIL。combo = (RVOL_rn − ATR%_rn)/2, 但 −ATR% leg 是反向的(見上), 把組合拖成
      LS spread 6/18 年正、成本後 −0.95~−1.15%。「組合 IC > max(單因子 IC)」可交易性檢驗未過:
      combo 的 rank-IC +0.042 看似 > RVOL 的 +0.013, 但那又是 non-tradeable rank 假象——
      可交易的 LS spread combo(−0.45%) 比 RVOL 單因子(+0.74%) 更差。組合沒有價值。
```

---

## 方法與資料品質註記

- **訊號定義**：完全 import 自既有 `tools/indicator_ic_analysis.py::_compute_one_ticker`
  （`sig_rvol_log = log(clip(Volume/Volume.rolling(20).mean(), 0.01))`、`sig_atr_pct = ATR(14)/Close`）
  與 `indicator_combo_analysis.py::{rank_normalize_signals, build_combos}`（`combo_rvol_lowatr`）。未重寫公式。
- **為何需要新驅動**：當前 panel 已被 `refresh_universe_prices.py` 改 schema（`stock_id` + raw OHLCV，
  **無 AdjClose / 無 yf_ticker**），既有 `load_ohlcv` 直接 crash。新驅動只做 schema alias + 加驗證項目。
- **無 AdjClose 限制（披露）**：forward return 用 raw `Close`（panel 無還原價）。配息缺口會在除息日造成
  人工 gap-down，系統性**壓低高動能股、可能小幅虛抬低波動股**——對 ATR% 是「對低波動更有利」的偏差，
  而 ATR% 仍 FAIL，故此限制不改變結論方向。RVOL 的薄 edge 可能受配息缺口少量污染，列為 PIT 重跑時的待確認項。
- **資料清洗（披露）**：剔 12,823 列 Close≤0；`pct_change(fill_method=None)` 不跨缺口；
  |20d 報酬| ∉ (−95%, +500%) 視為 reprice artifact 剔除（清洗前 max +621,818%）。
- **2026 為部分年**（80 交易日），其極端值（ATR% IC 翻 +0.13 / spread −12%）為小樣本噪音，
  walk-forward 統計以 2008-2025 穩定窗為準。

## 產出檔案

| 檔案 | 內容 |
|---|---|
| `reports/rvol_atr_factor_validation.md` | 本報告 |
| `reports/rvol_atr_decile_returns.csv` | 4 流動性檔位 × {RVOL, ATR%, combo} × h=10/20 × decile 1-10 |
| `reports/rvol_atr_topn_portfolio.csv` | Top/Bot 10/20/50 portfolio（mean/win/sharpe_proxy） |
| `reports/rvol_atr_walkforward_annual.csv` | 2006-2026 年度 20d IC + LS spread（2 流動性檔位） |
| `reports/rvol_atr_net_spread.csv` | 成本前後淨 LS spread（0.25% / 0.35% round-trip） |
| `reports/rvol_atr_atr_forensic.csv` | ATR% D1/D2/D3/D9/D10 成分股流動性 + 報酬結構 + 右尾佔比 |
| `tools/rvol_atr_validate.py` | 驗證腳本（沿用既有訊號定義） |
