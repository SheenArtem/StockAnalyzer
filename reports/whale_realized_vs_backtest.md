# Whale Picks — Realized (live) vs Backtest baseline 追蹤

> 產生: 2026-06-09 · 工具: `tools/whale_realized_tracker.py` · 累積檔: `data/whale_picks/realized_tracking.parquet`
> Baseline: `data/whale_picks/trade_ledger_meta.json` (win 0.514 / avg +0.0311 / K=10 / M15 rebal / N=833 backtest positions)

---

## TL;DR — 誠實結論

1. **歷史快照是 backtest 重構,不是 live → 回填無 OOS 價值。** 結論:**跳過回填**,改建「從現在累積」的 forward tracker。
2. **真 live OOS 史只有 1 個 cohort (2026-05-15) 且在飛行中** — 策略 2026-05-22 才切 M15,這是切換後第一個(也是目前唯一)真實 live 持倉。下次 rebal 2026-06-15 才會產生第二個 cohort。
3. 那唯一 cohort 目前 mark-to-market **+16.8% avg / 80% win(N=10, 21 天, 未平倉)** — 數字漂亮但 **N=1 cohort 在飛行中,統計上不具意義**,只能當「目前唯一 live data point」報告,不能拿來宣稱策略驗證通過。

---

## A. 歷史回填的誠實判斷

### A.1 快照來源鑑定 (live vs backtest)

對 `data/whale_picks/{YYYY-MM-DD}.parquet` 逐一檢查檔案 mtime + git 歷史 + 內容:

| 快照日期區間 | 檔案 mtime | 判定 | OOS 價值 |
|---|---|---|---|
| 2026-05-16 ~ 2026-05-23 | 全部寫於 2026-05-22~23 (batch) | **backtest 重構** (由 `whale_picks_backfill_snapshots.py` 對 stale 日重算) | 無 |
| 2026-05-24 ~ 2026-06-09 (今) | 每日 00:30-01:00 各自寫一次 | **live point-in-time** (daily scanner 隔夜跑出來) | 見下方限制 |

**驗證證據** (追蹤持股 6414 的 snapshot Close):
- 06-03 Close=375.5 / 06-05 Close=385.0 / 06-09 Close=384.0 → 每天不同收盤價 = 真 point-in-time 重評分。
- 但 05-24/05-27/05-28 全是 Close=350.5(凍結在 05-23 週五收盤,檔案大小都 610898 bytes)→ 非交易日 snapshot 是 stale 複本(scanner 每天跑但台股週末無新資料)。
- 06-02 出現 NaN Close(composite 仍算)→ 對應 memory `project_yfinance_nanclose_corruption` 的 yfinance NaN-close 事件。**已在 tracker 加防呆**(剔 NaN/<=0 收盤)。

### A.2 為什麼「live 的每日 snapshot」仍**不能直接當 OOS**

關鍵:**Whale Picks 的 OOS 單位是 M15「持有組合」,不是每日 snapshot 的 would-be top-10。**

- 每日 dated snapshot 是「今天若 rebal 會選誰」的 ranking,**不是實際持倉**。實際持倉只在 M15 換(K=10 等權持有到下次 M15)。
- 06-01 的 would-be top-10 含 2344,06-09 已掉出;06-01→06-09 top-10 只剩 6414 還在 — 每日 ranking 漂移劇烈。**拿每日 top-10 當進場 = 製造策略根本沒有的換手率**,會嚴重高估報酬/失真。
- 真正的 live 持倉是 `_active_holdings.json`(M15 當天 live 存的 `entry_close`),這才是 OOS 的進場錨點。

### A.3 真 live 持倉史 = 只有 1 個 cohort

`_active_holdings.json` 的 git 歷史只出現過:
- `2026-05-16 "forced"` 一筆 bootstrap seed(early-entry feature commit e2bdc05,**非真 M15 rebal**,是強制 backfill 種子)。
- `2026-05-15 "m15_rebal"`(a6474bc 起,即目前這筆)。

策略 2026-05-22 才從月底切 M15(commit a6474bc)。**第一個真實 live M15 cohort = 2026-05-15,持有至今(N=1,未平倉)。** 在那之前 ledger 833 筆全是 backtest 重構,**無 live OOS 價值**。

⇒ **誠實結論:Whale Picks 目前實質上沒有 live OOS 歷史**,只有 1 個飛行中的 cohort。

### A.4 最接近的替代 proxy(明確標示「非 OOS」)

任務指示「若快照是 backtest 重構 → 用 naive walk-forward holdout 當最接近的 proxy(標明)」。
backtest ledger 的近期切片即是該 proxy,但**它與 baseline 是同一母體(同一 backtest),不是獨立 OOS**:

| 近期切片 (closed positions, by entry_date) | N | win | avg | median |
|---|---|---|---|---|
| 近 6 月 (entry ≥ 2025-11-15) | 32 | 0.531 | +0.0836 | +0.0284 |
| 近 12 月 (entry ≥ 2025-05-15) | 85 | 0.506 | +0.0501 | +0.0026 |
| 近 24 月 (entry ≥ 2024-05-15) | 161 | 0.516 | +0.0522 | +0.0039 |
| 全 ledger (= baseline 母體) | 823 | 0.519 | +0.0314 | — |

**解讀**: 近期 regime 的 win rate 在 baseline 附近徘徊(50.6~53.1%),avg PnL 在多頭中略高(+5~+8%)。這只說明「近期回測表現與全期 baseline 一致」,**不是 OOS 驗證**,不可拿來當 live 績效。

---

## B. Forward tracker 工具 (`tools/whale_realized_tracker.py`)

### B.1 三件事

1. **快照累積** — 每跑一次把當前 `_active_holdings.json`(real held cohort)+ entry 價 upsert 進 `data/whale_picks/realized_tracking.parquet`,key=(rebalance_date, stock_id),**idempotent**(同 cohort 重跑只更新不重複;新 M15 cohort 自動 append)。含 `alert_adds`(entry_type 標 system/alert)。
2. **realized 報酬** — 對每個 cohort 用 clean `ohlcv_tw`(剔 V=0 凍結列 + NaN/<=0 收盤)算 fwd 報酬。下個 cohort 存在 → 前 cohort 視「完整週期」(exit = 下次 rebal 前一交易日);否則 mark-to-latest(in-flight)。
3. **月度對照報告** — per-cohort + aggregate realized win/avg/Sharpe(cross-sectional)vs baseline;trailing-N cohort 全低於 baseline → degradation flag。

### B.2 PIT / robustness 保證

- 進場錨點用 rebal 當天 live 存的 `entry_close`(只用 ≤ rebal 日資料)。
- fwd 報酬用 clean `ohlcv_tw` 剔 V=0(對齊 memory `project_v0_frozen_rows`)+ 剔 NaN/<=0(對齊 yfinance NaN-close 防呆)。
- **fail loud**: 缺 `_active_holdings.json` / 缺 snapshot / 缺價 / NaN → WARN(per-stock realized=NaN,排除統計,不靜默當 0)。
- `--asof` 回測證明 PIT 正確:`--asof 2026-05-29` 得 hold_days=14、3209 exit=76.1;最新得 hold_days=21、3209 exit=73.5 — windowing 用 point-in-time 價,無未來資料。
- 多 cohort 邏輯已用合成 3-cohort 驗證:前 2 cohort 自動轉 `closed`(hold_days≈29 = 完整 M15 週期),最新轉 `in-flight`;degradation flag 在全低於 baseline 時正確 `[RAISED]`。

### B.3 實跑輸出 (2026-06-09, exit 0)

```
Backtest baseline: win=0.514  avg=+0.0311  (K=10 / M15)

PER-COHORT (M15 held portfolio):
  rebal         N     status     win      avg      med   shrp  win_vs_b  avg_vs_b
  2026-05-15   10  in-flight   0.800  +0.1681  +0.1058   0.82    +28.6p    +13.7p

AGGREGATE (all live cohorts pooled, position-level):
  cohorts=1  positions=10  win=0.800  avg=+0.1681  median=+0.1058  sharpe_xs=0.817
  vs baseline: win +0.286  avg +0.1370

DEGRADATION FLAG (trailing 3 cohorts): [clear]
  reason: insufficient live cohorts (1 < trailing 3) -- degradation flag NOT yet evaluable
```

In-flight cohort (2026-05-15, mark-to-latest 2026-06-08, 21 天) 個股:

| stock_id | name | entry | exit | ret | hold |
|---|---|---|---|---|---|
| 3209 | 全科 | 46.3 | 73.5 | +58.75% | 21d |
| 2356 | 英業達 | 52.2 | 76.8 | +47.13% | 21d |
| 6414 | 樺漢 | 319.5 | 384.0 | +20.19% | 21d |
| 9933 | 中鼎 | 37.6 | 43.5 | +15.56% | 21d |
| 2376 | 技嘉 | 326.5 | 369.0 | +13.02% | 21d |
| 3033 | 威健 | 47.9 | 51.8 | +8.14% | 21d |
| 2610 | 華航 | 18.5 | 20.0 | +7.84% | 21d |
| 3045 | 台灣大 | 114.5 | 116.0 | +1.31% | 21d |
| 2633 | 台灣高鐵 | 25.7 | 25.5 | -0.78% | 21d |
| 6658 | 聯策 | 194.0 | 188.0 | -3.09% | 21d |

---

## C. 月度 SOP 接法 (手動,**不自動排程** — 與 100% Whale SOP 對齊)

每月 M15 換倉日,照既有 Whale 換倉流程跑完後加跑本工具:

```
1. python tools/whale_picks_screener.py              # 產新 top-10
2. python tools/whale_picks_alerts.py --update-holdings   # 刷新 _active_holdings.json
   (scanner.bat 在 M15 day 會自動跑 1-2;手動換倉時自己跑)
3. python tools/whale_realized_tracker.py            # <<< 快照新 cohort + 印對照報告
4. 若 degradation flag [RAISED] (trailing 3 cohort 全低於 baseline) → 人工檢視策略漂移
   (informational tier per SPEC §13,不自動停用)
```

非 M15 日也可跑 `--report-only` 看 in-flight cohort 即時 MTM(不寫快照)。

---

## D. 誠實 caveats

1. **Live OOS 史從 2026-05-15 起算,N=1 cohort 且未平倉。** 目前 +16.8%/80% win 漂亮但**統計上不具意義**(單一 cohort、21 天、未走完一個 M15 週期)。**不可** 拿來宣稱策略 live 驗證通過。
2. **歷史回填無 OOS 價值**(快照是 backtest 重構 = ledger 本身,無新資訊)— 故未回填,改建 forward tracker。
3. realized 報酬**未扣交易成本/滑價/股息**(對齊 `portfolio_stats.json` 既有 caveat:~6 round-trips/年 × 0.3% ≈ -1.8%/年 haircut + 滑價 ~-0.5%/年)。in-flight MTM 用收盤價,實際成交價會有差。
4. degradation flag 需 ≥3 live cohort 才可評(即 ≥2026-08-15 後),在那之前永遠 `[clear] (insufficient)`。
5. cross-sectional Sharpe(`sharpe_xs` = cohort 內 mean/std)是「同期選股離散度調整後的命中品質」,**非年化 Sharpe**,不要跟 `portfolio_stats.json` 的 1.80 混淆。
6. 真正能對照 baseline 的時點:**2026-06-15 第二個 cohort 落地後** 1 個 closed + 1 個 in-flight;要累到 6-12 個 cohort(2026 下半年~2027)才開始有 meaningful live track record。
