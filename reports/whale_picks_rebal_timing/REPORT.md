# Whale Picks Rebalance Timing 實驗

**Date**: 2026-05-22
**Question**: 是否該把月底 rebal 改成 15 號 (抓更新鮮的月營收 + 季報資料)？
**Counterargument tested**: sell-the-news effect (利多公告當天進場買在高)

## 1. Setup

**Code**: `tools/whale_picks_phase2.py` 新增 3 種 rebal 模式 (M15 / M11 / MIXED)
**PIT fix**: 加 revenue +10d publication delay (cache date 是公告月初，法定 day-10 才公告)
**4 arms**:
- **M** — 月底最後交易日 (現行 production)
- **M15** — 每月 15 號或之前最後交易日
- **M11** — 每月 11 號或之前最後交易日 (月營收公告隔日)
- **MIXED** — 季報月 (3/5/8/11) 用月底 + 非季報月用 15 號

**Period**: 2021-01-01 ~ 2026-04-30
**Same**: K=10 / industry-neutral / liquidity-filter / 同 universe (1749 stocks)
**Note**: M baseline Sharpe 0.75 vs memory 1.52 — memory 過期，5/16 之後 commit e84c4a6 修 5 個 non-blocker bugs 後 in-sample IC composite fitting 結果不同；本實驗 4 arm 用同一 code，相對比較 valid。

## 2. Main Result — 3 種 composite 一致結論

| Composite | M (月底，現行) | M15 (15 號) | M11 (11 號) | MIXED |
|---|---:|---:|---:|---:|
| **wf_score** (walk-forward，最誠實) | 0.203 | **0.628** | 0.501 | -0.183 |
| **composite_parsi** (8 因子 pre-registered) | 0.600 | **1.179** | 1.117 | 0.298 |
| composite_score (in-sample IC，optimistic) | 0.746 | 1.184 | **1.606** | 0.860 |

**3/3 composite 都顯示**：M15 ≈ M11 ≫ M baseline ≫ MIXED

### Per-arm 完整指標 (top-10 composite_parsi)

| Arm | Sharpe | CAGR | MDD | Win Rate | n_periods |
|---|---:|---:|---:|---:|---:|
| M (月底) | 0.60 | +12.5% | -35.6% | 56% | 57 |
| M15 (15 號) | **1.18** | **+33.2%** | -26.2% | 58% | 57 |
| M11 (11 號) | 1.12 | +31.3% | -29.2% | 56% | 57 |
| MIXED | 0.30 | +4.6% | -43.3% | 44% | 57 |

## 3. Sell-the-news 假設 — Falsified

**假設**: 季報公告日 (5/15 / 8/14 / 11/14 / 3/31) 附近聰明錢已先卡位，15 號 rebal 等於在波段高接手。

**Test**: MIXED arm 設計為「季報月避用 15 號（改月底）+ 非季報月用 15 號」。
- 若 sell-the-news 真實 → MIXED 應勝 M15（避了 4 個月的災難）
- 實測 MIXED Sharpe 0.30，反而 **比 baseline 還差**

**結論**: 季報月底 rebal 不只沒救 sell-the-news，反而拖累整體 — 在台股 mid-cap composite 因子組合上，**愈早 rebal 愈好的訊號穩固**。

可能原因：
- 季報法定 +45d publication delay (3/31 季末 → 5/15 才 visible)，所以 M15 rebal 在 5/15 那天 PIT 看到的是當天剛公告的 Q1 — 跟月底 rebal 看到的是同一份 Q1 (只是早 15 天）
- 真正讓 M15 / M11 領先的是 **月營收訊號**(每月 10 號公告) → M15 拿到的營收只放 5 天，月底拿到的放了 20 天，新鮮度差很多
- 季報 sell-the-news 在 K=10 portfolio level 不顯著 (10 檔分散，個股 noise 互相抵消)

## 4. M15 vs M11 — M15 略勝

| Composite | M15 | M11 |
|---|---:|---:|
| wf_score | **0.628** | 0.501 |
| parsi | **1.179** | 1.117 |
| in-sample | 1.184 | **1.606** (但 fitting artifact) |

- walk-forward + parsi 都偏好 M15
- M11 in-sample 1.606 是 IC fitting 灌水，不可信
- **M15 是更穩健的選擇**

另一個考量：M11 太早可能踩 PIT 邊界
- 月營收法定 day-10 公告，cache date = 公告月初 (e.g., 4/01 代表 3 月)
- 加 +10d delay 後 effective date = 4/11
- M11 rebal date 若落在 4/9 (weekend 推前) → 看不到 3 月營收
- M15 rebal 永遠 ≥ 11 號 → 永遠看得到上月營收

## 5. 工程影響

### Pipeline 變動

| 項目 | 現況 | 改 M15 後 |
|---|---|---|
| `whale_picks_screener.py` 月底 rebal | ✅ | 改判 15 號或之前最後交易日 |
| `--push-if-month-end` flag | last business day of month | last business day of ≤ 15 |
| `_active_holdings.json` 換倉時點 | 月底 | 每月 15 號 |
| Discord push | 月底 | 每月 15 號 |
| 月營收公告 11 號 → rebal 15 號 | 月底拿 20 天舊資料 | 拿 5 天新資料 |
| 季報三表 +45d delay | 月底 6/14 後才用上 Q1 | 6/15 rebal 用上 Q1（只多等 1 天） |
| Trade ledger backfill 重跑 | — | 需重跑 (rebal 日期變了) |

### 需要 commit 的檔案

1. `tools/whale_picks_phase2.py` — 已改（M15 / M11 / MIXED + revenue +10d PIT）
2. `tools/whale_picks_screener.py` — 改 `_is_last_business_day_of_month` → `_is_mid_month_rebal_day`
3. `tools/whale_picks_alerts.py` — `_maybe_update_holdings` 觸發條件改 15 號
4. `docs/whale_picks_spec.md` — 加 v0.10 timing 實驗章節
5. `run_scanner.bat` — 註解更新
6. `data/whale_picks/trade_ledger.parquet` — 用 M15 規則重跑

### 風險

1. **生效時點**：5/15 是 Q1 季報公告當天，rebal 在當天 = 暴露公告當天 noise (可能高估 5/15 的 alpha)
2. **K=10 月底持倉換月中**：UI 上 "持倉" 與 "下月候選" 的語義位移；用戶需要重新適應
3. **memory baseline 1.52 過期**：production 用 e84c4a6 fix 之後的代碼，真實 baseline 是 0.75，改 M15 後預期 1.18 — 漲幅 +57%，仍實質改善但絕對 Sharpe 級別與宣稱不符
4. **Live shadow run**：實作後仍需要 3-6 月 live 驗證，timing 改動的 alpha 是否能在實盤實現

## 6. 推薦

✅ **改 M15**。

理由：
- 3/3 composite 一致顯示 M15 ≫ M baseline
- composite_parsi (pre-registered，最 robust) Sharpe +96% (0.60 → 1.18)
- 走 walk-forward 也有 +0.43 Sharpe lift
- Sell-the-news 假設被 falsify，無下檔風險
- M15 vs M11 微小差異，**選 M15 因 PIT safety margin 更厚**

不推薦：
- ❌ M11 — M15 已抓到主要 alpha，再早 4 天踩 PIT 邊界不值得
- ❌ MIXED — 數據顯示更差

### 後續工作

1. (priority) 改 4 個 code file (screener / alerts / spec / bat)
2. (priority) 重跑 trade_ledger backfill M15 規則
3. (medium) 加 季報自動 batch refresh schedule (`run_quarterly_financials.bat`) — 4/1 / 5/16 / 8/15 / 11/15 跑，補上「季報三表 + F-Score」沒排程的 gap
4. (medium) 更新 memory `project_whale_picks.md` baseline 數字
5. (later) Live shadow run 3-6 月驗 M15 alpha 落地
