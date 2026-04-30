# tests/

正式 pytest 測試套件。對照 `tools/_archive/manual_tests/` 是手寫一次性 test
（已歸檔），這裡才是 commit 前可重複跑的回歸防護網。

## 跑法

```bash
pytest                    # 全跑
pytest tests/test_piotroski.py -v  # 單檔 verbose
pytest -k fscore          # name filter
```

## 設計原則

1. **只測 pure functions** — 不 mock 網路（FinMind / yfinance）。M2 重構後
   `analysis_engine` / `pattern_detection` / `addon_factors` / `scenario_engine`
   都拆出大量純函式，這些是優先 cover 對象。
2. **Synthetic input** — 不從真實檔案讀資料，每個 test 自製 dict/df 餵函式。
   速度快、回歸穩、不依賴 cache 狀態。
3. **不追 coverage 數字** — UI / 排程 / 抓資料邏輯不寫 test（容易 flaky 又
   value 低）。聚焦在「算錯就交易決策錯」的金融公式 / 信號判斷。

## 已 cover 模組

| 檔案 | 範圍 | 測試數 |
|---|---|---|
| `test_piotroski.py` | `_compute_fscore` / `_compute_zscore` / `_compute_extra` / `_safe_div` / `_to_float` | 20 |

## TODO（高優先順序）

下批可加：
- `test_pattern_detection.py` — W 底 / M 頭 / 三角 / 背離（用 synthetic OHLC）
- `test_scenario_engine.py` — 劇本 A/B/C/D 出場邏輯
- `test_addon_factors.py` — 籌碼 / 情緒 / 營收 加分（C2-b IC 驗證版方向）
- `test_cache_manager.py` — TTL 計算（交易時段 5 min vs 收盤後整日）
