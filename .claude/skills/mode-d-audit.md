---
name: mode-d-audit
description: Use this skill when the user types `/mode-d-audit <ticker>` or `/audit <ticker>` to review a specific Mode D pick signal. Produces a structured audit verdict (thesis-ok / thesis-weak / thesis-broken) with reasoning, persisted to `data/latest/audits/<ticker>_<YYYYMMDD>.json` so the scanner Discord push can surface it. Manual trigger only — never auto-invoke per scanner run.
---

# Mode D Pick Audit (Layer 5 AI Review)

對 Mode D scanner 選出的 pick 做**signal 合理性審查**。由使用者手動觸發，
人工下單哲學不變；scanner 若偵測到已有 audit 結果會 best-effort 整合進
Discord push（Wave 3 #5 整合 capstone）。

## 🚧 骨架 (Wave 0 #2a, 2026-04-25)

**目前狀態**：contract / 觸發規則已定，Step 3 詳細 prompt + JSON schema 待
Wave 2/3 填實。Wave 0 只保證呼叫機制與輸出路徑不會在 Wave 3 scanner 整
合時需要改。

---

## 觸發條件

- 使用者明確輸入 `/mode-d-audit <ticker>` 或 `/audit <ticker>`
- 不接受「幫我審查今天所有 pick」整批模式（避免自動化）
- 若使用者語意上要求「審查 / 複核 / 把關 pick」並指定單一標的，亦可

## 執行步驟

### Step 1 — 載入 Mode D 當日 pick context

**禁止重複 API 呼叫**，先從下列檔案撈現成結果：

1. `data/latest/qm_result.json` — 當日 QM pick + composite_score / trigger_score / scenario_code / action_plan
2. `data/c1_tilt_flags.parquet` — 該 ticker 是否 C1 tilt ON
3. `data/sector_tags_dynamic.parquet` — 近 7 日 YT mention 次數 + sentiment + show 分佈
4. `data/sector_tags_manual.json` — ticker 的 manual sector tag

若 ticker 不在 `qm_result.json` 候選清單：告知使用者「非今日 Mode D pick，
無 audit 必要」並結束。

### Step 2 — 套用審查維度 (Wave 2 填實)

預計 4 個維度，各產 verdict + 1 句理由：

| 維度 | 檢查內容 (Wave 2 細化) |
|------|-------------------|
| **D1 mechanical** | QM composite_score 分項是否均衡？trigger_score 在合理區間？ |
| **D2 thesis** | YT mention 是否支持 scanner scenario？sentiment 與方向一致？ |
| **D3 regime** | C1 tilt 狀態與 regime 配合？非 AI era 時 tilt 應 OFF |
| **D4 risk** | rec_sl_price 與 rec_entry 之 R:R 是否 ≥ 1:1？scenario D 是否誤進 Pick？|

Wave 2 細化時可引用 `prompts/songfen_framework.md` 的反面論點章節做審查
框架；不重造整套 `/songfen` 深度分析（太重）。

### Step 3 — 輸出 JSON (Wave 3 scanner 消費 schema)

寫入 `data/latest/audits/<ticker>_<YYYYMMDD>.json`：

```json
{
  "ticker": "2330",
  "audit_date": "2026-04-25",
  "scanner_scan_date": "2026-04-25",
  "verdict": "thesis-ok | thesis-weak | thesis-broken",
  "verdict_short": "1 句話結論 (Discord 用)",
  "dimensions": {
    "D1_mechanical": {"verdict": "ok | weak | broken", "reason": "..."},
    "D2_thesis":     {"verdict": "ok | weak | broken", "reason": "..."},
    "D3_regime":     {"verdict": "ok | weak | broken", "reason": "..."},
    "D4_risk":       {"verdict": "ok | weak | broken", "reason": "..."}
  },
  "counter_thesis": ["反面 1", "反面 2", "反面 3"],
  "audited_by_model": "claude-opus-4-7",
  "audited_at": "2026-04-25T11:30:00+08:00"
}
```

**verdict 規則**：4 維度任一 broken → overall broken；3 ok + 1 weak →
overall ok；2+ weak → overall weak。

### Step 4 — UI 呈現 (Wave 2 Thesis Panel 擴充時加)

Mode D tab → Thesis Panel → 新增 section「📋 Audit 結果」顯示最近 N 筆
audit verdict。

### Step 5 — Scanner 整合 (Wave 3 capstone)

`scanner_job.py` Discord push 時 best-effort 掃 `data/latest/audits/` 目
錄，若目標 ticker 有當日 audit，附上 `verdict_short` 1 行在 pick 下方。
**無 audit 不阻擋 push**（使用者可能還沒手動審，正常情況）。

---

## 與 `/songfen` 差異

| 項目 | `/songfen` | `/mode-d-audit` |
|------|-----------|-----------------|
| 觸發 | `/songfen <ticker>` | `/mode-d-audit <ticker>` |
| 目的 | Ad-hoc 個股宋分視角深度分析 | 對 Mode D 選中的 pick 做 signal 合理性複核 |
| 輸出 | Markdown 到對話 | JSON 到 `data/latest/audits/` + Markdown 對話 |
| 時長 | 長（5 step 深度拆解） | 短（4 維度 quick check） |
| Scanner 整合 | 無 | Wave 3 有 |

**使用時機**：
- 想深度理解一檔股票的 re-rate / 損益表 → `/songfen`
- 今日 Mode D 選了 X 想快速驗證 thesis 成立 → `/mode-d-audit`

## 限制 (Robustness First)

- **不自動觸發**：絕不把此 skill 塞進 scanner TUE-SAT 00:00 pipeline。人工下單 = 人工審查
- **不改 QM / Value score**：純獨立 verdict，不 feedback 到選股分數
- **不阻擋 scanner**：Wave 3 整合時只 best-effort 讀 audit，找不到不報錯
- **audit 結果有時效**：scanner_scan_date 與當日不符 → 標記 stale，不使用

## 典型呼叫範例

```
/mode-d-audit 2330
/mode-d-audit 3017
/audit 4958
請幫我審查今日 Mode D 挑的 6172
```

## 相關檔案

- `data/latest/qm_result.json` — scanner pick 來源
- `data/c1_tilt_flags.parquet` — C1 tilt 狀態
- `data/sector_tags_dynamic.parquet` — YT mention panel
- `.claude/skills/songfen.md` — 深度分析 SKILL (本 skill 的互補)
- Wave 2/3 落地時要加: `data/latest/audits/` 目錄、Thesis Panel UI 整合、scanner_job.py 消費邏輯
