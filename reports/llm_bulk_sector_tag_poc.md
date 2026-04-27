# LLM Bulk Sector Tag — POC 報告 (2026-04-27)

**結論：accuracy 40%，不到 production 門檻，不建議 scale to 全 universe**

## 設定
- LLM: Claude Sonnet 4.6 (claude CLI)
- Schema: 23 themes from `data/sector_tags_manual.json`
- POC 範圍: 100 tickers (universe minus manual-tagged minus ETF)
- Batch size: 50 / call
- Prompt: 含 anti-patterns + good examples + "empty 永遠 safe" 強約束

## Run stats
- 100 tickers processed
- 10 got ≥1 theme (10% tag rate — LLM 對冷門股保守)
- 0 failed batches / 0 schema warnings
- 400s elapsed (2 batches × ~200s/each)

## 10 tagged 個股 audit 結果

| # | Ticker | Name | Industry | LLM tag | Audit | 評估 |
|---|---|---|---|---|---|---|
| 1 | 1590 | 亞德客 | Metal Fabrication | robotics_automation | 工業氣動元件，非 NVIDIA/Tesla Optimus 機器人零件 | ⚠️ borderline |
| 2 | 2355 | 敬鵬 | Electronic Components | pcb_hard | 車用 PCB + 工業，非 AI server 主板 | ❌ over-tag |
| 3 | 2379 | 瑞昱 | Semiconductors | networking_5g | 網通 PHY/Wi-Fi IC | ✅ OK |
| 4 | 3105 | 穩懋 | Semiconductors | networking_5g | GaAs RF for 5G/WiFi | ✅ OK (但更該是 gaas_compound_semi 主題) |
| 5 | 3376 | 新日興 | Computer Peripherals | apple_supply_chain | MacBook 鉸鏈供應 | ✅ OK |
| 6 | 3653 | 健策 | Electronic Components | ai_cooling | 散熱龍頭 | ✅ OK (manual.json 散熱 theme 應補進 3653) |
| 7 | 6196 | 帆宣 | Industrial Machinery | semi_equipment | 半導體廠務工程，非 CoWoS/先進封裝 | ⚠️ borderline |
| 8 | 6290 | 良維 | Electronic Components | networking_5g | 電源線/power cord，跟 5G 無關 | ❌ over-tag |
| 9 | 6526 | 達運 | Electronic Components | networking_5g | Connector 連接器，跟 5G 無直接關 | ❌ over-tag |
| 10 | 8064 | 東捷 | Industrial Machinery | semi_equipment | PV/LCD 設備，非半導體先進封裝 | ❌ over-tag |

**Accuracy**: 4 OK / 2 borderline / 4 錯 = **40% strict / 60% lenient**

## Failure pattern 分析

LLM 對 industry='Electronic Components' / 'Semiconductors' 的冷門 ticker 傾向**過度延伸主題**：
- networking_5g description 提「光纖 / 5G CPE」→ LLM 把 connector / 電源線廠 也套上去
- semi_equipment description 提「CoWoS 設備」→ LLM 把 PV/LCD 設備也套上

**核心問題**：純 TV sector/industry 兩欄資訊不足以判斷個股業務細節，缺 business description。

## 為什麼 manual.json 137 ticker 可能已夠用

- 23 themes 已 cover 台股 AI era 主流 ~100-150 個有意義標的
- 1972 universe 中真正有 AI era theme 的可能只 ~15-20%
- 剩下 80%+ 是傳產/小型股，LLM 強推 tag 反而 over-tag
- 既有 manual.json 137 ticker 已涵蓋 QM/Value/Mode D 主要 picks

## 三條後續路徑（待 user 決策）

### Path A: 跑完整 1972 + confidence filter
- 改 prompt 要 LLM 給每個 tag 0-1 confidence 分
- 只保留 ≥0.8 → 預計 tag rate 從 10% 降到 4-5%
- Trade-off: 漏掉 borderline 但避免 over-tag
- **Risk**: 仍可能有 over-tag（confidence 不一定校準）
- 工時: 1h 改 prompt + 重跑

### Path B: 改 rule-based TV industry → theme mapping
- 手動 map 112 TV industries → 23 themes (1-2h 一次性人工)
- 確定性高、無 hallucination
- 缺點: 同 industry 的多家公司會全 tag 同 theme（粒度粗）
- 工時: 2-3h

### Path C: 接受 manual.json 137 ticker 已夠用
- AI era 主流標的已涵蓋
- Multi-theme metadata 對 1972 universe 中 137 ticker 有效
- 剩下 fall back TV/FinMind 大類比較
- 工時: 0
- **這是最 robust 的選擇**

### Path D: Harvest business description 後再 LLM tag
- 從 yfinance / FinMind 抓 ~2000 公司業務描述
- LLM 讀業務描述 + theme schema 才 tag
- 工時: 1-2 天 (data harvest + re-prompt)
- 預計 accuracy 可從 40% 提升到 70-80%

## 建議

**Path C**（最 robust）。理由：
1. POC 顯示 LLM 對冷門 ticker 結構性 weak
2. Manual.json 137 ticker 涵蓋 AI era 主流 ~80%
3. 1972 universe 中 ~80% 是傳產/小型股不該強推 AI 主題
4. 多 tag 反而稀釋 multi-theme 訊號質量

**Path A 為次選**（保守化 LLM tag），但工程 ROI 邊際。

## Tool 已建好供未來重用
- `tools/llm_bulk_sector_tag.py` — CLI 完整 (POC + 批跑 + resume + audit)
- `tools/tmp/llm_tag_batches/` — raw prompt + output cache (debug 用)
- 若未來改 schema 或補業務描述，重跑只需改 prompt

## 已知 schema 殘缺 (順手抓出)
- `ai_cooling` theme 該補進 3653 健策 (今天 manual fix peer_comparison 已加)
- `gaas_compound_semi` theme 缺，目前 GaAs 三雄 8086/3105/2455 沒有對應 theme
- 可在下次 manual.json 維護時補
