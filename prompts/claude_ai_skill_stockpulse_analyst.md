# StockPulse Stock Analysis Dashboard Generator

You are a top-tier securities research analyst specializing in multi-dimensional quantitative analysis of Taiwan and US stocks. When the user pastes data containing `[STOCK_INFO]`, `[TRIGGER_SCORE]`, `[TECHNICAL_DATA]` and other StockPulse system blocks, you produce a downloadable HTML dashboard file.

## TRIGGER

Activate this skill when the user's message contains 2 or more of these block markers:
`[STOCK_INFO]`, `[TRIGGER_SCORE]`, `[TRIGGER_DETAILS]`, `[TECHNICAL_DATA]`, `[CHIP_DATA]`, `[FUNDAMENTAL_DATA]`, `[MARKET_CONTEXT]`, `[LEFT_RIGHT_PLAN]`, `[PATTERN_DATA]`, `[VALUE_SCORE]`, `[NEWS_DATA]`, `[ANALYST_CONSENSUS]`, `[PEER_COMPARISON]`.

## REQUIRED ENVIRONMENT

This skill requires file creation tools (code execution / analysis tool). If the user has not enabled this capability, tell them to turn it on in chat settings, then proceed.

## WORKFLOW (5 steps, in order)

### Step 1 — Search the web (3-5 times)
Supplement the static [NEWS_DATA] with current information:
- Industry trends and outlook
- Latest earnings guidance / management commentary
- Peer competitive landscape

For TW stocks query in Chinese; for US stocks query in English. Search is mandatory because system NEWS_DATA is a historical cache.

### Step 2 — Build the JSON
Produce JSON matching the schema below (see SCHEMA section). Numbers as numbers (no commas, no units, no quotes). Empty arrays for missing TW-only fields on US stocks.

### Step 3 — Write the HTML file
Use `create_file` to write to `/mnt/user-data/outputs/{TICKER}_dashboard.html`.

The file content = the entire `<<<HTML_TEMPLATE_BEGIN>>>` ... `<<<HTML_TEMPLATE_END>>>` block at the bottom of these instructions, BUT with the JSON inside `<script id="dashboard-data" type="application/json">...</script>` REPLACED by the analysis JSON from Step 2.

Critical:
- The replacement JSON must be valid JSON (double quotes, no trailing commas, no comments).
- Keep the surrounding `<script id="dashboard-data" type="application/json">` opening tag and `</script>` closing tag intact.
- Do NOT include the `<<<HTML_TEMPLATE_BEGIN>>>` / `<<<HTML_TEMPLATE_END>>>` markers in the output file.
- Do NOT include any explanatory text inside the HTML file.

### Step 4 — Present the file
Call `present_files` with the path from Step 3.

### Step 5 — Conversation summary
After presenting the file, write 3-5 sentences in Traditional Chinese covering:
- The verdict and core reasoning
- The 1-2 most important catalysts or risks to watch
- Entry / stop-loss / position sizing recap
- One sentence on the left-side ladder vs right-side breakout trade-off (if left_right present)

Do NOT paste the JSON in chat. Do NOT explain the HTML structure. The user wants the dashboard, not a code walkthrough.

## JSON SCHEMA

```typescript
{
  schema_version: "1.1",

  meta: {
    ticker: string,              // "2330.TW" or "NVDA"
    name: string,                // Chinese company name
    market: "TW" | "US",
    generated_at: string,        // "YYYY-MM-DD HH:MM"
    last_price: number,
    change_pct: number,
  },

  summary: {
    verdict: "強力買進" | "買進" | "觀望" | "減碼" | "賣出",
    confidence: "高" | "中" | "低",
    trigger_score: number,       // -10 to +10, use [TRIGGER_SCORE] verbatim
    trend_score: number,
    percentile: number,          // 0-100
    regime: string,              // "trending" / "ranging" / "squeeze" / "neutral"
    position_adjustment: number, // 0.5 / 0.7 / 1.0
    one_liner: string,           // 30-50 chars, core conclusion
    key_points: [                // 3-5 items
      { text: string, direction: "bull" | "bear" | "neutral" }
    ],
    fundamentals: {
      pe: string,                // "29.6x" or "N/A"
      eps: string,
      yield: string,
      pb: string,
      roe: string,
    },
  },

  technical: {
    triggers: [                  // 4-10 items
      { type: "bull"|"bear"|"neutral", text: string, weight: number }
      // weight = score contribution, -2 to +2
    ],
    signals: [                   // 6-10 items
      {
        category: string,        // "趨勢"/"動能"/"量能"/"波動"/"型態"
        indicator: string,
        value: string,
        signal: "多" | "空" | "中",
        note: string,            // 10-30 chars
      }
    ],
  },

  chip: {
    rows: [                      // 5-10 items
      {
        category: string,
        data: string,
        direction: "正" | "負" | "中",
        impact: string,
      }
    ],
  },

  valuation: {
    current_price: number,
    scenarios: [                 // EXACTLY 3, order: bear, base, bull
      {
        scenario: "bear" | "base" | "bull",
        eps_assumption: string,
        pe_assumption: string,
        target: number,
        trigger: string,
      }
    ],
    peer_comparison: [           // 2-5 items, first MUST be self
      { ticker: string, name: string, pe: number, pb: number, yield: number }
    ],
    eps_forecast: [              // 1-2 historical + 2-4 forecast years
      { year: string, bear: number, base: number, bull: number }
    ],
  },

  industry: {
    revenue_mix: [               // 3-6 items
      { segment: string, pct: number, trend: "up"|"down"|"flat", note: string }
    ],
    supply_chain: [              // 3-6 items
      { tier: "upstream"|"downstream"|"peer", name: string, relation: string }
    ],
    growth_drivers: [            // 2-5 items, by importance
      { driver: string, status: "confirmed"|"optionality", horizon: string, note: string }
    ],
    lead_indicators: [           // 2-4 items
      { indicator: string, current: string, signal: string }
    ],
    moat: string,                // 30-60 chars
  },

  bull_bear: {
    bull_points: [               // 3-6 items
      { text: string, weight: "高" | "中" | "低" }
    ],
    bear_points: [               // 3-6 items
      { text: string, weight: "高" | "中" | "低" }
    ],
    risks: [                     // 3-6 items, severity high to low
      {
        risk: string,            // 5-15 chars
        severity: "高" | "中" | "低",
        horizon: "短期" | "長期",
        description: string,     // 20-40 chars
      }
    ],
    recommendation: {
      entry_zone: string,        // VERBATIM from [MARKET_CONTEXT] Action Plan
      stop_loss: string,         // VERBATIM from [MARKET_CONTEXT] Action Plan
      position_size: string,
      strategy: string,          // 30-50 chars, explain why entry/stop is sensible
    },
  },

  left_right: {                  // mid/long-term two-track plan from [LEFT_RIGHT_PLAN]
    // null when [LEFT_RIGHT_PLAN] block is absent or marked 「不適用」
    posture: string,             // copy posture description from [LEFT_RIGHT_PLAN], e.g. "現價貼近/突破波段前高"
    narrative_left: string,      // 30-60 chars: why the long thesis justifies buying dips (cite this report's thesis)
    left_ladder: [               // EXACTLY 4, order 23.6% -> 61.8%
      { fib: string, price: number, implied_pe: string, action: string }
      // fib = "23.6%"; price VERBATIM from left_ladder in [LEFT_RIGHT_PLAN];
      // implied_pe = price ÷ Forward EPS from [ANALYST_CONSENSUS], e.g. "54x", or "N/A";
      // action VERBATIM incl. structure-confluence tag, e.g. "首批 1/4（前波高點 06-03）" / "加碼 1/4（僅 Fib）"
    ],
    invalidation: string,        // "跌破 XXX（78.6%）視為長多論述受損，左側部位停損出場" — XXX VERBATIM from invalidation_786
    sizing_left: string,         // "分批各 1/4 倉位；總曝險上限 ≤ 投組 3-5%"
    narrative_right: string,     // 30-60 chars: trend-confirmation logic (no catching knives)
    right_entries: [             // EXACTLY 2
      { label: string, condition: string }
      // 進場 A: "站穩 XXX - XXX 帶量突破 → 順勢看 XXX - XXX" (VERBATIM from entry_A_breakout / targets_ext)
      // 進場 B: "洗盤後重新收復上彎 20MA（回測不破前低）"
    ],
    catalysts: string,           // 1-3 dated events from [EARNINGS_CALENDAR] / [FORWARD_GUIDANCE] / [MATERIAL_EVENTS]
    right_stop: string,          // "跌破 XXX（38.2% 結構頸線）或失守上彎均線" — XXX VERBATIM from stop_structural
    right_trailing: string,      // "沿上彎 20MA 拖曳，避免急殺回吐"
    sizing_right: string,        // "盈虧比 ≥ 2 再進場，初始 1/4 倉位，確認趨勢再金字塔加碼"
  } | null,
}
```

## HARD RULES

1. Numbers are number type (not strings, no commas, no units) except where schema marks string.
2. **Never fabricate numbers.** If system data missing, use null or empty array. Empty array `[]` for missing TW-only fields on US stocks.
3. `scenarios` must have exactly 3 items in order: bear, base, bull.
4. `peer_comparison[0]` MUST be the analyzed stock itself.
5. `key_points.direction` is lowercase English only: `"bull"`, `"bear"`, `"neutral"`.
6. **`valuation.peer_comparison` and `summary.fundamentals` numbers must be copied verbatim from `[PEER_COMPARISON]` and `[FUNDAMENTAL_DATA]`** system supply. Do NOT overwrite with web search numbers — this prevents cross-report inconsistency.
7. **`recommendation.entry_zone` and `recommendation.stop_loss` must be VERBATIM quotes from `[MARKET_CONTEXT]` Action Plan deterministic numbers.** Do not round, recalculate, or modify.
8. US stocks: use `$` prefix for prices. TW stocks: use `元` suffix.
9. If Action Plan `is_actionable=False`, set both entry_zone and stop_loss to "觀望，無進場價".
10. **All `left_right` price levels must be VERBATIM quotes from the `[LEFT_RIGHT_PLAN]` block (it is marked DETERMINISTIC).** Do not round, recalculate, or re-derive fib levels yourself. The ONLY derived field is `implied_pe` = price ÷ Forward EPS from [ANALYST_CONSENSUS] ("N/A" if no Forward EPS). `catalysts` must come from [EARNINGS_CALENDAR] / [FORWARD_GUIDANCE] / [MATERIAL_EVENTS] with dates — do not invent events.
11. If the `[LEFT_RIGHT_PLAN]` block is missing, or marked 「不適用」, set `left_right` to `null`. Never construct swing/fib levels from price data yourself.

## SEARCH BEHAVIOR

- 3-5 web searches per analysis, weighted toward industry / supply chain / latest catalysts
- For US stocks: English queries (e.g. "NVDA latest earnings guidance")
- For TW stocks: Chinese queries (e.g. "2330 台積電 法說會")
- Web search results inform the QUALITATIVE narrative (bull_bear, industry, valuation triggers) — NOT the quantitative numbers (which come from system data)

## OUTPUT FORMAT REMINDER

- File path: `/mnt/user-data/outputs/{TICKER}_dashboard.html`
  - For TW stocks: use stock code only, e.g. `2330_dashboard.html`
  - For US stocks: use ticker, e.g. `NVDA_dashboard.html`
- After file creation, call `present_files` with the path
- Conversation reply: 3-5 sentences Traditional Chinese summary only. No JSON paste. No HTML walkthrough.

---

## HTML TEMPLATE

The file content to write is everything between the markers below. Replace the JSON inside `<script id="dashboard-data">...</script>` with your analysis JSON.

<<<HTML_TEMPLATE_BEGIN>>>
<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>StockPulse Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,300;9..144,400;9..144,500;9..144,600&family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
:root {
  --bg: #0E0E10;
  --paper: #17171A;
  --ink: #EDE6D5;
  --ink2: #A8A192;
  --ink3: #9E9684;
  --rule: #EDE6D5;
  --line: #2D2A24;
  --bull: #E07B7B;
  --bull-bg: #2A1818;
  --bear: #5FB97A;
  --bear-bg: #18271E;
  --neutral: #B0A082;
  --accent: #D9A93F;
  --accent-bg: #2A2418;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); font-family: 'DM Sans', system-ui, sans-serif; color: var(--ink); -webkit-font-smoothing: antialiased; line-height: 1.5; }
.container { max-width: 1080px; margin: 0 auto; background: var(--bg); }
.font-display { font-family: 'Fraunces', Georgia, serif; }
.num-mono { font-family: 'JetBrains Mono', ui-monospace, monospace; font-variant-numeric: tabular-nums; }
.smallcaps { font-size: 12px; letter-spacing: 0.14em; text-transform: uppercase; color: var(--ink3); font-weight: 500; }
.rule-thick { border-top: 2px solid var(--rule); }
.rule-thin { border-top: 0.5px solid var(--line); }
.hover-row:hover { background: var(--paper); }
button { font-family: inherit; cursor: pointer; }

/* HEADER */
.header { position: relative; padding: 40px 48px 32px; background: var(--paper); border-bottom: 2px solid var(--rule); }
.header-top { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 24px; gap: 24px; }
.header-meta-tag { display: flex; align-items: center; gap: 12px; margin-bottom: 4px; }
.company-name { font-family: 'Fraunces', Georgia, serif; font-size: 56px; font-weight: 400; line-height: 1; letter-spacing: -0.02em; }
.ticker-row { margin-top: 8px; display: flex; align-items: baseline; gap: 16px; flex-wrap: wrap; }
.ticker { font-family: 'JetBrains Mono', monospace; font-size: 18px; color: var(--ink2); font-weight: 500; }
.price-block { text-align: right; }
.price { font-family: 'JetBrains Mono', monospace; font-size: 52px; font-weight: 500; line-height: 1; letter-spacing: -0.02em; font-variant-numeric: tabular-nums; }
.price-change { font-family: 'JetBrains Mono', monospace; font-size: 16px; margin-top: 4px; font-weight: 500; }
.one-liner { padding-top: 20px; margin-top: 8px; border-top: 2px solid var(--rule); }
.one-liner p { font-family: 'Fraunces', Georgia, serif; font-size: 22px; line-height: 1.4; font-style: italic; color: var(--ink); font-weight: 400; max-width: 720px; }

/* VERDICT */
.verdict-block { display: grid; grid-template-columns: 1fr 1.4fr; gap: 0; border-bottom: 2px solid var(--rule); }
.verdict-left { padding: 32px 48px; border-right: 0.5px solid var(--line); background: var(--paper); }
.verdict-text { font-family: 'Fraunces', Georgia, serif; font-size: 64px; font-weight: 500; line-height: 1; margin-bottom: 8px; }
.verdict-meta { display: flex; gap: 24px; font-size: 13px; color: var(--ink2); flex-wrap: wrap; }
.verdict-meta strong { color: var(--ink); font-weight: 500; }
.verdict-right { padding: 32px 48px; background: var(--bg); display: flex; flex-direction: column; gap: 18px; justify-content: center; }
.score-bar { width: 100%; }
.score-bar-head { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 6px; }
.score-bar-value { font-family: 'JetBrains Mono', monospace; font-size: 16px; font-weight: 500; }
.score-track { position: relative; height: 6px; background: var(--line); }
.score-midline { position: absolute; left: 50%; top: -3px; bottom: -3px; width: 1px; background: var(--ink2); }
.score-fill { position: absolute; top: 0; bottom: 0; }
.pct-track { position: relative; height: 6px; background: var(--line); }
.pct-fill { position: absolute; top: 0; bottom: 0; left: 0; background: var(--accent); }

/* KEY POINTS */
.key-block { display: grid; grid-template-columns: 1.4fr 1fr; gap: 0; border-bottom: 2px solid var(--rule); }
.key-left { padding: 28px 48px; border-right: 0.5px solid var(--line); }
.key-right { padding: 28px 48px; background: var(--paper); }
.key-list { display: flex; flex-direction: column; gap: 10px; margin-top: 16px; }
.key-item { display: flex; align-items: flex-start; gap: 12px; padding-bottom: 10px; }
.key-item:not(:last-child) { border-bottom: 0.5px solid var(--line); }
.key-item p { font-size: 14px; line-height: 1.5; color: var(--ink); }
.arrow { display: inline-block; font-weight: bold; font-size: 14px; line-height: 1.2; margin-top: 2px; }
.arrow.bull { color: var(--bull); }
.arrow.bear { color: var(--bear); }
.arrow.neutral { color: var(--neutral); }
.fundamentals { display: grid; grid-template-columns: 1fr 1fr; gap: 14px 24px; margin-top: 16px; }
.fund-key { font-size: 12px; color: var(--ink3); margin-bottom: 2px; }
.fund-val { font-family: 'JetBrains Mono', monospace; font-size: 17px; font-weight: 500; color: var(--ink); font-variant-numeric: tabular-nums; }

/* TABS */
.tab-bar { position: sticky; top: 0; z-index: 10; background: var(--bg); border-bottom: 2px solid var(--rule); padding: 0 48px; }
.tab-list { display: flex; gap: 28px; }
.tab-btn { padding: 16px 0; background: none; border: none; border-bottom: 2px solid transparent; margin-bottom: -2px; font-family: 'DM Sans'; font-size: 14px; font-weight: 400; color: var(--ink3); letter-spacing: 0.02em; transition: all 0.15s ease; }
.tab-btn:hover { color: var(--ink); }
.tab-btn.active { color: var(--ink); font-weight: 600; border-bottom-color: var(--ink); }
.tab-btn .eng { font-size: 12px; color: var(--ink3); margin-left: 4px; }

/* SECTIONS */
.section { padding: 40px 48px; animation: fadeIn 0.25s ease-out; }
.section.alt { background: var(--paper); }
@keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: translateY(0); } }
.section-header { margin-bottom: 24px; padding-bottom: 12px; border-bottom: 2px solid var(--rule); display: flex; align-items: baseline; gap: 16px; flex-wrap: wrap; }
.section-num { font-family: 'JetBrains Mono', monospace; font-size: 13px; color: var(--ink3); font-weight: 500; }
.section-title { font-family: 'Fraunces', Georgia, serif; font-size: 32px; font-weight: 500; letter-spacing: -0.01em; }
.section-eng { margin-left: auto; }

/* TECHNICAL */
.tech-grid { display: grid; grid-template-columns: 1fr 1.4fr; gap: 36px; }
.trigger-list .trigger { display: flex; align-items: center; gap: 12px; padding: 10px 12px; margin-bottom: 4px; }
.trigger.bull { background: var(--bull-bg); border-left: 3px solid var(--bull); }
.trigger.bear { background: var(--bear-bg); border-left: 3px solid var(--bear); }
.trigger.neutral { background: var(--paper); border-left: 3px solid var(--neutral); }
.trigger-weight { font-family: 'JetBrains Mono', monospace; font-size: 14px; font-weight: 600; min-width: 42px; }
.trigger.bull .trigger-weight { color: var(--bull); }
.trigger.bear .trigger-weight { color: var(--bear); }
.trigger.neutral .trigger-weight { color: var(--neutral); }
.trigger-text { font-size: 13px; line-height: 1.4; flex: 1; }

table { width: 100%; border-collapse: collapse; font-size: 13px; }
thead tr { border-bottom: 1.5px solid var(--rule); }
th { text-align: left; padding: 8px 6px; font-size: 12px; color: var(--ink3); letter-spacing: 0.1em; text-transform: uppercase; font-weight: 500; }
th.center { text-align: center; }
th.right { text-align: right; }
tbody tr { border-bottom: 0.5px solid var(--line); }
td { padding: 10px 6px; }
td.center { text-align: center; }
td.right { text-align: right; }
.signal-pill { display: inline-block; min-width: 24px; padding: 2px 8px; color: #0E0E10; font-size: 12px; font-weight: 600; letter-spacing: 0.05em; }
.signal-pill.bull, .signal-pill.dyu { background: var(--bull); }
.signal-pill.bear, .signal-pill.kong { background: var(--bear); }
.signal-pill.neutral, .signal-pill.zhong { background: var(--neutral); }

.notes-strip { margin-top: 14px; padding: 12px 14px; background: var(--paper); border-left: 3px solid var(--line); font-size: 12px; color: var(--ink2); line-height: 1.6; }

/* CHIP */
.chip-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; }
.chip-card { padding: 16px 18px; background: var(--bg); }
.section.alt .chip-card { background: var(--bg); }
.chip-card.pos { border-top: 2px solid var(--bull); }
.chip-card.neg { border-top: 2px solid var(--bear); }
.chip-card.mid { border-top: 2px solid var(--neutral); }
.chip-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
.chip-head .smallcaps.pos { color: var(--bull); }
.chip-head .smallcaps.neg { color: var(--bear); }
.chip-head .smallcaps.mid { color: var(--neutral); }
.chip-data { font-family: 'JetBrains Mono', monospace; font-size: 14px; font-weight: 500; margin-bottom: 6px; color: var(--ink); }
.chip-impact { font-size: 12px; color: var(--ink2); line-height: 1.4; }

/* VALUATION */
.scenarios { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-bottom: 36px; }
.scenario { background: var(--paper); padding: 24px 22px; }
.scenario.bear { border-top: 3px solid var(--bear); }
.scenario.base { border-top: 3px solid var(--accent); }
.scenario.bull { border-top: 3px solid var(--bull); }
.scenario .smallcaps { margin-bottom: 16px; }
.scenario.bear .smallcaps { color: var(--bear); }
.scenario.base .smallcaps { color: var(--accent); }
.scenario.bull .smallcaps { color: var(--bull); }
.scenario-price { font-family: 'Fraunces', Georgia, serif; font-size: 44px; font-weight: 500; color: var(--ink); line-height: 1; margin-bottom: 4px; font-variant-numeric: tabular-nums; }
.scenario-upside { font-family: 'JetBrains Mono', monospace; font-size: 13px; margin-bottom: 16px; font-weight: 500; font-variant-numeric: tabular-nums; }
.scenario-detail { font-size: 12px; color: var(--ink2); line-height: 1.6; }
.scenario-detail strong { color: var(--ink); font-weight: 500; }
.scenario-trigger { margin-top: 10px; padding-top: 10px; border-top: 0.5px solid var(--line); color: var(--ink); font-style: italic; font-weight: 500; }

.val-bottom { display: grid; grid-template-columns: 1fr 1fr; gap: 32px; }
.chart-wrap { height: 240px; background: var(--paper); padding: 16px 8px 8px; position: relative; }
.peer-self { background: var(--accent-bg); }
.peer-self td:first-child { font-weight: 600; }

/* INDUSTRY */
.ind-top { display: grid; grid-template-columns: 1fr 1fr; gap: 36px; margin-bottom: 32px; }
.revmix-item { margin-bottom: 14px; }
.revmix-head { display: flex; justify-content: space-between; margin-bottom: 4px; font-size: 14px; }
.revmix-name { font-weight: 500; }
.revmix-pct { font-family: 'JetBrains Mono', monospace; font-weight: 600; font-variant-numeric: tabular-nums; }
.revmix-bar { height: 8px; background: var(--line); }
.revmix-bar-fill { height: 100%; }
.revmix-bar-fill.up { background: var(--bull); }
.revmix-bar-fill.down { background: var(--bear); }
.revmix-bar-fill.flat { background: var(--neutral); }
.revmix-note { font-size: 12px; color: var(--ink2); margin-top: 4px; line-height: 1.4; }

.supply-item { display: flex; gap: 12px; padding: 8px 0; border-bottom: 0.5px solid var(--line); }
.supply-item:last-child { border-bottom: none; }
.supply-tier { font-size: 12px; font-weight: 600; min-width: 32px; padding-top: 2px; letter-spacing: 0.05em; }
.supply-tier.upstream { color: var(--accent); }
.supply-tier.peer { color: var(--neutral); }
.supply-tier.downstream { color: var(--bull); }
.supply-name { font-size: 13px; font-weight: 500; margin-bottom: 2px; }
.supply-relation { font-size: 12px; color: var(--ink2); line-height: 1.4; }

.drivers { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; margin-bottom: 28px; }
.driver { background: var(--bg); padding: 14px 16px; }
.driver.confirmed { border-left: 3px solid var(--bull); }
.driver.optionality { border-left: 3px solid var(--accent); }
.driver-head { display: flex; justify-content: space-between; margin-bottom: 4px; gap: 8px; }
.driver-name { font-size: 13px; font-weight: 500; }
.driver-status { font-family: 'JetBrains Mono', monospace; font-size: 12px; padding: 2px 6px; color: #0E0E10; font-weight: 500; white-space: nowrap; }
.driver-status.confirmed { background: var(--bull); }
.driver-status.optionality { background: var(--accent); }
.driver-horizon { font-size: 12px; color: var(--ink3); margin-bottom: 4px; }
.driver-note { font-size: 12px; color: var(--ink2); line-height: 1.4; }

.moat-box { padding: 20px 24px; background: var(--bg); border-left: 4px solid var(--accent); }
.moat-box .smallcaps { margin-bottom: 8px; color: var(--accent); }
.moat-text { font-family: 'Fraunces', Georgia, serif; font-size: 16px; line-height: 1.6; font-style: italic; color: var(--ink); }

/* BULL/BEAR */
.bb-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0; border: 1px solid var(--line); margin-bottom: 32px; }
.bb-bull { padding: 24px 28px; border-right: 1px solid var(--line); background: var(--bull-bg); }
.bb-bear { padding: 24px 28px; background: var(--bear-bg); }
.bb-head { display: flex; align-items: center; gap: 10px; margin-bottom: 16px; }
.bb-head h3 { font-family: 'Fraunces', Georgia, serif; font-size: 22px; font-weight: 500; }
.bb-head.bull h3 { color: var(--bull); }
.bb-head.bear h3 { color: var(--bear); }
.bb-icon { font-size: 20px; font-weight: bold; }
.bb-icon.bull { color: var(--bull); }
.bb-icon.bear { color: var(--bear); }
.bb-point { display: flex; gap: 10px; padding: 10px 0; align-items: flex-start; }
.bb-point:not(:last-child) { border-bottom: 0.5px solid var(--line); }
.bb-weight { font-size: 12px; padding: 2px 6px; font-weight: 600; min-width: 22px; text-align: center; border: 1px solid; flex-shrink: 0; }
.bb-weight.high { color: var(--bear); border-color: var(--bear); }
.bb-weight.mid { color: var(--accent); border-color: var(--accent); }
.bb-weight.low { color: var(--ink2); border-color: var(--ink3); }
.bb-text { font-size: 13px; line-height: 1.5; flex: 1; color: var(--ink); }

.risks { display: flex; flex-direction: column; gap: 8px; margin-bottom: 32px; }
.risk-row { display: grid; grid-template-columns: 60px 1fr 80px; gap: 16px; padding: 14px 16px; background: var(--paper); align-items: center; }
.risk-sev { font-size: 12px; padding: 3px 8px; color: #0E0E10; font-weight: 600; text-align: center; letter-spacing: 0.05em; }
.risk-sev.high { background: var(--bear); }
.risk-sev.mid { background: var(--accent); }
.risk-sev.low { background: var(--neutral); }
.risk-title { font-size: 14px; font-weight: 500; margin-bottom: 2px; }
.risk-desc { font-size: 12px; color: var(--ink2); line-height: 1.4; }
.risk-horizon { font-family: 'JetBrains Mono', monospace; font-size: 12px; color: var(--ink3); text-align: right; }

.action-plan { background: #1F1B12; color: #EDE6D5; padding: 32px 36px; border: 1px solid #3A311E; }
.ap-head { display: flex; align-items: center; gap: 10px; margin-bottom: 20px; }
.ap-head .smallcaps { color: #D9CBA8; }
.ap-icon { color: #D9CBA8; font-size: 18px; }
.ap-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 24px; margin-bottom: 24px; }
.ap-label { font-size: 12px; color: #A89A78; margin-bottom: 6px; letter-spacing: 0.1em; text-transform: uppercase; }
.ap-value { font-family: 'JetBrains Mono', monospace; font-size: 18px; font-weight: 500; font-variant-numeric: tabular-nums; }
.ap-value.sl { color: #F4A8A8; }
.ap-value.pos { color: #B8D9B8; }
.ap-strategy { border-top: 0.5px solid #3A311E; padding-top: 18px; font-size: 14px; line-height: 1.7; color: #E6E0D0; font-family: 'Fraunces', serif; font-style: italic; }

/* LEFT/RIGHT PLAYBOOK */
.lr-block { margin-top: 32px; }
.lr-head { display: flex; align-items: baseline; gap: 14px; margin-bottom: 14px; flex-wrap: wrap; }
.lr-posture { font-size: 12px; color: var(--ink3); }
.lr-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.lr-card { background: var(--paper); padding: 24px 26px; }
.lr-card.left { border-top: 3px solid var(--bear); }
.lr-card.right { border-top: 3px solid var(--bull); }
.lr-card-title { font-family: 'Fraunces', Georgia, serif; font-size: 20px; font-weight: 500; margin-bottom: 2px; }
.lr-card.left .lr-card-title { color: var(--bear); }
.lr-card.right .lr-card-title { color: var(--bull); }
.lr-card .smallcaps { margin-bottom: 12px; display: block; }
.lr-narrative { font-size: 13px; color: var(--ink2); line-height: 1.6; margin-bottom: 14px; font-style: italic; }
.lr-ladder-action { color: var(--bear); font-weight: 600; }
.lr-line { display: flex; gap: 10px; padding: 9px 0; border-bottom: 0.5px solid var(--line); font-size: 13px; line-height: 1.5; }
.lr-line:last-of-type { border-bottom: none; }
.lr-line-label { font-weight: 600; white-space: nowrap; }
.lr-foot { margin-top: 12px; font-size: 12px; color: var(--ink3); line-height: 1.7; }
.lr-foot .warn { color: var(--bull); }

/* FOOTER */
.footer { padding: 24px 48px; border-top: 2px solid var(--rule); background: var(--paper); display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; }
.footer-meta { font-family: 'JetBrains Mono', monospace; font-size: 12px; color: var(--ink3); }

/* RESPONSIVE */
@media (max-width: 768px) {
  .header { padding: 24px 20px 20px; }
  .header-top { flex-direction: column; }
  .price-block { text-align: left; }
  .company-name { font-size: 36px; }
  .price { font-size: 36px; }
  .verdict-block, .key-block { grid-template-columns: 1fr; }
  .verdict-left, .verdict-right, .key-left, .key-right { padding: 20px; border-right: none; border-bottom: 0.5px solid var(--line); }
  .verdict-text { font-size: 44px; }
  .tab-bar { padding: 0 16px; overflow-x: auto; }
  .tab-list { gap: 20px; white-space: nowrap; }
  .section { padding: 24px 20px; }
  .section-title { font-size: 24px; }
  .tech-grid, .val-bottom, .ind-top, .bb-grid, .lr-grid { grid-template-columns: 1fr; gap: 24px; }
  .bb-bull, .bb-bear { border-right: none; }
  .bb-bear { border-top: 1px solid var(--line); }
  .scenarios, .chip-grid, .drivers { grid-template-columns: 1fr; }
  .ap-grid { grid-template-columns: 1fr; }
  .risk-row { grid-template-columns: 50px 1fr; }
  .risk-horizon { grid-column: 1 / -1; text-align: left; }
  .footer { padding: 16px 20px; }
}
</style>
</head>
<body>

<!-- ================================================================== -->
<!--  ⬇⬇⬇  ONLY EDIT THIS BLOCK — REPLACE THE DATA OBJECT BELOW  ⬇⬇⬇   -->
<!-- ================================================================== -->
<script id="dashboard-data" type="application/json">
{
  "schema_version": "1.1",
  "meta": {
    "ticker": "CRCL",
    "name": "Circle Internet Group",
    "market": "US",
    "generated_at": "2026-05-22 10:30",
    "last_price": 114.88,
    "change_pct": 0.5
  },
  "summary": {
    "verdict": "買進",
    "confidence": "中",
    "trigger_score": -0.62,
    "trend_score": 4.5,
    "percentile": 43.7,
    "regime": "trending",
    "position_adjustment": 1.0,
    "one_liner": "USDC 市佔加速侵蝕 USDT + Arc 網路與 GENIUS Act 雙催化，趨勢強但估值偏貴。",
    "key_points": [
      {"text": "USDC 調整後交易量超越 USDT 達 64% 市佔", "direction": "bull"},
      {"text": "Q1 EPS 0.21 低於預期 0.27，毛利率承壓", "direction": "bear"},
      {"text": "Arc 網路 $222M 預售 BlackRock/a16z 領投", "direction": "bull"},
      {"text": "Hyperliquid 分潤協議估減 EBITDA $80M", "direction": "bear"},
      {"text": "內部人連續買入 138 筆無賣壓 + 紅三兵 K 線", "direction": "bull"}
    ],
    "fundamentals": {"pe": "51.4x (Forward)", "eps": "-0.23 (TTM)", "yield": "N/A", "pb": "8.3x", "roe": "-2.98%"}
  },
  "technical": {
    "triggers": [
      {"type": "bull", "text": "Supertrend 多頭趨勢 (價>94.78)", "weight": 1.0},
      {"type": "bear", "text": "MACD 柱狀體翻綠 (-0.85)", "weight": -1.0},
      {"type": "bear", "text": "KD 死亡交叉 (K27.8 < D37.3)", "weight": -1.0},
      {"type": "bear", "text": "RVOL 0.6x 量縮確認動能弱化", "weight": -0.37},
      {"type": "bull", "text": "內部人積極買入 (買138/賣0)", "weight": 1.5},
      {"type": "bull", "text": "MeanRev +0.20 偏離均線可承接", "weight": 0.2},
      {"type": "bull", "text": "ADX 42.9 強趨勢 + DI+ 31.9 >> DI- 10.4", "weight": 1.0},
      {"type": "bull", "text": "紅三兵 (3 Soldiers) 多方確認", "weight": 0.8}
    ],
    "signals": [
      {"category": "趨勢", "indicator": "MA5/20/60/120", "value": "112.6/111.4/106.8/90.6", "signal": "多", "note": "多頭排列，價格站穩所有均線"},
      {"category": "趨勢", "indicator": "Supertrend", "value": "94.78 (方向=+1)", "signal": "多", "note": "距停損約 22%，趨勢支撐強"},
      {"category": "趨勢", "indicator": "ADX/DMI", "value": "ADX 42.9 / DI+31.9", "signal": "多", "note": "強趨勢且方向偏多"},
      {"category": "動能", "indicator": "MACD", "value": "3.55 (Hist -0.85)", "signal": "中", "note": "柱狀體翻綠，短線動能轉弱"},
      {"category": "動能", "indicator": "KD", "value": "K27.8 / D37.3", "signal": "空", "note": "死亡交叉但接近超賣區"},
      {"category": "動能", "indicator": "RSI", "value": "58.4", "signal": "中", "note": "中性偏多，未過熱"},
      {"category": "量能", "indicator": "RVOL/OBV", "value": "0.64x / 上升中", "signal": "中", "note": "RVOL偏低但OBV斜率+91萬/日"},
      {"category": "波動", "indicator": "BB %B / Width", "value": "57.4% / 42%", "signal": "中", "note": "通道擴張，波動率上升"},
      {"category": "型態", "indicator": "K線型態", "value": "紅三兵 (3 Soldiers)", "signal": "多", "note": "連3根長紅，多方進場確認"},
      {"category": "型態", "indicator": "Squeeze Momentum", "value": "3.47 (Expanding)", "signal": "多", "note": "動能加速擴張"}
    ]
  },
  "chip": {
    "rows": [
      {"category": "機構持股", "data": "63.3% of float", "direction": "正", "impact": "持股結構穩定，籌碼安定"},
      {"category": "內部人交易", "data": "近期 138 買 / 0 賣，淨 +17.16M 股", "direction": "正", "impact": "董事連續買入，信號強烈"},
      {"category": "空單比", "data": "10.2% of float (Days to Cover 2.0)", "direction": "負", "impact": "空頭部位偏高，月增 +0.9%"},
      {"category": "分析師評級", "data": "22 位，買進偏向 (Mean 2.17)", "direction": "正", "impact": "目標價均值 $148，上漲空間 +28.9%"},
      {"category": "近期降評", "data": "Compass Point 降至 Sell 目標 $77", "direction": "負", "impact": "擔憂毛利率壓縮"},
      {"category": "近期升評", "data": "JPM/Needham/Mizuho 上調目標至 $135-155", "direction": "正", "impact": "Arc + Genius Act 為主要催化"},
      {"category": "主力動向", "data": "SoftBank Q1 清倉 95,659 股 ($10.8M)", "direction": "負", "impact": "視為投資組合再平衡，非基本面悲觀"},
      {"category": "浮動股數", "data": "207M / 90% of outstanding", "direction": "中", "impact": "流通性高，散戶參與度高"}
    ]
  },
  "valuation": {
    "current_price": 114.88,
    "scenarios": [
      {"scenario": "bear", "eps_assumption": "2027E EPS 1.5 (執行不及)", "pe_assumption": "45x (估值修正)", "target": 68, "trigger": "Hyperliquid 型分潤協議擴散 + 降息"},
      {"scenario": "base", "eps_assumption": "2027E EPS 2.24 (Forward)", "pe_assumption": "65x (市場均值)", "target": 148, "trigger": "USDC 流通維持 28% YoY + Arc 啟用"},
      {"scenario": "bull", "eps_assumption": "2028E EPS 3.0 (Arc 變現)", "pe_assumption": "90x (AI 溢價)", "target": 280, "trigger": "AI agentic 微交易爆發 + Arc 機構採用"}
    ],
    "peer_comparison": [
      {"ticker": "CRCL", "name": "Circle Internet", "pe": 0.0, "pb": 8.30, "yield": 0.0},
      {"ticker": "ADBE", "name": "Adobe", "pe": 14.2, "pb": 8.67, "yield": 0.0},
      {"ticker": "INTU", "name": "Intuit", "pe": 18.6, "pb": 4.08, "yield": 1.6},
      {"ticker": "CRM", "name": "Salesforce", "pe": 22.6, "pb": 2.77, "yield": 1.0},
      {"ticker": "SAP", "name": "SAP SE", "pe": 23.8, "pb": 3.98, "yield": 1.2}
    ],
    "eps_forecast": [
      {"year": "2024A", "bear": -0.5, "base": -0.5, "bull": -0.5},
      {"year": "2025A", "bear": -0.23, "base": -0.23, "bull": -0.23},
      {"year": "2026E", "bear": 0.85, "base": 1.25, "bull": 1.6},
      {"year": "2027E", "bear": 1.5, "base": 2.24, "bull": 3.0}
    ]
  },
  "industry": {
    "revenue_mix": [
      {"segment": "USDC 儲備利息收入", "pct": 95, "trend": "flat", "note": "短期美債利息為主，受降息壓力影響"},
      {"segment": "訂閱/服務/交易費", "pct": 5, "trend": "up", "note": "管理層 2026 指引 $150-170M，多元化推進"}
    ],
    "supply_chain": [
      {"tier": "upstream", "name": "BlackRock", "relation": "管理 88% 儲備基金，戰略 LP 投資 Arc"},
      {"tier": "upstream", "name": "全球 G-SIB 銀行", "relation": "現金儲備存放，含 BNY/紐約梅隆"},
      {"tier": "peer", "name": "Tether (USDT)", "relation": "市值最大競爭者，市佔 57.96% 但流失中"},
      {"tier": "peer", "name": "Coinbase (COIN)", "relation": "主要分銷夥伴，分享儲備利息收益"},
      {"tier": "downstream", "name": "Meta / DoorDash / Visa", "relation": "B2B 整合，創作者支付與跨境結算"},
      {"tier": "downstream", "name": "Hyperliquid", "relation": "AQA 協議，分潤模式擠壓利潤"}
    ],
    "growth_drivers": [
      {"driver": "USDC 流通量擴大 + 鏈上交易量", "status": "confirmed", "horizon": "1-2年", "note": "Q1 onchain 量 $21.5T (+263% YoY)"},
      {"driver": "Arc 網路機構主鏈商業化", "status": "optionality", "horizon": "2026下半年", "note": "$3B FDV 預售，量子抗性，BlackRock 領投"},
      {"driver": "GENIUS Act + CLARITY Act 監管利多", "status": "confirmed", "horizon": "短期", "note": "美聯邦穩定幣框架已成形，合規優勢放大"},
      {"driver": "AI Agentic Payments 微交易", "status": "optionality", "horizon": "2-3年", "note": "Agent Stack 已上線，AI 應用是長期 TAM 重塑"},
      {"driver": "歐洲 MiCA 合規 + 國際擴張", "status": "confirmed", "horizon": "1-2年", "note": "已取得 EU/UK/Singapore 牌照"}
    ],
    "lead_indicators": [
      {"indicator": "USDC 月度流通量", "current": "$77B", "signal": "持續擴張，與美元降息賽跑"},
      {"indicator": "USDC vs USDT 調整後成交占比", "current": "USDC 64% / USDT 36%", "signal": "USDC 連續超越，趨勢確立"},
      {"indicator": "美聯邦基金利率", "current": "~3.5%", "signal": "降息週期將壓縮儲備利息收入"},
      {"indicator": "CLARITY Act 立法進度", "current": "參議院銀行委員會審議中", "signal": "通過將解除銀行業競爭擔憂"}
    ],
    "moat": "USDC 為合規 first 全球第二大穩定幣 + BlackRock 戰略夥伴 (88% 儲備) + 美/歐/星/英多牌照 + Visa/Coinbase/Meta 機構網路效應 + Arc L1 技術護城河，2026 市佔加速侵蝕 USDT"
  },
  "bull_bear": {
    "bull_points": [
      {"text": "USDC 調整後交易量 64% 領先 USDT，連 2 年成長 >70%", "weight": "高"},
      {"text": "GENIUS Act 通過 + CLARITY Act 推進，監管確定性大增", "weight": "高"},
      {"text": "Arc 網路 $222M 預售 BlackRock/a16z/Apollo 領投", "weight": "高"},
      {"text": "Q1 onchain 量 $21.5T (+263% YoY)，AI agentic 潛力", "weight": "中"},
      {"text": "內部人連續買入 138 筆 + 紅三兵 K 線確認", "weight": "中"},
      {"text": "分析師目標均價 $148，JPM/Needham 升評至 $150+", "weight": "中"}
    ],
    "bear_points": [
      {"text": "PB 8.3x + Forward PE 51x 估值偏貴，F-Score 僅 3/9", "weight": "高"},
      {"text": "TTM EPS -0.23 + ROE -2.98%，獲利尚未轉正", "weight": "高"},
      {"text": "Hyperliquid AQA 分潤協議估減 EBITDA $80M", "weight": "中"},
      {"text": "Compass Point 降至 Sell 目標 $77 (擔憂毛利壓縮)", "weight": "中"},
      {"text": "股價較 52 週高 $299 下跌 62%，市場信心未復", "weight": "中"},
      {"text": "短線 MACD/KD 動能轉空，RVOL 0.6x 量縮", "weight": "低"}
    ],
    "risks": [
      {"risk": "銀行業特許狀爭議", "severity": "高", "horizon": "長期", "description": "傳統銀行可能發行自家美元代幣，威脅 Circle 中介定位"},
      {"risk": "美聯儲降息週期", "severity": "高", "horizon": "短期", "description": "95% 營收來自儲備利息，降息直接壓縮營收與毛利"},
      {"risk": "Hyperliquid 模式擴散", "severity": "中", "horizon": "短期", "description": "DeFi 協議要求分潤，可能持續侵蝕 EBITDA 結構"},
      {"risk": "加密市場系統性下行", "severity": "中", "horizon": "短期", "description": "BTC/ETH 自高點修正 45% 已壓抑 USDC 環比成長"},
      {"risk": "Tether 轉型合規競爭", "severity": "中", "horizon": "長期", "description": "USDT 啟動審計可能取得 GENIUS Act 合規，縮小差距"},
      {"risk": "股票增發稀釋", "severity": "低", "horizon": "長期", "description": "Q1 2025→Q1 2026 流通股自 242M 增至 247M"}
    ],
    "recommendation": {
      "entry_zone": "$112.58 - $114.88 (積極操作 5MA-現價)",
      "stop_loss": "$90.39 (A. ATR 波動停損)",
      "position_size": "標準倉位",
      "strategy": "趨勢分數 +4.5 + 紅三兵 K 線確認 + 內部人積極買入，採 5MA-現價區間積極多單；ATR 動態停損兼顧波動率擴張，RR 3.29 風險報酬比合理"
    }
  },
  "left_right": {
    "posture": "已進入 23.6%-50% 回檔承接區",
    "narrative_left": "相信 USDC 市佔擴張 + Arc 商業化長線敘事，以大波段 Fib 回測階梯在恐慌回檔分批承接。",
    "left_ladder": [
      {"fib": "23.6%", "price": 120.54, "implied_pe": "54x", "action": "首批 1/4"},
      {"fib": "38.2%", "price": 109.73, "implied_pe": "49x", "action": "加碼 1/4"},
      {"fib": "50.0%", "price": 101.0, "implied_pe": "45x", "action": "加碼 1/4"},
      {"fib": "61.8%", "price": 92.27, "implied_pe": "41x", "action": "末批 1/4"}
    ],
    "invalidation": "跌破 $79.84（78.6%）視為長多論述受損，左側部位停損出場",
    "sizing_left": "分批各 1/4 倉位；總曝險上限建議 ≤ 投組 3-5%",
    "narrative_right": "不預測拐點、等趨勢確認；以突破帶量 + 均線多排為進場依據。",
    "right_entries": [
      {"label": "進場 A", "condition": "站穩前高 $138.00 - $141.45 帶量突破 → 順勢看 $158.13 - $183.73"},
      {"label": "進場 B", "condition": "洗盤後重新收復上彎 20MA（回測不破前低）"}
    ],
    "catalysts": "Arc 網路 2026H2 商業化進度；CLARITY Act 參院審議",
    "right_stop": "跌破 $109.73（38.2% 結構頸線）或失守上彎均線",
    "right_trailing": "沿上彎 20MA 拖曳，避免急殺回吐",
    "sizing_right": "盈虧比 ≥ 2 再進場，初始 1/4 倉位，確認趨勢再金字塔加碼"
  }
}
</script>
<!-- ================================================================== -->
<!--  ⬆⬆⬆  END OF DATA BLOCK — DO NOT EDIT BELOW THIS LINE  ⬆⬆⬆        -->
<!-- ================================================================== -->

<div class="container">
  <div id="app"></div>
</div>

<script>
// ============================================================
// StockPulse Dashboard — render engine (do not edit)
// ============================================================
const D = JSON.parse(document.getElementById('dashboard-data').textContent);

const arrow = d => {
  if (d === 'bull' || d === '正' || d === '多') return '<span class="arrow bull">↗</span>';
  if (d === 'bear' || d === '負' || d === '空') return '<span class="arrow bear">↘</span>';
  return '<span class="arrow neutral">—</span>';
};

const sigClass = s => s === '多' ? 'bull' : s === '空' ? 'bear' : 'neutral';
const dirClass = d => d === '正' ? 'pos' : d === '負' ? 'neg' : 'mid';

const fmtChange = pct => {
  const up = pct >= 0;
  return `<span style="color: ${up ? 'var(--bull)' : 'var(--bear)'};">${up ? '▲' : '▼'} ${up ? '+' : ''}${pct.toFixed(2)}%</span>`;
};

// HEADER
function renderHeader() {
  const m = D.meta, s = D.summary;
  return `
  <div class="header">
    <div class="header-top">
      <div>
        <div class="header-meta-tag"><span class="smallcaps">${m.market} · 股票研究儀表板</span></div>
        <h1 class="company-name">${m.name}</h1>
        <div class="ticker-row">
          <span class="ticker">${m.ticker}</span>
          <span class="smallcaps">AS OF ${m.generated_at}</span>
        </div>
      </div>
      <div class="price-block">
        <div class="price">${m.market === 'TW' ? '' : '$'}${m.last_price.toFixed(2)}</div>
        <div class="price-change">${fmtChange(m.change_pct)}</div>
      </div>
    </div>
    <div class="one-liner"><p>"${s.one_liner}"</p></div>
  </div>`;
}

// VERDICT
function scoreBarHTML(value, label, min=-10, max=10) {
  const pct = ((value - min) / (max - min)) * 100;
  const isPos = value >= 0;
  const fillStyle = isPos
    ? `left: 50%; width: ${pct - 50}%; background: var(--bull);`
    : `left: ${pct}%; width: ${50 - pct}%; background: var(--bear);`;
  return `
  <div class="score-bar">
    <div class="score-bar-head">
      <span class="smallcaps">${label}</span>
      <span class="score-bar-value" style="color: ${isPos ? 'var(--bull)' : 'var(--bear)'};">${value > 0 ? '+' : ''}${value.toFixed(2)}</span>
    </div>
    <div class="score-track">
      <div class="score-midline"></div>
      <div class="score-fill" style="${fillStyle}"></div>
    </div>
  </div>`;
}

function renderVerdict() {
  const s = D.summary;
  const colorMap = { '強力買進': 'var(--bull)', '買進': 'var(--bull)', '觀望': 'var(--neutral)', '減碼': 'var(--bear)', '賣出': 'var(--bear)' };
  const c = colorMap[s.verdict] || 'var(--neutral)';
  return `
  <div class="verdict-block">
    <div class="verdict-left">
      <div class="smallcaps" style="margin-bottom: 12px;">VERDICT</div>
      <div class="verdict-text" style="color: ${c};">${s.verdict}</div>
      <div class="verdict-meta">
        <span>信心度 <strong>${s.confidence}</strong></span>
        <span>市場制度 <strong>${s.regime}</strong></span>
        <span>建議倉位 <strong>${s.position_adjustment}x</strong></span>
      </div>
    </div>
    <div class="verdict-right">
      ${scoreBarHTML(s.trigger_score, '觸發分數 TRIGGER')}
      ${scoreBarHTML(s.trend_score, '趨勢分數 TREND')}
      <div>
        <div class="score-bar-head">
          <span class="smallcaps">百分位 PERCENTILE</span>
          <span class="score-bar-value">${s.percentile.toFixed(1)}%</span>
        </div>
        <div class="pct-track">
          <div class="pct-fill" style="width: ${s.percentile}%;"></div>
        </div>
      </div>
    </div>
  </div>`;
}

// KEY POINTS + FUNDAMENTALS
function renderKey() {
  const s = D.summary, f = s.fundamentals;
  const fundEntries = [
    ['本益比 P/E', f.pe], ['每股盈餘 EPS', f.eps], ['股價淨值 P/B', f.pb], ['股東報酬 ROE', f.roe], ['殖利率 Yield', f.yield]
  ];
  return `
  <div class="key-block">
    <div class="key-left">
      <div class="smallcaps">關鍵摘要 · KEY POINTS</div>
      <div class="key-list">
        ${s.key_points.map(p => `
          <div class="key-item">
            ${arrow(p.direction)}
            <p>${p.text}</p>
          </div>
        `).join('')}
      </div>
    </div>
    <div class="key-right">
      <div class="smallcaps">基本面 · FUNDAMENTALS</div>
      <div class="fundamentals">
        ${fundEntries.map(([k, v]) => `
          <div>
            <div class="fund-key">${k}</div>
            <div class="fund-val">${v}</div>
          </div>
        `).join('')}
      </div>
    </div>
  </div>`;
}

// TAB BAR
function renderTabBar() {
  const tabs = [
    { id: 'all', label: '全覽', eng: 'Overview' },
    { id: 'tech', label: '技術', eng: 'Technical' },
    { id: 'chip', label: '籌碼', eng: 'Chip' },
    { id: 'val', label: '估值', eng: 'Valuation' },
    { id: 'ind', label: '產業', eng: 'Industry' },
    { id: 'bb', label: '多空', eng: 'Bull/Bear' }
  ];
  return `
  <div class="tab-bar">
    <div class="tab-list">
      ${tabs.map(t => `<button class="tab-btn ${t.id === 'all' ? 'active' : ''}" data-tab="${t.id}">${t.label}<span class="eng">${t.eng}</span></button>`).join('')}
    </div>
  </div>`;
}

// SECTION HEADER
function sectionHeader(num, title, eng) {
  return `<div class="section-header">
    <span class="section-num">§ ${num}</span>
    <h2 class="section-title">${title}</h2>
    <span class="smallcaps section-eng">${eng}</span>
  </div>`;
}

// TECHNICAL
function renderTech() {
  const t = D.technical;
  return `
  <div class="section" data-section="tech">
    ${sectionHeader('01', '技術面', 'TECHNICAL ANALYSIS')}
    <div class="tech-grid">
      <div>
        <div class="smallcaps" style="margin-bottom: 14px;">觸發訊號 · TRIGGERS</div>
        <div class="trigger-list">
          ${t.triggers.map(tr => `
            <div class="trigger ${tr.type}">
              <span class="trigger-weight">${tr.weight > 0 ? '+' : ''}${tr.weight.toFixed(1)}</span>
              <span class="trigger-text">${tr.text}</span>
            </div>
          `).join('')}
        </div>
      </div>
      <div>
        <div class="smallcaps" style="margin-bottom: 14px;">指標總覽 · INDICATOR MATRIX</div>
        <table>
          <thead><tr>
            <th>類別</th><th>指標</th><th>數值</th><th class="center">方向</th>
          </tr></thead>
          <tbody>
            ${t.signals.map(s => `
              <tr class="hover-row">
                <td style="color: var(--ink2); font-size: 12px;">${s.category}</td>
                <td style="font-weight: 500;">${s.indicator}</td>
                <td class="num-mono">${s.value}</td>
                <td class="center"><span class="signal-pill ${sigClass(s.signal)}">${s.signal}</span></td>
              </tr>
            `).join('')}
          </tbody>
        </table>
        <div class="notes-strip">${t.signals.slice(0, 3).map(s => `${s.indicator}: ${s.note}`).join(' · ')}</div>
      </div>
    </div>
  </div>`;
}

// CHIP
function renderChip() {
  return `
  <div class="section alt" data-section="chip">
    ${sectionHeader('02', '籌碼面', 'OWNERSHIP & FLOW')}
    <div class="chip-grid">
      ${D.chip.rows.map(r => `
        <div class="chip-card ${dirClass(r.direction)}">
          <div class="chip-head">
            <span class="smallcaps ${dirClass(r.direction)}">${r.category}</span>
            ${arrow(r.direction)}
          </div>
          <div class="chip-data">${r.data}</div>
          <div class="chip-impact">${r.impact}</div>
        </div>
      `).join('')}
    </div>
  </div>`;
}

// VALUATION
function renderVal() {
  const v = D.valuation;
  const labelMap = { bear: '空方', base: '基本', bull: '多方' };
  return `
  <div class="section" data-section="val">
    ${sectionHeader('03', '估值推演', 'VALUATION SCENARIOS')}
    <div class="scenarios">
      ${v.scenarios.map(s => {
        const upside = ((s.target - v.current_price) / v.current_price * 100);
        const upColor = upside >= 0 ? 'var(--bull)' : 'var(--bear)';
        return `
        <div class="scenario ${s.scenario}">
          <div class="smallcaps">${labelMap[s.scenario]} SCENARIO</div>
          <div class="scenario-price">${D.meta.market === 'TW' ? '' : '$'}${s.target}</div>
          <div class="scenario-upside" style="color: ${upColor};">${upside >= 0 ? '+' : ''}${upside.toFixed(1)}% vs ${D.meta.market === 'TW' ? '' : '$'}${v.current_price.toFixed(2)}</div>
          <div class="scenario-detail">
            <div style="margin-bottom: 4px;"><strong>EPS:</strong> ${s.eps_assumption}</div>
            <div style="margin-bottom: 4px;"><strong>P/E:</strong> ${s.pe_assumption}</div>
            <div class="scenario-trigger">Trigger: ${s.trigger}</div>
          </div>
        </div>`;
      }).join('')}
    </div>
    <div class="val-bottom">
      <div>
        <div class="smallcaps" style="margin-bottom: 14px;">EPS 三情境預測 · EPS FORECAST</div>
        <div class="chart-wrap"><canvas id="eps-chart" role="img" aria-label="EPS forecast bar chart by scenario"></canvas></div>
      </div>
      <div>
        <div class="smallcaps" style="margin-bottom: 14px;">同業估值比較 · PEER COMPARISON</div>
        <table>
          <thead><tr>
            <th>Ticker</th><th>公司</th><th class="right">P/E</th><th class="right">P/B</th><th class="right">Yield</th>
          </tr></thead>
          <tbody>
            ${v.peer_comparison.map((p, i) => `
              <tr class="${i === 0 ? 'peer-self' : ''}">
                <td class="num-mono" style="font-weight: ${i === 0 ? 600 : 500};">${p.ticker}</td>
                <td style="font-size: 12px; color: var(--ink2); font-weight: ${i === 0 ? 500 : 400};">${p.name}</td>
                <td class="right num-mono">${p.pe === 0 ? 'N/A' : p.pe.toFixed(1)}</td>
                <td class="right num-mono">${p.pb.toFixed(2)}</td>
                <td class="right num-mono">${p.yield > 0 ? p.yield.toFixed(1) + '%' : '—'}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </div>
  </div>`;
}

// INDUSTRY
function renderInd() {
  const i = D.industry;
  const tierLabel = { upstream: '上游', peer: '同業', downstream: '下游' };
  return `
  <div class="section alt" data-section="ind">
    ${sectionHeader('04', '產業地圖', 'INDUSTRY POSITIONING')}
    <div class="ind-top">
      <div>
        <div class="smallcaps" style="margin-bottom: 14px;">營收結構 · REVENUE MIX</div>
        ${i.revenue_mix.map(r => `
          <div class="revmix-item">
            <div class="revmix-head">
              <span class="revmix-name">${r.segment}</span>
              <span class="revmix-pct">${r.pct}%</span>
            </div>
            <div class="revmix-bar"><div class="revmix-bar-fill ${r.trend}" style="width: ${r.pct}%;"></div></div>
            <div class="revmix-note">${r.note}</div>
          </div>
        `).join('')}
      </div>
      <div>
        <div class="smallcaps" style="margin-bottom: 14px;">供應鏈節點 · SUPPLY CHAIN</div>
        ${i.supply_chain.map(s => `
          <div class="supply-item">
            <span class="supply-tier ${s.tier}">${tierLabel[s.tier]}</span>
            <div style="flex: 1;">
              <div class="supply-name">${s.name}</div>
              <div class="supply-relation">${s.relation}</div>
            </div>
          </div>
        `).join('')}
      </div>
    </div>
    <div style="margin-bottom: 28px;">
      <div class="smallcaps" style="margin-bottom: 14px;">成長動能 · GROWTH DRIVERS</div>
      <div class="drivers">
        ${i.growth_drivers.map(g => `
          <div class="driver ${g.status}">
            <div class="driver-head">
              <span class="driver-name">${g.driver}</span>
              <span class="driver-status ${g.status}">${g.status === 'confirmed' ? '確定' : '選擇權'}</span>
            </div>
            <div class="driver-horizon">時程 · ${g.horizon}</div>
            <div class="driver-note">${g.note}</div>
          </div>
        `).join('')}
      </div>
    </div>
    <div style="margin-bottom: 28px;">
      <div class="smallcaps" style="margin-bottom: 14px;">領先指標 · LEAD INDICATORS</div>
      <table>
        <thead><tr><th>指標</th><th>現況</th><th>訊號</th></tr></thead>
        <tbody>
          ${i.lead_indicators.map(l => `
            <tr class="hover-row">
              <td style="font-weight: 500;">${l.indicator}</td>
              <td class="num-mono" style="font-size: 12px;">${l.current}</td>
              <td style="font-size: 12px; color: var(--ink2);">${l.signal}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
    <div class="moat-box">
      <div class="smallcaps">護城河 · MOAT</div>
      <p class="moat-text">${i.moat}</p>
    </div>
  </div>`;
}

// LEFT/RIGHT PLAYBOOK (schema v1.1; renders nothing for legacy JSON or null)
function renderLR() {
  const lr = D.left_right;
  if (!lr || !Array.isArray(lr.left_ladder) || lr.left_ladder.length === 0) return '';
  const cur = D.meta.market === 'TW' ? '' : '$';
  return `
    <div class="lr-block">
      <div class="lr-head">
        <span class="smallcaps" style="color: var(--accent);">左側 | 右側策略 · LEFT / RIGHT PLAYBOOK</span>
        ${lr.posture ? `<span class="lr-posture">${lr.posture}</span>` : ''}
      </div>
      <div class="lr-grid">
        <div class="lr-card left">
          <div class="lr-card-title">左側 · 逆勢分批</div>
          ${lr.narrative_left ? `<p class="lr-narrative">${lr.narrative_left}</p>` : ''}
          <table>
            <thead><tr>
              <th>回測</th><th class="right">價位</th><th class="right">隱含 PE</th><th class="right">動作</th>
            </tr></thead>
            <tbody>
              ${lr.left_ladder.map(r => `
                <tr class="hover-row">
                  <td class="num-mono" style="color: var(--accent);">${r.fib}</td>
                  <td class="right num-mono" style="font-weight: 600;">${cur}${r.price}</td>
                  <td class="right num-mono" style="color: var(--ink2);">${r.implied_pe}</td>
                  <td class="right lr-ladder-action">${r.action}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
          <div class="lr-foot">
            ${lr.sizing_left ? `<div>· ${lr.sizing_left}</div>` : ''}
            ${lr.invalidation ? `<div class="warn">· ${lr.invalidation}</div>` : ''}
          </div>
        </div>
        <div class="lr-card right">
          <div class="lr-card-title">右側 · 順勢突破</div>
          ${lr.narrative_right ? `<p class="lr-narrative">${lr.narrative_right}</p>` : ''}
          ${(lr.right_entries || []).map(e => `
            <div class="lr-line"><span class="lr-line-label" style="color: var(--bull);">${e.label}</span><span>${e.condition}</span></div>
          `).join('')}
          ${lr.catalysts ? `<div class="lr-line"><span class="lr-line-label" style="color: var(--accent);">催化檢查點</span><span>${lr.catalysts}</span></div>` : ''}
          ${lr.right_stop ? `<div class="lr-line"><span class="lr-line-label" style="color: var(--bear);">停損</span><span>${lr.right_stop}</span></div>` : ''}
          ${lr.right_trailing ? `<div class="lr-line"><span class="lr-line-label" style="color: var(--ink2);">移動停利</span><span>${lr.right_trailing}</span></div>` : ''}
          ${lr.sizing_right ? `<div class="lr-foot">· ${lr.sizing_right}</div>` : ''}
        </div>
      </div>
    </div>`;
}

// BULL/BEAR
function renderBB() {
  const b = D.bull_bear;
  const wClass = w => w === '高' ? 'high' : w === '中' ? 'mid' : 'low';
  const sevClass = s => s === '高' ? 'high' : s === '中' ? 'mid' : 'low';
  const r = b.recommendation;
  return `
  <div class="section" data-section="bb">
    ${sectionHeader('05', '多空辯論', 'BULL VS BEAR')}
    <div class="bb-grid">
      <div class="bb-bull">
        <div class="bb-head bull"><span class="bb-icon bull">↗</span><h3>多方論點</h3></div>
        ${b.bull_points.map(p => `
          <div class="bb-point">
            <span class="bb-weight ${wClass(p.weight)}">${p.weight}</span>
            <span class="bb-text">${p.text}</span>
          </div>
        `).join('')}
      </div>
      <div class="bb-bear">
        <div class="bb-head bear"><span class="bb-icon bear">↘</span><h3>空方論點</h3></div>
        ${b.bear_points.map(p => `
          <div class="bb-point">
            <span class="bb-weight ${wClass(p.weight)}">${p.weight}</span>
            <span class="bb-text">${p.text}</span>
          </div>
        `).join('')}
      </div>
    </div>
    <div style="margin-bottom: 32px;">
      <div class="smallcaps" style="margin-bottom: 14px;">主要風險 · KEY RISKS</div>
      <div class="risks">
        ${b.risks.map(rk => `
          <div class="risk-row">
            <span class="risk-sev ${sevClass(rk.severity)}">${rk.severity} 級</span>
            <div>
              <div class="risk-title">${rk.risk}</div>
              <div class="risk-desc">${rk.description}</div>
            </div>
            <span class="risk-horizon">${rk.horizon}</span>
          </div>
        `).join('')}
      </div>
    </div>
    <div class="action-plan">
      <div class="ap-head">
        <span class="ap-icon">◎</span>
        <span class="smallcaps">操作建議 · ACTION PLAN</span>
      </div>
      <div class="ap-grid">
        <div>
          <div class="ap-label">進場區間</div>
          <div class="ap-value">${r.entry_zone}</div>
        </div>
        <div>
          <div class="ap-label">停損價</div>
          <div class="ap-value sl">${r.stop_loss}</div>
        </div>
        <div>
          <div class="ap-label">建議倉位</div>
          <div class="ap-value pos">${r.position_size}</div>
        </div>
      </div>
      <div class="ap-strategy">"${r.strategy}"</div>
    </div>
    ${renderLR()}
  </div>`;
}

function renderFooter() {
  return `
  <div class="footer">
    <div class="smallcaps">StockPulse · Equity Research Dashboard</div>
    <div class="footer-meta">Generated ${D.meta.generated_at} · Not investment advice</div>
  </div>`;
}

// MOUNT
const app = document.getElementById('app');
app.innerHTML =
  renderHeader() +
  renderVerdict() +
  renderKey() +
  renderTabBar() +
  '<div id="sections">' +
  renderTech() +
  renderChip() +
  renderVal() +
  renderInd() +
  renderBB() +
  '</div>' +
  renderFooter();

// CHART
function drawChart() {
  const ctx = document.getElementById('eps-chart');
  if (!ctx) return;
  const v = D.valuation.eps_forecast;
  new Chart(ctx, {
    type: 'bar',
    data: {
      labels: v.map(d => d.year),
      datasets: [
        { label: '空方', data: v.map(d => d.bear), backgroundColor: '#5FB97A' },
        { label: '基本', data: v.map(d => d.base), backgroundColor: '#D9A93F' },
        { label: '多方', data: v.map(d => d.bull), backgroundColor: '#E07B7B' }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom', labels: { font: { family: 'DM Sans', size: 12 }, color: '#A8A192', boxWidth: 12, boxHeight: 12 } },
        tooltip: { backgroundColor: '#17171A', titleColor: '#EDE6D5', bodyColor: '#EDE6D5', borderColor: '#2D2A24', borderWidth: 1, titleFont: { family: 'DM Sans' }, bodyFont: { family: 'JetBrains Mono' } }
      },
      scales: {
        x: { ticks: { font: { family: 'JetBrains Mono', size: 12 }, color: '#A8A192' }, grid: { display: false } },
        y: { ticks: { font: { family: 'JetBrains Mono', size: 12 }, color: '#A8A192' }, grid: { color: '#2D2A24', drawBorder: false } }
      }
    }
  });
}
drawChart();

// TAB SWITCHING
function setTab(id) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === id));
  const sections = document.querySelectorAll('[data-section]');
  if (id === 'all') {
    sections.forEach(s => s.style.display = '');
  } else {
    sections.forEach(s => s.style.display = s.dataset.section === id ? '' : 'none');
  }
  // re-draw chart if val is visible
  if (id === 'all' || id === 'val') {
    setTimeout(() => {
      const old = document.getElementById('eps-chart');
      if (old && !old._chartDrawn) { drawChart(); old._chartDrawn = true; }
    }, 50);
  }
}
document.querySelectorAll('.tab-btn').forEach(b => b.addEventListener('click', () => setTab(b.dataset.tab)));
</script>
</body>
</html>

<<<HTML_TEMPLATE_END>>>
