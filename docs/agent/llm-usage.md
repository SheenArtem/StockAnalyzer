# LLM Usage Rules (mandatory)

任何呼叫 Claude CLI / LLM SDK 的程式碼必讀（route 自 `AGENTS.md`）。

| Module | LLM | model flag | effort | extra flag | timeout |
|---|---|---|---|---|---|
| **AI Report** (`ai_report.py` / `ai_report_pipeline.py` / `strong_stocks_ai_analysis.py`) | Claude | `--model claude-opus-4-8[1m]` | `--effort max` | `--allowedTools "*"` | 7200s (2h) |
| **News / short-form / metadata extract** | Claude | `--model sonnet` | `--effort xhigh` | (optional) `--allowedTools` | 600s |
| **Calendar / structured table extract** | Claude | `--model haiku` | — (fast+cheap, no thinking) | — | 600s |
| **Sector tag extract (YT VTT / batch)** | Claude | `--model sonnet` | `--effort xhigh` | — | 600s |
| **Brokerage YT extract** (`tools/extract_yt_brokerage.py`) | codex GPT-5.5 (primary) + Claude Sonnet (fallback) | codex `-c model_reasoning_effort=medium` / claude `--model sonnet` | claude `--effort xhigh` | — | 600s |
| **Multi-agent debate / exploratory** + **AI Report 研究階段** (`report_web_research.py`) | Claude | `--model sonnet` | `--effort xhigh` | `--allowedTools "WebSearch,WebFetch"` | 600s |
| **Macro Compass 第二視角** (`tools/macro_compass_report.py`) | Claude | `--model sonnet`/`opus` | `--effort xhigh` | `--allowedTools "WebSearch,WebFetch"` | 7200s (2h) |
| **Theme curation** (`tools/curate_themes_pipeline.py`) | Claude | `--model sonnet` | `--effort xhigh` | `--allowedTools "WebSearch,WebFetch" --output-format json` | 420s/單題材 |
| **知識庫文章清洗** (`tools/build_baihua_kb.py`) | Claude | `--model sonnet` | `--effort xhigh` | `--output-format json` | 600s/單篇 |

**⚠️ `--effort` 強制**：`claude -p` **不繼承** `~/.claude/settings.json` 的 `effortLevel`（即使設 max 也 0 reasoning tokens）— 必須 CLI 顯式帶 `--effort`。Haiku 例外（不開 thinking）。

**How to apply**:

- New call → pick from table；model + effort + timeout 必須照表
- Grep before changing — `claude.*-p` / `--model` / `--effort`
- AI Report 必須 Opus 4.8 1M + effort max + `--allowedTools "*"`
- No `timeout=None` — 一律明確秒數
- 新增 codex / OpenAI / 其他 provider → 先在此表加列 + 註明 fallback 順序

> 設計緣由（Opus/Sonnet/Haiku 分工、Gemini 撤除史、codex A/B 結果）見 Claude memory `feedback_llm_usage_rules`。
