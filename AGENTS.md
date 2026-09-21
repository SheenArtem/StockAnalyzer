# Repository Agent Rules — StockAnalyzer

StockAnalyzer is a TW/US financial decision-support system. This file is the cross-agent authority;
tool-specific adapters import it and own no project rules.

## Read First

- Before continuing or scoping work, read `.ai/HANDOFF.md`; it records current and uncommitted state.
- Before reviewing, changing, running, or testing application, scanner, scheduler, cache, or report
  code, read `docs/agent/PROJECT_GUIDE.md`.
- Before any code that calls Claude CLI or an LLM SDK, read `docs/agent/llm-usage.md`. Model, effort,
  timeout, and provider choices are prescribed there.
- Before changing a fetcher, data source, cache, fallback, market classification, panel, or date
  alignment, read `docs/agent/data-sources.md`.
- Before reopening tech-debt or won't-fix items, read `docs/project_review_and_roadmap.md` Part 1.

## Always-On Boundaries

- Correctness outranks convenience: this system influences financial decisions. Validate executable
  paths and fail loudly; never present an unverified experiment as a production-ready result.
- Preserve scheduled-data and other user changes. Commit, push, schedule mutation, network calls that
  consume quota, and other external actions require the applicable explicit request.
- `.bat` files are ASCII-only and CRLF. Preserve every tracked file's existing encoding and EOL.
- Never store credential values in the repository or `.ai/`.
- Keep current state in HANDOFF, history in `.ai/WORKLOG.md`, decisions in `.ai/DECISIONS.md`, and
  detailed durable rules in the routed guides instead of expanding this file.
