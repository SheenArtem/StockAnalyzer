"""End-to-end deep research: single-prompt + WebSearch -> multi-agent stress-test.

Two-stage pipeline (commit c81cf80 architecture):
  Stage 1: ai_report_pipeline.generate_one_report()
    - 1 Claude CLI call with WebSearch/WebFetch
    - 30+ URL search, 14-section data block, ~4 min, 1 quota call
  Stage 2: multi_agent_debate_poc.run_pipeline(input_report=...)
    - 10 Claude CLI calls (Bull/Bear x2 rounds + ResearchMgr + Trader + 3 Risk + PM)
    - Pure reasoning over Stage 1 markdown (no extra WebSearch)
    - ~5 min, 10 quota calls

Output: reports/deep_research_<ticker>_<date>.md (Stage 1 + Stage 2 combined)

CLI:
    python tools/single_then_multi.py --ticker NVDA              # default 2 rounds
    python tools/single_then_multi.py --ticker 2330 --rounds 1   # faster
    python tools/single_then_multi.py --ticker NVDA --skip-stage1 \
        --stage1-report data/ai_reports/NVDA_xxx.md  # reuse cached Stage 1

Total cost: 11 CLI calls, 8-10 min, $0 API (Team Plan quota).
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("deep_research")

OUT_DIR = REPO / "reports"
AI_REPORTS_DIR = REPO / "data" / "ai_reports"


def _run_stage1(ticker: str) -> Path:
    """Run ai_report_pipeline single-prompt + WebSearch. Return markdown path."""
    from ai_report_pipeline import generate_one_report

    logger.info(f"=== STAGE 1: single-prompt + WebSearch for {ticker} ===")
    t0 = time.time()
    result = generate_one_report(ticker, fmt='md', progress_cb=lambda m: None)
    elapsed = time.time() - t0

    if not result['ok']:
        raise RuntimeError(f"Stage 1 failed: {result.get('error')}")

    rid = result['rid']
    md_path = AI_REPORTS_DIR / f"{rid}.md"
    if not md_path.exists():
        raise RuntimeError(f"Stage 1 output not found at {md_path}")

    logger.info(f"Stage 1 OK ({elapsed:.0f}s, {md_path.stat().st_size // 1024} KB) -> {md_path.name}")
    return md_path


def _run_stage2(ticker: str, rounds: int, input_report: Path) -> dict:
    """Run multi-agent debate over Stage 1 report."""
    from multi_agent_debate_poc import run_pipeline

    logger.info(f"=== STAGE 2: multi-agent debate (rounds={rounds}) ===")
    t0 = time.time()
    out = run_pipeline(ticker, rounds=rounds, input_report=input_report)
    elapsed = time.time() - t0
    logger.info(f"Stage 2 OK ({elapsed:.0f}s)")
    out['_stage2_elapsed_s'] = elapsed
    return out


def _write_combined_report(ticker: str, stage1_md: Path, stage2: dict,
                            rounds: int, total_elapsed: float) -> Path:
    """Write combined Stage 1 + Stage 2 markdown report."""
    stage1_text = stage1_md.read_text(encoding='utf-8')

    md = [
        f"# Deep Research — {ticker}",
        f"\n**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"\n**Total elapsed**: {total_elapsed/60:.1f} min  ",
        f"\n**CLI calls**: 11 (1 Stage 1 + 10 Stage 2)  ",
        f"\n**Cost**: $0 (Team Plan quota)",
        "\n\n---\n",
        "# Stage 2: Multi-Agent Debate (Adversarial Stress-Test)",
        "\n*Order: Stage 2 first because PM final decision is the actionable output.*",
        "\n*Stage 1 (single-prompt + WebSearch broad data) appended below as supporting material.*",
        "\n\n## Portfolio Manager Final Decision\n",
        stage2['portfolio_mgr'],
        "\n\n## Trader Plan\n",
        stage2['trader'],
        "\n\n## Research Manager Verdict\n",
        stage2['research_mgr'],
        "\n\n## Risk Debate\n",
        "\n### Aggressive\n", stage2['risk_aggressive'],
        "\n\n### Conservative\n", stage2['risk_conservative'],
        "\n\n### Neutral (Arbitration)\n", stage2['risk_neutral'],
        "\n\n## Bull vs Bear Debate\n",
        stage2['debate_history'],
        "\n\n---\n",
        "# Stage 1: Single-Prompt + WebSearch Broad Data\n",
        f"*Source: `{stage1_md.relative_to(REPO)}`*\n",
        stage1_text,
    ]

    date_str = datetime.now().strftime("%Y%m%d_%H%M")
    out_path = OUT_DIR / f"deep_research_{ticker}_{date_str}.md"
    out_path.write_text("\n".join(md), encoding='utf-8')
    logger.info(f"Combined report: {out_path}")
    return out_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", required=True)
    ap.add_argument("--rounds", type=int, default=2,
                    help="Bull/Bear debate rounds in Stage 2 (default 2)")
    ap.add_argument("--skip-stage1", action="store_true",
                    help="Skip Stage 1 and use --stage1-report (cached run)")
    ap.add_argument("--stage1-report", type=Path,
                    help="Path to existing Stage 1 markdown (with --skip-stage1)")
    args = ap.parse_args()

    if args.skip_stage1 and not args.stage1_report:
        sys.stderr.write("--skip-stage1 requires --stage1-report\n")
        sys.exit(1)

    t0 = time.time()

    if args.skip_stage1:
        stage1_md = args.stage1_report.resolve()  # absolute for relative_to() later
        if not stage1_md.exists():
            sys.stderr.write(f"Stage 1 report not found: {stage1_md}\n")
            sys.exit(1)
        logger.info(f"Using cached Stage 1: {stage1_md}")
    else:
        stage1_md = _run_stage1(args.ticker)

    stage2 = _run_stage2(args.ticker, rounds=args.rounds, input_report=stage1_md)

    total_elapsed = time.time() - t0
    out_path = _write_combined_report(args.ticker, stage1_md, stage2,
                                       rounds=args.rounds, total_elapsed=total_elapsed)

    logger.info(f"\n=== DEEP RESEARCH COMPLETE ===")
    logger.info(f"Ticker: {args.ticker}")
    logger.info(f"Total elapsed: {total_elapsed/60:.1f} min")
    logger.info(f"Output: {out_path}")
    logger.info(f"Final decision excerpt:")
    pm_first_lines = stage2['portfolio_mgr'].split('\n')[:6]
    for ln in pm_first_lines:
        logger.info(f"  {ln}")


if __name__ == "__main__":
    main()
