"""C3 Mini-POC: 5-layer multi-agent debate pipeline (Claude CLI, no LangGraph).

借 TradingAgents 的辯論架構，但抽掉 LangGraph + LangChain + langchain-anthropic
三層依賴。資料用我們既有 ai_report.py 的 assemble_prompt 預先組好，
每個 agent 拿同一份資料 + 自己的 role prompt，純 Python while loop 串。

Pipeline:
  1. Data assembly (reuse ai_report.assemble_prompt)
  2. Bull <-> Bear debate (N rounds, default 2)
  3. Research Manager judge (Buy/Hold/Sell)
  4. Trader plan (entry/exit/sizing)
  5. 3 Risk debators (Aggressive / Conservative / Neutral)
  6. Portfolio Manager final (Buy/Overweight/Hold/Underweight/Sell + size)

Output: reports/multi_agent_debate_<ticker>_<date>.md (full debate log)

CLI:
    python tools/multi_agent_debate_poc.py --ticker NVDA              # default 2 rounds
    python tools/multi_agent_debate_poc.py --ticker NVDA --rounds 1   # faster
    python tools/multi_agent_debate_poc.py --ticker 2330.TW --rounds 2

Cost: 10-12 Claude CLI calls (Team Plan), $0 API. ~5-10 min sequential.
"""
from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("multi_agent_poc")

_CLAUDE_CLI = shutil.which("claude") or "claude"

OUT_DIR = REPO / "reports"
OUT_DIR.mkdir(exist_ok=True)


# ============================================================
# Agent role prompts (system instructions per role)
# ============================================================

_LANG = "**全部用繁體中文回覆**（專業術語可保留英文，e.g. ADX / RSI / Forward PE）。"

BULL_SYS = f"""你是 Bull Analyst（多方分析師），任務是用提供的資料建立最強的「**做多**」論證。

直接反駁對手 Bear 上一輪的論點，cite 具體數字。

聚焦：
- 成長動能、毛利擴張、市場機會
- 護城河、產業順風、近期正向催化
- 為什麼估值疑慮被誇大（給成長 profile 不是貴）
- **逐點反駁 Bear 具體 claim**（用數字打）

風格：直接、犀利，200-400 字。最強論點開頭。
結尾一句：「Bull thesis in one line: ...」

{_LANG}
"""

BEAR_SYS = f"""你是 Bear Analyst（空方分析師），任務是用提供的資料建立最強的「**做空 / 不買**」論證。

直接反駁對手 Bull 上一輪的論點，cite 具體數字。

聚焦：
- 估值風險、毛利壓力、減速訊號
- 競爭威脅、監管風險、需求循環
- Bull 在哪些地方用敘事帶過真正的 risk
- **逐點反駁 Bull 具體 claim**（用數字打）

風格：直接、犀利，200-400 字。最強論點開頭。
結尾一句：「Bear thesis in one line: ...」

{_LANG}
"""

RESEARCH_MGR_SYS = f"""你是 Research Manager，裁判 Bull vs Bear 辯論。

你看到：完整辯論逐字稿 + 原始資料。輸出：

1. **Verdict**：BUY / HOLD / SELL（不准 hedge，三選一）
2. **理由**：哪邊論點較強為什麼 — cite 辯論中具體 point，不要通用套話
3. **未解問題**：什麼會改變你的判定？

殘酷一點。**雙方論點都弱時，HOLD 才是誠實的**。

風格：300-500 字。「Verdict: BUY/HOLD/SELL」獨立一行。

{_LANG}
"""

TRADER_SYS = f"""你是 Trader，把 Research Manager 的 verdict 轉成可執行交易計畫。

你看到：完整辯論 + Manager verdict + 原始資料。

輸出：
1. **部位大小**：% of portfolio（HOLD/SELL 時 = 0%）
2. **進場計畫**：具體價位 / 觸發條件 / 時間 horizon
3. **停損價**：具體價（用 ATR 或 support level）
4. **目標價**：1-2 個 TP 帶理由
5. **時間 horizon**：天 / 週 / 月

數字要具體。不准用「持續觀察」這種 vague 字眼。
風格：200-300 字編號列表。

{_LANG}
"""

RISK_AGGRESSIVE_SYS = f"""你是 AGGRESSIVE Risk Debator（激進派風控辯手）。

你看到：Trader 計畫。任務：論證計畫**太保守**。
- 該不該加碼？
- 停損是不是太緊？
- 是不是把 alpha 留在桌上？

風格：150-250 字。cite trader 計畫具體數字提你的改法。

{_LANG}
"""

RISK_CONSERVATIVE_SYS = f"""你是 CONSERVATIVE Risk Debator（保守派風控辯手）。

你看到：Trader 計畫。任務：論證計畫**太激進**。
- 該不該降碼或直接 pass？
- Trader 忽視什麼下檔情境？
- 哪個 max-drawdown 點變得無法接受？

風格：150-250 字。cite trader 計畫具體數字提你的改法。

{_LANG}
"""

RISK_NEUTRAL_SYS = f"""你是 NEUTRAL Risk Debator（中性派風控仲裁）。

你看到：Trader 計畫 + Aggressive 批評 + Conservative 批評。
任務：仲裁，哪邊批評更有 merit？

風格：150-250 字。結尾**具體調整提案**（e.g.「部位由 5% 降到 3%，停損由 -8% 放寬到 -10%」）。

{_LANG}
"""

PM_SYS = f"""你是 Portfolio Manager，做**最終**決策。

你看到：完整辯論 + Trader 計畫 + 3 個 Risk debator 意見 + 原始資料。

輸出（**按此順序**）：

1. **最終決策**：BUY / OVERWEIGHT / HOLD / UNDERWEIGHT / SELL
2. **最終部位大小**：% of portfolio（具體數字）
3. **信心水準**：Low / Medium / High（一句話 justify）
4. **最重要的單一監控風險**：一件會觸發你重新評估的事
5. **一句話 thesis 總結**

果斷。這是要被執行的。
風格：編號列表，200-300 字。

{_LANG}
"""


# ============================================================
# CLI subprocess helper
# ============================================================

def call_cli(system_prompt: str, user_prompt: str, role: str,
             timeout: int = 600) -> str:
    """Call Claude CLI with system + user prompts. Returns response text.

    Combines into one stdin since CLI doesn't support separate system prompts
    via flag in this version.
    """
    full = f"{system_prompt}\n\n---\n\n{user_prompt}"
    t0 = time.time()
    logger.info(f"[{role}] calling CLI (prompt {len(full)} chars)...")
    try:
        # LLM 規範 (2026-05-01)：debate role 屬 News/分析類 → Sonnet + 10 min timeout
        result = subprocess.run(
            [_CLAUDE_CLI, "-p",
             "--model", "sonnet",
             "--allowedTools", "WebSearch,WebFetch",
             "--output-format", "text"],
            input=full,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding='utf-8',
        )
    except subprocess.TimeoutExpired:
        logger.error(f"[{role}] timeout after {timeout}s")
        return f"[ERROR: {role} timeout]"

    elapsed = time.time() - t0
    if result.returncode != 0:
        err = result.stderr.strip()[:500] if result.stderr else "Unknown"
        logger.error(f"[{role}] CLI exit {result.returncode}: {err}")
        return f"[ERROR: {role} CLI exit {result.returncode}]"

    output = result.stdout.strip()
    logger.info(f"[{role}] done in {elapsed:.0f}s ({len(output)} chars)")
    return output


# ============================================================
# Pipeline
# ============================================================

def _load_input_report(path: Path) -> str:
    """Load existing single-prompt ai_report markdown as enriched data block.

    Two-stage architecture: stage 1 = ai_report.py with WebSearch broad-data,
    stage 2 = multi-agent debate stress-test on stage-1 output.
    """
    text = path.read_text(encoding="utf-8")
    return (f"以下是已經過 single-prompt + WebSearch 處理過的「資料密集型 Stage 1 報告」"
            f"（含 30+ web sources 引用 + 全 14 個資料區塊綜合）。"
            f"你的任務是對這份報告做**對抗性壓測**，不是重新做研究。\n\n"
            f"=== Stage 1 Report (源檔: {path.name}) ===\n\n"
            f"{text}\n\n"
            f"=== Stage 1 Report END ===")


def run_pipeline(ticker: str, rounds: int = 2,
                 input_report: Path | None = None) -> dict:
    """Run full debate pipeline. Returns dict of all stage outputs.

    Args:
        ticker: stock id
        rounds: Bull/Bear debate rounds
        input_report: optional path to existing ai_report.py markdown.
            If given, skips data assembly and uses report as data block
            (two-stage: ai_report broaden -> multi-agent stress-test).
    """

    if input_report is not None:
        logger.info(f"=== Stage 0: loading input report from {input_report} ===")
        data_prompt = _load_input_report(input_report)
        logger.info(f"Data block (from report): {len(data_prompt)} chars")
    else:
        # Stage 0: data assembly — mirror ai_report_pipeline.py:generate_one_report
        logger.info(f"=== Stage 0: data assembly for {ticker} ===")
        from technical_analysis import plot_dual_timeframe
        from analysis_engine import TechnicalAnalyzer
        from fundamental_analysis import get_fundamentals
        from ai_report import assemble_prompt

        is_tw = ticker.replace('.TW', '').replace('.TWO', '').isdigit()

        # 1. Price data
        _figs, _errs, df_week, df_day, _meta = plot_dual_timeframe(ticker, force_update=False)
        if df_day is None or df_day.empty:
            raise RuntimeError(f"No price data for {ticker}")

        # 2. Chip data
        chip_data, us_chip_data = None, None
        if is_tw:
            try:
                from chip_analysis import ChipAnalyzer, ChipFetchError
                chip_data = ChipAnalyzer().fetch_chip(ticker, scan_mode=False)
            except ChipFetchError as e:
                logger.warning(f"TW chip fetch failed: {e}")
            except Exception as e:
                logger.warning(f"TW chip unexpected: {type(e).__name__}: {e}")
        else:
            try:
                from us_stock_chip import USStockChipAnalyzer
                us_chip_data, _err = USStockChipAnalyzer().get_chip_data(ticker)
            except Exception as e:
                logger.warning(f"US chip failed: {type(e).__name__}: {e}")

        # 3. Fundamentals
        try:
            fund_data = get_fundamentals(ticker)
        except Exception as e:
            logger.warning(f"fundamental load failed: {type(e).__name__}: {e}")
            fund_data = None

        # 4. Technical analyzer
        analyzer = TechnicalAnalyzer(ticker, df_week, df_day,
                                      chip_data=chip_data, us_chip_data=us_chip_data)
        report = analyzer.run_analysis()

        data_prompt = assemble_prompt(ticker, report, chip_data, us_chip_data, fund_data, df_day)
        logger.info(f"Data block: {len(data_prompt)} chars")

    out = {"ticker": ticker, "rounds": rounds, "data_chars": len(data_prompt)}

    # Stage 1: Bull <-> Bear debate
    debate_history = ""
    bull_arg = ""
    bear_arg = ""
    for r in range(1, rounds + 1):
        logger.info(f"=== Round {r}/{rounds}: Bull turn ===")
        user = (f"== Original data ==\n{data_prompt}\n\n"
                f"== Debate history so far ==\n{debate_history or '(opening round)'}\n\n"
                f"== Last bear argument ==\n{bear_arg or '(none yet - state your opening case)'}\n\n"
                f"Now make your Bull argument for round {r}.")
        bull_arg = call_cli(BULL_SYS, user, f"Bull-R{r}")
        debate_history += f"\n\n## Round {r} - Bull\n{bull_arg}"

        logger.info(f"=== Round {r}/{rounds}: Bear turn ===")
        user = (f"== Original data ==\n{data_prompt}\n\n"
                f"== Debate history so far ==\n{debate_history}\n\n"
                f"== Last bull argument ==\n{bull_arg}\n\n"
                f"Now make your Bear argument for round {r}.")
        bear_arg = call_cli(BEAR_SYS, user, f"Bear-R{r}")
        debate_history += f"\n\n## Round {r} - Bear\n{bear_arg}"

    out["debate_history"] = debate_history

    # Stage 2: Research Manager
    logger.info("=== Stage 2: Research Manager judges ===")
    user = (f"== Original data ==\n{data_prompt}\n\n"
            f"== Full debate transcript ==\n{debate_history}\n\n"
            f"Now deliver your verdict.")
    judge = call_cli(RESEARCH_MGR_SYS, user, "ResearchMgr")
    out["research_mgr"] = judge

    # Stage 3: Trader
    logger.info("=== Stage 3: Trader plans ===")
    user = (f"== Original data ==\n{data_prompt}\n\n"
            f"== Debate ==\n{debate_history}\n\n"
            f"== Manager verdict ==\n{judge}\n\n"
            f"Now write your trade plan.")
    trader = call_cli(TRADER_SYS, user, "Trader")
    out["trader"] = trader

    # Stage 4: 3 Risk debators
    logger.info("=== Stage 4: Risk Aggressive ===")
    user = f"== Trader plan ==\n{trader}\n\n== Original data ==\n{data_prompt}"
    aggressive = call_cli(RISK_AGGRESSIVE_SYS, user, "Risk-Aggressive")
    out["risk_aggressive"] = aggressive

    logger.info("=== Stage 4: Risk Conservative ===")
    conservative = call_cli(RISK_CONSERVATIVE_SYS, user, "Risk-Conservative")
    out["risk_conservative"] = conservative

    logger.info("=== Stage 4: Risk Neutral ===")
    user = (f"== Trader plan ==\n{trader}\n\n"
            f"== Aggressive critique ==\n{aggressive}\n\n"
            f"== Conservative critique ==\n{conservative}")
    neutral = call_cli(RISK_NEUTRAL_SYS, user, "Risk-Neutral")
    out["risk_neutral"] = neutral

    # Stage 5: Portfolio Manager final
    logger.info("=== Stage 5: Portfolio Manager final ===")
    user = (f"== Original data ==\n{data_prompt}\n\n"
            f"== Debate ==\n{debate_history}\n\n"
            f"== Manager verdict ==\n{judge}\n\n"
            f"== Trader plan ==\n{trader}\n\n"
            f"== Aggressive risk ==\n{aggressive}\n\n"
            f"== Conservative risk ==\n{conservative}\n\n"
            f"== Neutral risk ==\n{neutral}\n\n"
            f"Make the final call.")
    pm = call_cli(PM_SYS, user, "PortfolioMgr")
    out["portfolio_mgr"] = pm

    return out


def write_markdown(out: dict, path: Path):
    """Dump full debate log + final decision to markdown."""
    md = [f"# Multi-Agent Debate — {out['ticker']}",
          f"\n**Run date**: {datetime.now().strftime('%Y-%m-%d %H:%M')}  "
          f"\n**Rounds**: {out['rounds']}  "
          f"\n**Data block size**: {out['data_chars']} chars",
          "\n\n---\n\n# Bull vs Bear Debate",
          out['debate_history'],
          "\n\n---\n\n# Research Manager Verdict\n",
          out['research_mgr'],
          "\n\n---\n\n# Trader Plan\n",
          out['trader'],
          "\n\n---\n\n# Risk: Aggressive\n",
          out['risk_aggressive'],
          "\n\n---\n\n# Risk: Conservative\n",
          out['risk_conservative'],
          "\n\n---\n\n# Risk: Neutral\n",
          out['risk_neutral'],
          "\n\n---\n\n# Portfolio Manager FINAL\n",
          out['portfolio_mgr']]
    path.write_text("\n".join(md), encoding='utf-8')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", required=True)
    ap.add_argument("--rounds", type=int, default=2,
                    help="Bull/Bear debate rounds (default 2 = 4 calls)")
    ap.add_argument("--input-report", type=Path, default=None,
                    help="Path to existing ai_report.py markdown for two-stage "
                         "stress-test mode (skips data assembly)")
    args = ap.parse_args()

    t0 = time.time()
    out = run_pipeline(args.ticker, rounds=args.rounds,
                      input_report=args.input_report)
    elapsed = time.time() - t0

    date_str = datetime.now().strftime("%Y%m%d_%H%M")
    out_path = OUT_DIR / f"multi_agent_debate_{args.ticker}_{date_str}.md"
    write_markdown(out, out_path)

    n_calls = 2 * args.rounds + 1 + 1 + 3 + 1  # bull/bear + mgr + trader + 3 risk + pm
    logger.info(f"\n=== POC Complete ===")
    logger.info(f"Ticker: {args.ticker}")
    logger.info(f"Total CLI calls: {n_calls}")
    logger.info(f"Elapsed: {elapsed/60:.1f} min")
    logger.info(f"Output: {out_path}")


if __name__ == "__main__":
    main()
