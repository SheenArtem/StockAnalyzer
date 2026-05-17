"""Scanner deep research auto-trigger (C3 Phase C, 2026-05-17).

掃完 QM 後對「重要 picks」(rank<=3 + scenario A / trigger>7 / regime 切換)
背景跑 multi-agent debate, 寫 data/deep_research/{date}_{ticker}.json 給
discord_daily_summary 後續顯示。

24h cache (in run_deep_research()) 確保同檔同日不重跑。
Team Plan quota: 11 calls/檔 x 3 檔/天 = 33 calls/天 = ~1k/月 (吸收得了)。

Robustness First:
- Best-effort: 任一檔失敗繼續下一檔; 全失敗回非零 exit code
- Dry-run mode 完全不呼叫 LLM, 只 list 該跑的 picks (for testing)
- Idempotent: cache hit 即使同日多次呼叫也只算一次 quota

Usage (run_scanner.bat 接 scanner_job.py 後):
  python tools/scanner_deep_research_trigger.py
  python tools/scanner_deep_research_trigger.py --dry-run
  python tools/scanner_deep_research_trigger.py --max-picks 5
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
QM_RESULT = REPO / "data" / "latest" / "qm_result.json"
DR_DIR = REPO / "data" / "deep_research"
REGIME_LOG = REPO / "data" / "tracking" / "regime_log.jsonl"

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("dr_trigger")


def _load_qm_picks() -> list[dict]:
    if not QM_RESULT.exists():
        logger.warning(f"qm_result.json 不存在 {QM_RESULT}, skip")
        return []
    try:
        data = json.loads(QM_RESULT.read_text(encoding='utf-8'))
        return data.get('results', [])[:10]
    except Exception as e:
        logger.exception(f"讀 qm_result 失敗: {e}")
        return []


def _detect_regime_changed() -> bool:
    """讀 regime_log.jsonl 最近兩筆，看是否切換首日。"""
    if not REGIME_LOG.exists():
        return False
    try:
        lines = [ln for ln in REGIME_LOG.read_text(encoding='utf-8').splitlines() if ln.strip()]
        if len(lines) < 2:
            return False
        prev = json.loads(lines[-2]).get('regime')
        curr = json.loads(lines[-1]).get('regime')
        return bool(prev and curr and prev != curr)
    except Exception:
        return False


def _select_priority_picks(picks: list[dict], max_picks: int,
                            regime_changed: bool) -> list[tuple[dict, str]]:
    """Returns list of (pick, reason) for high-priority picks (up to max_picks)。"""
    from deep_research_priority import is_high_priority_pick
    selected = []
    for i, p in enumerate(picks, start=1):
        # 把 rank 注入 pick dict 給 priority helper 用
        pick_with_rank = dict(p, qm_rank=i)
        is_pri, reason = is_high_priority_pick(pick_with_rank, regime_changed=regime_changed)
        if is_pri:
            selected.append((pick_with_rank, reason))
            if len(selected) >= max_picks:
                break
    return selected


def _write_verdict_json(pick: dict, reason: str, dr_out: dict,
                        today: date) -> Path:
    """寫 data/deep_research/{date}_{ticker}.json 給 discord_daily_summary 讀。"""
    DR_DIR.mkdir(parents=True, exist_ok=True)
    ticker = str(pick.get('stock_id', '')).strip()
    out_path = DR_DIR / f"{today.isoformat()}_{ticker}.json"
    payload = {
        'ticker': ticker,
        'name': pick.get('name', ''),
        'date': today.isoformat(),
        'qm_rank': pick.get('qm_rank'),
        'trigger_score': pick.get('trigger_score'),
        'composite_score': pick.get('composite_score'),
        'scenario_code': (pick.get('action_plan') or {}).get('scenario_code'),
        'priority_reason': reason,
        'pm_excerpt': dr_out.get('pm_excerpt', ''),
        'report_path': str(dr_out.get('report_path')) if dr_out.get('report_path') else None,
        'cached': dr_out.get('cached', False),
        'elapsed_min': round(dr_out.get('elapsed_min', 0), 2),
        'ok': dr_out.get('ok', False),
        'error': dr_out.get('error'),
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                        encoding='utf-8')
    return out_path


def run_for_top_picks(max_picks: int = 3, dry_run: bool = False) -> int:
    """Returns exit code (0 OK, 1 partial fail)。"""
    today = date.today()
    picks = _load_qm_picks()
    if not picks:
        logger.info("無 QM picks, skip")
        return 0

    regime_changed = _detect_regime_changed()
    if regime_changed:
        logger.info("Regime 切換首日 detected — priority bar 放寬")

    selected = _select_priority_picks(picks, max_picks, regime_changed)
    if not selected:
        logger.info(f"前 {len(picks)} 檔皆未過 priority 門檻 (rank<=3 + scenario A / trigger>7 / regime 切換), skip")
        return 0

    logger.info(f"=== Deep Research trigger: {len(selected)} 檔 ===")
    for pick, reason in selected:
        sid = pick.get('stock_id', '?')
        logger.info(f"  - {sid} ({pick.get('name', '')}) — {reason}")

    if dry_run:
        logger.info("DRY-RUN: 不實際呼叫 LLM, 不寫 JSON")
        return 0

    from single_then_multi import run_deep_research

    fail_count = 0
    for pick, reason in selected:
        sid = str(pick.get('stock_id', '')).strip()
        if not sid:
            continue
        t0 = time.time()
        try:
            dr_out = run_deep_research(sid, rounds=2, cache_hours=24,
                                       progress_cb=lambda m: logger.info(f"  [{sid}] {m}"))
        except Exception as e:
            logger.exception(f"  [{sid}] run_deep_research crashed: {e}")
            dr_out = {'ok': False, 'cached': False, 'report_path': None,
                      'pm_excerpt': '', 'elapsed_min': (time.time() - t0) / 60,
                      'error': f"{type(e).__name__}: {e}"}
            fail_count += 1
        else:
            if not dr_out.get('ok'):
                fail_count += 1

        out_json = _write_verdict_json(pick, reason, dr_out, today)
        status = '(cache)' if dr_out.get('cached') else f"{dr_out.get('elapsed_min', 0):.1f}min"
        ok_mark = 'OK' if dr_out.get('ok') else 'FAIL'
        logger.info(f"  [{sid}] {ok_mark} {status} -> {out_json.name}")

    if fail_count == len(selected):
        logger.error(f"全部 {fail_count}/{len(selected)} 檔 deep research 失敗")
        return 1
    if fail_count > 0:
        logger.warning(f"{fail_count}/{len(selected)} 檔失敗 (部分成功)")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-picks", type=int, default=3,
                    help="最多跑幾檔 (default 3, Team Plan quota cap)")
    ap.add_argument("--dry-run", action="store_true",
                    help="不呼叫 LLM, 只 list 該跑的 picks")
    args = ap.parse_args()
    sys.exit(run_for_top_picks(max_picks=args.max_picks, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
