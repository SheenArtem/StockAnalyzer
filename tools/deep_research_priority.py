"""決定一檔 QM pick 是否值得跑深度辯論 (deep research)。

Phase A (C3 Phase A+B, 2026-05-17): 此 helper 供 UI button 加「⭐ 推薦」hint;
Phase C 未來會被 scanner_job 自動觸發複用。

判斷條件 (任一觸發即 priority):
- QM rank <= max_rank (default 3): 真的會考慮下單那幾檔才值得花 5 min
- AND (scenario == 'A' 積極買進  OR  trigger_score > min_trigger  OR  regime_changed)

理由: deep research 單次 4-5 min / 10-11 quota call, 不能對 1900 檔全跑。
"""
from __future__ import annotations


def is_high_priority_pick(
    qm_pick: dict,
    regime_changed: bool = False,
    max_rank: int = 3,
    min_trigger: float = 7.0,
) -> tuple[bool, str]:
    """Returns (is_priority, reason)。

    Args:
        qm_pick: scanner / qm_result.json 的單筆 dict
        regime_changed: 今天是 regime 切換首日 (HMM state transition)
        max_rank: QM 排名門檻 (default top 3)
        min_trigger: trigger_score 強訊號門檻 (default 7.0，符合 ±10 範圍 ~1.6σ 上)
    """
    rank = qm_pick.get('qm_rank') or qm_pick.get('rank')
    # qm_rank 不在 dict 也可能在 scanner_result 的 list index, caller 應已填入
    if rank is None:
        return False, "qm_rank 缺值"
    if rank > max_rank:
        return False, f"rank {rank} > {max_rank}"

    action_plan = qm_pick.get('action_plan') or {}
    scenario = action_plan.get('scenario_code', '')
    if scenario == 'A':
        return True, f"rank {rank} + scenario A (積極買進)"

    trigger = qm_pick.get('trigger_score', 0) or 0
    try:
        trigger = float(trigger)
    except (TypeError, ValueError):
        trigger = 0
    if trigger > min_trigger:
        return True, f"rank {rank} + trigger_score {trigger:.0f} > {min_trigger}"

    if regime_changed:
        return True, f"rank {rank} + regime 切換首日"

    return False, f"rank {rank} 但無強訊號 (scenario={scenario or '-'}, trigger={trigger:.0f})"
