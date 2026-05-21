"""
A/B ÊØîË? Claude Sonnet vs codex (GPT-5.5) extract ?ÅË≥™

Ë∑ëÂ?‰∏Ä??VTT ?®Áõ∏??prompt Áµ¶ÂÖ©??LLM (Sonnet ÁµêÊ??¥Êé•?®Êó¢??JSON,codex Ë∑ëÊñ∞??Ôº?ÊØîË?Ôº?1. mentions ?∏È?
2. ticker ?Ω‰∏≠??(vs sector_tags_manual.json)
3. ticker ÂπªË¶∫??4. entry/stop/target ?ìÂ???5. JSON schema compliance
6. ?óÊ?

CLI:
  python tools/ab_test_codex_vs_sonnet.py
  python tools/ab_test_codex_vs_sonnet.py --vtt <path>  # ?áÂ? VTT
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract_yt_brokerage import PROMPT_TEMPLATE, BROKERAGES, OUT_ROOT as EXTRACTS_OUT, VTT_ROOT
from extract_yt_sector_tags import (
    parse_vtt_filename, extract_json_from_output, load_known_tickers,
)

REPO = Path(__file__).resolve().parent.parent
DEFAULT_VTT = REPO / "data_cache/yt_brokerage_transcripts/moore/moore_chen/20260520_Xda2falreC0_?êÂ¥©?§Ë≠¶?äÔ?ËºùÈ?Ë≤°Â†±?íÊï∏ÔºåË??∂È?È©öÂÇ≥?åÁ?.zh.vtt"


def call_codex(prompt: str, vtt_text: str, timeout: int = 600,
               reasoning_effort: str = "medium") -> tuple[dict | None, dict]:
    """?ºÂè´ codex execÔºåÂ???(parsed_json, metadata)"""
    combined = f"{prompt}\n\n--- ‰ª•‰???VTT Â≠óÂ? ---\n{vtt_text}"
    last_msg_file = REPO / f"data_cache/codex_last_msg_{int(time.time())}.txt"
    last_msg_file.parent.mkdir(parents=True, exist_ok=True)

    cmd = (
        f'codex exec --skip-git-repo-check '
        f'--dangerously-bypass-approvals-and-sandbox '
        f'-o "{last_msg_file}" '
        f'-c model_reasoning_effort={reasoning_effort} -'
    )
    start = time.time()
    try:
        result = subprocess.run(
            cmd, input=combined, capture_output=True, text=True,
            timeout=timeout, encoding="utf-8", errors="replace",
            shell=True,
        )
    except subprocess.TimeoutExpired as e:
        return None, {"error": f"timeout {timeout}s", "elapsed": time.time() - start}

    elapsed = time.time() - start
    meta = {
        "elapsed": elapsed,
        "exit": result.returncode,
        "stderr_preview": (result.stderr or "")[:300],
    }

    # Try last_msg_file first
    output = ""
    if last_msg_file.exists():
        output = last_msg_file.read_text(encoding="utf-8", errors="replace")
        last_msg_file.unlink()
    if not output:
        output = result.stdout or ""

    parsed = extract_json_from_output(output)
    meta["raw_output_size"] = len(output)
    return parsed, meta


def compare(sonnet_json: dict, codex_json: dict | None, known_tickers: dict) -> dict:
    """ÊØîË???extract ÁµêÊ?"""
    def stats(d: dict | None, label: str) -> dict:
        if d is None or "error" in (d or {}):
            return {"label": label, "valid": False}
        mentions = d.get("mentions", []) or []
        tickers = [(m.get("ticker") or "").strip() for m in mentions]
        tickers_valid = [t for t in tickers if t and t.isdigit() and len(t) == 4]
        known_hits = [t for t in tickers_valid if t in known_tickers]
        priced = sum(
            1 for m in mentions
            if (m.get("entry") not in (None, "", 0))
            or (m.get("stop") not in (None, "", 0))
            or (m.get("target") not in (None, "", 0))
        )
        themes = d.get("themes_discussed", []) or []
        return {
            "label": label,
            "valid": True,
            "mentions_count": len(mentions),
            "tickers_valid": len(tickers_valid),
            "tickers_known": len(known_hits),
            "tickers_suspicious": len(tickers_valid) - len(known_hits),
            "hit_rate": (
                len(known_hits) / len(tickers_valid) if tickers_valid else 0
            ),
            "priced_mentions": priced,
            "themes": len(themes),
            "has_analyst_view": bool((d.get("analyst_view") or "").strip()),
            "has_recommended_action": bool((d.get("recommended_action") or "").strip()),
            "has_risk_warning": bool((d.get("risk_warning") or "").strip()),
            "has_host_name": bool((d.get("host_name") or "").strip()),
        }

    sonnet_stats = stats(sonnet_json, "Sonnet")
    codex_stats = stats(codex_json, "GPT-5.5")

    # ticker overlap
    sonnet_tickers = {(m.get("ticker") or "").strip()
                      for m in sonnet_json.get("mentions", [])
                      if m.get("ticker")}
    codex_tickers = set()
    if codex_json and "mentions" in codex_json:
        codex_tickers = {(m.get("ticker") or "").strip()
                         for m in codex_json.get("mentions", [])
                         if m.get("ticker")}

    return {
        "sonnet": sonnet_stats,
        "codex": codex_stats,
        "overlap": {
            "both": sorted(sonnet_tickers & codex_tickers),
            "sonnet_only": sorted(sonnet_tickers - codex_tickers),
            "codex_only": sorted(codex_tickers - sonnet_tickers),
            "agreement_rate": (
                len(sonnet_tickers & codex_tickers) /
                max(len(sonnet_tickers | codex_tickers), 1)
            ),
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vtt", type=str, default=None)
    ap.add_argument("--reasoning", type=str, default="medium",
                    choices=["minimal", "low", "medium", "high", "xhigh"])
    args = ap.parse_args()

    vtt_path = Path(args.vtt) if args.vtt else DEFAULT_VTT
    if not vtt_path.exists():
        # try resolving with default analyst chen 5/20
        cand = list(VTT_ROOT.glob("moore/moore_chen/20260520_*.vtt"))
        if cand:
            vtt_path = cand[0]
    if not vtt_path.exists():
        print(f"[ERR] VTT not found: {vtt_path}", file=sys.stderr)
        sys.exit(1)

    meta = parse_vtt_filename(vtt_path)
    print(f"VTT: {vtt_path.name}", file=sys.stderr)
    print(f"  date={meta['date']} video_id={meta['video_id']}", file=sys.stderr)
    print(f"  size={vtt_path.stat().st_size // 1024} KB", file=sys.stderr)

    # 1. Sonnet output (read existing JSON)
    bk, ak = "moore", "moore_chen"
    parts = vtt_path.resolve().parts
    for i, p in enumerate(parts):
        if p == "yt_brokerage_transcripts" and i + 2 < len(parts):
            bk, ak = parts[i+1], parts[i+2]
    sonnet_json_path = EXTRACTS_OUT / bk / ak / f"{meta['date']}_{meta['video_id']}.json"
    if not sonnet_json_path.exists():
        print(f"[ERR] Sonnet JSON not found: {sonnet_json_path}", file=sys.stderr)
        sys.exit(1)
    sonnet_json = json.loads(sonnet_json_path.read_text(encoding="utf-8"))
    print(f"  Sonnet JSON loaded: {len(sonnet_json.get('mentions', []))} mentions",
          file=sys.stderr)

    # 2. codex call (same prompt)
    info = BROKERAGES[bk]["analysts"][ak]
    prompt = PROMPT_TEMPLATE.format(
        brokerage_name=BROKERAGES[bk]["name"],
        analyst_name=info["name"],
        date=meta["date"],
        video_id=meta["video_id"],
        title=meta["title"],
    )
    vtt_text = vtt_path.read_text(encoding="utf-8", errors="replace")
    if len(vtt_text) > 200_000:
        vtt_text = vtt_text[:200_000] + "\n...(truncated)"

    print(f"\n>> Calling codex (reasoning={args.reasoning})...", file=sys.stderr)
    codex_json, codex_meta = call_codex(prompt, vtt_text, reasoning_effort=args.reasoning)
    print(f"  elapsed: {codex_meta['elapsed']:.1f}s exit={codex_meta.get('exit')}",
          file=sys.stderr)
    print(f"  raw_output_size: {codex_meta.get('raw_output_size')} chars",
          file=sys.stderr)
    if codex_meta.get("stderr_preview"):
        print(f"  stderr: {codex_meta['stderr_preview']}", file=sys.stderr)

    # Save codex JSON for inspection
    codex_out = REPO / f"data_cache/codex_ab_output_{meta['video_id']}.json"
    if codex_json:
        codex_out.write_text(json.dumps(codex_json, ensure_ascii=False, indent=2),
                             encoding="utf-8")
        print(f"  codex JSON saved: {codex_out}", file=sys.stderr)

    # 3. Compare
    known = load_known_tickers()
    cmp = compare(sonnet_json, codex_json, known)

    print("\n" + "=" * 70)
    print("A/B ÊØîË?ÁµêÊ?")
    print("=" * 70)
    print(f"{'Metric':<25} {'Sonnet':>15} {'GPT-5.5':>15}")
    print("-" * 70)
    s, c = cmp["sonnet"], cmp["codex"]
    if not c.get("valid"):
        print(f"  GPT-5.5 INVALID (no JSON parsed)")
    else:
        rows = [
            ("mentions_count", "mentions_count"),
            ("tickers_valid (4-digit)", "tickers_valid"),
            ("tickers_known (in db)", "tickers_known"),
            ("tickers_suspicious", "tickers_suspicious"),
            ("ticker hit_rate", "hit_rate"),
            ("priced_mentions", "priced_mentions"),
            ("themes", "themes"),
            ("has_analyst_view", "has_analyst_view"),
            ("has_recommended_action", "has_recommended_action"),
            ("has_risk_warning", "has_risk_warning"),
            ("has_host_name", "has_host_name"),
        ]
        for label, key in rows:
            sv, cv = s.get(key), c.get(key)
            if isinstance(sv, float):
                sv = f"{sv:.2%}"
            if isinstance(cv, float):
                cv = f"{cv:.2%}"
            print(f"  {label:<25} {str(sv):>15} {str(cv):>15}")

    print("\nTicker overlap:")
    o = cmp["overlap"]
    print(f"  agreement_rate: {o['agreement_rate']:.2%}")
    print(f"  both ({len(o['both'])}): {o['both']}")
    print(f"  sonnet_only ({len(o['sonnet_only'])}): {o['sonnet_only']}")
    print(f"  codex_only ({len(o['codex_only'])}): {o['codex_only']}")

    print(f"\nTiming:")
    print(f"  codex elapsed: {codex_meta['elapsed']:.1f}s")
    print(f"  (Sonnet typical: ~180-300s for 25 mentions)")


if __name__ == "__main__":
    main()
