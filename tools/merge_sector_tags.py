"""
Merge 3 sector tag agent outputs into unified data/sector_tags_manual.json.

Steps:
1. Read 3 raw agent JSON files
2. Build ticker -> set(themes) reverse index
3. Overwrite each stock's multi_theme field using the reverse index (ensures consistency)
4. Deduplicate any duplicate tickers within same tier
5. Write unified JSON
6. Print audit summary

Usage:
  python tools/merge_sector_tags.py
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RAW_DIR = REPO / "tools" / "tmp" / "sector_agents"
OUT_PATH = REPO / "data" / "sector_tags_manual.json"


def load_raw():
    files = ["agent1_raw.json", "agent2_raw.json", "agent3_raw.json"]
    all_themes = []
    for f in files:
        with (RAW_DIR / f).open(encoding="utf-8") as fh:
            data = json.load(fh)
            all_themes.extend(data["themes"])
    return all_themes


def build_reverse_index(themes):
    """ticker -> set(theme_id) for multi_theme cross-ref."""
    index = defaultdict(set)
    for theme in themes:
        tid = theme["theme_id"]
        for stock in theme.get("tier1", []) + theme.get("tier2", []):
            index[stock["ticker"]].add(tid)
    return index


def apply_multi_theme(themes, reverse_index):
    """For every stock in every theme, overwrite multi_theme with all OTHER themes it appears in."""
    for theme in themes:
        tid = theme["theme_id"]
        for tier_key in ("tier1", "tier2"):
            for stock in theme.get(tier_key, []):
                other_themes = sorted(reverse_index[stock["ticker"]] - {tid})
                stock["multi_theme"] = other_themes


def dedupe_within_tier(themes):
    """Remove duplicate tickers within same tier (keeps first)."""
    warnings = []
    for theme in themes:
        for tier_key in ("tier1", "tier2"):
            seen = {}
            deduped = []
            for stock in theme.get(tier_key, []):
                t = stock["ticker"]
                if t in seen:
                    warnings.append(
                        f"  {theme['theme_id']}.{tier_key}: dup ticker {t} "
                        f"('{seen[t]}' vs '{stock['name']}') -- kept first"
                    )
                else:
                    seen[t] = stock["name"]
                    deduped.append(stock)
            theme[tier_key] = deduped
    return warnings


def validate(themes):
    """Basic sanity checks."""
    issues = []
    theme_ids = set()
    for theme in themes:
        tid = theme["theme_id"]
        if tid in theme_ids:
            issues.append(f"duplicate theme_id: {tid}")
        theme_ids.add(tid)

        required = {"theme_id", "theme_name_zh", "theme_name_en", "description",
                    "tier1", "tier2", "pair_divergence_suitable", "liquidity_warnings",
                    "sources", "confidence"}
        missing = required - set(theme.keys())
        if missing:
            issues.append(f"{tid}: missing fields {missing}")

        for tier_key in ("tier1", "tier2"):
            for stock in theme.get(tier_key, []):
                for field in ("ticker", "name", "note", "multi_theme"):
                    if field not in stock:
                        issues.append(f"{tid}.{tier_key}.{stock.get('ticker','?')}: missing {field}")
    return issues


def audit_summary(themes, reverse_index):
    lines = []
    total_t1 = sum(len(t.get("tier1", [])) for t in themes)
    total_t2 = sum(len(t.get("tier2", [])) for t in themes)
    multi_theme_stocks = [(t, ts) for t, ts in reverse_index.items() if len(ts) > 1]
    multi_theme_stocks.sort(key=lambda x: (-len(x[1]), x[0]))

    lines.append(f"== Sector Tags Audit ==")
    lines.append(f"Themes: {len(themes)}")
    lines.append(f"Tier1 stocks (total): {total_t1}")
    lines.append(f"Tier2 stocks (total): {total_t2}")
    lines.append(f"Unique tickers: {len(reverse_index)}")
    lines.append(f"Multi-theme stocks (in >=2 themes): {len(multi_theme_stocks)}")
    lines.append("")
    lines.append("Top multi-theme stocks:")
    for ticker, themes_set in multi_theme_stocks[:15]:
        lines.append(f"  {ticker}: {len(themes_set)} themes -> {sorted(themes_set)}")

    lines.append("")
    lines.append("Themes by pair_divergence_suitable:")
    for t in themes:
        flag = "OK " if t.get("pair_divergence_suitable") else "NO "
        lines.append(f"  [{flag}] {t['theme_id']:<28s} t1={len(t.get('tier1',[]))} t2={len(t.get('tier2',[]))} conf={t.get('confidence','?')}")

    return "\n".join(lines)


def main():
    themes = load_raw()
    print(f"Loaded {len(themes)} themes from 3 agent files\n")

    reverse_index = build_reverse_index(themes)
    apply_multi_theme(themes, reverse_index)

    dedup_warnings = dedupe_within_tier(themes)
    # Rebuild index after dedupe
    reverse_index = build_reverse_index(themes)
    apply_multi_theme(themes, reverse_index)

    if dedup_warnings:
        print("DEDUP WARNINGS:")
        print("\n".join(dedup_warnings))
        print()

    issues = validate(themes)
    if issues:
        print("VALIDATION ISSUES:")
        print("\n".join(issues))
        print()
    else:
        print("Validation: OK\n")

    # Output JSON
    output = {
        "schema_version": 1,
        "generated_at": "2026-04-24",
        "description": "台股 AI era 主流題材 sector tag 手動清單 (Q12-c answer)。"
                       "peer_group 用於 Pair Divergence 驗證。multi_theme 由 merge_sector_tags.py 自動交叉索引。",
        "source": "3 x general-purpose agent web research (MoneyDJ, 鉅亨, 券商報告, 經濟日報) 2026-04-24",
        "themes": themes,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Written: {OUT_PATH}")
    print(f"File size: {OUT_PATH.stat().st_size / 1024:.1f} KB\n")

    print(audit_summary(themes, reverse_index))


if __name__ == "__main__":
    main()
