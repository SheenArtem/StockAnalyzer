"""
One-off manual.json schema patch (2026-04-27)

從 LLM bulk tag POC 順手抓到的 schema 殘缺:
1. ai_cooling theme 漏了 3653 健策 (今天散熱止血 fix peer_comparison _PEER_GROUPS
   有加，但 sector_tags_manual.json 沒同步)
2. 缺 gaas_compound_semi theme (GaAs 三雄 8086 宏捷科 / 3105 穩懋 / 2455 全新
   原在 peer_comparison _PEER_GROUPS 但 manual.json 無對應)

操作:
1. ai_cooling tier1 加 3653 健策
2. 新增 theme gaas_compound_semi (3 stocks)
3. 重建所有 ticker 的 multi_theme 反向索引
4. validate + write back

跑法 (一次性):
    python tools/manual_json_patch_2026_04_27.py
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JSON_PATH = ROOT / "data" / "sector_tags_manual.json"


def main():
    with JSON_PATH.open(encoding='utf-8') as f:
        manual = json.load(f)
    themes = manual['themes']

    # === Patch 1: 加 3653 健策 到 ai_cooling tier1 ===
    cooling = next(t for t in themes if t['theme_id'] == 'ai_cooling')
    if not any(s['ticker'] == '3653' for s in cooling.get('tier1', []) + cooling.get('tier2', [])):
        cooling['tier1'].append({
            'ticker': '3653',
            'name': '健策',
            'note': '散熱模組 (VC/均熱板) + 高階導熱方案，AI server / GPU 主力客戶',
            'multi_theme': [],  # 待反向索引重建
        })
        print("[+] ai_cooling.tier1 += 3653 健策")
    else:
        print("[=] 3653 already in ai_cooling, skip")

    # === Patch 2: 新增 gaas_compound_semi theme ===
    if not any(t['theme_id'] == 'gaas_compound_semi' for t in themes):
        new_theme = {
            'theme_id': 'gaas_compound_semi',
            'theme_name_zh': 'GaAs 化合物半導體',
            'theme_name_en': 'GaAs Compound Semiconductor',
            'description': ('GaAs (砷化鎵) 化合物半導體製程，主供 5G/WiFi PA、'
                            'iPhone/Android 高頻射頻元件、LED 光源、雷射感測。台廠三雄壟斷代工市場。'),
            'tier1': [
                {
                    'ticker': '3105',
                    'name': '穩懋',
                    'note': 'GaAs 全球代工龍頭，PA / 射頻 / 5G WiFi 主供，營收 60%+ 來自手機',
                    'multi_theme': [],
                },
                {
                    'ticker': '8086',
                    'name': '宏捷科',
                    'note': 'GaAs 第二大代工，PA / 5G / WiFi 同主軸；中美晶集團',
                    'multi_theme': [],
                },
            ],
            'tier2': [
                {
                    'ticker': '2455',
                    'name': '全新',
                    'note': 'GaAs 磊晶片廠 (供穩懋 / 宏捷科)，光通訊 / 數據中心 EML 雷射',
                    'multi_theme': [],
                },
            ],
            'pair_divergence_suitable': True,
            'pair_divergence_note': '三雄業務集中度高，pair trade 信號清晰；2455 上游磊晶可作為 lead indicator',
            'liquidity_warnings': [],
            'sources': [
                'https://www.iek.org.tw/EXEC/E03_iek_view_post_html.cfm?actiontype=view&postid=37289',
            ],
            'confidence': 90,
        }
        themes.append(new_theme)
        print("[+] new theme gaas_compound_semi (3 tickers)")
    else:
        print("[=] gaas_compound_semi already exists, skip")

    # === Patch 3: 重建所有 ticker 的 multi_theme 反向索引 ===
    reverse = defaultdict(set)
    for t in themes:
        tid = t['theme_id']
        for tier_key in ('tier1', 'tier2'):
            for s in t.get(tier_key, []):
                reverse[s['ticker']].add(tid)

    n_updated = 0
    for t in themes:
        tid = t['theme_id']
        for tier_key in ('tier1', 'tier2'):
            for s in t.get(tier_key, []):
                other = sorted(reverse[s['ticker']] - {tid})
                if s.get('multi_theme') != other:
                    s['multi_theme'] = other
                    n_updated += 1
    print(f"[*] multi_theme cross-ref rebuilt: {n_updated} entries updated")

    # === Patch 4: bump generated_at ===
    manual['generated_at'] = '2026-04-27'

    # === Validate ===
    theme_ids = {t['theme_id'] for t in themes}
    n_tickers = len({s['ticker'] for t in themes for tk in ('tier1', 'tier2') for s in t.get(tk, [])})
    print(f"[validate] {len(themes)} themes / {n_tickers} unique tickers")

    # === Write back ===
    with JSON_PATH.open('w', encoding='utf-8') as f:
        json.dump(manual, f, ensure_ascii=False, indent=2)
    print(f"[OK] written: {JSON_PATH}")


if __name__ == "__main__":
    main()
