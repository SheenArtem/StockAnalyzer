"""
LLM bulk sector tagging — 把 137 ticker 手動 manual.json 擴到 1972 全 universe。

Pipeline:
1. 載入 23 themes from sector_tags_manual.json (作為 LLM constraint)
2. 載入 TV industry map (~2680 tickers，含 sector + industry 細分類)
3. Filter 出尚未手動標的 ticker (manual.json 已涵蓋的不重 tag)
4. Batch 30 tickers/call → Claude CLI (Sonnet)
5. Parse + validate theme_id
6. 寫 data/sector_tags_llm.json (不覆蓋 manual.json)
7. peer_comparison loader 後續整合 (manual takes precedence over LLM)

POC mode: --limit 30 跑 1 batch + 印 audit 範例給 user 評估
Production: 不加 --limit 跑全部

CLI:
  python tools/llm_bulk_sector_tag.py --limit 30  # POC
  python tools/llm_bulk_sector_tag.py --resume   # 接續未完
  python tools/llm_bulk_sector_tag.py --batch-size 30
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

THEMES_PATH = REPO / "data" / "sector_tags_manual.json"
OUT_PATH = REPO / "data" / "sector_tags_llm.json"
TMP_DIR = REPO / "tools" / "tmp" / "llm_tag_batches"

CLAUDE_MODEL_FLAG = "--model claude-sonnet-4-6"  # 同 yt extract 用 Sonnet


def load_themes_schema() -> tuple[list[dict], set[str]]:
    """載入 23 themes 給 LLM prompt 用 + 已標 ticker set."""
    with THEMES_PATH.open(encoding='utf-8') as f:
        data = json.load(f)
    themes = data.get('themes', [])
    schema = []
    already_tagged = set()
    for t in themes:
        if not isinstance(t, dict):
            continue
        schema.append({
            'theme_id': t.get('theme_id'),
            'name_zh': t.get('theme_name_zh'),
            'description': t.get('description', '')[:120],
        })
        for tier_key in ('tier1', 'tier2'):
            for s in t.get(tier_key, []):
                already_tagged.add(str(s.get('ticker', '')).strip())
    return schema, already_tagged


def load_universe_with_industry() -> pd.DataFrame:
    """從 TV map + universe_tw_full 取 ticker + name + TV sector/industry。
    過濾掉 ETF / 權證 / 投信信託 (純概念 noise)."""
    from peer_comparison import _fetch_tv_industry_map
    tv = _fetch_tv_industry_map()
    if tv is None or tv.empty:
        raise RuntimeError("TV map fetch failed")
    tv_local = tv.reset_index().copy()
    tv_local['stock_id'] = tv_local['stock_id'].astype(str)

    # 過濾 ETF / 權證 / 信託 (沒有 single-business 標的意義)
    n_before = len(tv_local)
    tv_local = tv_local[
        ~tv_local['industry'].str.contains('Investment Trusts|Mutual Funds|Real Estate Investment',
                                            na=False, regex=True, case=False)
        & ~tv_local['stock_id'].str.match(r'^00\d', na=False)  # 00xx ETF / 權證 普遍
        & (tv_local['stock_id'].str.len() <= 4)  # 排除 5 位以上權證
    ].copy()
    logger.info(f"  ETF/權證/信託 filter: {n_before} -> {len(tv_local)}")

    # name from FinMind universe
    uni_path = REPO / "data_cache" / "backtest" / "universe_tw_full.parquet"
    name_map = {}
    if uni_path.exists():
        u = pd.read_parquet(uni_path)
        for col in ('stock_name', 'name'):
            if col in u.columns:
                name_map = dict(zip(u['stock_id'].astype(str), u[col]))
                break

    tv_local['stock_name'] = tv_local['stock_id'].map(name_map).fillna('')
    return tv_local[['stock_id', 'stock_name', 'sector', 'industry']]


def build_prompt(themes_schema: list[dict], batch_rows: list[dict]) -> str:
    """組 LLM prompt。"""
    theme_lines = []
    for t in themes_schema:
        theme_lines.append(f"- {t['theme_id']}: {t['name_zh']} — {t['description']}")
    themes_block = '\n'.join(theme_lines)

    ticker_lines = []
    for r in batch_rows:
        ticker_lines.append(f"{r['stock_id']} {r['stock_name']} | {r['sector']} / {r['industry']}")
    tickers_block = '\n'.join(ticker_lines)

    return f"""You are a Taiwan stock sector tagger. Tag each Taiwan ticker with 0-N AI-era themes from the FIXED list below.

THEMES (use exactly these theme_id values, never invent new ones):
{themes_block}

CRITICAL RULES (read carefully):
1. **Empty [] is the DEFAULT**. Only tag a theme if you are >80% confident the ticker's CORE BUSINESS clearly matches the theme description.
2. Traditional industry → always [] (cement, food, finance, transport, telecom, retail, real estate, textile, hospital, utilities, traditional construction)
3. Generic semiconductor/electronics tickers without specific AI-era exposure → []. Don't tag every Semiconductor as cowos_advanced_package or hbm_memory.
4. Don't over-tag apple_supply_chain. Only tag if ticker is specifically known to be Apple's supplier (e.g., 2317鴻海/2330台積電/3008大立光).
5. NEVER invent theme_ids not in the list above. NEVER hallucinate — when in doubt, [] is always safer.

ANTI-PATTERNS (do NOT do these):
- 3006 晶豪科 (utility DRAM/SRAM 設計) → empty (NOT hbm_memory; 晶豪科沒有 HBM 業務)
- 2303 聯電 (mature node 晶圓代工) → empty (NOT cowos_advanced_package)
- 2327 國巨 (被動元件) → empty (no matching theme; 不是 AI 散熱也不是 ASIC)
- 2360 致茂 (test equipment + 電池/汽車測試) → only ev_supply_chain if you're confident it has Tesla/EV 直接訂單; otherwise []

GOOD EXAMPLES:
- 2317 鴻海 → [ai_server_odm, apple_supply_chain, ev_supply_chain] (核心業務多元 AI/Apple/EV 都明確)
- 3008 大立光 → [optical_lens, apple_supply_chain] (鏡頭龍頭，iPhone 主要供應)
- 3017 奇鋐 → [ai_cooling] (純散熱龍頭)
- 1101 台泥 → [] (傳統水泥)
- 2882 國泰金 → [] (金控)

TICKERS TO TAG (stock_id name | TV_sector / TV_industry):
{tickers_block}

OUTPUT FORMAT (JSON dict, exact ticker IDs as keys):
{{"<stock_id>": ["theme_id_1", "theme_id_2"], "<stock_id>": [], ...}}

Output JSON only — no markdown fence, no explanations:"""


def call_claude(prompt: str, timeout: int = 300) -> tuple[str, Optional[str]]:
    """呼叫 Claude CLI (沿用 extract_yt_sector_tags 模式)."""
    try:
        result = subprocess.run(
            f'claude -p {CLAUDE_MODEL_FLAG}',
            input=prompt,
            capture_output=True, text=True, timeout=timeout,
            encoding="utf-8", errors="replace",
            shell=True,
        )
    except subprocess.TimeoutExpired:
        return "", f"claude CLI timeout after {timeout}s"

    if result.returncode != 0:
        return result.stdout or "", f"claude exit {result.returncode}: {result.stderr[:500]}"
    return result.stdout, None


def extract_json(output: str) -> Optional[dict]:
    """容錯 JSON parse (markdown fence / 前後文字)."""
    s = output.strip()
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", s, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    # Try to find outermost { ... }
    m = re.search(r"\{.*\}", s, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    return None


def validate_output(parsed: dict, batch_rows: list[dict],
                     valid_theme_ids: set[str]) -> tuple[dict, list[str]]:
    """檢查 output: ticker 在 batch 內 + theme_id 都在 valid set。回 (cleaned, warnings)."""
    expected_ids = {r['stock_id'] for r in batch_rows}
    cleaned = {}
    warnings = []
    for k, v in parsed.items():
        sk = str(k).strip()
        if sk not in expected_ids:
            warnings.append(f"unexpected ticker in output: {sk}")
            continue
        if not isinstance(v, list):
            warnings.append(f"{sk}: value not list ({type(v).__name__}), set to []")
            cleaned[sk] = []
            continue
        valid_themes = []
        for t in v:
            ts = str(t).strip()
            if ts in valid_theme_ids:
                valid_themes.append(ts)
            else:
                warnings.append(f"{sk}: invalid theme_id '{ts}' dropped")
        cleaned[sk] = sorted(set(valid_themes))
    # Tickers in batch but not in output → empty
    for sid in expected_ids:
        if sid not in cleaned:
            warnings.append(f"missing ticker in output: {sid} → empty")
            cleaned[sid] = []
    return cleaned, warnings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--batch-size', type=int, default=30)
    ap.add_argument('--limit', type=int, default=None,
                    help='POC mode: 只跑前 N tickers (測試用)')
    ap.add_argument('--out', type=str, default=str(OUT_PATH))
    ap.add_argument('--resume', action='store_true', help='接續既有 out 檔，不重跑已完成 ticker')
    ap.add_argument('--retry-failed', action='store_true', help='重跑前次 failed batches (從 tmp dir)')
    args = ap.parse_args()

    out_path = Path(args.out)
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Load schema
    logger.info("Loading themes schema...")
    schema, already_tagged = load_themes_schema()
    valid_theme_ids = {t['theme_id'] for t in schema}
    logger.info(f"  {len(schema)} themes, {len(already_tagged)} tickers already manual-tagged")

    # Step 2: Load universe + filter
    logger.info("Loading TW universe + TV industry...")
    df = load_universe_with_industry()
    df = df[df['sector'].notna() & df['industry'].notna()].copy()
    logger.info(f"  {len(df)} tickers with TV sector/industry")

    # Filter out already-tagged
    df = df[~df['stock_id'].isin(already_tagged)].copy()
    logger.info(f"  {len(df)} tickers after removing manual-tagged")

    if args.limit:
        df = df.head(args.limit)
        logger.info(f"  POC mode: limited to {len(df)} tickers")

    # Resume support
    existing = {}
    if args.resume and out_path.exists():
        with out_path.open(encoding='utf-8') as f:
            existing = json.load(f).get('tags', {})
        df = df[~df['stock_id'].isin(existing.keys())].copy()
        logger.info(f"  resuming, {len(existing)} already done; {len(df)} remaining")

    if df.empty:
        logger.info("Nothing to tag.")
        return

    # Step 3: Batch run
    batch_size = args.batch_size
    n_batches = (len(df) + batch_size - 1) // batch_size
    logger.info(f"Running {n_batches} batches of {batch_size} tickers...")

    all_tags = dict(existing)
    all_warnings = []
    failed_batches = []
    start_t = time.time()

    for batch_idx in range(n_batches):
        batch_start = batch_idx * batch_size
        batch_end = batch_start + batch_size
        batch_df = df.iloc[batch_start:batch_end]
        batch_rows = batch_df.to_dict('records')

        logger.info(f"  Batch {batch_idx+1}/{n_batches} ({len(batch_rows)} tickers)...")
        prompt = build_prompt(schema, batch_rows)

        # Save raw prompt for debug
        (TMP_DIR / f"batch_{batch_idx:03d}_prompt.txt").write_text(prompt, encoding='utf-8')

        output, err = call_claude(prompt, timeout=300)
        if err:
            logger.warning(f"    Batch {batch_idx+1} call error: {err}")
            failed_batches.append(batch_idx)
            (TMP_DIR / f"batch_{batch_idx:03d}_FAIL_err.txt").write_text(
                f"ERR: {err}\n\nOUT: {output}", encoding='utf-8')
            continue

        # Save raw output
        (TMP_DIR / f"batch_{batch_idx:03d}_output.txt").write_text(output, encoding='utf-8')

        parsed = extract_json(output)
        if parsed is None:
            logger.warning(f"    Batch {batch_idx+1} JSON parse failed, output[:200]={output[:200]}")
            failed_batches.append(batch_idx)
            continue

        cleaned, warnings = validate_output(parsed, batch_rows, valid_theme_ids)
        if warnings:
            logger.info(f"    Batch {batch_idx+1} warnings: {len(warnings)}")
            all_warnings.extend([f"batch{batch_idx}: {w}" for w in warnings])

        n_tagged = sum(1 for v in cleaned.values() if v)
        logger.info(f"    Batch {batch_idx+1} done: {n_tagged}/{len(cleaned)} tickers got >= 1 theme")
        all_tags.update(cleaned)

        # Incremental save (resume safety)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open('w', encoding='utf-8') as f:
            json.dump({
                'schema_version': 1,
                'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'source': 'claude-sonnet-4-6 + TV sector/industry context',
                'description': 'LLM bulk auto-tag (manual.json takes precedence)',
                'theme_ids_valid': sorted(valid_theme_ids),
                'tags': all_tags,
            }, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - start_t
    n_total_tagged = sum(1 for v in all_tags.values() if v)
    logger.info(f"\n=== DONE ===")
    logger.info(f"Total: {len(all_tags)} tickers processed, {n_total_tagged} got >= 1 theme")
    logger.info(f"Failed batches: {len(failed_batches)}")
    logger.info(f"Warnings: {len(all_warnings)}")
    logger.info(f"Elapsed: {elapsed:.1f}s")
    logger.info(f"Output: {out_path}")

    # Print POC sample
    if args.limit and args.limit <= 50:
        logger.info("\n=== POC SAMPLE (manual audit needed) ===")
        for sid, themes in sorted(all_tags.items())[:20]:
            row = df[df['stock_id'] == sid]
            if row.empty:
                continue
            r = row.iloc[0]
            t_str = ', '.join(themes) if themes else '(empty)'
            logger.info(f"  {sid} {r['stock_name']} | {r['industry'][:30]:<30} → {t_str}")


if __name__ == "__main__":
    main()
