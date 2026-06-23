"""
多市場題材策展半自動化 pipeline (S0 context harvest -> S1 3-agent research -> S2 merge+diff)。

設計見 memory/project_multimarket_theme_curation.md。
- UI 策展頁按鈕 (subprocess.Popen detached) 或 CLI 手動觸發
- 全程寫 data_cache/curation/<market>/status.json 供 Streamlit 策展頁輪詢
- 跑完寫 diff.json (added/removed/confidence_changed/new_themes)，UI REVIEW UI 讀它
- canonical 寫回 / approve 由 UI (curate_themes_review) 做，本 pipeline 只產 diff 不動 canonical

粒度：策展清單層級 (主流題材 tier1/2 龍頭)，不重蹈全 universe 自動 tag (40% 教訓)。

CLI:
  python tools/curate_themes_pipeline.py --market tw
  python tools/curate_themes_pipeline.py --market us
  python tools/curate_themes_pipeline.py --market tw --dry-run   # 跳過 LLM，stub agent 驗 plumbing
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools"))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# LLM 規範 (CLAUDE.md「Theme curation 3-agent」列)：Sonnet xhigh + WebSearch/WebFetch
# --output-format json：claude -p 在長/慢輸出會 streaming 掉開頭 (實測 24-theme JSON 掉 {"themes":[ 首段)。
# envelope 把整段 assistant 訊息 buffer 成單一 .result 物件最後一次吐出，無 head-loss。
CLAUDE_MODEL_FLAG = '--model sonnet --effort xhigh --allowedTools "WebSearch,WebFetch" --output-format json'
AGENT_TIMEOUT = 1200  # call_claude 預設 (未用到時的 fallback)

# 每題材並行設計 (2026-06-23 改)：原「3 agent × 全 24 題材」一個 call 又慢又脆 (timeout/截斷)；
# 改「每題材 × N 視角」的小任務 → 每 call 只研究 1 題材，快(~3-5min)、不超時、輸出短不被截，
# 且每題材仍有 N 票交叉驗證。N×題材數 + 發掘任務全丟進並發上限的 pool。
AGENTS_PER_THEME = 3       # 每題材幾個視角 agent (≥2 才有共識)；CLI --agents-per-theme 可調
MAX_CONCURRENCY = 8        # 同時最多幾個 claude (quota/機器上限)；CLI --concurrency 可調
DISCOVERY_AGENTS = 3       # 額外「發掘新題材」任務數
THEME_TASK_TIMEOUT = 420   # 7 min/單題材任務 (比全量小很多)

CANONICAL = {
    "tw": REPO / "data" / "sector_tags_manual.json",
    "us": REPO / "data" / "sector_tags_us.json",
}

# 三 agent 不同視角 (降低單一視角 systematic bias)
AGENT_ANGLES = {
    1: ("supply_chain", "供應鏈與上下游：從產業鏈角度找每個題材的關鍵 tier1 龍頭與 tier2 追隨者"),
    2: ("catalyst_news", "新聞催化與訂單動能：從近期新聞/法說/訂單能見度找正在升溫的題材與受惠股"),
    3: ("liquidity_peer", "流動性與估值同業：確認標的流動性足夠 (非殭屍股) 且與題材同業估值連動"),
}


def paths(market: str) -> dict:
    work = REPO / "data_cache" / "curation" / market
    return {
        "work": work,
        "status": work / "status.json",
        "context": work / "context.json",
        "agent_raw": [work / f"agent{i}_raw.json" for i in (1, 2, 3)],
        "diff": work / "diff.json",
        "archive": work / "archive",
    }


# ============================================================
# Status file (disk-based，跨 session 可讀；atomic write)
# ============================================================
def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# 三 agent 並行時共寫 status.json，read-modify-write 必須鎖 (否則 lost-update / 半寫)
_STATUS_LOCK = threading.Lock()


def write_status(market: str, **updates) -> None:
    with _STATUS_LOCK:
        _write_status_unlocked(market, **updates)


def _write_status_unlocked(market: str, **updates) -> None:
    p = paths(market)
    p["work"].mkdir(parents=True, exist_ok=True)
    cur = read_status(market) or {"market": market, "progress": []}
    cur.update(updates)
    cur["updated_at"] = _now()
    tmp = p["status"].with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(cur, f, ensure_ascii=False, indent=2)
    # Windows: os.replace 偶發 WinError 5 (Defender/索引器或 UI 並發讀 status.json 持有 handle)
    # → retry+backoff；最終仍失敗則 fallback 直接覆寫 (非 atomic 但不讓 pipeline 掛掉)
    for _attempt in range(8):
        try:
            tmp.replace(p["status"])
            return
        except PermissionError:
            time.sleep(0.15)
    try:
        with p["status"].open("w", encoding="utf-8") as f:
            json.dump(cur, f, ensure_ascii=False, indent=2)
        tmp.unlink(missing_ok=True)
    except Exception as e:
        logger.warning("status write fallback failed: %s", e)


def read_status(market: str) -> Optional[dict]:
    sp = paths(market)["status"]
    if not sp.exists():
        return None
    try:
        with sp.open(encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def append_progress(market: str, msg: str) -> None:
    with _STATUS_LOCK:  # read+append+write 整段鎖 (並行 agent 共寫)
        cur = read_status(market) or {"market": market, "progress": []}
        prog = list(cur.get("progress", []))
        prog.append(f"[{_now()}] {msg}")
        _write_status_unlocked(market, progress=prog)
    logger.info("[%s] %s", market, msg)


# ============================================================
# Canonical loaders
# ============================================================
def load_canonical(market: str) -> dict:
    cp = CANONICAL[market]
    if not cp.exists():
        return {"themes": []}
    with cp.open(encoding="utf-8") as f:
        return json.load(f)


def _canonical_ticker_themes(canonical: dict) -> dict:
    """ticker -> set(theme_id) from canonical (current truth)."""
    idx = defaultdict(set)
    for t in canonical.get("themes", []):
        tid = t.get("theme_id")
        for tier in ("tier1", "tier2"):
            for s in t.get(tier, []):
                tk = str(s.get("ticker", "")).strip()
                if tk:
                    idx[tk].add(tid)
    return dict(idx)


def _theme_name_map(canonical: dict) -> dict:
    return {t["theme_id"]: t.get("theme_name_zh", t["theme_id"]) for t in canonical.get("themes", [])}


# ============================================================
# S0 — Context harvest (複用既有 L2 YT / L3 news / theme_momentum / TV map)
# ============================================================
def harvest_context(market: str, canonical: dict) -> dict:
    ctx = {
        "market": market,
        "generated_at": _now(),
        "current_themes": [
            {
                "theme_id": t.get("theme_id"),
                "theme_name_zh": t.get("theme_name_zh"),
                "n_tier1": len(t.get("tier1", [])),
                "n_tier2": len(t.get("tier2", [])),
            }
            for t in canonical.get("themes", [])
        ],
        "current_ticker_count": len(_canonical_ticker_themes(canonical)),
        "signals": {},
    }

    if market == "tw":
        ctx["signals"] = _harvest_tw_signals()
    else:
        ctx["signals"] = _harvest_us_signals()
    return ctx


def _safe_read_parquet(path: Path):
    try:
        import pandas as pd
        if path.exists():
            return pd.read_parquet(path)
    except Exception as e:
        logger.warning("read parquet failed %s: %s", path, e)
    return None


def _harvest_tw_signals() -> dict:
    """L2 YT 法說提及 + L3 news themes + theme_momentum。Best-effort，缺檔降級。"""
    import pandas as pd
    sig = {}

    # L2: YT 動態題材 (近 60 日提及最多的 ticker/theme)
    yt = _safe_read_parquet(REPO / "data" / "sector_tags_dynamic.parquet")
    if yt is not None and not yt.empty and "date" in yt.columns:
        try:
            yt = yt.copy()
            yt["date"] = pd.to_datetime(yt["date"], errors="coerce")
            cutoff = yt["date"].max() - pd.Timedelta(days=60)
            recent = yt[yt["date"] >= cutoff]
            top = (recent.groupby(["ticker", "name"]).size()
                   .sort_values(ascending=False).head(40))
            sig["yt_recent_mentions"] = [
                {"ticker": str(tk), "name": str(nm), "mentions": int(c)}
                for (tk, nm), c in top.items()
            ]
        except Exception as e:
            logger.warning("YT signal harvest failed: %s", e)

    # L3: 升溫題材 (theme_momentum)
    mom = _safe_read_parquet(REPO / "data" / "news" / "theme_momentum.parquet")
    if mom is not None and not mom.empty:
        try:
            cols = [c for c in ("theme", "momentum", "trend", "count_30d", "count_recent") if c in mom.columns]
            sig["news_theme_momentum"] = mom[cols].head(30).to_dict("records") if cols else []
        except Exception as e:
            logger.warning("theme_momentum harvest failed: %s", e)

    # L3: 近期新聞題材 (news_themes legacy backup)
    nt = _safe_read_parquet(REPO / "data" / "news_themes.parquet")
    if nt is not None and not nt.empty:
        try:
            theme_col = next((c for c in ("theme", "topic", "tag") if c in nt.columns), None)
            if theme_col:
                top_themes = nt[theme_col].value_counts().head(25)
                sig["news_top_themes"] = [{"theme": str(k), "count": int(v)} for k, v in top_themes.items()]
        except Exception as e:
            logger.warning("news_themes harvest failed: %s", e)

    return sig


def _harvest_us_signals() -> dict:
    """US 無 YT/news 題材腿；給 TV america industry 分佈當 context 起點，其餘靠 agent WebSearch。"""
    sig = {"note": "US 無本地 YT/新聞題材管線，題材擴充主要靠 agent 網路研究 (WebSearch)。"}
    try:
        from tradingview_screener import Query
        result = (Query().select("name", "sector", "industry")
                  .set_markets("america").limit(5000).get_scanner_data())
        df = result[1]
        if df is not None and not df.empty:
            top_ind = df["industry"].value_counts().head(40)
            sig["tv_top_industries"] = [{"industry": str(k), "count": int(v)} for k, v in top_ind.items()]
    except Exception as e:
        logger.warning("US TV industry harvest failed: %s", e)
    return sig


# ============================================================
# S1 — 每題材 × N 視角 並行 LLM research
# ============================================================
def build_theme_prompt(market: str, theme: dict, angle_key: str, angle_desc: str, sig_snip: str) -> str:
    """單一題材的研究 prompt (小範圍 → 快、不超時、輸出短)。"""
    mkt = "台股" if market == "tw" else "美股 (US)"
    tid = theme.get("theme_id")
    zh = theme.get("theme_name_zh", tid)
    en = theme.get("theme_name_en", tid)
    members = [(s.get("ticker"), s.get("name", "")) for tier in ("tier1", "tier2") for s in theme.get(tier, [])]
    members_block = ", ".join(f"{tk}({nm})" for tk, nm in members) or "(目前無成員)"
    fmt = "純數字代號 (如 2330)" if market == "tw" else "大寫代號 (如 NVDA)"
    return f"""你是 {mkt} 題材策展研究員，視角【{angle_key}】：{angle_desc}
只研究「這一個」題材，找出它最新 (2025-2026) 的代表性龍頭。用 WebSearch 查證關鍵變動，不要拖。

題材：{tid} — {zh} ({en})
現有成員：{members_block}

近期本地訊號 (線索)：{sig_snip}

規則：
- tier1=絕對龍頭、tier2=明確追隨者；>80% 信心才列，寧缺勿濫。
- 只列「這個題材」的成員，不要碰別的題材。
- 每檔附 confidence (0-100)；有查證的附 sources URL。
- ticker 用{fmt}。

只輸出這個題材的 JSON (無 fence、無解釋)：
{{"theme_id":"{tid}","theme_name_zh":"{zh}","theme_name_en":"{en}","tier1":[{{"ticker":"...","name":"...","note":"...","confidence":90,"sources":["http..."]}}],"tier2":[...]}}"""


def build_discovery_prompt(market: str, theme_ids: list, sig_snip: str) -> str:
    """發掘「不在現有清單」的新興題材。"""
    mkt = "台股" if market == "tw" else "美股 (US)"
    existing = ", ".join(theme_ids)
    fmt = "純數字代號" if market == "tw" else "大寫代號"
    return f"""你是 {mkt} 題材策展研究員。任務：用 WebSearch 找出「不在現有清單」的**新興主流題材** (2025-2026)。

現有題材 id（這些不要重複提）：{existing}
近期訊號：{sig_snip}

規則：只提真正新興、有投資熱度、且不屬於上面任何 id 的題材；每個附 2-4 檔代表龍頭。寧缺勿濫 (0-3 個即可)。ticker 用{fmt}。

只輸出 JSON (無 fence)：
{{"new_themes":[{{"theme_id":"<新id>","theme_name_zh":"...","theme_name_en":"...","rationale":"...","tier1":[{{"ticker":"...","name":"...","confidence":80,"sources":["http"]}}],"tier2":[]}}]}}"""


def _extract_json(output: str) -> Optional[dict]:
    s = output.strip()
    for candidate in (s,):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass
    m = re.search(r"```(?:json)?\s*(\{.*\})\s*```", s, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    m = re.search(r"\{.*\}", s, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    return None


def _kill_tree(pid: int) -> None:
    """Kill 整棵 process tree。Windows shell=True 會生 cmd->claude->node，
    必須整樹砍，否則 grandchild 變 orphan 繼續跑/吃 quota。"""
    try:
        if os.name == "nt":
            subprocess.run(f"taskkill /F /T /PID {pid}", shell=True,
                           capture_output=True, timeout=30)
        else:
            import signal
            os.killpg(os.getpgid(pid), signal.SIGKILL)
    except Exception as e:
        logger.warning("kill_tree pid=%s failed: %s", pid, e)


def call_claude(prompt: str, timeout: int = AGENT_TIMEOUT) -> tuple[str, Optional[str]]:
    """呼叫 claude CLI。stdout=PIPE 但用獨立 reader thread 持續抽乾 (避免 pipe buffer 滿
    deadlock + 完整捕捉，不會像 file-redirect 那樣掉開頭)；timeout 時整樹 kill，pipe 收 EOF
    reader thread 自然結束。回 (output, err)。"""
    kwargs = dict(stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                  text=True, encoding="utf-8", errors="replace", shell=True)
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    proc = subprocess.Popen(f"claude -p {CLAUDE_MODEL_FLAG}", **kwargs)

    chunks: list[str] = []

    def _drain():
        try:
            for line in proc.stdout:
                chunks.append(line)
        except Exception:
            pass

    reader = threading.Thread(target=_drain, daemon=True)
    reader.start()
    try:
        proc.stdin.write(prompt)
        proc.stdin.close()
    except (OSError, ValueError):
        pass

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        _kill_tree(proc.pid)
        try:
            proc.wait(15)
        except Exception:
            pass
        reader.join(5)
        return "".join(chunks), f"claude CLI timeout after {timeout}s"

    reader.join(10)
    raw = "".join(chunks)
    # 即使 returncode!=0 也先解 envelope：claude -p 遇 API error (如 429) 會 exit 1 但 envelope 帶 api_error_status
    out, err = _unwrap_envelope(raw)
    if err is None and proc.returncode not in (0, None):
        err = f"claude exit {proc.returncode}"
    return out, err


# 帳號額度/session limit 命中 → 後續呼叫都會 429，沒必要再打，pipeline 應 graceful stop
RATE_LIMIT_ERR = "RATE_LIMIT"


def _unwrap_envelope(raw: str) -> tuple[str, Optional[str]]:
    """--output-format json 的 envelope: {type:result, result:<assistant text>, is_error, ...}。
    取出 .result 給下游 _extract_json。429/session limit → 回 RATE_LIMIT_ERR。非 envelope 原樣回。"""
    s = raw.strip()
    try:
        env = json.loads(s)
    except json.JSONDecodeError:
        return raw, None  # 不是 envelope，原樣 (容錯)
    if isinstance(env, dict) and env.get("is_error"):
        res = str(env.get("result", ""))
        if env.get("api_error_status") == 429 or "limit" in res.lower():
            return "", RATE_LIMIT_ERR
    if isinstance(env, dict) and "result" in env:
        if env.get("is_error"):
            return env.get("result", ""), f"claude is_error (stop_reason={env.get('stop_reason')})"
        return env.get("result", ""), None
    return raw, None


def _stub_theme_output(theme: dict, angle_idx: int) -> dict:
    """--dry-run：單題材 stub。angle 1/2 加 STUB1 (製造共識)，angle 3 砍首檔 (drop)。"""
    t = json.loads(json.dumps(theme))  # deep copy
    for tier in ("tier1", "tier2"):
        for s in t.get(tier, []):
            s.setdefault("confidence", 80)
            s.setdefault("sources", [])
    if angle_idx in (1, 2):
        t.setdefault("tier1", []).append(
            {"ticker": "STUB1", "name": "Stub A", "note": "dry-run", "confidence": 88, "sources": ["http://stub"]})
    if angle_idx == 3 and len(t.get("tier1", [])) > 1:
        t["tier1"] = t["tier1"][1:]
    return {"theme_id": t.get("theme_id"), "theme_name_zh": t.get("theme_name_zh"),
            "theme_name_en": t.get("theme_name_en"), "tier1": t.get("tier1", []), "tier2": t.get("tier2", [])}


def _stub_discovery_output(disc_idx: int) -> dict:
    if disc_idx <= 2:
        return {"new_themes": [{"theme_id": "stub_new_theme", "theme_name_zh": "測試新題材",
                                "theme_name_en": "Stub New Theme", "rationale": "dry-run",
                                "tier1": [{"ticker": "STUB2", "name": "Stub New B", "note": "", "confidence": 75, "sources": []}],
                                "tier2": []}]}
    return {"new_themes": []}


def run_theme_task(market: str, theme: dict, angle_idx: int, sig_snip: str, dry_run: bool) -> tuple[Optional[dict], Optional[str]]:
    """跑單一 (題材, 視角) → 回 (該題材的 {theme_id, tier1, tier2} 或 None, err 或 None)。"""
    tid = theme.get("theme_id")
    if dry_run:
        return _stub_theme_output(theme, angle_idx), None
    angle_key, angle_desc = AGENT_ANGLES[angle_idx]
    prompt = build_theme_prompt(market, theme, angle_key, angle_desc, sig_snip)
    output, err = call_claude(prompt, timeout=THEME_TASK_TIMEOUT)
    if err:
        (paths(market)["work"] / f"FAIL_theme_{tid}_a{angle_idx}.txt").write_text(
            f"ERR: {err}\n\n{output or ''}", encoding="utf-8")
        return None, err
    parsed = _extract_json(output)
    if parsed is None:
        (paths(market)["work"] / f"FAIL_theme_{tid}_a{angle_idx}.txt").write_text(output or "", encoding="utf-8")
        return None, "parse fail"
    # 容錯：模型若包成 {"themes":[{...}]} 取第一個
    if "theme_id" not in parsed and parsed.get("themes"):
        parsed = parsed["themes"][0]
    parsed.setdefault("theme_id", tid)
    return parsed, None


def run_discovery_task(market: str, disc_idx: int, theme_ids: list, sig_snip: str, dry_run: bool) -> tuple[Optional[dict], Optional[str]]:
    """跑單一發掘任務 → 回 ({new_themes:[...]} 或 None, err 或 None)。"""
    if dry_run:
        return _stub_discovery_output(disc_idx), None
    prompt = build_discovery_prompt(market, theme_ids, sig_snip)
    output, err = call_claude(prompt, timeout=THEME_TASK_TIMEOUT)
    if err:
        return None, err
    parsed = _extract_json(output)
    if parsed is None:
        return None, "parse fail"
    return {"new_themes": parsed.get("new_themes", []) or []}, None


# ============================================================
# S2 — Merge (consensus) + diff vs canonical
# ============================================================
def _agent_ticker_themes(agent_out: dict) -> dict:
    """從單一 agent 的 themes 取 ticker -> {theme_id: {name, confidence, sources}}。"""
    res = defaultdict(dict)
    for t in (agent_out or {}).get("themes", []):
        tid = t.get("theme_id")
        if not tid:
            continue
        for tier in ("tier1", "tier2"):
            for s in t.get(tier, []):
                tk = str(s.get("ticker", "")).strip()
                if tk:
                    res[tk][tid] = {
                        "name": s.get("name", ""),
                        "confidence": s.get("confidence", 0),
                        "sources": s.get("sources", []),
                        "tier": tier,
                    }
    return dict(res)


def merge_consensus(agent_outputs: list) -> dict:
    """合併 3 agent：(ticker,theme) 被幾個 agent 提議 -> votes。回 {(ticker,theme): {...}}。"""
    valid = [a for a in agent_outputs if a]
    pair_votes = defaultdict(lambda: {"votes": 0, "names": [], "confidences": [], "sources": [], "tiers": []})
    for a in valid:
        att = _agent_ticker_themes(a)
        for tk, themes in att.items():
            for tid, meta in themes.items():
                key = (tk, tid)
                pair_votes[key]["votes"] += 1
                if meta["name"]:
                    pair_votes[key]["names"].append(meta["name"])
                pair_votes[key]["confidences"].append(meta["confidence"])
                pair_votes[key]["sources"].extend(meta["sources"] or [])
                pair_votes[key]["tiers"].append(meta["tier"])
    return dict(pair_votes)


def _collect_new_themes(agent_outputs: list) -> list:
    out = {}
    for a in agent_outputs:
        for nt in (a or {}).get("new_themes", []):
            tid = nt.get("theme_id")
            if not tid:
                continue
            if tid not in out:
                out[tid] = {**nt, "proposed_by_agents": 1}
            else:
                out[tid]["proposed_by_agents"] += 1
    return list(out.values())


def compute_diff(market: str, canonical: dict, consensus: dict, new_themes: list,
                 covered_themes: Optional[set] = None) -> dict:
    """對 canonical 算 diff。added/removed/new_themes。
    covered_themes：有「成功研究過」的題材集合。removed 只在這些題材內判 —
    若某題材的任務全失敗 (沒覆蓋)，它的成員缺席是「沒研究到」而非 drift-out，不可判移除 (避免假性移除)。"""
    cur = _canonical_ticker_themes(canonical)  # ticker -> set(theme_id)
    cur_pairs = {(tk, tid) for tk, tids in cur.items() for tid in tids}
    theme_names = _theme_name_map(canonical)

    proposed_pairs = set(consensus.keys())

    added, removed = [], []
    for (tk, tid), v in consensus.items():
        if (tk, tid) not in cur_pairs:
            name = v["names"][0] if v["names"] else ""
            conf = round(sum(v["confidences"]) / len(v["confidences"])) if v["confidences"] else 0
            tier = max(set(v["tiers"]), key=v["tiers"].count) if v["tiers"] else "tier2"
            added.append({
                "ticker": tk, "theme_id": tid, "theme_name_zh": theme_names.get(tid, tid),
                "name": name, "tier": tier, "votes": v["votes"], "confidence": conf,
                "sources": sorted(set(v["sources"]))[:3],
                "proposed_by": "agent_consensus" if v["votes"] >= 2 else "agent_single",
                "auto_suggest": v["votes"] >= 2 and conf >= 85 and bool(v["sources"]),
            })

    for (tk, tid) in cur_pairs:
        # 只在「有成功覆蓋」的題材內判移除；未覆蓋題材的成員缺席=沒研究到，跳過
        if covered_themes is not None and tid not in covered_themes:
            continue
        if (tk, tid) not in proposed_pairs:
            removed.append({
                "ticker": tk, "theme_id": tid, "theme_name_zh": theme_names.get(tid, tid),
                "reason": "無 agent 再提議 (drift-out 候選)",
                "proposed_by": "manual", "auto_suggest": False,  # 移除永遠人工
            })

    added.sort(key=lambda x: (-x["votes"], -x["confidence"]))
    removed.sort(key=lambda x: (x["theme_id"], x["ticker"]))

    return {
        "market": market,
        "generated_at": _now(),
        "added": added,
        "removed": removed,
        "new_themes": new_themes,  # 永遠人工
        "summary": {
            "added": len(added), "added_consensus": sum(1 for a in added if a["votes"] >= 2),
            "removed": len(removed), "new_themes": len(new_themes),
        },
    }


# ============================================================
# Orchestration
# ============================================================
def run_pipeline(market: str, dry_run: bool = False) -> dict:
    p = paths(market)
    p["work"].mkdir(parents=True, exist_ok=True)
    write_status(market, status="running", stage="harvest", progress=[],
                 started_at=_now(), finished_at=None, error=None, diff_path=None, summary=None)
    try:
        canonical = load_canonical(market)
        append_progress(market, f"載入 canonical: {len(canonical.get('themes', []))} themes")

        # S0
        append_progress(market, "S0 context harvest...")
        context = harvest_context(market, canonical)
        with p["context"].open("w", encoding="utf-8") as f:
            json.dump(context, f, ensure_ascii=False, indent=2)
        append_progress(market, f"context 完成 (signals: {list(context.get('signals', {}).keys())})")

        # S1 — 每題材 × N 視角 + 發掘任務，並發 pool；支援 resume + graceful stop + 429-aware
        write_status(market, stage="research")
        themes = canonical.get("themes", [])
        theme_ids = [t.get("theme_id") for t in themes]
        sig_snip = json.dumps(context.get("signals", {}), ensure_ascii=False)[:3000]
        tasks_dir = p["work"] / "tasks"
        tasks_dir.mkdir(parents=True, exist_ok=True)
        stop_file = p["work"] / "STOP"
        stop_file.unlink(missing_ok=True)  # 新一輪先清殘留 stop

        def _tf(kind, t, idx):
            key = f"theme_{t.get('theme_id')}_a{idx}" if kind == "theme" else f"disc_{idx}"
            return tasks_dir / f"{key}.json"

        tasks = [("theme", t, aidx) for t in themes for aidx in range(1, AGENTS_PER_THEME + 1)]
        tasks += [("disc", None, didx) for didx in range(1, DISCOVERY_AGENTS + 1)]
        total = len(tasks)

        # resume：已存結果檔的任務直接載入跳過 (重跑不重做)
        cached, todo = [], []
        for task in tasks:
            kind, t, idx = task
            tf = _tf(kind, t, idx)
            if tf.exists():
                try:
                    cached.append((kind, idx, json.loads(tf.read_text(encoding="utf-8"))))
                    continue
                except Exception:
                    pass
            todo.append(task)
        append_progress(market, f"S1 {total} 任務 (並發 {MAX_CONCURRENCY})；resume 跳過 {len(cached)}，待跑 {len(todo)}")

        done = {"n": len(cached)}
        rate_limited = threading.Event()

        def _run_task(task):
            kind, t, idx = task
            label = f"{t.get('theme_id')}#a{idx}" if kind == "theme" else f"discovery#{idx}"
            if stop_file.exists() or rate_limited.is_set():
                return kind, idx, None  # 尚未起跑的任務在 stop/429 後直接略過
            try:
                if kind == "theme":
                    res, err = run_theme_task(market, t, idx, sig_snip, dry_run)
                else:
                    res, err = run_discovery_task(market, idx, theme_ids, sig_snip, dry_run)
            except Exception as e:
                logger.warning("task %s error: %s", task, e)
                res, err = None, str(e)
            if err == RATE_LIMIT_ERR:
                rate_limited.set()  # 命中帳號額度 → 通知後續任務別再打
            if res is not None:
                _tf(kind, t, idx).write_text(json.dumps(res, ensure_ascii=False), encoding="utf-8")
            with _STATUS_LOCK:
                done["n"] += 1
                n = done["n"]
            append_progress(market, f"[{n}/{total}] {label} -> {'ok' if res else ('RATE_LIMIT' if err == RATE_LIMIT_ERR else 'fail')}")
            return kind, idx, res

        with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as ex:
            results = list(ex.map(_run_task, todo)) + cached

        # graceful stop / 429：還有沒跑完的 → 標 paused，存檔任務留著供 resume
        n_have = sum(1 for _, _, r in results if r is not None)
        if (stop_file.exists() or rate_limited.is_set()) and n_have < total:
            stop_file.unlink(missing_ok=True)
            reason = "帳號額度 429 (等 reset 後重觸發接續)" if rate_limited.is_set() else "手動 stop"
            write_status(market, status="paused", stage="paused", finished_at=_now(),
                         summary={"completed": n_have, "total": total, "reason": reason})
            append_progress(market, f"已暫停 ({reason})：完成 {n_have}/{total}；重新觸發會 resume 接續")
            return {"paused": True, "completed": n_have, "total": total, "reason": reason}

        # 按視角重組成「虛擬 agent 輸出」(複用既有 merge_consensus)
        by_angle = {aidx: {"themes": [], "new_themes": []} for aidx in range(1, AGENTS_PER_THEME + 1)}
        discovery_outs = []
        covered_themes = set()  # 有成功研究過的題材 (removed 只在這些題材內判，避免失敗題材假性移除)
        for kind, idx, res in results:
            if res is None:
                continue
            if kind == "theme":
                by_angle.setdefault(idx, {"themes": [], "new_themes": []})["themes"].append(res)
                if res.get("theme_id"):
                    covered_themes.add(res["theme_id"])
            else:
                discovery_outs.append(res)
        agent_outputs = list(by_angle.values()) + discovery_outs

        n_theme_ok = sum(len(v["themes"]) for v in by_angle.values())
        n_disc_ok = len(discovery_outs)
        if n_theme_ok == 0 and n_disc_ok == 0:
            raise RuntimeError("所有任務失敗，無法產 diff")
        for aidx, pa in by_angle.items():
            with (p["work"] / f"agent_angle{aidx}_raw.json").open("w", encoding="utf-8") as f:
                json.dump(pa, f, ensure_ascii=False, indent=2)
        append_progress(market, f"研究完成：{n_theme_ok} 題材任務 + {n_disc_ok} 發掘任務成功，覆蓋 {len(covered_themes)} 題材")

        # S2
        write_status(market, stage="merge")
        append_progress(market, "S2 consensus 合併 + diff...")
        consensus = merge_consensus(agent_outputs)
        new_themes = _collect_new_themes(agent_outputs)
        diff = compute_diff(market, canonical, consensus, new_themes, covered_themes)
        with p["diff"].open("w", encoding="utf-8") as f:
            json.dump(diff, f, ensure_ascii=False, indent=2)

        shutil.rmtree(tasks_dir, ignore_errors=True)  # 完成 → 清 per-task 檔，下次觸發是全新一輪 (非 resume)
        write_status(market, status="done", stage="done", finished_at=_now(),
                     diff_path=str(p["diff"]), summary=diff["summary"])
        append_progress(market, f"完成。diff: {diff['summary']}")
        return diff
    except Exception as e:
        logger.exception("pipeline failed")
        write_status(market, status="error", error=str(e), finished_at=_now())
        append_progress(market, f"ERROR: {e}")
        raise


# ============================================================
# Apply (approve write-back) — UI REVIEW gate 按下 approve 後呼叫
# ============================================================
def _pair_key(ticker: str, theme_id: str) -> str:
    return f"{ticker}|{theme_id}"


def _ensure_provenance(stock: dict, proposed_by: str) -> dict:
    stock.setdefault("note", "")
    stock.setdefault("multi_theme", [])
    stock["status"] = "active"
    stock["proposed_by"] = proposed_by
    stock["last_reviewed"] = datetime.now().strftime("%Y-%m-%d")
    return stock


def apply_diff(market: str, approved_added: set, approved_removed: set, approved_new_themes: set) -> dict:
    """把使用者勾選 approve 的 diff 條目寫回 canonical。
    approved_added/removed = set of 'ticker|theme_id'；approved_new_themes = set of theme_id。
    回 {applied: {...}, canonical_path}。會重算 multi_theme + 蓋 last_reviewed + 歸檔 diff。"""
    from merge_sector_tags import build_reverse_index, apply_multi_theme

    p = paths(market)
    diff = json.loads(p["diff"].read_text(encoding="utf-8"))
    canonical = load_canonical(market)
    themes = canonical.get("themes", [])
    theme_by_id = {t["theme_id"]: t for t in themes}

    applied = {"added": 0, "removed": 0, "new_themes": 0}

    # 1. Added
    for item in diff.get("added", []):
        if _pair_key(item["ticker"], item["theme_id"]) not in approved_added:
            continue
        th = theme_by_id.get(item["theme_id"])
        if th is None:
            continue
        tier = item.get("tier", "tier2")
        th.setdefault(tier, [])
        if any(str(s.get("ticker")) == item["ticker"] for s in th[tier]):
            continue  # 已存在不重加
        th[tier].append(_ensure_provenance({
            "ticker": item["ticker"], "name": item.get("name", ""), "note": item.get("note", ""),
            "multi_theme": [], "confidence": item.get("confidence", 0),
            "sources": item.get("sources", []),
        }, item.get("proposed_by", "agent_consensus")))
        applied["added"] += 1

    # 2. Removed
    for item in diff.get("removed", []):
        if _pair_key(item["ticker"], item["theme_id"]) not in approved_removed:
            continue
        th = theme_by_id.get(item["theme_id"])
        if th is None:
            continue
        for tier in ("tier1", "tier2"):
            before = len(th.get(tier, []))
            th[tier] = [s for s in th.get(tier, []) if str(s.get("ticker")) != item["ticker"]]
            if len(th[tier]) != before:
                applied["removed"] += 1

    # 3. New themes
    for nt in diff.get("new_themes", []):
        if nt.get("theme_id") not in approved_new_themes:
            continue
        if nt["theme_id"] in theme_by_id:
            continue
        new_theme = {
            "theme_id": nt["theme_id"],
            "theme_name_zh": nt.get("theme_name_zh", nt["theme_id"]),
            "theme_name_en": nt.get("theme_name_en", nt["theme_id"]),
            "description": nt.get("rationale", ""),
            "tier1": [_ensure_provenance(dict(s), "agent_single") for s in nt.get("tier1", [])],
            "tier2": [_ensure_provenance(dict(s), "agent_single") for s in nt.get("tier2", [])],
            "sources": [], "confidence": 70,
        }
        if market == "tw":
            new_theme["pair_divergence_suitable"] = False
            new_theme["pair_divergence_note"] = ""
            new_theme["liquidity_warnings"] = []
        themes.append(new_theme)
        theme_by_id[nt["theme_id"]] = new_theme
        applied["new_themes"] += 1

    # 4. 重算 multi_theme 反向索引 (複用 merge_sector_tags)
    rev = build_reverse_index(themes)
    apply_multi_theme(themes, rev)

    # 5. 寫回 canonical
    canonical["themes"] = themes
    canonical["generated_at"] = datetime.now().strftime("%Y-%m-%d")
    with CANONICAL[market].open("w", encoding="utf-8") as f:
        json.dump(canonical, f, ensure_ascii=False, indent=2)

    # 6. 歸檔 diff + 清 status
    p["archive"].mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    (p["archive"] / f"diff_{stamp}.json").write_text(
        json.dumps({"diff": diff, "applied": applied}, ensure_ascii=False, indent=2), encoding="utf-8")
    p["diff"].unlink(missing_ok=True)
    write_status(market, status="applied", stage="applied", applied=applied, diff_path=None,
                 finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return {"applied": applied, "canonical_path": str(CANONICAL[market])}


def main():
    global AGENTS_PER_THEME, MAX_CONCURRENCY
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", choices=["tw", "us"], required=True)
    ap.add_argument("--dry-run", action="store_true", help="跳過 LLM，stub 驗 plumbing")
    ap.add_argument("--agents-per-theme", type=int, default=AGENTS_PER_THEME, help=f"每題材視角數 (預設 {AGENTS_PER_THEME})")
    ap.add_argument("--concurrency", type=int, default=MAX_CONCURRENCY, help=f"並發上限 (預設 {MAX_CONCURRENCY})")
    ap.add_argument("--stop", action="store_true", help="建立 STOP 旗標：進行中的 run 做完手邊任務即暫停 (可 resume)")
    args = ap.parse_args()
    if args.stop:
        sf = paths(args.market)["work"] / "STOP"
        sf.parent.mkdir(parents=True, exist_ok=True)
        sf.write_text("stop", encoding="utf-8")
        print(f"STOP flag set: {sf} (進行中的 run 會做完手邊任務後暫停；重新觸發可 resume)")
        return
    AGENTS_PER_THEME = args.agents_per_theme
    MAX_CONCURRENCY = args.concurrency
    diff = run_pipeline(args.market, dry_run=args.dry_run)
    print(json.dumps(diff.get("summary", diff), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
