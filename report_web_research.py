"""多代理 fan-out web 研究階段 (2026-06-16)

`generate_one_report` 在組裝主報告 prompt 前先跑此階段:6 個研究角度各開一個
Sonnet 子代理 (WebSearch/WebFetch) 並行查證 -> 彙整成 [WEB_RESEARCH] brief 注入主 prompt,
讓主報告下筆前先有一份「已查證 + 附來源」的研究底稿。

fail-soft:任何子代理失敗/逾時/查無資料,只略過該角度,不阻斷報告生成;
全部失敗則回傳 ok=False,主報告改用系統內建數據照常生成。

LLM 規範 (CLAUDE.md "Multi-agent debate / exploratory" 列):
  Sonnet + --effort xhigh + --allowedTools "WebSearch,WebFetch" + 600s
"""

import concurrent.futures
import logging
import subprocess
import time

from ai_report import _CLAUDE_CLI

logger = logging.getLogger(__name__)

_SUBAGENT_TIMEOUT = 600  # CLAUDE.md LLM 規範:Claude 10 min

# 研究角度:(key, 中文標題, TW query 提示, US query 提示)
_ANGLES = [
    ("industry_trend", "產業趨勢與供需",
     "{name} {sid} 產業趨勢 2026 上下游 供需", "{tk} industry trend 2026 supply demand"),
    ("guidance", "法說展望與營運",
     "{name} {sid} 法說會 營運展望 2026", "{tk} earnings call guidance outlook 2026"),
    ("competition", "競爭格局",
     "{name} {sid} 競爭對手 市佔 比較", "{tk} competitors market share comparison"),
    ("catalyst", "最新催化新聞",
     "{name} {sid} 最新 訂單 擴產 利多 2026", "{tk} latest catalyst orders expansion news 2026"),
    ("risk", "風險與利空",
     "{name} {sid} 風險 利空 下修 隱憂 2026", "{tk} risks downside concerns 2026"),
    ("analyst_etf", "券商評等與 ETF 動向",
     "{name} {sid} 券商 目標價 評等 調整 ETF 成分股 納入 2026",
     "{tk} analyst price target upgrade rating ETF index inclusion 2026"),
]


def _build_subagent_prompt(title, query_hint, ticker, stock_name):
    return f"""你是證券研究員。針對標的 {ticker} ({stock_name})，用 WebSearch / WebFetch 查證主題:**{title}**。

建議查詢方向: {query_hint}

要求:
1. 至少做 2 次獨立 WebSearch,優先第一手來源(公司公告 / 法說逐字稿 / 官方財報 / 主管機關)。
2. 只回報「查證到、有來源支撐」的事實,禁止臆測或填充看似合理但沒查到的內容。
3. 來源衝突時並列分歧,不選邊。
4. **時效防呆(會被修訂的數字務必這樣做)**：券商目標價 / 評等 / ETF 成分股這類會隨時間更新的資訊,**一定要找「最新一次」並核對發布日期**;**舊值已被新值取代,絕不可引用過期數字**(例:同券商先前喊 140、最新改喊 255,只能用 255)。多筆衝突時取**日期最新**那筆,且每條發現都要標日期。

輸出格式(純文字,精簡,最多 8 條,不要前後贅述):
- <一句發現>（來源: <出處簡稱>, <YYYY-MM>, <URL>）

若查無可靠資訊,只回一行: NO_RELIABLE_DATA
"""


def _run_one_angle(cli, angle, ticker, stock_name, is_us):
    """跑單一研究角度。回傳 (key, title, output_or_None)。"""
    key, title, tw_q, us_q = angle
    sid = ticker.replace('.TW', '')
    raw_q = us_q if is_us else tw_q
    try:
        query = raw_q.format(name=stock_name or ticker, sid=sid, tk=ticker)
    except (KeyError, IndexError, ValueError):
        # user_focus 角度的 query 是使用者原文，可能含 {}，不套模板直接用
        query = raw_q
    prompt = _build_subagent_prompt(title, query, ticker, stock_name or ticker)
    try:
        result = subprocess.run(
            [cli, "-p",
             "--model", "sonnet",
             "--effort", "xhigh",  # CLAUDE.md:fallback/子代理品質保證
             "--allowedTools", "WebSearch,WebFetch",
             "--output-format", "text"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=_SUBAGENT_TIMEOUT,
            encoding='utf-8',
        )
        if result.returncode != 0:
            logger.warning("[web_research] angle=%s exit=%s: %s",
                           key, result.returncode, (result.stderr or '')[:200])
            return key, title, None
        out = (result.stdout or '').strip()
        if not out or 'NO_RELIABLE_DATA' in out:
            return key, title, None
        return key, title, out
    except subprocess.TimeoutExpired:
        logger.warning("[web_research] angle=%s timeout (%ds)", key, _SUBAGENT_TIMEOUT)
        return key, title, None
    except Exception as e:  # noqa: BLE001 - fail-soft,單一角度不可拖垮整份報告
        logger.warning("[web_research] angle=%s error: %s", key, e)
        return key, title, None


def run_web_research(ticker, stock_name='', is_us=False, progress_cb=None, max_workers=5,
                     user_focus=None):
    """並行跑研究角度的 web 研究,彙整成可注入 [WEB_RESEARCH] 的 brief。

    Args:
        ticker: 股票代號 ('2330' / '2330.TW' / 'NVDA')
        stock_name: 公司名 (查詢用,可空)
        is_us: 美股 -> 用英文 query
        progress_cb: callable(msg)
        max_workers: 並行子代理數
        user_focus: 使用者補充關注 / 提問 -> 多加一個專屬研究角度,讓查詢偏向使用者關注

    Returns:
        dict {ok: bool, brief: str|None, n_angles: int, elapsed_s: float}
    """
    def _p(msg):
        if progress_cb:
            progress_cb(msg)

    t0 = time.time()
    if not _CLAUDE_CLI:
        logger.warning("[web_research] claude CLI 不存在,跳過研究階段")
        return {'ok': False, 'brief': None, 'n_angles': 0, 'elapsed_s': 0.0}

    # 固定 6 角度 + (使用者有補充關注時) 動態第 7 角度
    angles = list(_ANGLES)
    if user_focus and user_focus.strip():
        uf = user_focus.strip()
        angles.append(("user_focus", "使用者關注主題", uf, uf))

    _p(f"🔎 多代理研究啟動（{len(angles)} 角度並行 Sonnet）...")
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_run_one_angle, _CLAUDE_CLI, a, ticker, stock_name, is_us): a
                for a in angles}
        for fut in concurrent.futures.as_completed(futs):
            key, title, out = fut.result()
            results[key] = out
            _p(f"  研究「{title}」: {'✓ 有料' if out else '— 無可靠資料'}")

    # 依原順序彙整
    parts = []
    for key, title, *_ in angles:
        out = results.get(key)
        if out:
            parts.append(f"### {title}\n{out}")

    elapsed = time.time() - t0
    if not parts:
        _p(f"🔎 研究階段無可靠新資料（{elapsed:.0f}s）,報告改用系統內建數據")
        return {'ok': False, 'brief': None, 'n_angles': 0, 'elapsed_s': elapsed}

    brief = "\n\n".join(parts)
    _p(f"🔎 研究完成:{len(parts)}/{len(angles)} 角度有料（{elapsed:.0f}s）")
    return {'ok': True, 'brief': brief, 'n_angles': len(parts), 'elapsed_s': elapsed}
