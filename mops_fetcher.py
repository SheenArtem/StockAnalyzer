"""
mops_fetcher.py - MOPS REST API wrapper

對齊 FinMind DataFrame schema，讓下游 piotroski.py / fundamental_analysis.py 無需改動。

MOPS API 特性：
  - Base URL: https://mops.twse.com.tw/mops/api/
  - 認證: JSESSIONID cookie（GET https://mops.twse.com.tw/ 取得）
  - SSL: verify=False（MOPS 憑證缺 Subject Key Identifier）
  - 無 rate limit（PoC 驗證 100 burst 全過）
  - 民國年：西元年 - 1911

Schema 對齊說明：
  - 月營收: 欄位 date/stock_id/country/revenue/revenue_month/revenue_year
            單位：MOPS=千元 × 1000 -> 元（對齊 FinMind）
  - 財報 3 表: 欄位 date/stock_id/type/value/origin_name
               MOPS season=4 = 全年累計；season=1/2/3 = 累計 -> 差分算各季增量
  - 股利: 欄位對齊 FinMind taiwan_stock_dividend
"""

import json
import os
import random
import threading
import time
import logging
import warnings
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning

warnings.filterwarnings("ignore", category=InsecureRequestWarning)

logger = logging.getLogger(__name__)

_MOPS_ROOT = "https://mops.twse.com.tw"
_MOPS_API = f"{_MOPS_ROOT}/mops/api/"

# Browser-like headers（模擬 Chrome 130 + MOPS SPA 前端行為）。
# Referer + Sec-Fetch-* 是 2025 起多數 WAF 的基本門檻。
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Content-Type": "application/json",
    "Origin": _MOPS_ROOT,
    "Referer": f"{_MOPS_ROOT}/mops/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Ch-Ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
}

# 模組級 session 快取（含 JSESSIONID cookie）
_session = None
_session_created_at = None
_SESSION_TTL_SECONDS = 1800  # 30 分鐘後重新取 cookie

# ---------------------------------------------------------------
# Global rate limiter + circuit breaker + daily cap + backfill mutex
# ---------------------------------------------------------------
# Why: 2026-04-18 MOPS WAF ban 了本機 IP，肇因 backfill smoke test
# 短時間 ~8 req/sec 持續。Conservative 2 req/sec + jitter 模擬人類。
# 2026-04-20: 2 req/sec 還是被 ban (VPN 測試無效)，調整到 3s / 0.33 req/sec
# 2026-04-21: WAF 解禁後擬恢復用 5s 當起步（3 階段再降：5→4→3）+ 加 daily cap
#             + backfill mutex 避免併發。rollback 期間 USE_MOPS=false 不走本模組。
# 可用 env 調整：
#   MOPS_RATE_INTERVAL=5.0  (預設，1/5 = 0.2 req/sec)
#   MOPS_RATE_JITTER=0.3    (±30% 時間抖動)
#   MOPS_DAILY_CAP=500      (每日最多 500 req，爆 cap 拒絕)
_MIN_INTERVAL = float(os.getenv("MOPS_RATE_INTERVAL", "5.0"))
_JITTER_FACTOR = float(os.getenv("MOPS_RATE_JITTER", "0.3"))

_rate_lock = threading.Lock()
_last_request_ts = 0.0

# Circuit breaker：連續失敗達門檻 -> pause N 秒後重置
_BREAKER_THRESHOLD = int(os.getenv("MOPS_BREAKER_THRESHOLD", "5"))
_BREAKER_PAUSE = int(os.getenv("MOPS_BREAKER_PAUSE", "600"))  # 10 分鐘
_breaker_lock = threading.Lock()
_consecutive_errors = 0
_breaker_paused_until = 0.0

# Daily cap：每日 req 總數上限。狀態寫 data_cache/mops_daily_usage.json，
# 跨 process / 跨執行保留計數；過 00:00 自動歸零。
_DAILY_CAP = int(os.getenv("MOPS_DAILY_CAP", "500"))
_daily_lock = threading.Lock()
_DAILY_USAGE_FILE = Path(__file__).parent / "data_cache" / "mops_daily_usage.json"


class MopsDailyCapExceeded(RuntimeError):
    """Daily cap 達上限，本次 request 被拒絕。"""


# Backfill semaphore：歷史回填必須 serialize（禁止併發 MOPS call），
# live SCAN 不受影響。用 `with mops_fetcher.backfill_lock(): ...` 包起來。
_backfill_semaphore = threading.Semaphore(1)


def backfill_lock():
    """Backfill 的 contextmanager，確保只有一個 backfill job 在打 MOPS。
    用法：
        with mops_fetcher.backfill_lock():
            mops_fetcher.fetch_monthly_revenue(...)
    """
    return _backfill_semaphore


def _throttle() -> None:
    """全域 rate limit：跨 thread 共享 lock，基準 _MIN_INTERVAL + jitter。"""
    global _last_request_ts
    with _rate_lock:
        now = time.time()
        jitter = random.uniform(-_JITTER_FACTOR, _JITTER_FACTOR) * _MIN_INTERVAL
        wait = _MIN_INTERVAL + jitter - (now - _last_request_ts)
        if wait > 0:
            time.sleep(wait)
        _last_request_ts = time.time()


def _check_breaker() -> None:
    """若 circuit breaker 啟動中，等到 pause 結束。"""
    with _breaker_lock:
        now = time.time()
        if _breaker_paused_until > now:
            wait = _breaker_paused_until - now
            logger.warning("MOPS circuit breaker active, sleeping %.0fs", wait)
            time.sleep(wait)


def _record_success() -> None:
    """成功後 reset consecutive error counter。"""
    global _consecutive_errors
    with _breaker_lock:
        _consecutive_errors = 0


def _check_daily_cap() -> None:
    """檢查並 +1 今日 req 計數。超過 _DAILY_CAP 拋 MopsDailyCapExceeded。"""
    today = date.today().isoformat()
    with _daily_lock:
        state = {"date": today, "count": 0}
        if _DAILY_USAGE_FILE.exists():
            try:
                state = json.loads(_DAILY_USAGE_FILE.read_text(encoding="utf-8"))
                if state.get("date") != today:
                    state = {"date": today, "count": 0}  # 跨日歸零
            except Exception:
                pass
        if state["count"] >= _DAILY_CAP:
            raise MopsDailyCapExceeded(
                f"MOPS daily cap {_DAILY_CAP} reached ({state['count']} today), "
                f"refusing further requests until tomorrow"
            )
        state["count"] += 1
        try:
            _DAILY_USAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
            _DAILY_USAGE_FILE.write_text(
                json.dumps(state, ensure_ascii=False), encoding="utf-8"
            )
        except Exception as e:
            logger.debug("MOPS daily usage write failed: %s", e)


def _record_failure() -> None:
    """失敗 +1；達門檻 -> 啟動 circuit breaker。"""
    global _consecutive_errors, _breaker_paused_until
    with _breaker_lock:
        _consecutive_errors += 1
        if _consecutive_errors >= _BREAKER_THRESHOLD:
            _breaker_paused_until = time.time() + _BREAKER_PAUSE
            logger.warning(
                "MOPS circuit breaker TRIPPED after %d consecutive errors, "
                "pausing %ds (until %s)",
                _consecutive_errors, _BREAKER_PAUSE,
                datetime.fromtimestamp(_breaker_paused_until).strftime("%H:%M:%S"),
            )
            _consecutive_errors = 0  # reset 避免 re-trigger


def _get_session() -> requests.Session:
    """取得帶 JSESSIONID cookie 的 requests.Session，逾期自動重建。"""
    global _session, _session_created_at
    now = time.time()
    if _session is None or (
        _session_created_at and now - _session_created_at > _SESSION_TTL_SECONDS
    ):
        sess = requests.Session()
        sess.headers.update(_HEADERS)
        sess.verify = False
        try:
            # 先造訪 /mops/ (SPA 入口) 拿 JSESSIONID（比 / 更像真實使用者）
            r = sess.get(f"{_MOPS_ROOT}/mops/", timeout=15)
            jsid = sess.cookies.get("JSESSIONID", "")
            logger.debug("MOPS session init: JSESSIONID=%s... HTTP=%d", jsid[:8], r.status_code)
        except Exception as e:
            logger.warning("MOPS session init failed: %s", e)
        _session = sess
        _session_created_at = now
    return _session


def _post(endpoint: str, payload: dict) -> dict:
    """POST 到 MOPS API，回傳 result dict。失敗拋 RuntimeError。

    內建：全域 throttle、circuit breaker、daily cap、連續失敗追蹤。
    Daily cap 爆表時拋 MopsDailyCapExceeded（RuntimeError 子類），
    上層可以 catch 後 fallback 到 FinMind。
    """
    _check_breaker()
    _check_daily_cap()
    _throttle()
    sess = _get_session()
    try:
        r = sess.post(f"{_MOPS_API}{endpoint}", json=payload, timeout=20)
        data = r.json()
        if data.get("code") != 200:
            _record_failure()
            raise RuntimeError(
                f"MOPS API error: code={data.get('code')} msg={data.get('message')}"
            )
        _record_success()
        return data.get("result", {})
    except RuntimeError:
        raise
    except Exception as e:
        _record_failure()
        raise RuntimeError(f"MOPS POST {endpoint} failed: {e}") from e


def _roc_to_ad(roc_year: int) -> int:
    """民國年 -> 西元年"""
    return roc_year + 1911


def _parse_num(s) -> float:
    """移除千分位逗號並轉 float，失敗回 0.0。"""
    if s is None:
        return 0.0
    try:
        return float(str(s).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


# ================================================================
# 1. 月營收  (taiwan_stock_month_revenue)
# ================================================================

def fetch_monthly_revenue(stock_id: str, start_year: int = 2015) -> pd.DataFrame:
    """抓月營收，回傳 DataFrame 對齊 FinMind taiwan_stock_month_revenue。

    FinMind schema:
      date (object/str 'YYYY-MM-01'), stock_id (str), country (str),
      revenue (int64, 元), revenue_month (int64), revenue_year (int64)

    MOPS: 每次 POST 只回傳一個月的資料（dataType=2 = 歷史），
    以 yymm (民國年月, 如 11503=2026-03) 索引。
    MOPS 單位為「元」（PoC 確認 `本月` 數值直接等於 FinMind revenue 欄）。

    MOPS data[0][1] 單位為「千元」，需 × 1000 對齊 FinMind 的元。
    PoC Part4 ratio=1000x 即確認此差異（MOPS=千元，FinMind=元）。
    """
    today = date.today()
    end_year = today.year
    end_month = today.month - 1  # 上個月（當月尚未公告）
    if end_month == 0:
        end_month = 12
        end_year -= 1

    rows = []
    for year in range(start_year, end_year + 1):
        roc_year = year - 1911
        m_start = 1
        m_end = 12
        if year == end_year:
            m_end = end_month
        if year == start_year:
            m_start = 1

        for month in range(m_start, m_end + 1):
            try:
                result = _post("t05st10_ifrs", {
                    "companyId": stock_id,
                    "dataType": "2",   # 2=歷史單月
                    "month": str(month),
                    "year": str(roc_year),
                    "subsidiaryCompanyId": "",
                })
                # data[0] = 本月; data[1] = 去年同期
                data = result.get("data", [])
                if not data:
                    continue
                # 本月數值（千元）× 1000 = 元，對齊 FinMind
                rev_val_k = _parse_num(data[0][1]) if len(data) > 0 else 0.0
                if rev_val_k <= 0:
                    continue
                rev_val = int(rev_val_k * 1000)

                # FinMind date 欄 = 公告月（下個月）第 1 日
                # e.g. 3月營收在4月公告 -> date='2026-04-01', revenue_month=3
                if month == 12:
                    pub_date = f"{year + 1}-01-01"
                else:
                    pub_date = f"{year}-{month + 1:02d}-01"

                rows.append({
                    "date": pub_date,
                    "stock_id": stock_id,
                    "country": "Taiwan",
                    "revenue": rev_val,
                    "revenue_month": month,
                    "revenue_year": year,
                })
                # Rate limit 由 _post()._throttle() 統一處理，這裡不需 sleep
            except Exception as e:
                logger.debug("MOPS revenue %s %d/%02d: %s", stock_id, year, month, e)

    if not rows:
        return pd.DataFrame(columns=[
            "date", "stock_id", "country", "revenue", "revenue_month", "revenue_year"
        ])

    df = pd.DataFrame(rows)
    df["revenue"] = df["revenue"].astype("int64")
    df["revenue_month"] = df["revenue_month"].astype("int64")
    df["revenue_year"] = df["revenue_year"].astype("int64")
    df["date"] = df["date"].astype(object)
    df["stock_id"] = df["stock_id"].astype(object)
    df["country"] = df["country"].astype(object)
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 本地算 YoY / MoM（對齊 FinMind schema；MOPS 原始回傳無此欄位）
    df["revenue_last_year"] = 0
    df["revenue_year_growth"] = 0.0
    df["revenue_last_month"] = 0
    df["revenue_month_growth"] = 0.0
    for i in range(len(df)):
        cur_m = int(df.iloc[i]["revenue_month"])
        cur_y = int(df.iloc[i]["revenue_year"])
        cur_rev = int(df.iloc[i]["revenue"])
        # YoY: same month previous year
        ly_mask = (df["revenue_month"] == cur_m) & (df["revenue_year"] == cur_y - 1)
        if ly_mask.any():
            ly_rev = int(df[ly_mask].iloc[0]["revenue"])
            df.at[i, "revenue_last_year"] = ly_rev
            if ly_rev > 0:
                df.at[i, "revenue_year_growth"] = (cur_rev - ly_rev) / ly_rev * 100
        # MoM: previous month
        if i > 0:
            pm_rev = int(df.iloc[i - 1]["revenue"])
            df.at[i, "revenue_last_month"] = pm_rev
            if pm_rev > 0:
                df.at[i, "revenue_month_growth"] = (cur_rev - pm_rev) / pm_rev * 100

    return df


# ================================================================
# 2. 財報 3 表  (financial_statement / balance_sheet / cash_flows)
# ================================================================

# MOPS 損益表中文欄位 -> FinMind type 英文對應
# 所有 key 為 strip().strip('\u3000') 後的結果（不帶全形空格前綴）
_INCOME_MAP = {
    "營業收入合計": "Revenue",
    "營業毛利（毛損）": "GrossProfit",
    "營業毛利（毛損）淨額": "GrossProfit",
    "營業利益（損失）": "OperatingIncome",
    "稅前淨利（淨損）": "PreTaxIncome",
    "本期淨利（淨損）": "IncomeAfterTaxes",
    "繼續營業單位本期淨利（淨損）": "IncomeFromContinuingOperations",
    "基本每股盈餘": "EPS",        # strip 後的 EPS 標題行（有值）
    "稀釋每股盈餘": None,          # 跳過（與基本相同）
    "所得稅費用（利益）合計": "IncomeTax",
    "營業費用合計": "OperatingExpenses",
}

# MOPS 資產負債表中文欄位 -> FinMind type
# 注意：MOPS 欄位有全形空格（\u3000）前綴，_parse_financial_report 已 strip（含 \u3000）
_BALANCE_MAP = {
    "流動資產合計": "CurrentAssets",
    "非流動資產合計": "NonCurrentAssets",
    "資產總額": "TotalAssets",           # MOPS 用「總額」，不是「總計」
    "負債及權益總計": "TotalAssets",     # 另一種寫法（值相同）
    "流動負債合計": "CurrentLiabilities",
    "非流動負債合計": "NonCurrentLiabilities",
    "負債總額": "Liabilities",           # MOPS 用「總額」
    "歸屬於母公司業主之權益合計": "Equity",
    "權益總額": "Equity",                # MOPS 用「總額」
    "保留盈餘合計": "RetainedEarnings",  # MOPS 實際欄位名稱
    "股本合計": "OrdinaryShare",
}

# MOPS 現金流量表中文欄位 -> FinMind type
# 所有 key 為 strip().strip('\u3000') 後的結果
_CASHFLOW_MAP = {
    "營業活動之淨現金流入（流出）": "CashFlowsFromOperatingActivities",
    "營業活動之現金流量小計": "CashFlowsFromOperatingActivities",
    "投資活動之淨現金流入（流出）": "CashFlowsFromInvestingActivities",
    "投資活動之現金流量小計": "CashFlowsFromInvestingActivities",
    "籌資活動之淨現金流入（流出）": "CashFlowsProvidedFromFinancingActivities",
    "籌資活動之現金流量小計": "CashFlowsProvidedFromFinancingActivities",
    "取得不動產、廠房及設備": "AcquisitionOfPropertyPlantAndEquipment",
    "購置不動產、廠房及設備": "AcquisitionOfPropertyPlantAndEquipment",
}


def _season_to_quarter_end(roc_year: int, season: int) -> str:
    """民國年 + season (1-4) -> 西元季末日期字串 YYYY-MM-DD。"""
    ad_year = _roc_to_ad(roc_year)
    q_end = {1: f"{ad_year}-03-31", 2: f"{ad_year}-06-30",
             3: f"{ad_year}-09-30", 4: f"{ad_year}-12-31"}
    return q_end[season]


def _parse_financial_report(result: dict, stock_id: str, date_str: str,
                             field_map: dict) -> list:
    """將 MOPS reportList 轉成 FinMind 格式的 list of dict。

    reportList 每行: [中文欄位名, 當期值(str), ..., 上期值(str), ...]
    col[0] = 欄位名, col[1] = 當期金額（千元字串）
    """
    rows = []
    report_list = result.get("reportList", [])
    seen_types = set()  # 避免同一 type 重複（某些欄位多個中文名對應同 type）

    for item in report_list:
        if not item or not item[0]:
            continue
        # strip 全形空格（\u3000）和一般空格
        raw_name = str(item[0]).strip().strip("\u3000")
        fm_type = field_map.get(raw_name)
        if fm_type is None:
            continue
        # 有些欄位標記為 None（如 EPS 標題行），跳過
        if fm_type == "":
            continue
        # 避免重複
        if fm_type in seen_types:
            continue
        seen_types.add(fm_type)

        # 第 2 欄 (index 1) = 當期金額（千元字串，可能含逗號或空白）
        val_str = item[1] if len(item) > 1 else ""
        if not val_str or str(val_str).strip() in ("", "%"):
            continue
        val = _parse_num(val_str)
        # MOPS 財報單位為千元，轉元
        val_ntd = val * 1000.0

        rows.append({
            "date": date_str,
            "stock_id": stock_id,
            "type": fm_type,
            "value": val_ntd,
            "origin_name": raw_name,
        })

    return rows


def _fetch_financial_quarters(
    stock_id: str,
    endpoint: str,
    field_map: dict,
    start_year: int = 2015,
    cumulative_mode: str = "none",
) -> pd.DataFrame:
    """逐季抓財報 3 表其中之一，回傳合併 DataFrame。

    MOPS 財報語義差異：
      損益表 (t164sb04)：
        season=1/2/3 = 各季增量（直接用）
        season=4     = 全年累計（Q4 = FY - Q1 - Q2 - Q3）
        cumulative_mode='income'

      現金流量表 (t164sb05)：
        season=1/2/3/4 均為 YTD 累計
        Q1直接用；Q2=H1-Q1；Q3=9M-H1；Q4=FY-9M
        cumulative_mode='ytd'

      資產負債表 (t164sb03)：
        截面資料，每季各自獨立，不需差分
        cumulative_mode='none'
    """
    today = date.today()

    # 決定要抓哪些 (roc_year, season) 組合
    # 規則：Q1 5/15 前無資料; Q2 8/14 前; Q3 11/14 前; Q4 次年 3/31 前
    SEASON_DEADLINES = {1: (5, 15), 2: (8, 14), 3: (11, 14), 4: (3, 31)}

    quarters = []
    for year in range(start_year, today.year + 1):
        roc_year = year - 1911
        for season in range(1, 5):
            dl_month, dl_day = SEASON_DEADLINES[season]
            if season == 4:
                deadline = date(year + 1, dl_month, dl_day)
            else:
                deadline = date(year, dl_month, dl_day)
            if today >= deadline:
                quarters.append((roc_year, season, year))

    # 逐季抓取：若是累計表，需要多抓前一個季度做差分
    # 結構: {(year, season): {type: value}}
    cumulative_data = {}  # 用於差分計算

    all_rows = []
    for roc_year, season, ad_year in quarters:
        try:
            result = _post(endpoint, {
                "companyId": stock_id,
                "dataType": "2",
                "year": str(roc_year),
                "season": str(season),
                "subsidiaryCompanyId": "",
            })
            date_str = _season_to_quarter_end(roc_year, season)
            quarter_rows = _parse_financial_report(result, stock_id, date_str, field_map)

            if cumulative_mode != "none":
                # 儲存累計值，稍後差分
                period_data = {r["type"]: r["value"] for r in quarter_rows}
                cumulative_data[(ad_year, season)] = {
                    "date_str": date_str,
                    "data": period_data,
                    "origin": {r["type"]: r["origin_name"] for r in quarter_rows},
                }
            else:
                # 截面表直接加入
                all_rows.extend(quarter_rows)

            # Rate limit 由 _post()._throttle() 統一處理
        except Exception as e:
            logger.debug("MOPS %s %s %dQ%d: %s", endpoint, stock_id, ad_year, season, e)

    if cumulative_mode != "none" and cumulative_data:
        for (yr, s), period in sorted(cumulative_data.items()):
            if cumulative_mode == "income":
                # 損益表：season=1/2/3 = 各季增量；season=4 = FY 累計 -> Q4 差分
                if s in (1, 2, 3):
                    incremental = period["data"].copy()
                else:  # Q4 = FY - Q1 - Q2 - Q3
                    q1 = cumulative_data.get((yr, 1), {}).get("data", {})
                    q2 = cumulative_data.get((yr, 2), {}).get("data", {})
                    q3 = cumulative_data.get((yr, 3), {}).get("data", {})
                    incremental = {
                        k: period["data"].get(k, 0)
                           - q1.get(k, 0) - q2.get(k, 0) - q3.get(k, 0)
                        for k in period["data"]
                    }
            else:  # cumulative_mode == "ytd"
                # 現金流量表：全部季度均為 YTD 累計
                # Q1 直接用；Q2=H1-Q1；Q3=9M-H1；Q4=FY-9M
                if s == 1:
                    incremental = period["data"].copy()
                else:
                    prev = cumulative_data.get((yr, s - 1), {}).get("data", {})
                    incremental = {
                        k: period["data"].get(k, 0) - prev.get(k, 0)
                        for k in period["data"]
                    }

            for ftype, val in incremental.items():
                all_rows.append({
                    "date": period["date_str"],
                    "stock_id": stock_id,
                    "type": ftype,
                    "value": float(val),
                    "origin_name": period["origin"].get(ftype, ""),
                })

    if not all_rows:
        return pd.DataFrame(columns=["date", "stock_id", "type", "value", "origin_name"])

    df = pd.DataFrame(all_rows)
    df["value"] = df["value"].astype("float64")
    df.sort_values(["date", "type"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def fetch_financial_statement(stock_id: str, start_year: int = 2015) -> pd.DataFrame:
    """抓損益表，對齊 FinMind taiwan_stock_financial_statement（各季增量）。"""
    return _fetch_financial_quarters(stock_id, "t164sb04", _INCOME_MAP, start_year,
                                     cumulative_mode="income")


def fetch_balance_sheet(stock_id: str, start_year: int = 2015) -> pd.DataFrame:
    """抓資產負債表，對齊 FinMind taiwan_stock_balance_sheet（截面，不差分）。"""
    return _fetch_financial_quarters(stock_id, "t164sb03", _BALANCE_MAP, start_year,
                                     cumulative_mode="none")


def fetch_cash_flows(stock_id: str, start_year: int = 2015) -> pd.DataFrame:
    """抓現金流量表，對齊 FinMind taiwan_stock_cash_flows_statement（YTD 累計，直接用）。

    FinMind 現金流量表也是 YTD 累計（Q2=H1, Q3=9M, Q4=FY），
    MOPS 現金流量亦相同，因此無需差分，直接使用原始值。
    """
    return _fetch_financial_quarters(stock_id, "t164sb05", _CASHFLOW_MAP, start_year,
                                     cumulative_mode="none")


# piotroski.py 呼叫端的便利包裝（與 cache_manager.get_cached_fundamentals 搭配）
def fetch_financial_statement_income(stock_id: str) -> pd.DataFrame:
    return fetch_financial_statement(stock_id)

def fetch_financial_statement_balance(stock_id: str) -> pd.DataFrame:
    return fetch_balance_sheet(stock_id)

def fetch_financial_statement_cashflow(stock_id: str) -> pd.DataFrame:
    return fetch_cash_flows(stock_id)


# ================================================================
# 3. 股利  (taiwan_stock_dividend)
# ================================================================

def fetch_dividend(stock_id: str, start_year: int = 2015) -> pd.DataFrame:
    """抓股利資料，對齊 FinMind taiwan_stock_dividend。

    FinMind dividend schema 欄位（22 欄）:
      date, stock_id, year, StockEarningsDistribution, StockStatutorySurplus,
      StockExDividendTradingDate, TotalEmployeeStockDividend,
      TotalEmployeeStockDividendAmount, RatioOfEmployeeStockDividendOfTotal,
      RatioOfEmployeeStockDividend, CashEarningsDistribution,
      CashStatutorySurplus, CashExDividendTradingDate, CashDividendPaymentDate,
      TotalEmployeeCashDividend, TotalNumberOfCashCapitalIncrease,
      CashIncreaseSubscriptionRate, CashIncreaseSubscriptionpRrice,
      RemunerationOfDirectorsAndSupervisors, ParticipateDistributionOfTotalShares,
      AnnouncementDate, AnnouncementTime

    MOPS: t05st09_2 一次可查多年（firstYear/lastYear 為民國年）。
    MOPS commonStock.data 每行 20 欄，對應 titles。
    date 欄用「董事會決議日」（FinMind 用 CashExDividendTradingDate 即除息日）。
    為保持語義一致，date = 董事會決議日（MOPS col[4]）。
    """
    today = date.today()
    first_roc = start_year - 1911
    last_roc = today.year - 1911

    try:
        result = _post("t05st09_2", {
            "companyId": stock_id,
            "dataType": "2",
            "firstYear": str(first_roc),
            "lastYear": str(last_roc),
            "queryType": "1",
        })
    except Exception as e:
        logger.warning("MOPS dividend %s: %s", stock_id, e)
        return pd.DataFrame()

    common_data = result.get("commonStock", {}).get("data", [])
    if not common_data:
        return pd.DataFrame()

    rows = []
    for item in common_data:
        if not item or len(item) < 18:
            continue
        # MOPS titles (col index 對應):
        # [0]=決議進度, [1]=股利所屬年(季)度, [2]=股利所屬期間,
        # [3]=期別, [4]=董事會決議日, [5]=股東會日期,
        # [6]=期初未分配盈餘, [7]=本期淨利, [8]=可分配盈餘,
        # [9]=分配後期未分配盈餘
        # [10]=盈餘分配之現金股利(元/股), [11]=法定盈餘公積發放之現金,
        # [12]=資本公積發放之現金, [13]=股東配發現金總金額,
        # [14]=盈餘轉增資配股, [15]=法定盈餘公積轉增資配股,
        # [16]=資本公積轉增資配股, [17]=股東配股總股數
        # [18]=章程, [19]=備註, [20]=普通股每股面額

        # 日期：董事會決議日 (col[4]) 格式 "113/11/12"
        try:
            date_raw = str(item[4]).strip()
            if "/" in date_raw and len(date_raw) >= 7:
                parts = date_raw.split("/")
                ad_year = _roc_to_ad(int(parts[0]))
                date_str = f"{ad_year}-{int(parts[1]):02d}-{int(parts[2]):02d}"
            else:
                date_str = None
        except (ValueError, IndexError):
            date_str = None

        if date_str is None:
            continue

        # 股利所屬年度（當 year 欄）
        year_label = str(item[1]).strip()  # e.g. "113年第3季"

        cash_earnings = _parse_num(item[10]) if len(item) > 10 else 0.0
        cash_statutory = _parse_num(item[11]) if len(item) > 11 else 0.0
        stock_earnings = _parse_num(item[14]) if len(item) > 14 else 0.0
        stock_statutory = _parse_num(item[15]) if len(item) > 15 else 0.0
        total_cash_amount = _parse_num(item[13]) if len(item) > 13 else 0.0
        total_stock_shares = _parse_num(item[17]) if len(item) > 17 else 0.0

        rows.append({
            "date": date_str,
            "stock_id": stock_id,
            "year": year_label,
            "StockEarningsDistribution": stock_earnings,
            "StockStatutorySurplus": stock_statutory,
            "StockExDividendTradingDate": None,  # MOPS 無此欄位
            "TotalEmployeeStockDividend": 0.0,
            "TotalEmployeeStockDividendAmount": 0.0,
            "RatioOfEmployeeStockDividendOfTotal": 0.0,
            "RatioOfEmployeeStockDividend": 0.0,
            "CashEarningsDistribution": cash_earnings,
            "CashStatutorySurplus": cash_statutory,
            "CashExDividendTradingDate": None,  # MOPS 無確切除息日
            "CashDividendPaymentDate": None,
            "TotalEmployeeCashDividend": 0.0,
            "TotalNumberOfCashCapitalIncrease": 0.0,
            "CashIncreaseSubscriptionRate": 0.0,
            "CashIncreaseSubscriptionpRrice": 0.0,
            "RemunerationOfDirectorsAndSupervisors": 0.0,
            "ParticipateDistributionOfTotalShares": total_cash_amount,
            "AnnouncementDate": date_str[:10],
            "AnnouncementTime": None,
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # 型別對齊 FinMind
    float_cols = [
        "StockEarningsDistribution", "StockStatutorySurplus",
        "TotalEmployeeStockDividend", "TotalEmployeeStockDividendAmount",
        "RatioOfEmployeeStockDividendOfTotal", "RatioOfEmployeeStockDividend",
        "CashEarningsDistribution", "CashStatutorySurplus",
        "TotalEmployeeCashDividend", "TotalNumberOfCashCapitalIncrease",
        "CashIncreaseSubscriptionRate", "CashIncreaseSubscriptionpRrice",
        "RemunerationOfDirectorsAndSupervisors", "ParticipateDistributionOfTotalShares",
    ]
    for col in float_cols:
        df[col] = df[col].astype("float64")
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ================================================================
# 連線測試
# ================================================================

def test_connection() -> bool:
    """快速連線測試，回傳 True=成功。"""
    try:
        result = _post("t05st10_ifrs", {
            "companyId": "2330",
            "dataType": "1",
            "month": "3",
            "year": "115",
            "subsidiaryCompanyId": "",
        })
        return bool(result.get("data"))
    except Exception as e:
        logger.warning("MOPS connection test failed: %s", e)
        return False
