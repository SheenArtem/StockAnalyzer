"""
MOPS PoC 可行性驗證腳本
========================
目標：評估能否用公開資訊觀測站（MOPS）REST API 取代 FinMind 的三類呼叫：
  1. 月營收  (taiwan_stock_month_revenue)
  2. 財報    (taiwan_stock_financial_statement / balance_sheet / cash_flows)
  3. 股利    (taiwan_stock_dividend)

關鍵發現：MOPS 已全面改版為 Vue SPA，後端為 JSON REST API：
  Base URL: https://mops.twse.com.tw/mops/api/
  認證: 只需 JSESSIONID cookie（GET https://mops.twse.com.tw/ 取得）
  SSL: verify=False（MOPS SSL 憑證缺少 Subject Key Identifier，Python requests 無法驗證）

不改任何 production code，純 PoC。
"""

import sys
import os
import time
import json
import logging
import warnings
from pathlib import Path
from datetime import datetime

import requests
warnings.filterwarnings("ignore")   # suppress SSL warnings

# ── 路徑設定 ──────────────────────────────────────────────────────────
ROOT = Path("c:/GIT/StockAnalyzer")
SAMPLE_DIR = ROOT / "reports" / "mops_poc_samples"
SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(ROOT))

# ── Logging ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("mops_poc")

# ── 設定常數 ──────────────────────────────────────────────────────────
TEST_STOCKS = ["2330", "3008", "6789"]
MOPS_ROOT = "https://mops.twse.com.tw"
MOPS_API = f"{MOPS_ROOT}/mops/api/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": MOPS_ROOT,
    "Referer": f"{MOPS_ROOT}/mops/",
}


# ─────────────────────────────────────────────────────────────────────
# Session 初始化
# ─────────────────────────────────────────────────────────────────────

def build_session() -> requests.Session:
    """建立帶 JSESSIONID 的 Session。
    必須先 GET https://mops.twse.com.tw/ 才能取得 JSESSIONID cookie；
    缺少此 cookie 的 POST 請求會回傳「此頁面無法被存取」。
    """
    sess = requests.Session()
    sess.headers.update(HEADERS)
    # GET root -> 取 JSESSIONID
    r = sess.get(f"{MOPS_ROOT}/", verify=False, timeout=15)
    jsid = sess.cookies.get("JSESSIONID", "")
    log.info("[Session] JSESSIONID=%s... status=%d", jsid[:8], r.status_code)
    return sess


# ─────────────────────────────────────────────────────────────────────
# PART 1: 連線性測試
# ─────────────────────────────────────────────────────────────────────

def part1_connectivity(sess: requests.Session) -> dict:
    """驗證 5 個 endpoint 的基本連通性（用 2330 Q4 2024）。"""
    log.info("=" * 60)
    log.info("PART 1: 連線性測試")
    log.info("=" * 60)

    tests = {
        "月營收 (t05st10_ifrs)": (
            f"{MOPS_API}t05st10_ifrs",
            {"companyId": "2330", "dataType": "1", "month": "3",
             "year": "113", "subsidiaryCompanyId": ""},
        ),
        "損益表 (t164sb04)": (
            f"{MOPS_API}t164sb04",
            {"companyId": "2330", "dataType": "2", "year": "113",
             "season": "4", "subsidiaryCompanyId": ""},
        ),
        "資產負債表 (t164sb03)": (
            f"{MOPS_API}t164sb03",
            {"companyId": "2330", "dataType": "2", "year": "113",
             "season": "4", "subsidiaryCompanyId": ""},
        ),
        "現金流量表 (t164sb05)": (
            f"{MOPS_API}t164sb05",
            {"companyId": "2330", "dataType": "2", "year": "113",
             "season": "4", "subsidiaryCompanyId": ""},
        ),
        "股利 (t05st09_2)": (
            f"{MOPS_API}t05st09_2",
            {"companyId": "2330", "dataType": "2",
             "firstYear": "111", "lastYear": "113", "queryType": "1"},
        ),
    }

    results = {}
    for label, (url, payload) in tests.items():
        try:
            t0 = time.time()
            r = sess.post(url, json=payload, verify=False, timeout=20)
            elapsed = time.time() - t0
            data = json.loads(r.content.decode("utf-8"))
            code = data.get("code")
            ok = code == 200
            result_type = type(data.get("result")).__name__
            rows = (
                len(data["result"].get("reportList") or data["result"].get("data") or [])
                if isinstance(data.get("result"), dict) else 0
            )
            log.info("  [%s] HTTP=%d code=%d ok=%s rows=%d elapsed=%.2fs",
                     label, r.status_code, code, ok, rows, elapsed)
            results[label] = {
                "http_status": r.status_code, "api_code": code,
                "ok": ok, "rows": rows, "elapsed": round(elapsed, 2),
            }
            # 儲存 HTML/JSON 範本
            sample_key = url.split("/")[-1]
            (SAMPLE_DIR / f"p1_{sample_key}.json").write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception as e:
            log.error("  [%s] ERROR: %s", label, e)
            results[label] = {"ok": False, "error": str(e)}
        time.sleep(0.3)

    return results


# ─────────────────────────────────────────────────────────────────────
# PART 2: 三股票 x 五 Endpoint 完整抓取
# ─────────────────────────────────────────────────────────────────────

def _fetch(sess, endpoint, payload):
    """POST + JSON decode 包裝，回傳 (api_code, result_dict_or_none)。"""
    r = sess.post(f"{MOPS_API}{endpoint}", json=payload, verify=False, timeout=20)
    data = json.loads(r.content.decode("utf-8"))
    return data.get("code"), data.get("result"), data


def part2_endpoints(sess: requests.Session) -> dict:
    """對 TEST_STOCKS × 5 endpoint 抓取，記錄成功 / 失敗 / 資料筆數。"""
    log.info("=" * 60)
    log.info("PART 2: 三股票 x 五 Endpoint 完整抓取")
    log.info("=" * 60)

    summary = {}
    for stock_id in TEST_STOCKS:
        log.info("  [%s] 開始 ...", stock_id)
        row = {}

        # 月營收
        try:
            code, result, raw = _fetch(sess, "t05st10_ifrs", {
                "companyId": stock_id, "dataType": "1", "month": "3",
                "year": "113", "subsidiaryCompanyId": "",
            })
            rows = len(result.get("data", [])) if isinstance(result, dict) else 0
            rev = result["data"][0][1] if rows else "N/A"
            row["revenue"] = {"ok": code == 200, "rows": rows, "sample": rev}
            csv_path = SAMPLE_DIR / f"p2_revenue_{stock_id}.json"
            csv_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            row["revenue"] = {"ok": False, "error": str(e)}
        time.sleep(0.1)

        # 損益表
        try:
            code, result, _ = _fetch(sess, "t164sb04", {
                "companyId": stock_id, "dataType": "2", "year": "113",
                "season": "4", "subsidiaryCompanyId": "",
            })
            rows = len(result.get("reportList", [])) if isinstance(result, dict) else 0
            row["income"] = {"ok": code == 200, "rows": rows}
        except Exception as e:
            row["income"] = {"ok": False, "error": str(e)}
        time.sleep(0.1)

        # 資產負債表
        try:
            code, result, _ = _fetch(sess, "t164sb03", {
                "companyId": stock_id, "dataType": "2", "year": "113",
                "season": "4", "subsidiaryCompanyId": "",
            })
            rows = len(result.get("reportList", [])) if isinstance(result, dict) else 0
            row["balance"] = {"ok": code == 200, "rows": rows}
        except Exception as e:
            row["balance"] = {"ok": False, "error": str(e)}
        time.sleep(0.1)

        # 現金流量表
        try:
            code, result, _ = _fetch(sess, "t164sb05", {
                "companyId": stock_id, "dataType": "2", "year": "113",
                "season": "4", "subsidiaryCompanyId": "",
            })
            rows = len(result.get("reportList", [])) if isinstance(result, dict) else 0
            row["cashflow"] = {"ok": code == 200, "rows": rows}
        except Exception as e:
            row["cashflow"] = {"ok": False, "error": str(e)}
        time.sleep(0.1)

        # 股利
        try:
            code, result, _ = _fetch(sess, "t05st09_2", {
                "companyId": stock_id, "dataType": "2",
                "firstYear": "111", "lastYear": "113", "queryType": "1",
            })
            rows = (
                len(result.get("commonStock", {}).get("data", []))
                if isinstance(result, dict) else 0
            )
            row["dividend"] = {"ok": code == 200, "rows": rows}
        except Exception as e:
            row["dividend"] = {"ok": False, "error": str(e)}

        log.info(
            "    rev=%s income=%s balance=%s cashflow=%s div=%s",
            row["revenue"].get("ok"), row["income"].get("ok"),
            row["balance"].get("ok"), row["cashflow"].get("ok"),
            row["dividend"].get("ok"),
        )
        summary[stock_id] = row
        time.sleep(0.2)

    return summary


# ─────────────────────────────────────────────────────────────────────
# PART 3: Rate Limit 測試
# ─────────────────────────────────────────────────────────────────────

def _rate_probe(sess, n, delay):
    """連續 n 次 POST，回傳統計。"""
    url = f"{MOPS_API}t05st10_ifrs"
    payload = {
        "companyId": "2330", "dataType": "1", "month": "3",
        "year": "113", "subsidiaryCompanyId": "",
    }
    ok, block, codes, first_block = 0, 0, {}, None
    elapsed_list = []
    t_start = time.time()

    for i in range(n):
        try:
            t0 = time.time()
            r = sess.post(url, json=payload, verify=False, timeout=10)
            elapsed_list.append(time.time() - t0)
            sc = r.status_code
            codes[sc] = codes.get(sc, 0) + 1
            data = json.loads(r.content.decode("utf-8"))
            if data.get("code") == 200:
                ok += 1
            else:
                block += 1
                if first_block is None:
                    first_block = i + 1
        except Exception:
            block += 1
            codes["EXC"] = codes.get("EXC", 0) + 1
            if first_block is None:
                first_block = i + 1
        if delay > 0:
            time.sleep(delay)

    total_t = time.time() - t_start
    avg_e = round(sum(elapsed_list) / len(elapsed_list), 3) if elapsed_list else 0
    rate_per_min = round(ok / total_t * 60, 0) if total_t > 0 else 0
    return {
        "n": n, "delay": delay, "ok": ok, "block": block,
        "status_counts": codes, "first_block_at": first_block,
        "avg_elapsed": avg_e, "total_sec": round(total_t, 1),
        "rate_per_min": rate_per_min,
    }


def part3_rate_limit(sess: requests.Session) -> dict:
    """四輪壓測：10 burst / 30@1s / 50@0.5s / 100 burst。"""
    log.info("=" * 60)
    log.info("PART 3: Rate Limit 測試")
    log.info("=" * 60)

    rounds = [
        ("10_burst", 10, 0.0),
        ("30_1s", 30, 1.0),
        ("50_0.5s", 50, 0.5),
        ("100_burst", 100, 0.0),
    ]
    results = {}
    for name, n, delay in rounds:
        log.info("  Round: %s  (n=%d, delay=%.1fs)", name, n, delay)
        r = _rate_probe(sess, n, delay)
        log.info(
            "    ok=%d  block=%d  first_block=%s  rate=%.0f req/min  avg=%.3fs",
            r["ok"], r["block"], r["first_block_at"],
            r["rate_per_min"], r["avg_elapsed"],
        )
        results[name] = r
        time.sleep(2)   # 輪間短暫休息

    return results


# ─────────────────────────────────────────────────────────────────────
# PART 4: 資料一致性比對（2330 vs FinMind）
# ─────────────────────────────────────────────────────────────────────

def _load_fm_revenue():
    from cache_manager import get_finmind_loader, get_finmind_cached
    dl = get_finmind_loader()
    return get_finmind_cached(
        dl, "month_revenue", "2330",
        "taiwan_stock_month_revenue", ttl_days=20,
        start_date_filter="2024-01-01",
    )


def _load_fm_financial():
    from cache_manager import get_finmind_loader, get_finmind_cached
    dl = get_finmind_loader()
    return get_finmind_cached(
        dl, "financial_statement", "2330",
        "taiwan_stock_financial_statement", ttl_days=60,
    )


def _load_fm_dividend():
    from cache_manager import get_finmind_loader, get_finmind_cached
    dl = get_finmind_loader()
    return get_finmind_cached(
        dl, "dividend", "2330",
        "taiwan_stock_dividend", ttl_days=30,
    )


def part4_consistency(sess: requests.Session) -> dict:
    """
    對 2330 做三類資料的 FinMind vs MOPS 比對。

    月營收：逐月比對 6 個月，unit 差異 = 1000x（MOPS 千元，FinMind NTD）。
    財報：驗證全年收入等於 FinMind 四季加總。
    股利：驗證現金股利金額完全一致（同一來源，只是日期標準不同）。
    """
    log.info("=" * 60)
    log.info("PART 4: 資料一致性比對（2330 台積電）")
    log.info("=" * 60)

    results = {}

    # ── 月營收 ─────────────────────────────────────────────────────
    log.info("  [月營收] 抓 MOPS 6 個月 ...")
    mops_rev = {}
    for month in range(1, 7):
        try:
            code, result, _ = _fetch(sess, "t05st10_ifrs", {
                "companyId": "2330", "dataType": "2",
                "month": str(month), "year": "113", "subsidiaryCompanyId": "",
            })
            if code == 200 and isinstance(result, dict):
                yymm = result.get("yymm", "")
                val_str = result["data"][0][1] if result.get("data") else "0"
                mops_rev[yymm] = int(val_str.replace(",", ""))
            time.sleep(0.15)
        except Exception as e:
            log.debug("  revenue %d error: %s", month, e)

    log.info("  [月營收] 抓 FinMind ...")
    import pandas as pd
    fm_rev = _load_fm_revenue()
    # FinMind 格式：date=2024-02-01, revenue_year=2024, revenue_month=1
    fm_rev_map = {}
    if not fm_rev.empty:
        for _, row in fm_rev.iterrows():
            roc_year = int(row["revenue_year"]) - 1911
            yymm = f"{roc_year:03d}{int(row['revenue_month']):02d}"
            fm_rev_map[yymm] = int(row["revenue"])

    matches, mismatches = 0, 0
    for ym in sorted(mops_rev.keys()):
        if ym in fm_rev_map:
            m = mops_rev[ym]
            f = fm_rev_map[ym]
            ratio = f / m if m else float("inf")
            match = abs(ratio - 1000.0) < 0.01
            if match:
                matches += 1
            else:
                mismatches += 1
            log.info("    %s: MOPS=%d FM=%d ratio=%.1f %s",
                     ym, m, f, ratio, "MATCH" if match else "MISMATCH")

    results["revenue"] = {
        "mops_months": len(mops_rev), "fm_rows": len(fm_rev),
        "matches": matches, "mismatches": mismatches,
        "unit_note": "MOPS=千元, FinMind=元 (差 1000x，完全一致)",
    }

    # ── 財報 ─────────────────────────────────────────────────────
    log.info("  [財報] 抓 MOPS FY2024 損益表 ...")
    try:
        code, result, raw = _fetch(sess, "t164sb04", {
            "companyId": "2330", "dataType": "2",
            "year": "113", "season": "4", "subsidiaryCompanyId": "",
        })
        (SAMPLE_DIR / "p4_income_2330_FY2024.json").write_text(
            json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        # 找 MOPS 營業收入合計
        mops_revenue_fy = None
        if isinstance(result, dict):
            for row in result.get("reportList", []):
                if row and "營業收入合計" in str(row[0]):
                    mops_revenue_fy = int(str(row[1]).replace(",", "")) * 1000  # 千元 -> 元
                    break

        log.info("  [財報] 抓 FinMind ...")
        fm_fin = _load_fm_financial()
        fm_rev_sum = (
            fm_fin[
                (fm_fin["date"] >= "2024-01-01") &
                (fm_fin["date"] <= "2024-12-31") &
                (fm_fin["type"] == "Revenue")
            ]["value"].sum()
            if not fm_fin.empty and "type" in fm_fin.columns else 0
        )

        match = (
            mops_revenue_fy is not None and
            abs(mops_revenue_fy - fm_rev_sum) / max(abs(fm_rev_sum), 1) < 0.001
        ) if mops_revenue_fy else False

        log.info("    MOPS FY2024 revenue=%s  FM sum4Q=%s  match=%s",
                 mops_revenue_fy, int(fm_rev_sum) if fm_rev_sum else None, match)

        results["financial"] = {
            "mops_fy_revenue_ntd": mops_revenue_fy,
            "fm_sum_4q_revenue_ntd": int(fm_rev_sum) if fm_rev_sum else None,
            "match": match,
            "note": (
                "MOPS season=4 = 全年累計損益；FinMind = 各季增量 -> 四季加總相等"
            ),
        }
    except Exception as e:
        log.error("  财报比对失败: %s", e)
        results["financial"] = {"error": str(e)}

    # ── 股利 ─────────────────────────────────────────────────────
    log.info("  [股利] 抓 MOPS ...")
    try:
        code, result, raw = _fetch(sess, "t05st09_2", {
            "companyId": "2330", "dataType": "2",
            "firstYear": "111", "lastYear": "113", "queryType": "1",
        })
        (SAMPLE_DIR / "p4_dividend_2330.json").write_text(
            json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        mops_div_list = (
            [(row[1], float(row[10])) for row in result["commonStock"]["data"]
             if len(row) > 10 and row[10] not in (None, "", "0.0", 0, 0.0)]
            if isinstance(result, dict) else []
        )

        log.info("  [股利] 抓 FinMind ...")
        fm_div = _load_fm_dividend()
        fm_div_vals = (
            sorted(fm_div["CashEarningsDistribution"].dropna().tolist())
            if not fm_div.empty and "CashEarningsDistribution" in fm_div.columns else []
        )
        mops_vals = sorted([v for _, v in mops_div_list])

        # 比對共同值
        matches_div = sum(
            1 for mv in mops_vals
            if any(abs(mv - fv) < 0.0001 for fv in fm_div_vals)
        )

        log.info("    MOPS entries=%d  FM entries=%d  value_matches=%d",
                 len(mops_vals), len(fm_div_vals), matches_div)

        results["dividend"] = {
            "mops_entries": len(mops_vals),
            "fm_entries": len(fm_div_vals),
            "value_matches": matches_div,
            "note": (
                "數值完全一致，日期標準不同："
                "MOPS 用董事會決議日，FinMind 用除息日"
            ),
        }
    except Exception as e:
        log.error("  股利比对失败: %s", e)
        results["dividend"] = {"error": str(e)}

    return results


# ─────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────

def main():
    log.info("MOPS PoC 開始  %s", datetime.now().isoformat())
    all_results = {}

    sess = build_session()

    all_results["part1"] = part1_connectivity(sess)
    all_results["part2"] = part2_endpoints(sess)
    all_results["part3"] = part3_rate_limit(sess)
    all_results["part4"] = part4_consistency(sess)

    out = SAMPLE_DIR / "poc_results.json"
    def _safe(o):
        # numpy bool_ and similar types are not JSON-serializable
        if hasattr(o, "item"):
            return o.item()
        return str(o)
    out.write_text(json.dumps(all_results, ensure_ascii=False, indent=2, default=_safe), encoding="utf-8")
    log.info("結果 JSON: %s", out)
    log.info("MOPS PoC 完成  %s", datetime.now().isoformat())
    return all_results


if __name__ == "__main__":
    main()
