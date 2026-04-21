"""
MOPS WAF 解禁探針 — daily 1-req lightweight probe

2026-04-18 MOPS WAF ban 本機 IP 後，USE_MOPS=false rollback 到 FinMind。
此腳本每日跑一次（Windows Task Scheduler），只打 1 個 MOPS request 探測，
連續 3 天成功就 Discord 通知「可恢復 USE_MOPS=true」。

設計原則：
- 繞過 mops_fetcher 的 circuit breaker + throttle，避免探針失敗污染正常流程
- 1 req/day 完全不會撞 WAF（即使目前還在 ban 期也只加深 1 req）
- 狀態檔 data_cache/mops_probe_state.json 記錄連續成功次數
- 只通知一次（consecutive=3 的那天），避免 spam
- 失敗時 consecutive 歸零，下次成功重新累積

使用：
  python tools/mops_probe.py
  排到 Task Scheduler 每日一次（建議 09:00，MOPS 晨間流量低）
"""

import json
import logging
import sys
import urllib3
from datetime import datetime
from pathlib import Path

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("mops_probe")

STATE_FILE = Path(__file__).parent.parent / "data_cache" / "mops_probe_state.json"
NOTIFY_THRESHOLD = 3
MOPS_URL = "https://mops.twse.com.tw/mops/api/t05st10_ifrs"
MOPS_ROOT = "https://mops.twse.com.tw"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": MOPS_ROOT,
    "Referer": f"{MOPS_ROOT}/mops/",
}


def probe_mops() -> tuple[bool, str]:
    """打 1 個 MOPS request 探測 WAF 是否解禁。

    Returns:
        (success, detail) — success=True 表示 MOPS API 正常回 code=200
    """
    try:
        sess = requests.Session()
        sess.headers.update(HEADERS)
        sess.verify = False

        # 先 GET root 拿 JSESSIONID
        r = sess.get(f"{MOPS_ROOT}/mops/", timeout=15)
        if r.status_code != 200:
            return False, f"GET root HTTP {r.status_code}"

        # 打 2330 2025-10 月營收（最穩定的熱門股 + 已公告月份）
        r = sess.post(MOPS_URL, json={
            "companyId": "2330",
            "dataType": "2",
            "month": "10",
            "year": "114",  # 2025 民國年
            "subsidiaryCompanyId": "",
        }, timeout=20)

        if r.status_code != 200:
            return False, f"POST HTTP {r.status_code}"

        data = r.json()
        code = data.get("code")
        if code != 200:
            return False, f"API code={code} msg={data.get('message')}"

        result = data.get("result", {}).get("data", [])
        if not result:
            return False, "empty result.data"

        return True, f"2330 rev_k={result[0][1] if result else '?'}"
    except Exception as e:
        return False, f"exception: {type(e).__name__}: {e}"


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"consecutive_successes": 0, "notified": False, "history": []}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def read_discord_webhook() -> str | None:
    env_path = Path(__file__).parent.parent / "local" / ".env"
    if not env_path.exists():
        return None
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("DISCORD_WEBHOOK_URL="):
            return line.split("=", 1)[1].strip()
    return None


def notify_discord(msg: str) -> bool:
    url = read_discord_webhook()
    if not url:
        log.warning("No DISCORD_WEBHOOK_URL in local/.env, skip notify")
        return False
    try:
        r = requests.post(url, json={"content": msg}, timeout=10)
        return r.status_code == 204
    except Exception as e:
        log.error("Discord notify failed: %s", e)
        return False


def main() -> int:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    success, detail = probe_mops()
    log.info("MOPS probe: %s (%s)", "SUCCESS" if success else "FAIL", detail)

    state = load_state()
    state.setdefault("history", []).append({"ts": now, "ok": success, "detail": detail})
    state["history"] = state["history"][-30:]  # keep last 30

    if success:
        state["consecutive_successes"] = state.get("consecutive_successes", 0) + 1
        log.info("Consecutive successes: %d / %d", state["consecutive_successes"], NOTIFY_THRESHOLD)

        if state["consecutive_successes"] >= NOTIFY_THRESHOLD and not state.get("notified"):
            msg = (
                f"✅ **MOPS WAF 解禁** — {now}\n"
                f"連續 {state['consecutive_successes']} 天探測成功，可恢復 USE_MOPS=true：\n"
                f"改 `cache_manager.py:21` 預設回 `\"true\"`，或 `set USE_MOPS=true`"
            )
            if notify_discord(msg):
                log.info("Discord notified")
                state["notified"] = True
            else:
                log.warning("Discord notify failed, will retry next run")
    else:
        if state.get("consecutive_successes", 0) > 0:
            log.info("Reset consecutive from %d to 0", state["consecutive_successes"])
        state["consecutive_successes"] = 0
        state["notified"] = False  # reset so 下次連續成功可再通知

    save_state(state)
    return 0  # always 0，不想讓 Task Scheduler 標紅


if __name__ == "__main__":
    sys.exit(main())
