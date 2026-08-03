"""reconcile_panel_vs_official.py -- panel 與官方 EOD 逐日對帳（抽樣）

## 為什麼需要

`tools/scan_panel_price_outliers.py` 抓的是「價格量級不可能」那類毀損（3666 被 ×10000）。
但 2026-08-02 回填 panel 時撞到另一類**它抓不到**的異常：`2016-09-12` 那天約 24% 的
股票價格與官方原始價的還原係數對不上，偏差 0.1%~5.6%（中位 0.37%）。

那類異常在 panel 內部完全看不出來 —— 每一欄都是正數、量級正常、時間序列連續。
**只有跟官方對帳才看得見**，而全歷史 5,054 個交易日逐日打官方不現實（單日兩市兩次
請求 + 節流）。所以本工具做**抽樣**，目的是**界定範圍**而不是逐日修。

## 比法：用報酬而不是價格

panel 存的是還原價、官方給原始價，直接比價格會被還原係數干擾。改比「相鄰交易日
報酬」—— 還原係數在報酬裡相消，兩邊可以直接比：

    panel_ret(D-1 -> D)  vs  official_ret(D-1 -> D)

兩者不一致的檔數比例就是該日的「對帳失敗率」。

## ⚠️ 最大的陷阱：回填日不能當比對基準（會給出假乾淨）

`tools/backfill_panel_gaps.py` 補進去的列是「官方價 × 還原係數」算出來的，所以那些列
**跟官方報酬必然吻合**。若某日的前一交易日是回填日，對帳就變成「拿官方比官方」，
失敗率會趨近 0 —— 那是自我印證，不是資料乾淨。

實例（2026-08-03 踩到）：`2016-09-12` 的前一交易日回填後變成 `2016-09-10`，於是對帳
回報 0.09% 看起來完美；但同一週未被回填污染的 `09-08 / 09-09 / 09-13 / 09-14` 全都是
**24~27%**。我第一次的「對照組」`2017-02-20` 也中同一個坑（前一交易日 `2017-02-18`
也是回填日）。

所以本工具**預設跳過**「自己或前一交易日在回填清單內」的日期，並在報告裡列出跳過原因。
要看那些日子得明確給 `--allow-backfilled`，且結果只能當下限。

## 已排除的兩個假設（別再重複驗）

- **日期錯位**：panel 的 2016-09-12 那列最吻合官方 2016-09-12（報酬吻合 75.6%），
  不是鄰日（09-08 只 3.9% / 09-13 只 11.0%）。**那列裝的是對的那天。**
- **低價股量化雜訊**：官方原始價只有 2 位小數，本來懷疑是量化造成的假不一致。
  實測推翻 —— 不一致率在各價位帶都是 20~28%（平坦），且偏差中位數是半 tick 的
  **19.7 倍**、p90 達 99 倍，遠超量化下限。對照日 2017-02-20 只有 0.2% 不一致。

## 2026-08-03 的範圍結論（本工具第一次實跑）

**異常是一個有界的時間窗，不是散佈全歷史**：

- **窗口 = 2016-08-22 ~ 2016-10-03，共 30 個交易日**（佔全歷史 5,054 天的 **0.59%**）。
  兩端都釘到相鄰交易日：`2016-08-19`（五）0.62% 乾淨 → `2016-08-22`（一）21.98% 開始壞；
  `2016-10-03` 21.76% 壞 → `2016-10-04` 0.21% 恢復。
- 窗口內每天約 **22~26% 的股票**對不上（320~400 檔），偏差中位 **0.34~0.38%**，
  但**尾巴可到 19~21%**（少數個股），所以不能一句「誤差很小」帶過。
- **窗口外乾淨**：跨 2015-2026 抽 27 天（已排除回填污染），失敗率中位 **0.75%**、
  p90 1.41%、**零個超過 5%**。

**根因仍未知**（日期錯位與量化雜訊兩個假設都已排除，見上）。因為範圍有界且量級不大，
未修 —— 修的話是覆寫既有列，那跟 `backfill_panel_gaps`「絕不覆寫」的原則相反，屬決策。

用法：
    python tools/reconcile_panel_vs_official.py --sample 30        # 跨年份抽 30 天
    python tools/reconcile_panel_vs_official.py --dates 2016-09-12 2017-02-20
    python tools/reconcile_panel_vs_official.py --sample 40 --seed-dates thin
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

PANEL = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"

# 報酬吻合的容忍度。0.05% 已遠大於官方 2 位小數的量化下限（實測半 tick 相對大小
# 中位 0.016%），又小到抓得出 2016-09-12 那類 0.1%+ 的偏差。
RET_TOL = 0.0005
# 該日「對帳失敗率」超過這個值就列為可疑。2016-09-12 是 24.4%，正常日約 0.2%，
# 中間留很寬的餘裕避免假警報。
SUSPECT_RATE = 0.05
# 可比檔數低於此不做判斷（避免早期歷史或部分橫斷面日給出無意義的比例）
MIN_COMPARABLE = 200
THROTTLE_SEC = 1.5

# 已知的部分橫斷面日（回填過但仍偏低者的鄰日值得一起看）
THIN_DATES = [
    "2016-01-30", "2016-06-04", "2016-09-10", "2017-02-18", "2017-06-03",
    "2017-09-30", "2018-03-31", "2018-12-22", "2019-09-09", "2021-04-06",
    "2025-08-01",
]
# 2026-08-02 由 backfill_panel_gaps 補過的所有日期。這些列是官方價推導出來的，
# 拿它們當比對基準等於「官方比官方」—— 見檔頭的陷阱說明。
BACKFILLED = set(THIN_DATES) | {
    "2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17",
    "2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24",
    "2026-04-27", "2026-04-28", "2026-04-29",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("reconcile_panel_vs_official")


class Official:
    def __init__(self):
        from twse_api import TWSEOpenData
        self._api = TWSEOpenData()
        self._memo: dict[str, dict] = {}

    def closes(self, day: str) -> dict:
        if day in self._memo:
            return self._memo[day]
        time.sleep(THROTTLE_SEC)
        try:
            df = self._api.get_market_daily_all(
                date=datetime.strptime(day, "%Y-%m-%d"), strict_date=True)
        except Exception as exc:
            log.warning("官方 %s 抓取失敗：%s", day, repr(exc)[:100])
            self._memo[day] = {}
            return {}
        m = {}
        if df is not None and not df.empty:
            for r in df.itertuples():
                try:
                    c = float(r.close)
                except (TypeError, ValueError):
                    continue
                if c > 0:
                    m[str(r.stock_id)] = c
        self._memo[day] = m
        return m


def audit_day(day, prev_day, panel_closes, src):
    """回 (可比檔數, 不吻合檔數, 偏差中位數) 或 None。"""
    pb, pt = panel_closes.get(prev_day), panel_closes.get(day)
    if not pb or not pt:
        return None
    ob, ot = src.closes(prev_day), src.closes(day)
    if not ob or not ot:
        return None
    devs = []
    n = 0
    for s, ptc in pt.items():
        pbc, obc, otc = pb.get(s), ob.get(s), ot.get(s)
        if not (pbc and obc and otc) or pbc <= 0 or obc <= 0:
            continue
        n += 1
        d = abs((ptc / pbc) - (otc / obc))
        devs.append(d)
    if n < MIN_COMPARABLE:
        return None
    # 回「不吻合者」的偏差中位數，不是全體 —— 全體的中位數幾乎恆為 0（多數股票吻合），
    # 印出來看起來永遠很漂亮，完全無法判斷嚴重程度。
    bad = sorted(d for d in devs if d > RET_TOL)
    return n, len(bad), (bad[len(bad) // 2] if bad else 0.0), (bad[-1] if bad else 0.0)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", type=int, default=0,
                    help="跨年份等距抽 N 個交易日")
    ap.add_argument("--dates", nargs="*", help="指定日期")
    ap.add_argument("--seed-dates", choices=["thin"], help="額外加入已知可疑日清單")
    ap.add_argument("--allow-backfilled", action="store_true",
                    help="連回填日一起對帳。⚠️ 那會給出假乾淨（見檔頭），結果只能當下限")
    args = ap.parse_args()

    if not PANEL.exists():
        log.error("找不到 %s", PANEL)
        return 2
    df = pd.read_parquet(PANEL, columns=["date", "stock_id", "Close"])
    df["date"] = pd.to_datetime(df["date"])
    df["stock_id"] = df["stock_id"].astype(str)
    by_day = {d.strftime("%Y-%m-%d"): g.set_index("stock_id")["Close"].astype(float).to_dict()
              for d, g in df.groupby("date")}
    all_days = sorted(by_day)

    targets = list(args.dates or [])
    if args.seed_dates == "thin":
        targets += THIN_DATES
    if args.sample:
        # 只抽 2015 年後（官方端點覆蓋穩定、panel 檔數也夠）
        pool = [d for d in all_days if d >= "2015-01-01"]
        step = max(1, len(pool) // args.sample)
        targets += pool[::step][:args.sample]
    targets = sorted(set(targets))
    if not targets:
        log.error("沒有目標日期；用 --sample N 或 --dates")
        return 2

    log.info("對帳 %d 個日期（容忍度 %.2f%%，可疑門檻 %.0f%%）",
             len(targets), 100 * RET_TOL, 100 * SUSPECT_RATE)

    src = Official()
    rows, skipped = [], []
    for i, day in enumerate(targets, 1):
        if day not in by_day:
            skipped.append((day, "panel 無此日"))
            continue
        pos = all_days.index(day)
        if pos == 0:
            skipped.append((day, "無前一交易日"))
            continue
        prev = all_days[pos - 1]
        if not args.allow_backfilled and (day in BACKFILLED or prev in BACKFILLED):
            which = day if day in BACKFILLED else prev
            skipped.append((day, f"{which} 是回填日，當基準會假乾淨"))
            continue
        res = audit_day(day, prev, by_day, src)
        if res is None:
            skipped.append((day, "可比檔數不足或官方回空"))
            continue
        n, bad, med, mx = res
        rows.append((day, n, bad, bad / n, med, mx))
        if i % 10 == 0:
            log.info("[%d/%d] 已對帳 %d 天", i, len(targets), len(rows))

    rows.sort(key=lambda r: -r[3])
    print()
    print("=" * 76)
    print("panel vs 官方 逐日對帳（抽樣 %d 天，成功 %d）" % (len(targets), len(rows)))
    print("=" * 76)
    print("%-12s %7s %7s %9s %12s %10s"
          % ("日期", "可比", "不吻合", "失敗率",
             "不吻合偏差中位", "最大"))
    for day, n, bad, rate, med, mx in rows:
        flag = "  <== 可疑" if rate > SUSPECT_RATE else ""
        print("%-12s %7d %7d %8.2f%% %11.3f%% %9.3f%%%s"
              % (day, n, bad, 100 * rate, 100 * med, 100 * mx, flag))
    if skipped:
        print("\n跳過 %d 天：%s%s" % (len(skipped), skipped[:6], " …" if len(skipped) > 6 else ""))

    sus = [r for r in rows if r[3] > SUSPECT_RATE]
    rates = sorted(r[3] for r in rows)
    print("\n" + "-" * 76)
    if rates:
        print("失敗率分布：中位 %.2f%%  p90 %.2f%%  max %.2f%%"
              % (100 * rates[len(rates) // 2], 100 * rates[int(len(rates) * 0.9)],
                 100 * rates[-1]))
    print("可疑日（失敗率 > %.0f%%）：%d / %d 天" % (100 * SUSPECT_RATE, len(sus), len(rows)))
    for day, n, bad, rate, med, mx in sus:
        print("   %s  %.1f%%（%d/%d 檔）偏差中位 %.3f%% / 最大 %.3f%%"
              % (day, 100 * rate, bad, n, 100 * med, 100 * mx))
    if not sus:
        print("   無 —— 抽樣範圍內只有已知的 2016-09-12 那類，未擴散")
    return 0


if __name__ == "__main__":
    sys.exit(main())
