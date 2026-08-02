"""backfill_panel_gaps.py -- 用官方 EOD 補 ohlcv_tw 的缺口交易日

## 為什麼需要這支

`ohlcv_tw.parquet` 有兩類缺口，成因都是 yfinance 端缺資料（官方端點證實那些日子
有正常成交）：

1. **部分橫斷面日**：11 個真實交易日只存了 33~38% 的橫斷面，且連 2330 / 2317 /
   2454 / 1101 四大權值股一起缺（多為台股補行交易的週六）。
2. **2026-04-13~04-29 抓取斷層**：13 個交易日只有約 1,700 檔而非 1,965，04-30 一次全回。

## 三個「照直覺做就會毀資料」的地方

**① 補 CSV，不要補 parquet。** `ohlcv_tw.parquet` 是衍生檔，來源是
`data_cache/{id}_price.csv`。直接改 parquet 會在下次 `refresh_backtest_panels`
聚合時被蓋回去。本工具寫 CSV，再由 `--rebuild-panel` 觸發既有聚合。

**② CSV 存的是還原價，官方給的是原始價。** 實測 2021-04-01：2330 panel 548.97 /
官方 602.0（係數 0.912）、2454 是 681.61 / 961.0（0.709）。無公司行為的股票係數
剛好是 1，所以當天有 68.5% 看起來相同 —— **別被這個比例騙成「panel 是原始價」**。
直接塞原始價會在 2330 那類股票製造假跳空，比留著缺口更糟。

**③ 還原係數要從相鄰交易日反推，且前後必須一致。** 係數在兩次公司行為之間是常數，
所以取缺口日 D 的前一交易日 P 與後一交易日 N（panel 兩邊都有），算
    price_factor = panel_close(鄰日) / official_close(鄰日)
前後兩個 factor 一致才套用；不一致代表 P~N 之間有除權息，該檔**跳過不硬補**。
實測一致率：2021-04-06 = 1,652/1,667 (99.1%)、2025-08-01 = 1,831/1,833 (99.9%)。

**④ 成交量不能套同一招。** 一開始我對量也要求「前後 factor 一致到 0.1%」，結果
2025-08-01 有 912 檔（84%）被這條擋掉。查了才知道前提就不成立：**yfinance 的量與
官方成交股數本來就有出入**，實測只有 41~43% 完全相同，比值 p1=0.32 / p50≈1.00 /
p99=1.46 —— 那是資料源差異，不是公司行為，根本沒有乾淨的乘法係數可反推。
所以量改成：預設直接用官方成交股數（它才是權威值，而且比鄰列的 yfinance 量更準），
只有在前後兩個比值**彼此接近且明顯偏離 1**（＝真的發生過分割／股票股利，yfinance
會把歷史量乘上比例）時才套係數。量的不確定性不該讓一整檔的價格補不進來。

## 安全設計

- 預設 `--dry-run`，要寫入必須明確給 `--apply`。
- **絕不覆寫既有列**：CSV 已經有該日期就跳過（那是真資料，不管值對不對）。
- factor 超出 `[MIN_FACTOR, MAX_FACTOR]` 一律跳過 —— 這道界線在本工具首跑時就派上
  用場：`3666` 當時整段被 ×10000（2021-04-01 記 653000.0，官方 65.3），不設界會把它
  當成合法係數擴散到補進去的每一列。**該檔已由 `tools/repair_3666_bogus_split.py`
  修好，但界線要留** —— 下一個壞掉的不會有人先通知我們，且常設掃描
  `tools/scan_panel_price_outliers.py` 是事後偵測，這裡是事前阻擋。
- 只補「已經有 CSV 檔」的股票；不為官方有、本地沒有的代號建新檔（那會改動 universe）。
- 寫入走暫存檔 + `os.replace` 原子替換，中途中斷不會留半截 CSV。

用法：
    python tools/backfill_panel_gaps.py --dry-run            # 全部已知缺口，只報告
    python tools/backfill_panel_gaps.py --dates 2025-08-01 --dry-run
    python tools/backfill_panel_gaps.py --apply --rebuild-panel
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

CACHE_DIR = REPO / "data_cache"
PANEL_PATH = CACHE_DIR / "backtest" / "ohlcv_tw.parquet"

# 11 個部分橫斷面日（官方 MI_INDEX 證實都有成交，是 yfinance 端缺資料）
THIN_DATES = [
    "2016-01-30", "2016-06-04", "2016-09-10", "2017-02-18", "2017-06-03",
    "2017-09-30", "2018-03-31", "2018-12-22", "2019-09-09", "2021-04-06",
    "2025-08-01",
]
# 2026-04 抓取斷層（04-30 一次全回，所以 04-29 為止）
GAP_DATES = [
    "2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17",
    "2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24",
    "2026-04-27", "2026-04-28", "2026-04-29",
]

# 價格 factor：前後差多少內算「期間無公司行為」。實測一致率 99.1~99.9%，可以嚴格。
FACTOR_TOL = 0.001
# 量 factor：只用來抓分割／股票股利。yfinance 與官方的量本來就有 ±數%的雜訊
# （只有 41~43% 完全相同），所以要「兩邊比值彼此接近」**且**「明顯偏離 1」才算數，
# 否則一律視為無調整、直接用官方量。
VOL_SPLIT_MIN_DEVIATION = 0.20   # 偏離 1 超過 20% 才可能是分割
VOL_SPLIT_AGREE_TOL = 0.05       # 前後兩個比值要在 5% 內一致
# factor 合理界線。放這麼寬是因為減資／大額股票股利真的會到數倍，
# 但 10000 倍只可能是資料毀損（見檔頭）。
MIN_FACTOR, MAX_FACTOR = 0.02, 50.0
# 官方端點節流（兩個市場各一次請求，別打太快）
THROTTLE_SEC = 1.5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("backfill_panel_gaps")


# --------------------------------------------------------------------------- #
#  官方橫斷面（含 in-run memo，相鄰日會被多個缺口日共用）
# --------------------------------------------------------------------------- #

class OfficialSource:
    def __init__(self):
        from twse_api import TWSEOpenData
        self._api = TWSEOpenData()
        self._memo: dict[str, dict] = {}

    def cross_section(self, day: str) -> dict:
        """回 {stock_id: (open, high, low, close, volume)}；非交易日回 {}。"""
        if day in self._memo:
            return self._memo[day]
        time.sleep(THROTTLE_SEC)
        try:
            df = self._api.get_market_daily_all(
                date=datetime.strptime(day, "%Y-%m-%d"), strict_date=True)
        except Exception as exc:
            log.warning("官方 EOD %s 抓取失敗：%s", day, repr(exc)[:120])
            self._memo[day] = {}
            return {}
        out = {}
        if df is not None and not df.empty:
            for r in df.itertuples():
                try:
                    o, h, l, c = (float(r.open), float(r.high),
                                  float(r.low), float(r.close))
                    v = float(r.volume)
                except (TypeError, ValueError):
                    continue
                # 全欄為正才收；0 或負數是停牌／填充列，補進去只會製造毒資料
                if min(o, h, l, c) > 0 and v > 0:
                    out[str(r.stock_id)] = (o, h, l, c, v)
        self._memo[day] = out
        return out


# --------------------------------------------------------------------------- #
#  per-stock CSV
# --------------------------------------------------------------------------- #

def csv_path(sid: str) -> Path:
    return CACHE_DIR / f"{sid}_price.csv"


def load_csv(sid: str) -> pd.DataFrame | None:
    p = csv_path(sid)
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p, index_col=0)
    except Exception as exc:
        log.warning("讀 %s 失敗：%s", p.name, repr(exc)[:80])
        return None
    df.index = pd.to_datetime(df.index, errors="coerce")
    return df[df.index.notna()]


def write_csv(sid: str, df: pd.DataFrame) -> None:
    """原子寫回。index 名保持空字串（原檔頭是 `,Open,High,...`）。"""
    p = csv_path(sid)
    tmp = p.with_suffix(".csv.tmp")
    out = df.sort_index()
    out.index.name = None
    out.to_csv(tmp, date_format="%Y-%m-%d")
    os.replace(tmp, p)


# --------------------------------------------------------------------------- #
#  還原係數
# --------------------------------------------------------------------------- #

def _factor(panel_val: float, official_val: float) -> float | None:
    if official_val is None or official_val <= 0 or panel_val is None or panel_val <= 0:
        return None
    return panel_val / official_val


def resolve_factors(csv_df, sid, prev_day, next_day, off_prev, off_next):
    """回 (price_factor, volume_factor, 跳過原因)。

    價格：前後 factor 必須一致，否則跳過（期間有除權息，硬補會製造假跳空）。
    成交量：預設 1.0（直接用官方成交股數），只有偵測到分割才套係數 —— 理由見檔頭 ④。
    """
    if sid not in off_prev or sid not in off_next:
        return None, None, "官方鄰日無此檔"
    try:
        row_p = csv_df.loc[pd.Timestamp(prev_day)]
        row_n = csv_df.loc[pd.Timestamp(next_day)]
    except KeyError:
        return None, None, "CSV 鄰日無資料"
    if isinstance(row_p, pd.DataFrame) or isinstance(row_n, pd.DataFrame):
        return None, None, "CSV 鄰日重複列"

    # --- 價格 ---
    fp = _factor(_num(row_p.get("Close")), off_prev[sid][3])
    fn = _factor(_num(row_n.get("Close")), off_next[sid][3])
    if fp is None or fn is None:
        return None, None, "價格 factor 算不出"
    if abs(fp - fn) / max(fp, 1e-9) > FACTOR_TOL:
        return None, None, "價格前後 factor 不一致（期間有除權息）"
    if not (MIN_FACTOR <= fp <= MAX_FACTOR):
        return None, None, f"價格 factor {fp:.4g} 超出合理範圍"

    # --- 成交量 ---
    vp = _factor(_num(row_p.get("Volume")), off_prev[sid][4])
    vn = _factor(_num(row_n.get("Volume")), off_next[sid][4])
    vf = 1.0
    if vp is not None and vn is not None:
        agree = abs(vp - vn) / max(vp, 1e-9) <= VOL_SPLIT_AGREE_TOL
        deviates = min(abs(vp - 1.0), abs(vn - 1.0)) >= VOL_SPLIT_MIN_DEVIATION
        if agree and deviates and MIN_FACTOR <= vp <= MAX_FACTOR:
            vf = (vp + vn) / 2.0      # 判定為分割／股票股利，比照鄰列縮放
    return fp, vf, None


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(f) else f


# --------------------------------------------------------------------------- #
#  主流程
# --------------------------------------------------------------------------- #

def neighbours(panel_dates: list[pd.Timestamp], day: str, exclude: set):
    """panel 裡該日的前一個 / 後一個**健康**日期。

    ⚠️ 必須排除其他缺口日：2026-04-13~04-29 是連續 13 天的缺口，若只取「最近的既有
    日期」會挑到隔壁同樣殘缺的那天當基準 —— 缺的股票在鄰日多半也缺，於是整批被
    「CSV 鄰日無資料」擋掉，等於白做。改取缺口區塊外的最近健康日（例：04-10 / 04-30）。
    還原係數在兩次公司行為之間是常數，拉長到 2.5 週仍成立；期間真有除權息的個股
    會被前後 factor 一致性檢查擋下來，不會硬補。
    """
    ts = pd.Timestamp(day)
    ok = [d for d in panel_dates if d.strftime("%Y-%m-%d") not in exclude]
    before = [d for d in ok if d < ts]
    after = [d for d in ok if d > ts]
    if not before or not after:
        return None, None
    return before[-1].strftime("%Y-%m-%d"), after[0].strftime("%Y-%m-%d")


def backfill_day(day, panel_dates, src, apply_changes, exclude):
    prev_day, next_day = neighbours(panel_dates, day, exclude)
    if prev_day is None:
        log.warning("%s：panel 沒有前後鄰日，跳過", day)
        return {"date": day, "official": 0, "added": 0, "skipped": {"無鄰日": 1}}

    off_d = src.cross_section(day)
    if not off_d:
        log.warning("%s：官方回空（非交易日或抓取失敗），跳過", day)
        return {"date": day, "official": 0, "added": 0, "skipped": {"官方回空": 1}}
    off_p = src.cross_section(prev_day)
    off_n = src.cross_section(next_day)

    added = 0
    skipped: dict[str, int] = {}
    samples: list[str] = []

    def bump(reason):
        skipped[reason] = skipped.get(reason, 0) + 1

    for sid, (o, h, l, c, v) in sorted(off_d.items()):
        df = load_csv(sid)
        if df is None:
            bump("本地無 CSV（不建新檔）")
            continue
        if pd.Timestamp(day) in df.index:
            bump("CSV 已有該日（不覆寫）")
            continue
        pf, vf, why = resolve_factors(df, sid, prev_day, next_day, off_p, off_n)
        if why:
            bump(why)
            continue

        row = {"Open": o * pf, "High": h * pf, "Low": l * pf,
               "Close": c * pf, "Volume": v * vf}
        # Adj Close 沿用鄰列慣例：早期為空、近期等於 Close。
        # 用 float('nan') 而不是 pd.NA —— 後者是 object dtype，塞進整欄皆空的
        # `Adj Close` 會觸發 pandas 的 all-NA concat dtype 推斷 FutureWarning。
        if "Adj Close" in df.columns:
            nb = _num(df.loc[pd.Timestamp(prev_day)].get("Adj Close"))
            row["Adj Close"] = row["Close"] if nb is not None else float("nan")

        if len(samples) < 3:
            samples.append(f"{sid} 原始 close={c:.2f} ×{pf:.4f} -> {row['Close']:.2f}")
        if apply_changes:
            df.loc[pd.Timestamp(day)] = row
            write_csv(sid, df.reindex(columns=list(df.columns)))
        added += 1

    return {"date": day, "prev": prev_day, "next": next_day,
            "official": len(off_d), "added": added, "skipped": skipped,
            "samples": samples}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True,
                   help="只報告不寫入（預設）")
    g.add_argument("--apply", action="store_true",
                   help="實際寫入 CSV（必須明確指定）")
    ap.add_argument("--dates", nargs="*",
                    help="指定要補的日期（YYYY-MM-DD）；預設補全部已知缺口")
    ap.add_argument("--rebuild-panel", action="store_true",
                    help="寫入後重跑 aggregate_csv_to_parquet 重建 ohlcv_tw.parquet")
    args = ap.parse_args()

    apply_changes = bool(args.apply)
    targets = args.dates if args.dates else (THIN_DATES + GAP_DATES)

    if not PANEL_PATH.exists():
        log.error("找不到 %s", PANEL_PATH)
        return 2
    panel_dates = sorted(pd.to_datetime(
        pd.read_parquet(PANEL_PATH, columns=["date"])["date"]).unique())
    panel_dates = [pd.Timestamp(d) for d in panel_dates]

    log.info("模式=%s；目標 %d 個日期；panel 現有 %d 個日期",
             "APPLY（會寫入）" if apply_changes else "DRY-RUN（不寫入）",
             len(targets), len(panel_dates))

    src = OfficialSource()   # 單一實例：相鄰日的橫斷面會被多個缺口日共用，靠 memo 省請求
    # 鄰日一律排除「所有已知缺口日」，不只是這次要補的那幾天 —— 否則單獨補
    # --dates 2026-04-15 時仍會拿到同樣殘缺的 04-14 當基準。
    exclude = set(THIN_DATES) | set(GAP_DATES) | set(targets)
    results = [backfill_day(d, panel_dates, src, apply_changes, exclude) for d in targets]

    total_added = sum(r["added"] for r in results)
    print()
    print("=" * 78)
    print("回填報告（%s）" % ("已寫入" if apply_changes else "DRY-RUN，未寫入"))
    print("=" * 78)
    for r in results:
        print("\n%s  官方 %d 檔 -> 可補 %d 檔   (鄰日 %s / %s)"
              % (r["date"], r["official"], r["added"],
                 r.get("prev", "-"), r.get("next", "-")))
        for reason, n in sorted(r["skipped"].items(), key=lambda kv: -kv[1]):
            print("     跳過 %-5d %s" % (n, reason))
        for s in r.get("samples", []):
            print("     例：%s" % s)
    print("\n" + "-" * 78)
    print("合計可補 %d 列 / %d 個日期" % (total_added, len(results)))
    if not apply_changes:
        print("這是 dry-run。確認無誤後加 --apply（可再加 --rebuild-panel）。")

    if apply_changes and args.rebuild_panel:
        log.info("重建 ohlcv_tw.parquet …")
        from tools.refresh_backtest_panels import aggregate_csv_to_parquet
        aggregate_csv_to_parquet()
        log.info("重建完成")

    return 0


if __name__ == "__main__":
    sys.exit(main())
