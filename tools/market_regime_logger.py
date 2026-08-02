"""
market_regime_logger.py
=======================
每日 scan 時執行，計算當天 market regime 並 append 到 regime_log.jsonl。

用途：VF-G4 shadow run — 不改 scanner 邏輯，但累積 regime log 供事後比對
「如果只 volatile 時 scan」的表現。

Regime rules（對齊 qm_historical_simulator.build_regime_series）：
  - trending:  20d return > 5%
  - volatile:  20d high-low range / avg > 8%
  - ranging:   abs(20d ret) < 2% and range <= 8%
  - neutral:   其他

Market proxy：等權 top300（對齊 VF-G4 驗證邏輯）。

Output: data/tracking/regime_log.jsonl
  {"date":"2026-04-21","regime":"volatile","ret_20d":0.024,"range_20d":0.092,"sharpe_60d":1.8}
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("regime_log")

OHLCV_PATH = ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"
LOG_PATH = ROOT / "data" / "tracking" / "regime_log.jsonl"


def load_top300():
    """從 top300_universe.json 讀 universe。"""
    p = ROOT / "data_cache" / "backtest" / "top300_universe.json"
    if p.exists():
        return json.loads(p.read_text(encoding='utf-8'))
    logger.warning("top300_universe.json 不存在，fallback 用 qm_result.json 內所有 picks")
    qm = ROOT / "data" / "latest" / "qm_result.json"
    if qm.exists():
        data = json.loads(qm.read_text(encoding='utf-8'))
        return [p['stock_id'] for p in data.get('results', data.get('picks', []))[:300]]
    return []


def _twse_trading_days_between(start: pd.Timestamp, end: pd.Timestamp) -> list:
    """回傳 start < d <= end 之間的候選日（含 end，排除 start 當日）。

    用 pandas 工作日（週一到五）當候選，**不是真正的交易日曆** —— 國定假日與尚未
    開盤的今天都會被列進來。真正的把關在 `_fetch_close_supplement`：官方 EOD 對非
    交易日不會回該日資料，該日就自然被跳過。
    """
    days = pd.bdate_range(start=start + pd.Timedelta(days=1), end=end)
    return [d for d in days]


def _fetch_close_supplement(members: set, dates: list,
                            min_coverage: float = 0.90) -> pd.Series:
    """對每個日期呼叫官方 EOD，用「與 parquet 相同的成分股集合」算等權 close。

    兩個條件缺一不可，否則補出來的點不是行情而是雜訊：

    1. **日期以 payload 自報為準**（`strict_date=True`）。TPEX 的**舊** stk_quote
       端點無視 date 參數（現已改打認日期的 `dailyQuotes`，但驗證照做），
       對尚未開盤的今天／假日會回「上一場」的完整橫斷面。舊版拿它
       當今天用，而 TWSE 該日正確地回空，於是 `df` 變成純上櫃 —— 過濾 top300 後只
       剩 51 檔上櫃成分股，而高價股正集中在上櫃（信驊 14525、旺矽 5280），等權均價
       被抬成 1.835 倍，`ret_20d` 直接飆到 +70%，`range_20d` 中位數 0.934（門檻
       0.08），使週一~週四永遠被判成 volatile（2026-08-02 查證）。

    2. **成分股必須與 parquet 那段序列相同**。等權「均價」對成分極度敏感，序列混用
       不同成分時，`pct_change` 算出來的是成分變動而不是報酬。所以這裡只取 parquet
       自己那批成分股，且命中率不足 `min_coverage` 就整天不補（寧缺勿錯）。
    """
    from twse_api import TWSEOpenData
    api = TWSEOpenData()
    result = {}
    members = set(map(str, members))
    if not members:
        return pd.Series(dtype='float64')
    floor = max(1, int(len(members) * min_coverage))
    for d in dates:
        try:
            df = api.get_market_daily_all(date=d.to_pydatetime(), strict_date=True)
            if df is None or df.empty:
                logger.info("  official EOD %s: no data for that date (假日/尚未收盤) -- skipped",
                            d.strftime('%Y-%m-%d'))
                continue
            stamped = sorted({pd.Timestamp(x).normalize()
                              for x in df.get('data_date', pd.Series(dtype='object')).dropna().unique()})
            if stamped != [pd.Timestamp(d).normalize()]:
                logger.warning("  official EOD %s: payload self-reports %s -- skipped",
                               d.strftime('%Y-%m-%d'),
                               [str(x.date()) for x in stamped] or None)
                continue
            df = df[df['stock_id'].astype(str).isin(members)]
            close = pd.to_numeric(df['close'], errors='coerce')
            close = close[close > 0].dropna()
            if len(close) < floor:
                logger.warning("  official EOD %s: only %d/%d proxy members present "
                               "(< %d) -- skipped to keep the series composition-stable",
                               d.strftime('%Y-%m-%d'), len(close), len(members), floor)
                continue
            avg = float(close.mean())
            result[pd.Timestamp(d).normalize()] = avg
            logger.info("  official EOD %s: %d/%d proxy members, avg=%.2f",
                        d.strftime('%Y-%m-%d'), len(close), len(members), avg)
        except Exception as e:
            logger.warning("  official EOD %s failed: %s", d.strftime('%Y-%m-%d'), e)
    return pd.Series(result).sort_index() if result else pd.Series(dtype='float64')


def _regime_features(daily_avg: pd.Series) -> tuple:
    """由等權均價序列算出 (ret20, range20, sharpe60) 三條 Series。"""
    ret20 = daily_avg.pct_change(20)
    rolling_max = daily_avg.rolling(20, min_periods=10).max()
    rolling_min = daily_avg.rolling(20, min_periods=10).min()
    rolling_avg = daily_avg.rolling(20, min_periods=10).mean()
    range20 = (rolling_max - rolling_min) / rolling_avg.replace(0, np.nan)
    daily_ret = daily_avg.pct_change()
    sharpe60 = (
        daily_ret.rolling(60, min_periods=30).mean() /
        daily_ret.rolling(60, min_periods=30).std().replace(0, np.nan) *
        np.sqrt(60)
    )
    return ret20, range20, sharpe60


def classify_regime(r20, rng20) -> str:
    """Rule-based classification（對齊 VF-G4 驗證）。

    唯一實作 —— `compute_today_regime` 與 `--repair-history` 都走這裡，避免規則在
    兩處漂移（`tools/line3_liquidity_regime.py` 已因不信任 jsonl 而自帶一份）。
    """
    if pd.isna(rng20) or pd.isna(r20):
        return 'neutral'
    if rng20 > 0.08:
        return 'volatile'
    if r20 > 0.05:
        return 'trending'
    if abs(r20) < 0.02 and rng20 <= 0.08:
        return 'ranging'
    return 'neutral'


def _entry(date, r20, rng20, s60) -> dict:
    return {
        'date': pd.Timestamp(date).strftime('%Y-%m-%d'),
        'regime': classify_regime(r20, rng20),
        'ret_20d': round(float(r20), 4) if not pd.isna(r20) else None,
        'range_20d': round(float(rng20), 4) if not pd.isna(rng20) else None,
        'sharpe_60d': round(float(s60), 3) if not pd.isna(s60) else None,
        'proxy': 'equal_weight_top300',
    }


MIN_PROXY_COVERAGE = 0.80


def _drop_thin_proxy_dates(proxy: pd.DataFrame,
                           min_coverage: float = MIN_PROXY_COVERAGE) -> pd.DataFrame:
    """剔除代理成分數明顯不足的日期。

    等權「均價」對成分極度敏感，序列混用不同成分時 `pct_change` 算出來的是成分變動
    而不是報酬。`ohlcv_tw.parquet` 有 11 個**真實交易日**只存了 33~38% 的橫斷面
    （多為台股補行交易的週六，另有 2019-09-09 / 2021-04-06 / 2025-08-01；官方
    MI_INDEX 證實這些日子有 1,100~1,300 檔成交，是 yfinance 端缺資料，連 2330 /
    2317 / 2454 / 1101 都沒有）。那些日子 top300 代理只剩約 100~132 檔而非 294，
    2016-06-04 因此產生 |ret_20d| > 30% 的假值（2026-08-02 查證）。

    門檻用「前後 21 個交易日的成分數中位數」而非固定值，才不會誤殺早期歷史
    （2006-2009 全市場本來就只有數百檔）。

    ✅ **2026-08-02 更新：上述 11 天已由 `tools/backfill_panel_gaps.py` 回填**
    （官方 EOD 逐日補進 per-stock CSV，共 13,004 列）。其中 **10 天覆蓋率回到
    84~91%、超過本門檻不再被剔除**，放行後 top300 成分數回到 297~300 檔、
    `ret_20d` 全落在 −1.4%~+11.1%（2016-06-04 由 >30% 的假值變成合理的 +9.2%）。
    只剩 `2016-09-10` 仍是 64.2% 會被擋（它的鄰日 2016-09-12 價格本身不可信，
    詳 `docs/agent/data-sources.md`）。
    → **本函式從「症狀處理」退回成安全網**：根因（panel 缺資料）已除，但門檻保留，
    因為下一批缺資料不會有人先通知我們。
    """
    counts = proxy.groupby('date')['stock_id'].nunique().sort_index()
    baseline = counts.rolling(21, center=True, min_periods=5).median()
    ratio = counts / baseline
    thin = ratio[ratio < min_coverage]
    if len(thin):
        logger.warning("Dropping %d date(s) with thin proxy coverage (< %.0f%% of the "
                       "21-session median member count): %s",
                       len(thin), min_coverage * 100,
                       ', '.join(f"{d.date()}({counts[d]}/{baseline[d]:.0f})"
                                 for d in thin.index[:12]))
    return proxy[~proxy['date'].isin(thin.index)]


def _load_proxy() -> tuple:
    """回 (proxy DataFrame, 等權均價 Series)。純 parquet，不打 API。"""
    logger.info("Loading OHLCV: %s", OHLCV_PATH)
    ohlcv = pd.read_parquet(OHLCV_PATH)
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])

    universe = load_top300()
    if not universe:
        raise RuntimeError("Cannot determine universe for market proxy")
    logger.info("Universe: %d stocks", len(universe))

    proxy = ohlcv[ohlcv['stock_id'].isin(universe)].copy()
    proxy = _drop_thin_proxy_dates(proxy)
    daily_avg = proxy.groupby('date')['Close'].mean().sort_index()
    if len(daily_avg) < 60:
        raise RuntimeError(f"Insufficient history: {len(daily_avg)} days")
    return proxy, daily_avg


def compute_today_regime() -> dict:
    """Compute today's market regime from cached OHLCV + official EOD supplement."""
    proxy, daily_avg = _load_proxy()

    # 若 parquet 落後，從官方 EOD 補齊到今天。
    # 補值必須與 parquet 用同一批成分股：universe 有 300 檔但 parquet 實際只有
    # 其中一部分有價，拿「全部命中的」去算會讓水位跳動（見 _fetch_close_supplement）。
    parquet_latest = daily_avg.index[-1]
    members = set(proxy.loc[proxy['date'] == parquet_latest, 'stock_id'].astype(str))
    today = pd.Timestamp.now().normalize()
    missing_days = _twse_trading_days_between(parquet_latest, today)
    if missing_days:
        logger.info("Parquet latest=%s, today=%s → probing %d candidate days from "
                    "official EOD (proxy members=%d)",
                    parquet_latest.date(), today.date(), len(missing_days), len(members))
        supplement = _fetch_close_supplement(members, missing_days)
        # 只留 parquet 沒有的日期：重複 index 會讓 pct_change(20) 的 20 是「位置」
        # 而不是交易日，回看窗會被壓短。
        supplement = supplement[~supplement.index.isin(daily_avg.index)]
        if not supplement.empty:
            daily_avg = pd.concat([daily_avg, supplement]).sort_index()
            logger.info("Extended daily_avg: %d → %d days (now through %s)",
                        len(daily_avg) - len(supplement), len(daily_avg),
                        daily_avg.index[-1].date())
        else:
            logger.info("No usable supplement day; series stays at %s",
                        daily_avg.index[-1].date())
    else:
        logger.info("Parquet is up to date (latest=%s)", parquet_latest.date())

    if daily_avg.index.has_duplicates:
        raise RuntimeError(
            f"daily_avg has duplicate dates ({int(daily_avg.index.duplicated().sum())}) "
            "-- pct_change(20) would no longer span 20 trading days")

    ret20, range20, sharpe60 = _regime_features(daily_avg)

    # 今天（最後 1 日）
    last = daily_avg.index[-1]
    return _entry(last, ret20.iloc[-1], range20.iloc[-1], sharpe60.iloc[-1])


def recompute_history_from_panel() -> dict:
    """由 clean panel 重算每個日期的 regime，回 {date_str: entry}。不打 API。

    用途：修補 `regime_log.jsonl` 內已落盤的毀損值。2026-08-02 查出舊版補值把
    「上一場的純上櫃橫斷面」當成今天寫進序列，使等權均價被抬成 1.835 倍 ——
    2026 年有 54 筆 `|ret_20d| > 30%`（等權 300 檔代理的物理不可能值），且週一~週四
    的 `range_20d` 中位數 0.934、最小值 0.135 都在 volatile 門檻 0.08 之上，那四天
    因此被永久釘在 volatile。
    """
    _proxy, daily_avg = _load_proxy()
    ret20, range20, sharpe60 = _regime_features(daily_avg)
    return {d.strftime('%Y-%m-%d'): _entry(d, ret20.get(d), range20.get(d),
                                           sharpe60.get(d))
            for d in daily_avg.index}


def _read_log() -> dict:
    """讀 regime_log.jsonl 成 {date: entry}。

    解析失敗一律 raise：這個函式的結果會被 `_write_log` **整檔重寫**回去，靜默跳過
    壞行等於把它永久刪掉，而且原檔已被覆寫、事後無從還原。
    """
    existing = {}
    if not LOG_PATH.exists():
        return existing
    bad = []
    for lineno, line in enumerate(LOG_PATH.read_text(encoding='utf-8').splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            existing[rec['date']] = rec
        except Exception as e:
            bad.append(f"line {lineno}: {type(e).__name__} {str(e)[:80]}")
    if bad:
        raise RuntimeError(
            f"{LOG_PATH} has {len(bad)} unparseable line(s); refusing to rewrite the "
            f"file because that would delete them permanently:\n  " + "\n  ".join(bad[:10]))
    return existing


def _write_log(entries: dict) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, 'w', encoding='utf-8') as f:
        for date in sorted(entries.keys()):
            f.write(json.dumps(entries[date], ensure_ascii=False) + '\n')


def append_log(entry: dict) -> bool:
    """Append to regime_log.jsonl；若 date 已存在則覆蓋。"""
    existing = _read_log()
    replaced = entry['date'] in existing
    existing[entry['date']] = entry
    _write_log(existing)
    return replaced


# ⚠️ 這是「值得人工複核」的篩選門檻，**不是物理上限**。
# 2026-08-02 更正：起初把 |ret_20d| > 30% 寫成「等權 300 檔代理不可能出現的值」，
# 那是錯的 —— 20.5 年 5,034 個觀測裡有 6 次超過 30%（0.12%）、從未超過 40%，而那 6 次
# 多數是真實極端行情：2020-03-19 是 COVID 崩盤（-0.307）、2026-04-30~05-06 是急漲
# （2330 自己同期 ret_20d 也有 +0.21~+0.24）。唯一的假值是 2016-06-04，成因是成分不足
# （見 _drop_thin_proxy_dates），已在產生端處理掉。
SUSPICIOUS_RET_20D = 0.30
IMPLAUSIBLE_RET_20D = SUSPICIOUS_RET_20D   # 舊名保留，避免外部引用斷掉


def drop_non_panel_dates(dry_run: bool = True) -> dict:
    """刪掉 log 裡「panel 完全沒有的日期」—— 那些是台股休市日的假列。

    舊版補值的 `_twse_trading_days_between` 用 `bdate_range` 產候選日，會把**平日的
    國定假日**也當工作日；那天 TWSE 正確回空、TPEX 卻回上一場的完整橫斷面，於是為
    一個「市場沒開」的日期寫進一筆 regime。2026-08-02 實測留下 3 筆：2026-05-01
    （勞動節）、2026-06-19、2026-07-10，全是週五休市，`ret_20d` 分別 1.66 / 1.11 / 0.90。

    休市與否以**官方 MI_INDEX 為準**（三筆皆回 stat="很抱歉，沒有符合條件的資料!"）。
    不要拿 `2330_price.csv` 當交易日曆 —— 它漏了 11 個真實交易日（見
    `_drop_thin_proxy_dates`）。

    `repair_history` 刻意只修不刪（它無法分辨「panel 還沒補到這天」與「這天不存在」），
    所以刪除獨立成這個明確動作。
    """
    clean = recompute_history_from_panel()
    existing = _read_log()
    orphans = sorted(d for d in existing if d not in clean)

    logger.info("Non-panel dates in log: %d", len(orphans))
    for d in orphans:
        r = existing[d]
        logger.info("  %s  regime=%s ret_20d=%s  <- 市場沒開，假列",
                    d, r.get('regime'), r.get('ret_20d'))

    if dry_run:
        logger.info("--dry-run: nothing written")
    elif orphans:
        for d in orphans:
            del existing[d]
        _write_log(existing)
        logger.info("Rewrote %s (dropped %d spurious date(s), %d entries left)",
                    LOG_PATH, len(orphans), len(existing))
    else:
        logger.info("Nothing to drop")
    return {'dropped': len(orphans), 'dates': orphans, 'left': len(existing)}


def repair_history(dry_run: bool = True, rebuild_all: bool = False,
                   threshold: float = SUSPICIOUS_RET_20D) -> dict:
    """修補 log 內既有日期的毀損值。只改既有日期，不新增也不刪除。

    **預設只修「物理不可能」的那些**，不是所有與今天 panel 不同的日期。理由是
    2026-08-02 的實測分布：3,717 筆裡有 3,712 筆與現行 panel 不同，但其中 78.1%
    的 `ret_20d` 差異 < 1pp —— 那是 panel 版本差（垃圾價清零、V=0 凍結列、
    yfinance NaN 修復都改過歷史值），不是毀損；把它們一起重寫只是無依據的大幅
    改動。真正站得住腳的是等權 300 檔代理**不可能**出現的值：59 筆
    `|ret_20d| > 30%`（2026 年 51 筆，來自補值 bug）。

    `rebuild_all=True` 才會把所有差異日期一起重寫 —— 那等於宣告「現行 panel 是
    唯一權威版本」，是產品決策，不是預設行為。
    """
    clean = recompute_history_from_panel()
    existing = _read_log()
    changed, label_changed, skipped, still_bad = [], [], 0, []
    for date, old in existing.items():
        new = clean.get(date)
        if new is None:
            skipped += 1
            continue
        if all(old.get(k) == new.get(k)
               for k in ('regime', 'ret_20d', 'range_20d', 'sharpe_60d')):
            continue
        old_ret = old.get('ret_20d')
        implausible = old_ret is not None and abs(old_ret) > threshold
        if not rebuild_all and not implausible:
            continue
        changed.append((date, old, new))
        new_ret = new.get('ret_20d')
        if new_ret is not None and abs(new_ret) > threshold:
            still_bad.append(date)
        if old.get('regime') != new.get('regime'):
            label_changed.append((date, old.get('regime'), new.get('regime')))

    scope = 'ALL differing dates' if rebuild_all else f'|ret_20d| > {threshold:.0%} only'
    logger.info("Repair scan (%s): %d logged dates, %d to repair "
                "(%d change the regime label), %d not in panel (left untouched)",
                scope, len(existing), len(changed), len(label_changed), skipped)
    for date, old, new in changed[:15]:
        logger.info("  %s  ret_20d %s -> %s   range_20d %s -> %s   regime %s -> %s",
                    date, old.get('ret_20d'), new.get('ret_20d'),
                    old.get('range_20d'), new.get('range_20d'),
                    old.get('regime'), new.get('regime'))
    if len(changed) > 15:
        logger.info("  ... and %d more", len(changed) - 15)
    for date, was, now in label_changed:
        logger.info("  LABEL %s: %s -> %s", date, was, now)
    if still_bad:
        # 不假裝重算完美：panel 自己還有殘留尖刺（2026-08-02 實測 6 筆）。
        logger.warning("  %d repaired date(s) are STILL implausible after recompute "
                       "-- the panel itself has residual spikes on %s",
                       len(still_bad), still_bad[:8])

    if dry_run:
        logger.info("--dry-run: nothing written")
    elif changed:
        for date, _old, new in changed:
            existing[date] = new
        _write_log(existing)
        logger.info("Rewrote %s (%d entries repaired)", LOG_PATH, len(changed))
    else:
        logger.info("Nothing to repair")
    return {'logged': len(existing), 'changed': len(changed),
            'label_changed': len(label_changed), 'skipped': skipped,
            'still_implausible': len(still_bad)}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--repair-history', action='store_true',
                    help='用 clean panel 重算並修補 log 內物理不可能的值')
    ap.add_argument('--dry-run', action='store_true',
                    help='只列出會改什麼，不寫檔（僅對 --repair-history 有效）')
    ap.add_argument('--rebuild-all', action='store_true',
                    help='連「僅版本差異」的日期一併重寫（宣告現行 panel 為唯一權威，'
                         '2026-08-02 實測會動到 3,712 筆／692 筆標籤，預設不做）')
    ap.add_argument('--drop-non-panel-dates', action='store_true',
                    help='刪掉 panel 完全沒有的日期（台股休市日的假列，舊補值 bug 產物）')
    args = ap.parse_args()

    if args.drop_non_panel_dates:
        drop_non_panel_dates(dry_run=args.dry_run)
        if not args.repair_history:
            return
    if args.repair_history:
        repair_history(dry_run=args.dry_run, rebuild_all=args.rebuild_all)
        return
    if args.rebuild_all:
        ap.error('--rebuild-all 只能搭 --repair-history 使用')

    entry = compute_today_regime()
    replaced = append_log(entry)
    logger.info("Regime: %s (ret_20d=%s, range_20d=%s)  [%s]",
                entry['regime'], entry['ret_20d'], entry['range_20d'],
                'replaced' if replaced else 'new')
    logger.info("Log: %s", LOG_PATH)


if __name__ == "__main__":
    main()
