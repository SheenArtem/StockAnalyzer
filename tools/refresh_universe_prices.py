#!/usr/bin/env python
"""
refresh_universe_prices.py -- 全市場 price CSV 日更 (standalone, yfinance batch)

背景：2026-05-23 commit 56dcc6c 停掉 QM/Value 全市場掃描省 CPU/LLM 後，
per-stock data_cache/{sid}_price.csv 的日更連帶停擺（refresh 原本「搭」在
scanner_job.py 的 stage-2 candidate 迭代裡，不是獨立 job）。本工具把「純價格
刷新」抽出來：不跑 QM/Value 評分、不呼叫任何 LLM，只把 data_cache 既有的每檔
{sid}_price.csv 增量更新到最新交易日。

為何用 yfinance 批次而非 load_and_resample(FinMind)：
  FinMind free tier 600 req/hr，cache_manager 在 580/600 會 hard-sleep 到下個
  整點（實測 pausing 2096s）。全市場 ~2549 檔 per-stock FinMind 要分 ~5 個
  小時跑。yfinance 無額度、批次下載（threads）~2549 檔約 1-2 分鐘，且實測
  .TW / .TWO 都能正確回 5 日增量。故這裡走 yfinance 批次，FinMind 留給
  load_and_resample 的個股/盤中即時路徑。

下游受益（這些原本都因 CSV 凍結而吃舊價）：
  - tools/build_tw_breadth.py        -> 市場廣度 macro panel
  - tools/refresh_backtest_panels.py -> ohlcv_tw.parquet -> scans and research tools
  - 個股技術分析 / 估值 panel

CSV 格式沿用 cache_manager：DatetimeIndex(無名) + [Open,High,Low,Close,Volume,Adj Close]。

執行：python tools/refresh_universe_prices.py [--limit N] [--chunk 160] [--lookback-days 30]
"""
import sys
import time
import logging
import argparse
import warnings
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
CACHE = REPO / "data_cache"
# cache_manager 的 price CSV 欄位順序（Volume 在 Adj Close 之前）
COLS = ["Open", "High", "Low", "Close", "Volume", "Adj Close"]
MIN_MARKET_ROWS = 500
MIN_MARKET_COVERAGE_RATIO = 0.80
# 單檔 merge 失敗容忍比例（下限 5 檔）；覆蓋率檢查才是真正的安全網
MAX_MERGE_FAIL_RATIO = 0.01
# 官方 EOD 在 lookback 內完全取不到時，允許落後的最大日曆天數
# （台股最長連假＝春節，2026 年 2/11 -> 2/23 相隔 12 天）
MAX_NO_OFFICIAL_GAP_DAYS = 14

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("refresh_universe_prices")


def _yf_batch(sids, suffix, start_date, chunk):
    """yfinance 批次下載一組 sid（加 suffix），回 {sid: DataFrame}。
    只收有資料的；空的（如 .TW 抓不到的 TPEX 股）留給上層改 .TWO 重試。"""
    import yfinance as yf
    out = {}
    dropped_nanclose = 0
    for i in range(0, len(sids), chunk):
        batch = sids[i:i + chunk]
        tickers = [f"{s}{suffix}" for s in batch]
        try:
            df = yf.download(tickers, start=start_date, interval="1d",
                             progress=False, auto_adjust=False, threads=True,
                             group_by="ticker")
        except Exception as e:
            log.warning("yf batch fail [%s..%s] %s: %s",
                        batch[0], batch[-1], suffix, repr(e)[:80])
            continue
        if df is None or df.empty:
            continue
        multi = isinstance(df.columns, pd.MultiIndex)
        for s in batch:
            tk = f"{s}{suffix}"
            try:
                sub = df[tk] if multi else df
                sub = sub.dropna(how="all")
            except Exception:
                continue
            # 防呆：yfinance 偶爾回「有量無收盤價」的列 (實測 2026-06-01 全市場 Close=NaN
            # 但 Volume 正常)。此列對 OHLCV 下游是毒：Close*股數=NaN -> 總市值塌成 0 ->
            # 融資佔市值 inf；breadth 算不出漲跌。直接砍掉不寫進 CSV，留待下次 yfinance
            # 補上真收盤再進 (dropna(how=all) 擋不掉，因 Volume 在故非全 NaN 列)。
            # 2026-06-07 擴：Close<0.01 同砍 (TWSE 最低 tick 0.01, 更低=物理不可能) —
            # 冷門股「無成交日」yfinance 會回填充列 (Open=前值, H=L=C=V=0)，歷史累積
            # 12,589 列零值 + 234 列還原殘渣負值 + 121 列微正值殘渣 (8039 2008-09,
            # Close=0.0039)，害 ATR%/breadth/市值失真 (見 reports/rvol_atr_factor_validation.md)。
            if sub is not None and "Close" in getattr(sub, "columns", []):
                before = len(sub)
                sub = sub[sub["Close"].notna() & (sub["Close"] >= 0.01)]
                dropped_nanclose += before - len(sub)
            if sub is not None and len(sub):
                # yfinance 偶有 tz-aware index；cache CSV 為 tz-naive
                if getattr(sub.index, "tz", None) is not None:
                    sub.index = sub.index.tz_localize(None)
                out[s] = sub
    if dropped_nanclose:
        log.warning("  [%s] 砍 %d 個有量無價列 (yfinance Close=NaN, 不寫入 CSV)",
                    suffix, dropped_nanclose)
    return out


def _official_daily_overlay(sids, target_date, lookback_days=7):
    """Fetch one complete TWSE+TPEX EOD cross-section from official APIs.

    The daily official endpoints are two market-wide calls and are used as the
    authoritative last-day overlay.  Yahoo remains useful for the overlapping
    history window, but a partial/rate-limited Yahoo batch must not decide the
    production market date.

    日期以 **payload 自報的 `data_date`** 為準，不是我們請求的日期：TPEX 的**舊**
    `stk_quote_result.php` 完全無視 `d` 參數（2026-08-02 實測請求 6 週前回的是最新
    橫斷面），若拿請求日期蓋章，就會把「上一場」的 OHLCV 寫進 1900+ 支 CSV，而且每
    一欄都是正數的合理價格，健康度檢查抓不到。`strict_date=True` 讓不符的橫斷面在
    API 層就被丟掉，這裡再確認一次日期單一且等於請求日。

    ✅ 該端點已改為認日期的 `dailyQuotes`，所以這個 overlay **現在也能用在歷史日期**
    （原本只能拿「最新」那天）—— panel 回填就是靠這條路。防線照留。
    """
    from twse_api import TWSEOpenData

    expected = set(map(str, sids))
    min_rows = max(1, math.ceil(len(expected) * MIN_MARKET_COVERAGE_RATIO))
    api = TWSEOpenData()
    for offset in range(lookback_days + 1):
        day = pd.Timestamp(target_date).normalize() - pd.Timedelta(days=offset)
        try:
            frame = api.get_market_daily_all(date=day.to_pydatetime(), strict_date=True)
        except Exception as exc:
            log.warning("Official EOD fetch failed for %s: %s", day.date(), repr(exc)[:120])
            continue
        if frame is None or frame.empty:
            continue
        frame = frame.copy()
        if 'data_date' not in frame.columns:
            log.warning("Official EOD %s: payload carries no data_date -- refusing to "
                        "stamp with the requested date", day.date())
            continue
        stamped = sorted({pd.Timestamp(d).normalize()
                          for d in frame['data_date'].dropna().unique()})
        if len(stamped) != 1 or stamped[0] != day:
            log.warning("Official EOD %s rejected: payload self-reports %s",
                        day.date(), [str(d.date()) for d in stamped] or None)
            continue
        frame['stock_id'] = frame['stock_id'].astype(str)
        frame = frame[frame['stock_id'].isin(expected)]
        for col in ['open', 'high', 'low', 'close', 'volume']:
            frame[col] = pd.to_numeric(frame[col], errors='coerce')
        healthy = frame[['open', 'high', 'low', 'close', 'volume']].gt(0).all(axis=1)
        frame = frame[healthy].copy()
        if len(frame) < min_rows:
            log.warning("Official EOD %s incomplete: %d rows < %d", day.date(), len(frame), min_rows)
            continue
        log.info("Official TWSE/TPEX EOD: %s (payload-verified), %d complete OHLCV rows",
                 stamped[0].date(), len(frame))
        return stamped[0], frame
    return None, pd.DataFrame()


def _merge_official_overlay(data, official_date, official):
    """Overlay official one-day rows into the per-sid Yahoo result mapping."""
    if official_date is None or official is None or official.empty:
        return data
    for row in official.itertuples(index=False):
        sid = str(row.stock_id)
        one = pd.DataFrame({
            'Open': [float(row.open)],
            'High': [float(row.high)],
            'Low': [float(row.low)],
            'Close': [float(row.close)],
            'Volume': [float(row.volume)],
            # Filled from the cached adjustment ratio before CSV merge.
            'Adj Close': [np.nan],
        }, index=pd.DatetimeIndex([official_date]))
        prior = data.get(sid)
        if prior is not None and not prior.empty:
            one = pd.concat([prior, one])
            one = one[~one.index.duplicated(keep='last')].sort_index()
        data[sid] = one
    return data


def _market_date_health(data, expected_count):
    """Return (healthy latest date, unhealthy dates, coverage stats)."""
    rows = []
    for sid, frame in data.items():
        if frame is None or frame.empty or 'Volume' not in frame.columns:
            continue
        dates = pd.to_datetime(frame.index).normalize()
        volumes = pd.to_numeric(frame['Volume'], errors='coerce').fillna(0).to_numpy()
        rows.extend(zip(dates, volumes > 0))
    if not rows:
        raise RuntimeError("price refresh returned no dated volume rows")

    stats = pd.DataFrame(rows, columns=['date', 'positive']).groupby('date').agg(
        rows=('positive', 'size'), positive_volume=('positive', 'sum')).sort_index()
    reference = int(stats['positive_volume'].max())
    absolute_floor = min(MIN_MARKET_ROWS, max(1, int(expected_count * 0.50)))
    threshold = max(absolute_floor, int(np.ceil(reference * MIN_MARKET_COVERAGE_RATIO)))
    healthy = stats[stats['positive_volume'] >= threshold]
    if healthy.empty:
        raise RuntimeError(
            f"no healthy batch market date (threshold={threshold}, stats={stats.tail(5).to_dict('index')})")
    latest = healthy.index.max()
    unhealthy = set(stats.index[stats['positive_volume'] < threshold])
    log.info("Batch market health: latest=%s positive=%d threshold=%d; dropping %d incomplete dates",
             latest.date(), int(stats.loc[latest, 'positive_volume']), threshold, len(unhealthy))
    return latest, unhealthy, stats


def _latest_adjustment_ratio(cached):
    """Return the last finite Adj Close / Close ratio, defaulting to 1."""
    if not {'Adj Close', 'Close'} <= set(cached.columns):
        return 1.0
    ratio = (pd.to_numeric(cached['Adj Close'], errors='coerce') /
             pd.to_numeric(cached['Close'], errors='coerce').replace(0, np.nan))
    ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
    return float(ratio.iloc[-1]) if len(ratio) else 1.0


def _merge_cached_price_frame(cached, new, unhealthy_incoming_dates):
    """Merge an incoming batch without deleting valid history from disk.

    Market health is computed from the current network batch.  A date can be
    partial in that batch even when the existing cross-section on disk is
    complete, so only incoming rows may be discarded here.
    """
    new = new.reindex(columns=COLS)
    new = new[~new.index.normalize().isin(unhealthy_incoming_dates)]
    if new.empty:
        return None
    if new['Adj Close'].isna().any():
        adj_ratio = _latest_adjustment_ratio(cached)
        mask = new['Adj Close'].isna() & new['Close'].notna()
        new.loc[mask, 'Adj Close'] = new.loc[mask, 'Close'] * adj_ratio
    merged = pd.concat([cached, new])
    return merged[~merged.index.duplicated(keep='last')].sort_index()


def _replace_with_retry(tmp_path, path, attempts=3, delay=0.2):
    """os.replace + 短暫重試，容忍 Windows 的暫時性檔案佔用。

    常駐的 Streamlit（App Autostart）會讀同一批 {sid}_price.csv；Windows 上
    MoveFileEx 覆蓋目標時若目標仍有未帶 FILE_SHARE_DELETE 的開啟 handle
    （Python 內建 open 讀檔就是），會丟 PermissionError（實測 WinError 5）。
    這是秒級的暫時狀態，重試即可，不必讓整支非 0 結束。
    """
    for i in range(attempts):
        try:
            os.replace(tmp_path, path)
            return
        except PermissionError:
            if i == attempts - 1:
                raise
            time.sleep(delay)


def _merge_fail_budget(expected_count):
    """單檔 merge 失敗的容忍上限（至少 5 檔或 1%）。

    merge 迴圈刻意讓單檔失敗不中斷整批（CSV 毀損、磁碟暫時錯誤、Windows 上
    App Autostart 正在讀同一支 CSV 造成 os.replace 拒絕存取）。原本收尾卻是
    `if fail: raise`，1964 檔裡壞 1 檔就讓整條行情面板鏈被 run_scanner.bat 的
    goto skip_market_panels 跳過 —— 價格 CSV 其實已是今天的，parquet 卻沒重建，
    形成 CSV 新 / parquet 舊的不一致（2026-08-02 code review）。
    真正的安全網是下面的覆蓋率檢查：失敗檔不會計入 healthy_written，失敗一多
    自然掉到門檻以下照樣 raise。
    """
    return max(5, math.ceil(expected_count * MAX_MERGE_FAIL_RATIO))


def _validate_refresh_summary(healthy_written, expected_count, fail):
    """Fail the process when the promoted healthy-date batch is incomplete."""
    required = max(
        min(MIN_MARKET_ROWS, expected_count),
        math.ceil(expected_count * MIN_MARKET_COVERAGE_RATIO),
    )
    budget = _merge_fail_budget(expected_count)
    if fail > budget:
        raise RuntimeError(
            f"{fail} price CSV merge(s) failed, over the tolerance of {budget}")
    if healthy_written < required:
        raise RuntimeError(
            f"healthy market date written for only {healthy_written}/{expected_count} "
            f"stocks; required at least {required}")


def _validate_market_freshness(healthy_date, official_date, target_date):
    """批次是否跟上最新的實際交易日。

    官方 EOD 是唯一可靠的「哪天有開市」判準，不能用日曆天硬編門檻：原本的
    `(target_date - healthy_date).days > 4` 在台股長假必然成立 —— 以真實交易日曆
    模擬 2006-2026，休市間隔 >= 6 個日曆天的事件 24 次共造成 132 個假 FAIL 夜，
    2026 春節（2/11 -> 2/23）連續 7 夜（2026-08-02 code review）。每次 FAIL 都讓
    run_scanner.bat 跳過整段行情面板。
    """
    if official_date is not None:
        if healthy_date < official_date:
            raise RuntimeError(
                f"batch latest healthy date {healthy_date.date()} is behind the official "
                f"trading day {official_date.date()}")
        return
    # 官方 lookback 內找不到任何交易日：市場休市（長假）或官方端點掛掉。
    # 兩者都不該誤報，但也不能無上限放行 —— 台股最長連假（春節）約 12 個日曆天。
    gap = (target_date - healthy_date).days
    if gap > MAX_NO_OFFICIAL_GAP_DAYS:
        raise RuntimeError(
            f"no official trading day in lookback and latest healthy batch date "
            f"{healthy_date.date()} is {gap} calendar days behind {target_date.date()}")
    log.warning(
        "Official EOD unavailable within lookback; latest healthy batch date %s is %d "
        "calendar days behind %s (long holiday or official outage) -- continuing",
        healthy_date.date(), gap, target_date.date())


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=0, help="只刷前 N 檔（debug 用）")
    ap.add_argument("--chunk", type=int, default=160, help="yfinance 批次大小")
    ap.add_argument("--lookback-days", type=int, default=30,
                    help="批次下載起始回看天數（重疊列會 dedupe）")
    args = ap.parse_args()

    files = sorted(CACHE.glob("*_price.csv"))
    sids = [f.name[:-len("_price.csv")] for f in files
            if f.name[:-len("_price.csv")].isdigit()]
    if args.limit:
        sids = sids[:args.limit]
    log.info("Refreshing %d TW price CSVs via yfinance batch (chunk=%d)...",
             len(sids), args.chunk)

    start_date = (pd.Timestamp.now().normalize()
                  - pd.Timedelta(days=args.lookback_days)).strftime("%Y-%m-%d")
    t0 = time.time()

    # 1. 先全部試 .TW
    data = _yf_batch(sids, ".TW", start_date, args.chunk)
    log.info("  .TW batch: %d/%d got data (%.0fs)", len(data), len(sids), time.time() - t0)
    # 2. .TW 抓不到的改 .TWO 重試（TPEX 上櫃）
    missing = [s for s in sids if s not in data]
    if missing:
        data2 = _yf_batch(missing, ".TWO", start_date, args.chunk)
        data.update(data2)
        log.info("  .TWO retry: +%d/%d (%.0fs)", len(data2), len(missing), time.time() - t0)

    # 2a. Overlay the latest complete official EOD cross-section.  Before
    # 13:35, today's bar is incomplete, so target the previous calendar day
    # and walk backward across weekends/market holidays.
    now = pd.Timestamp.now()
    target_date = now.normalize()
    if now < now.normalize() + pd.Timedelta(hours=13, minutes=35):
        target_date -= pd.Timedelta(days=1)
    official_date, official = _official_daily_overlay(sids, target_date)
    data = _merge_official_overlay(data, official_date, official)

    # 2b. 防呆：盤中 (TW 收盤 13:30) 手動/早班執行時 yfinance 會回當日「未完成」盤中 bar，
    #     臨時收盤價會被寫進 CSV 污染當日資料。排程都在收盤後跑不受影響；此處只擋手動早跑。
    #     ⚠️ 必須在 _market_date_health 之前剔除：盤中今日已成交檔數（實測中後盤約
    #     1900/1964）會跨過健康度門檻，讓 healthy_date 被判成「今天」；剔除今日後
    #     merged 與磁碟 CSV 都沒有今日列，healthy_written 停在 0，收尾必然丟
    #     「healthy market date written for only 0/1964 stocks」這個完全誤導的錯誤，
    #     而且 run_scanner.bat 會因非 0 exit 跳過整段行情面板（2026-08-02 code review）。
    if now < now.normalize() + pd.Timedelta(hours=13, minutes=35):
        today = now.normalize()
        n_trim = 0
        for s in list(data.keys()):
            sub = data[s]
            keep = sub[sub.index.normalize() != today]
            if len(keep) != len(sub):
                data[s] = keep
                n_trim += 1
        if n_trim:
            log.info("盤中執行 (%s)：剔除 %d 檔今日(%s)未完成 bar，待收盤後排程補",
                     now.strftime("%H:%M"), n_trim, today.date())

    healthy_date, unhealthy_dates, _coverage = _market_date_health(data, len(sids))
    _validate_market_freshness(healthy_date, official_date, target_date)

    # 3. 合併進每檔 CSV（沿用 cache_manager 格式）
    ok = fail = skipped = healthy_written = 0
    newest_seen = ""
    for sid in sids:
        new = data.get(sid)
        if new is None or new.empty:
            skipped += 1
            continue
        try:
            path = CACHE / f"{sid}_price.csv"
            cached = pd.read_csv(path, index_col=0, parse_dates=True)
            merged = _merge_cached_price_frame(cached, new, unhealthy_dates)
            if merged is None:
                skipped += 1
                continue
            tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            try:
                merged.to_csv(tmp_path)
                _replace_with_retry(tmp_path, path)
            finally:
                tmp_path.unlink(missing_ok=True)
            ok += 1
            on_healthy = merged.index.normalize() == healthy_date
            if on_healthy.any():
                healthy_volume = pd.to_numeric(
                    merged.loc[on_healthy, 'Volume'], errors='coerce').fillna(0)
                if healthy_volume.gt(0).any():
                    healthy_written += 1
            last = str(merged.index.max())[:10]
            if last > newest_seen:
                newest_seen = last
        except Exception as e:                        # 單檔失敗不可中斷整批
            fail += 1
            log.warning("merge fail %s: %s", sid, repr(e)[:100])

    log.info("Done: %d merged / %d skipped(no yf data) / %d fail in %.0fs",
             ok, skipped, fail, time.time() - t0)
    log.info("Newest date reached: %s", newest_seen)
    log.info("Healthy date %s written for %d/%d stocks",
             healthy_date.date(), healthy_written, len(sids))
    _validate_refresh_summary(healthy_written, len(sids), fail)


if __name__ == "__main__":
    main()
