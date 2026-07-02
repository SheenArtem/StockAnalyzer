"""
Retail-Exit + Dual-Inst composite signal validation (one-off RD study).

兩腿籌碼複合訊號 forward-return alpha 驗證:
  Leg 1 — 散戶撤退: margin_balance 5d/20d 變動 / 20d avg volume (減幅大者為訊號)
  Leg 2 — 法人進駐: 外資 5d sum > 0 AND 投信 5d sum > 0 (sync); 嚴格變體 = 連續>=3d 同買

Frameworks:
  (A) AND 事件/filter: {融資 20d 減幅 top 30%} ∩ {雙法人同買} vs 非候選集
  (B) 連續複合分數: z(融資減幅) + z(雙法人買強度) -> cross-sectional IC + decile

Pools:
  1. 全市場 (排除 ETF 00xx)
  2. 品質池: F-Score >= 6 (PIT, publication-lagged 45d) -- 代理 QM/Whale quality gate

Period: 2021-04 ~ 2026-06 (margin/inst 交集)
Horizons: 5/10/20/40/60d

Bias controls:
  - Volume==0 frozen rows excluded from vol normalization
  - F-Score joined with +45d publication lag (no look-ahead)
  - forward return uses Close pct_change shift(-h) per stock (survivor note: ohlcv_tw
    includes delisted while data exists, drops out naturally;但 universe 仍偏survivor)

Output: reports/retail_exit_dual_inst_ic.md + supporting CSVs
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / "data_cache" / "backtest"
CHIP = ROOT / "data_cache" / "chip_history"
OUT = ROOT / "reports"
OUT.mkdir(exist_ok=True)

HORIZONS = [5, 10, 20, 40, 60]
MIN_XS = 30          # min cross-section per day for IC
PUB_LAG_DAYS = 45    # F-Score publication delay
FSCORE_QUALITY = 6   # quality pool threshold
START = "2021-04-16"
END = "2026-06-26"


def log(m): print(f"[REDI] {m}", flush=True)


# ---------------------------------------------------------------
# 1. Load & build panel
# ---------------------------------------------------------------
def load_panel():
    log("Loading OHLCV...")
    o = pd.read_parquet(CACHE / "ohlcv_tw.parquet", columns=['stock_id', 'date', 'Close', 'Volume'])
    o['date'] = pd.to_datetime(o['date'])
    o = o[(o['date'] >= START) & (o['date'] <= END)].copy()
    o['stock_id'] = o['stock_id'].astype(str)
    o['Close'] = pd.to_numeric(o['Close'], errors='coerce')
    o['Volume'] = pd.to_numeric(o['Volume'], errors='coerce')
    # poison guard
    o = o[(o['Close'] > 0) & o['Close'].notna()].copy()
    # exclude ETF (00xx)
    o = o[~o['stock_id'].str.startswith('00')].copy()
    o = o.sort_values(['stock_id', 'date']).reset_index(drop=True)
    log(f"  OHLCV: {len(o):,} rows, {o['stock_id'].nunique()} stocks (ETF excluded)")

    # min history filter
    cnt = o.groupby('stock_id').size()
    keep = cnt[cnt >= 100].index
    o = o[o['stock_id'].isin(keep)].copy()

    # 20d avg volume (exclude frozen V==0 by treating as NaN for the mean)
    vol = o['Volume'].where(o['Volume'] > 0, np.nan)
    o['vol_20d_avg'] = vol.groupby(o['stock_id']).transform(
        lambda s: s.rolling(20, min_periods=10).mean()).replace(0, np.nan)

    log("Loading margin...")
    m = pd.read_parquet(CHIP / "margin.parquet", columns=['date', 'stock_id', 'margin_balance'])
    m['date'] = pd.to_datetime(m['date'])
    m['stock_id'] = m['stock_id'].astype(str)
    m['margin_balance'] = pd.to_numeric(m['margin_balance'], errors='coerce')

    log("Loading institutional...")
    inst = pd.read_parquet(CHIP / "institutional.parquet",
                           columns=['date', 'stock_id', 'foreign_net', 'trust_net'])
    inst['date'] = pd.to_datetime(inst['date'])
    inst['stock_id'] = inst['stock_id'].astype(str)
    for c in ['foreign_net', 'trust_net']:
        inst[c] = pd.to_numeric(inst[c], errors='coerce')

    log("Loading quality (F-Score, PIT)...")
    q = pd.read_parquet(CACHE / "quality_scores.parquet", columns=['stock_id', 'date', 'f_score'])
    q['date'] = pd.to_datetime(q['date'])
    q['stock_id'] = q['stock_id'].astype(str)
    # publication lag: a quarter-end row becomes known +45d later
    q['avail_date'] = q['date'] + pd.Timedelta(days=PUB_LAG_DAYS)
    q = q.sort_values(['stock_id', 'avail_date'])

    # merge
    df = o.merge(m, on=['stock_id', 'date'], how='left')
    df = df.merge(inst, on=['stock_id', 'date'], how='left')
    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)

    # ---- Leg 1: margin balance change (raw diff, negative = retail exit) ----
    g = df.groupby('stock_id')['margin_balance']
    df['margin_chg_5d_raw'] = g.diff(5)
    df['margin_chg_20d_raw'] = g.diff(20)
    df['margin_chg_5d'] = df['margin_chg_5d_raw'] / df['vol_20d_avg']
    df['margin_chg_20d'] = df['margin_chg_20d_raw'] / df['vol_20d_avg']
    # retail-exit strength = how MUCH margin dropped = -change (bigger drop -> bigger positive)
    df['retail_exit_5d'] = -df['margin_chg_5d']
    df['retail_exit_20d'] = -df['margin_chg_20d']

    # ---- Leg 2: dual-inst buying ----
    gf = df.groupby('stock_id')['foreign_net']
    gt = df.groupby('stock_id')['trust_net']
    f5 = gf.transform(lambda s: s.rolling(5, min_periods=3).sum())
    t5 = gt.transform(lambda s: s.rolling(5, min_periods=3).sum())
    df['f5'] = f5
    df['t5'] = t5
    df['dual_sync'] = ((f5 > 0) & (t5 > 0)).astype(float)  # AND condition (binary)
    # buying strength (vol-normalized sum of both)
    df['dual_buy_strength'] = (f5.fillna(0) + t5.fillna(0)) / df['vol_20d_avg']
    # strict variant: foreign AND trust both net-buy for >=3 consecutive days
    fpos = (df['foreign_net'] > 0).astype(int)
    tpos = (df['trust_net'] > 0).astype(int)
    both_pos = ((fpos == 1) & (tpos == 1)).astype(int)
    df['both_pos'] = both_pos
    # consecutive count via groupby rolling
    df['dual_consec3'] = (df.groupby('stock_id')['both_pos']
                          .transform(lambda s: s.rolling(3, min_periods=3).sum()) >= 3).astype(float)

    # ---- F-Score PIT join (merge_asof per stock) ----
    df = df.sort_values(['date']).reset_index(drop=True)
    q_sorted = q.sort_values('avail_date')
    df = pd.merge_asof(df.sort_values('date'),
                       q_sorted[['stock_id', 'avail_date', 'f_score']].rename(columns={'avail_date': 'date'}),
                       on='date', by='stock_id', direction='backward')
    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)

    # ---- forward returns ----
    gc = df.groupby('stock_id')['Close']
    for h in HORIZONS:
        df[f'fwd_{h}d'] = gc.transform(lambda s: s.pct_change(h).shift(-h))

    log(f"Panel built: {len(df):,} rows")
    return df


# ---------------------------------------------------------------
# 2. Cross-sectional IC
# ---------------------------------------------------------------
def daily_ic(df, sig, ret):
    x = df.dropna(subset=[sig, ret])
    ics, dates = [], []
    for d, gp in x.groupby('date'):
        if len(gp) < MIN_XS:
            continue
        if gp[sig].nunique() < 5:
            continue
        ic, _ = stats.spearmanr(gp[sig], gp[ret])
        if not np.isnan(ic):
            ics.append(ic); dates.append(d)
    return pd.Series(ics, index=pd.to_datetime(dates), name='ic')


def summarize(ic):
    arr = ic.dropna().values
    n = len(arr)
    if n < 20:
        return dict(mean=np.nan, ir=np.nan, t=np.nan, p=np.nan, win=np.nan, n=n)
    m = arr.mean(); s = arr.std(ddof=1)
    ir = m / s if s > 0 else np.nan
    t = m * np.sqrt(n) / s if s > 0 else 0
    p = 2 * (1 - stats.t.cdf(abs(t), df=n - 1))
    win = (arr > 0).mean() * 100
    return dict(mean=float(m), ir=float(ir), t=float(t), p=float(p), win=float(win), n=n)


def run_ic_battery(df, signals, tag, rows):
    for sig in signals:
        for h in HORIZONS:
            ic = daily_ic(df, sig, f'fwd_{h}d')
            st = summarize(ic)
            rows.append(dict(pool=tag, signal=sig, horizon=h, **st))
            # per-year for the composite z signals
    return rows


# ---------------------------------------------------------------
# 3. Decile spread + monotonicity
# ---------------------------------------------------------------
def decile_spread(df, sig, ret):
    """Cross-sectional decile by signal, mean fwd return per decile (pooled over days)."""
    x = df.dropna(subset=[sig, ret]).copy()
    # rank within each day into deciles
    def dec(g):
        if len(g) < 30:
            return pd.Series(np.nan, index=g.index)
        return pd.qcut(g[sig].rank(method='first'), 10, labels=False, duplicates='drop')
    x['decile'] = x.groupby('date', group_keys=False).apply(dec)
    x = x.dropna(subset=['decile'])
    means = x.groupby('decile')[ret].mean()
    return means


def monotonicity(means):
    """Spearman corr of decile index vs mean return; +1 perfectly increasing."""
    if len(means) < 3:
        return np.nan
    rho, _ = stats.spearmanr(means.index.astype(float), means.values)
    return rho


# ---------------------------------------------------------------
# 4. Event spread (framework A)
# ---------------------------------------------------------------
def event_spread(df, ret, q_thresh=0.30):
    """Candidate = margin 20d drop in top 30% (i.e. retail_exit_20d top 30%) AND dual_sync==1.
    Compare candidate fwd ret vs rest."""
    x = df.dropna(subset=['retail_exit_20d', 'dual_sync', ret]).copy()
    # daily rank of retail_exit_20d (high = big drop)
    x['re_pct'] = x.groupby('date')['retail_exit_20d'].rank(pct=True)
    x['cand'] = ((x['re_pct'] >= (1 - q_thresh)) & (x['dual_sync'] == 1)).astype(int)
    cand = x[x['cand'] == 1][ret]
    rest = x[x['cand'] == 0][ret]
    if len(cand) < 20:
        return dict(n_cand=len(cand), mu_cand=np.nan, mu_rest=np.nan, spread=np.nan, t=np.nan, p=np.nan, win_cand=np.nan)
    t, p = stats.ttest_ind(cand, rest, equal_var=False, nan_policy='omit')
    return dict(n_cand=int(len(cand)), mu_cand=float(cand.mean()), mu_rest=float(rest.mean()),
                spread=float(cand.mean() - rest.mean()), t=float(t), p=float(p),
                win_cand=float((cand > 0).mean() * 100))


# ---------------------------------------------------------------
# 5. Per-year regime
# ---------------------------------------------------------------
def per_year_ic(df, sig, h):
    ic = daily_ic(df, sig, f'fwd_{h}d')
    if len(ic) == 0:
        return {}
    out = {}
    for y, g in ic.groupby(ic.index.year):
        out[y] = dict(mean=float(g.mean()), n=len(g), win=float((g > 0).mean() * 100))
    return out


# ---------------------------------------------------------------
# 6. Top-N portfolio vs 0050 (monthly rebalance)
# ---------------------------------------------------------------
def topn_portfolio(df, sig, N=20, hold_h=20):
    """Monthly rebalance: each month-end pick top-N by sig, hold ~1 month (use fwd_20d as monthly ret).
    Returns annualized stats + benchmark."""
    x = df.dropna(subset=[sig, 'fwd_20d']).copy()
    x['ym'] = x['date'].dt.to_period('M')
    # pick last trading day per month per stock
    monthly = x.sort_values('date').groupby(['stock_id', 'ym']).tail(1)
    rets = []
    dates = []
    for ym, g in monthly.groupby('ym'):
        if len(g) < N:
            continue
        top = g.nlargest(N, sig)
        rets.append(top['fwd_20d'].mean())
        dates.append(ym.to_timestamp())
    if len(rets) < 6:
        return None
    sr = pd.Series(rets, index=pd.to_datetime(dates)).sort_index()
    # annualize (monthly)
    mean_m = sr.mean(); std_m = sr.std(ddof=1)
    sharpe = (mean_m / std_m) * np.sqrt(12) if std_m > 0 else np.nan
    cagr = (1 + sr).prod() ** (12 / len(sr)) - 1
    # MDD
    nav = (1 + sr).cumprod()
    mdd = (nav / nav.cummax() - 1).min()
    return dict(series=sr, cagr=float(cagr), sharpe=float(sharpe), mdd=float(mdd),
                n_months=len(sr), mean_m=float(mean_m), std_m=float(std_m))


def benchmark_0050():
    """0050 monthly returns from ohlcv_tw."""
    o = pd.read_parquet(CACHE / "ohlcv_tw.parquet", columns=['stock_id', 'date', 'Close'])
    o['date'] = pd.to_datetime(o['date'])
    o['stock_id'] = o['stock_id'].astype(str)
    b = o[(o['stock_id'] == '0050') & (o['date'] >= START) & (o['date'] <= END)].copy()
    b = b.sort_values('date')
    b['Close'] = pd.to_numeric(b['Close'], errors='coerce')
    b['ym'] = b['date'].dt.to_period('M')
    me = b.groupby('ym').tail(1).set_index('ym')['Close']
    mret = me.pct_change().dropna()
    mret.index = mret.index.to_timestamp()
    return mret


def main():
    df = load_panel()

    # define pools
    df_all = df  # full market (ETF already excluded)
    df_q = df[df['f_score'] >= FSCORE_QUALITY].copy()
    log(f"Quality pool (F>={FSCORE_QUALITY}): {len(df_q):,} rows, {df_q['stock_id'].nunique()} stocks")

    # composite z scores (framework B), computed cross-sectionally per day
    def add_composite(d):
        d = d.copy()
        for col, src in [('z_retail_exit', 'retail_exit_20d'), ('z_dual_buy', 'dual_buy_strength')]:
            d[col] = d.groupby('date')[src].transform(
                lambda s: (s - s.mean()) / s.std(ddof=0) if s.std(ddof=0) > 0 else s * 0)
        d['composite_z'] = d['z_retail_exit'].fillna(0) + d['z_dual_buy'].fillna(0)
        # require both legs present
        d.loc[d['retail_exit_20d'].isna() | d['dual_buy_strength'].isna(), 'composite_z'] = np.nan
        return d

    df_all = add_composite(df_all)
    df_q = add_composite(df_q)

    SIGNALS_B = ['retail_exit_20d', 'retail_exit_5d', 'dual_buy_strength', 'dual_sync', 'composite_z']

    # ---- IC battery ----
    ic_rows = []
    log("IC battery: full market...")
    run_ic_battery(df_all, SIGNALS_B, 'all', ic_rows)
    log("IC battery: quality pool...")
    run_ic_battery(df_q, SIGNALS_B, 'quality', ic_rows)
    ic_df = pd.DataFrame(ic_rows)
    ic_df.to_csv(OUT / "retail_exit_dual_inst_ic_matrix.csv", index=False, encoding='utf-8-sig')

    # ---- decile spread composite_z ----
    dec_rows = []
    for pool, d in [('all', df_all), ('quality', df_q)]:
        for sig in ['composite_z', 'retail_exit_20d', 'dual_buy_strength']:
            for h in [20, 60]:
                means = decile_spread(d, sig, f'fwd_{h}d')
                if len(means) >= 5:
                    spread = means.iloc[-1] - means.iloc[0]
                    mono = monotonicity(means)
                    dec_rows.append(dict(pool=pool, signal=sig, horizon=h,
                                         d1=float(means.iloc[0]), d10=float(means.iloc[-1]),
                                         spread=float(spread), monotonicity=float(mono)))
    dec_df = pd.DataFrame(dec_rows)
    dec_df.to_csv(OUT / "retail_exit_dual_inst_decile.csv", index=False, encoding='utf-8-sig')

    # ---- event spread (framework A) ----
    ev_rows = []
    for pool, d in [('all', df_all), ('quality', df_q)]:
        for h in HORIZONS:
            r = event_spread(d, f'fwd_{h}d')
            ev_rows.append(dict(pool=pool, horizon=h, **r))
    ev_df = pd.DataFrame(ev_rows)
    ev_df.to_csv(OUT / "retail_exit_dual_inst_event.csv", index=False, encoding='utf-8-sig')

    # ---- per-year regime (composite_z, 20d & 60d) ----
    yr_rows = []
    for pool, d in [('all', df_all), ('quality', df_q)]:
        for h in [20, 60]:
            yr = per_year_ic(d, 'composite_z', h)
            for y, v in yr.items():
                yr_rows.append(dict(pool=pool, signal='composite_z', horizon=h, year=y, **v))
    yr_df = pd.DataFrame(yr_rows)
    yr_df.to_csv(OUT / "retail_exit_dual_inst_yearly.csv", index=False, encoding='utf-8-sig')

    # ---- Top-N portfolio vs 0050 ----
    bench = benchmark_0050()
    pf_rows = []
    for pool, d in [('all', df_all), ('quality', df_q)]:
        for sig in ['composite_z', 'retail_exit_20d', 'dual_buy_strength']:
            pf = topn_portfolio(d, sig, N=20, hold_h=20)
            if pf is None:
                continue
            # align benchmark over same months
            sr = pf['series']
            common = sr.index.intersection(bench.index)
            if len(common) >= 6:
                strat = sr.loc[common]; bm = bench.loc[common]
                excess = strat - bm
                ir = (excess.mean() / excess.std(ddof=1)) * np.sqrt(12) if excess.std(ddof=1) > 0 else np.nan
                bm_sharpe = (bm.mean() / bm.std(ddof=1)) * np.sqrt(12) if bm.std(ddof=1) > 0 else np.nan
                bm_cagr = (1 + bm).prod() ** (12 / len(bm)) - 1
            else:
                ir = np.nan; bm_sharpe = np.nan; bm_cagr = np.nan
            pf_rows.append(dict(pool=pool, signal=sig, cagr=pf['cagr'], sharpe=pf['sharpe'],
                                mdd=pf['mdd'], n_months=pf['n_months'],
                                bench_cagr=float(bm_cagr) if not np.isnan(bm_cagr) else np.nan,
                                bench_sharpe=float(bm_sharpe) if not np.isnan(bm_sharpe) else np.nan,
                                ir_vs_0050=float(ir) if not np.isnan(ir) else np.nan))
    pf_df = pd.DataFrame(pf_rows)
    pf_df.to_csv(OUT / "retail_exit_dual_inst_portfolio.csv", index=False, encoding='utf-8-sig')

    # ---- print everything ----
    pd.set_option('display.width', 200, 'display.max_columns', 30, 'display.float_format', lambda x: f'{x:.4f}')
    print("\n===== IC MATRIX =====")
    print(ic_df.to_string(index=False))
    print("\n===== DECILE SPREAD + MONOTONICITY =====")
    print(dec_df.to_string(index=False))
    print("\n===== EVENT SPREAD (framework A) =====")
    print(ev_df.to_string(index=False))
    print("\n===== PER-YEAR REGIME (composite_z) =====")
    print(yr_df.to_string(index=False))
    print("\n===== TOP-20 PORTFOLIO vs 0050 =====")
    print(pf_df.to_string(index=False))
    print(f"\nBenchmark 0050: {len(bench)} months, CAGR={((1+bench).prod()**(12/len(bench))-1):.4f}, "
          f"Sharpe={(bench.mean()/bench.std(ddof=1))*np.sqrt(12):.4f}")

    log("DONE")


if __name__ == "__main__":
    main()
