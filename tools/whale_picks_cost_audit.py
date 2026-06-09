"""
Whale Picks — Realistic Execution Cost Theoretical Audit
=========================================================

目的：稽核 `tools/whale_picks_portfolio_backtest.py` 的成本假設
      ('未扣 slippage,估 -0.5%/年' + '~6 round-trips/年 × 0.3% = -1.8%/年')
      是否站得住，並在現實中小型執行成本下重算誠實 CAGR / Sharpe。

=== 這是理論模型 (目前無真實成交資料)。所有假設寫明於下 ===

成本三層 (台股):
  1. 固定成本 (確定):
     - 賣出證交稅 0.3% (一律,賣方)
     - 手續費 0.1425%/邊 (官方上限) → 兩情境: 0.1425% (無折) / 0.0855% (4 折,現實券商常見)
     - round-trip 固定 = 賣稅 0.3% + 手續費×2
       · 無折: 0.3% + 0.1425%×2 = 0.585%
       · 4 折: 0.3% + 0.0855%×2 = 0.471%
  2. 真實換手率 (關鍵, 推翻 flat 0.5%/年):
     - 從 ledger 算每個 portfolio slot 實際 round-trip 頻率
     - 平均持有 1.77 月 → 每 slot 每年 round-trip ~6.79 次
     - 年固定成本 = (每年每 slot round-trip 次數) × round-trip 固定% (而非 flat 0.5%)
  3. 市場衝擊 (中小型關鍵, 平方根模型):
     - impact_oneway ≈ c · sigma_daily · sqrt(部位名目 / ADV_60d)
     - c = 0.6 (學術常用 0.5~1, 取中間偏保守下緣)
     - round-trip 衝擊 = impact_buy + impact_sell ≈ 2 × impact_oneway
     - 部位名目 = AUM_per_stock (= 總 AUM / K, K≈portfolio 平均持股數)
     - participation = 部位名目 / ADV → 越薄越貴

AUM 情境 (每檔部位名目):
     NT$1M / NT$5M / NT$20M / NT$50M (對應總 AUM ≈ ×K)

模型把成本灌入 daily NAV:
     每個 position 在 entry_date 課 (手續費_buy + impact_buy) × weight
     每個 position 在 exit_date  課 (手續費_sell + 證交稅 + impact_sell) × weight
     weight = 1 / n_held(該日) (equal-weight，與 baseline backtest 一致)

輸出:
     reports/whale_cost_audit.md
     reports/whale_cost_audit_breakdown.csv  (每 AUM × 成本拆解)
     reports/whale_cost_audit_netnav.csv     (每 AUM 的淨 NAV/CAGR/Sharpe)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

LEDGER_PATH = REPO / "data" / "whale_picks" / "trade_ledger.parquet"
OHLCV_PATH = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"
TWII_PATH = REPO / "data_cache" / "backtest" / "_twii_bench.parquet"
REPORTS = REPO / "reports"

# ---- Cost model assumptions (寫明) ----
SELL_TAX = 0.003                # 證交稅 0.3% (賣方)
FEE_FULL = 0.001425             # 手續費 0.1425%/邊 (官方上限, 無折)
FEE_DISC = 0.000855             # 手續費 0.0855%/邊 (4 折, 現實常見)
IMPACT_C = 0.6                  # 平方根衝擊係數 (學術 0.5~1)
TRADING_DAYS = 252

# 每檔部位名目 (TWD) — 4 AUM 情境
AUM_PER_STOCK_SCENARIOS = {
    "1M": 1_000_000,
    "5M": 5_000_000,
    "20M": 20_000_000,
    "50M": 50_000_000,
}


def load_data():
    led = pd.read_parquet(LEDGER_PATH)
    led["entry_date"] = pd.to_datetime(led["entry_date"])
    led["exit_date"] = pd.to_datetime(led["exit_date"])

    ohlcv = pd.read_parquet(OHLCV_PATH, columns=["stock_id", "date", "Close", "Volume"])
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])

    twii = pd.read_parquet(TWII_PATH)
    twii.columns = [c[0] if isinstance(c, tuple) else c for c in twii.columns]
    twii = twii.reset_index().rename(columns={"index": "date", "Date": "date"})
    twii["date"] = pd.to_datetime(twii["date"])
    return led, ohlcv, twii


def compute_liquidity(led, ohlcv):
    """每個 ledger position 在 entry 前的 trailing-60d ADV(TWD) + daily sigma。"""
    o = ohlcv.sort_values(["stock_id", "date"]).copy()
    o["dollar_tv"] = o["Close"] * o["Volume"]   # Volume 已確認為股數 → 成交額 TWD
    o["ret"] = o.groupby("stock_id")["Close"].pct_change(fill_method=None)
    grp = {sid: sub.set_index("date") for sid, sub in o.groupby("stock_id")}

    rows = []
    for idx, r in led.iterrows():
        sid, ed = r["stock_id"], r["entry_date"]
        adv = sigma = np.nan
        if sid in grp:
            w = grp[sid][grp[sid].index < ed].tail(60)
            if len(w) >= 20:
                adv = float(w["dollar_tv"].mean())
                sigma = float(w["ret"].std())
        rows.append((idx, adv, sigma))
    liq = pd.DataFrame(rows, columns=["lidx", "adv_60d_twd", "sigma_daily"]).set_index("lidx")
    led = led.join(liq)
    # 缺資料 (極少數) 用中位數填 (保守: 用整體中位 ADV/sigma)
    led["adv_60d_twd"] = led["adv_60d_twd"].fillna(led["adv_60d_twd"].median())
    led["sigma_daily"] = led["sigma_daily"].fillna(led["sigma_daily"].median())
    return led


def build_daily_returns(led, ohlcv, start, end):
    """重建 equal-weight daily portfolio return (gross) + 每日 n_held。"""
    ohlcv_dates = pd.DatetimeIndex(sorted(ohlcv["date"].unique()))
    tdays = ohlcv_dates[(ohlcv_dates >= start) & (ohlcv_dates <= end)]
    wide = ohlcv.pivot(index="date", columns="stock_id", values="Close")
    rets = wide.pct_change(fill_method=None)

    recs = []
    for t in tdays:
        held = led[(led["entry_date"] <= t) & ((led["exit_date"].isna()) | (led["exit_date"] > t))]
        ids = held["stock_id"].tolist()
        if not ids:
            recs.append((t, 0.0, 0))
            continue
        vals = [rets.loc[t, s] for s in ids if s in rets.columns and t in rets.index and pd.notna(rets.loc[t, s])]
        recs.append((t, float(np.mean(vals)) if vals else 0.0, len(ids)))
    port = pd.DataFrame(recs, columns=["date", "gross_ret", "n_held"])
    return port, tdays


def cost_per_roundtrip(adv, sigma, aum_per_stock, fee_oneway):
    """回傳 (fixed_rt, impact_rt) 兩段 round-trip 成本 (小數, 佔部位)。"""
    fixed_rt = SELL_TAX + 2 * fee_oneway            # 賣稅 + 兩邊手續費
    participation = aum_per_stock / adv if adv and adv > 0 else 0.0
    impact_oneway = IMPACT_C * sigma * np.sqrt(participation)
    impact_rt = 2 * impact_oneway                   # 進出各一次
    return fixed_rt, impact_rt


def apply_costs(led, port, aum_per_stock, fee_oneway):
    """把每筆 round-trip 的成本當作 return 扣項灌入對應 entry/exit 交易日。

    weight = 1 / n_held(交易日)；買進日扣 (fee_buy+impact_buy)*w，
    賣出日扣 (fee_sell+tax+impact_sell)*w。
    回傳 (net_port_df, breakdown_dict)。
    """
    n_held_by_date = dict(zip(port["date"], port["n_held"]))
    # 預先算每筆成本兩段
    fee_buy = fee_oneway
    fee_sell = fee_oneway + SELL_TAX
    cost_buy_map = {}   # date -> sum of (fee_buy+impact_buy)*w
    cost_sell_map = {}
    agg = {"fixed": 0.0, "impact": 0.0, "n_legs": 0, "w_sum_buy": 0.0}

    for _, r in led.iterrows():
        adv, sigma = r["adv_60d_twd"], r["sigma_daily"]
        part = aum_per_stock / adv if adv and adv > 0 else 0.0
        impact_oneway = IMPACT_C * sigma * np.sqrt(part)

        ed = r["entry_date"]
        w_e = 1.0 / n_held_by_date.get(ed, np.nan) if ed in n_held_by_date else np.nan
        if pd.notna(w_e):
            c = (fee_buy + impact_oneway) * w_e
            cost_buy_map[ed] = cost_buy_map.get(ed, 0.0) + c
            agg["fixed"] += fee_buy * w_e
            agg["impact"] += impact_oneway * w_e
            agg["w_sum_buy"] += w_e
            agg["n_legs"] += 1

        xd = r["exit_date"]
        if pd.notna(xd):
            w_x = 1.0 / n_held_by_date.get(xd, np.nan) if xd in n_held_by_date else np.nan
            if pd.notna(w_x):
                c = (fee_sell + impact_oneway) * w_x
                cost_sell_map[xd] = cost_sell_map.get(xd, 0.0) + c
                agg["fixed"] += fee_sell * w_x
                agg["impact"] += impact_oneway * w_x
                agg["n_legs"] += 1

    p = port.copy()
    p["cost_buy"] = p["date"].map(cost_buy_map).fillna(0.0)
    p["cost_sell"] = p["date"].map(cost_sell_map).fillna(0.0)
    p["cost"] = p["cost_buy"] + p["cost_sell"]
    p["net_ret"] = p["gross_ret"] - p["cost"]
    p["net_nav"] = (1 + p["net_ret"]).cumprod()
    p["gross_nav"] = (1 + p["gross_ret"]).cumprod()
    return p, agg


def stats(nav, ret):
    days = ret.notna().sum()
    years = days / TRADING_DAYS
    cagr = float(nav.iloc[-1] ** (1 / years) - 1) if years > 0 else 0.0
    vol = float(ret.std() * np.sqrt(TRADING_DAYS))
    sharpe = float((ret.mean() * TRADING_DAYS) / vol) if vol > 0 else 0.0
    peak = nav.cummax()
    mdd = float(((nav - peak) / peak).min())
    return dict(cagr=cagr, sharpe=sharpe, vol=vol, mdd=mdd,
                total=float(nav.iloc[-1] - 1), years=round(years, 2))


def main():
    led, ohlcv, twii = load_data()
    start = pd.to_datetime(led["entry_date"].min())
    end = pd.to_datetime("2026-05-23")  # 對齊 meta.json baseline end

    led = compute_liquidity(led, ohlcv)
    port, tdays = build_daily_returns(led, ohlcv, start, end)

    gross = stats(port["gross_ret"].pipe(lambda s: (1 + s).cumprod()), port["gross_ret"])

    # TWII bench
    tw = twii[(twii["date"] >= start) & (twii["date"] <= end)].copy()
    tw["ret"] = tw["Close"].pct_change(fill_method=None)
    tw["nav"] = (1 + tw["ret"].fillna(0)).cumprod()
    tw_stats = stats(tw["nav"], tw["ret"])

    # 換手率事實
    avg_hold = led["holding_months"].mean()
    rt_per_slot_yr = 12.0 / avg_hold
    avg_held = port.loc[port["n_held"] > 0, "n_held"].mean()
    rt_total_yr = avg_held * rt_per_slot_yr

    results = []
    breakdown_rows = []
    for fee_label, fee_oneway in [("full_0.1425", FEE_FULL), ("disc_0.0855", FEE_DISC)]:
        for aum_label, aum in AUM_PER_STOCK_SCENARIOS.items():
            p, agg = apply_costs(led, port, aum, fee_oneway)
            net = stats(p["net_nav"], p["net_ret"])
            # 年化成本拆解 (總扣 / years)
            yrs = net["years"]
            total_fixed = agg["fixed"]
            total_impact = agg["impact"]
            # 注意: agg fixed/impact 已是 weight 加權的「return 點數」累加，
            # 直接 /years 得年化 return drag
            ann_fixed = total_fixed / yrs
            ann_impact = total_impact / yrs
            ann_total_cost = ann_fixed + ann_impact
            results.append(dict(
                fee=fee_label, aum_per_stock=aum_label,
                gross_cagr=gross["cagr"], net_cagr=net["cagr"],
                gross_sharpe=gross["sharpe"], net_sharpe=net["sharpe"],
                net_vol=net["vol"], net_mdd=net["mdd"],
                ann_cost_pct=ann_total_cost * 100,
                ann_fixed_pct=ann_fixed * 100,
                ann_impact_pct=ann_impact * 100,
                cagr_haircut_pp=(gross["cagr"] - net["cagr"]) * 100,
                alpha_vs_twii_pp=(net["cagr"] - tw_stats["cagr"]) * 100,
            ))
            breakdown_rows.append(dict(
                fee=fee_label, aum_per_stock=aum_label,
                ann_fixed_cost_pct=round(ann_fixed * 100, 3),
                ann_impact_cost_pct=round(ann_impact * 100, 3),
                ann_total_cost_pct=round(ann_total_cost * 100, 3),
                net_cagr_pct=round(net["cagr"] * 100, 2),
                net_sharpe=round(net["sharpe"], 3),
                net_mdd_pct=round(net["mdd"] * 100, 2),
                cagr_haircut_pp=round((gross["cagr"] - net["cagr"]) * 100, 2),
                alpha_vs_twii_pp=round((net["cagr"] - tw_stats["cagr"]) * 100, 2),
            ))

    res_df = pd.DataFrame(results)
    bd_df = pd.DataFrame(breakdown_rows)

    REPORTS.mkdir(parents=True, exist_ok=True)
    bd_df.to_csv(REPORTS / "whale_cost_audit_breakdown.csv", index=False, encoding="utf-8-sig")

    # net nav per AUM (用 4 折 fee 當主情境輸出 NAV 表)
    netnav_rows = []
    for aum_label, aum in AUM_PER_STOCK_SCENARIOS.items():
        p, agg = apply_costs(led, port, aum, FEE_DISC)
        net = stats(p["net_nav"], p["net_ret"])
        netnav_rows.append(dict(aum_per_stock=aum_label, **{f"net_{k}": v for k, v in net.items()}))
    pd.DataFrame(netnav_rows).to_csv(REPORTS / "whale_cost_audit_netnav.csv", index=False, encoding="utf-8-sig")

    # ---- print summary ----
    print("=" * 70)
    print("WHALE PICKS COST AUDIT — gross anchor & turnover facts")
    print("=" * 70)
    print(f"Gross  CAGR {gross['cagr']*100:.2f}%  Sharpe {gross['sharpe']:.3f}  "
          f"Vol {gross['vol']*100:.2f}%  MDD {gross['mdd']*100:.2f}%  ({gross['years']}y)")
    print(f"TWII   CAGR {tw_stats['cagr']*100:.2f}%  Sharpe {tw_stats['sharpe']:.3f}")
    print()
    print(f"Avg holding = {avg_hold:.2f} months  ->  each slot round-trips {rt_per_slot_yr:.2f} x/yr")
    print(f"Avg portfolio size = {avg_held:.2f} slots  ->  total ~{rt_total_yr:.1f} round-trips/yr")
    print(f"==> 'flat 0.5%/yr slippage' & '~6 round-trips/yr' assumption check:")
    rt_fixed_full = (SELL_TAX + 2 * FEE_FULL)
    rt_fixed_disc = (SELL_TAX + 2 * FEE_DISC)
    print(f"    FIXED-ONLY annual cost (no impact) = {rt_per_slot_yr:.2f} rt/yr x "
          f"{rt_fixed_disc*100:.3f}%(4折)~{rt_fixed_full*100:.3f}%(無折)")
    print(f"    = {rt_per_slot_yr*rt_fixed_disc*100:.2f}% ~ {rt_per_slot_yr*rt_fixed_full*100:.2f}% / yr "
          f"(vs backtest 1.8% fixed + 0.5% slip = 2.3%)")
    print()
    print("=" * 70)
    print("NET RESULTS BY AUM (per-stock notional)")
    print("=" * 70)
    print(bd_df.to_string(index=False))
    print()
    print("ADV percentiles (TWD, 億):")
    advq = (led["adv_60d_twd"] / 1e8).quantile([.05, .1, .25, .5, .75, .9])
    print(advq.round(3).to_string())

    # stash for report builder
    ctx = dict(gross=gross, twii=tw_stats, avg_hold=avg_hold,
               rt_per_slot_yr=rt_per_slot_yr, avg_held=avg_held, rt_total_yr=rt_total_yr,
               rt_fixed_full=rt_fixed_full, rt_fixed_disc=rt_fixed_disc,
               bd=bd_df.to_dict("records"),
               adv_q=(led["adv_60d_twd"] / 1e8).quantile([.05, .1, .25, .5, .75, .9, .95]).to_dict(),
               sigma_med=float(led["sigma_daily"].median()))
    (REPORTS / "_whale_cost_audit_ctx.json").write_text(
        json.dumps(ctx, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    print("\nWrote: reports/whale_cost_audit_breakdown.csv, _netnav.csv, _ctx.json")


if __name__ == "__main__":
    main()
