"""build_historical_dcf_panel.py -- 計算歷史 DCF panel for IC 驗證

混合資料源:
  - 從 FinMind 拉 fresh BS/CF (offline cache 缺 cash/debt 細項；OrdinaryShare 也只有部分)
    Cache 到 data_cache/dcf_historical/{stock_id}_bs.parquet etc. (TTL 不限，年報歷史不變)
  - OHLCV 用已有 data_cache/backtest/ohlcv_tw.parquet
  - TWII 用 data_cache/backtest/_twii_for_audit.parquet
  - Industry 用 data_cache/tw_stock_info.parquet
  - Universe 用 data_cache/backtest/top300_universe.json (扣除 financial/utility)

每檔 stock 拉 BS + CF 兩個 dataset = 2 calls；top300 - 23 fin = 277 candidate stocks
~554 FinMind calls 總；600/hr 限制下約 1 hr 跑完，加 sleep(0.5s) 保險。
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

REPO = Path(__file__).resolve().parent.parent
load_dotenv(REPO / "local" / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
TOKEN = os.getenv("FINMIND_API_TOKEN")
FETCH_SLEEP = 0.4  # 防 rate limit

# DCF constants
DEFAULT_RF = 0.0155
DEFAULT_ERP = 0.065
DEFAULT_RD = 0.025
DEFAULT_ETR = 0.17

SECTOR_SCENARIOS = {
    "semi":       {"Bull": (0.18, 0.04), "Base": (0.10, 0.03),  "Bear": (0.02,  0.02)},
    "tech":       {"Bull": (0.14, 0.04), "Base": (0.07, 0.03),  "Bear": (0.00,  0.02)},
    "biotech":    {"Bull": (0.12, 0.03), "Base": (0.06, 0.025), "Bear": (-0.02, 0.02)},
    "consumer":   {"Bull": (0.08, 0.03), "Base": (0.04, 0.025), "Bear": (0.00,  0.02)},
    "industrial": {"Bull": (0.08, 0.03), "Base": (0.04, 0.025), "Bear": (-0.02, 0.02)},
    "financial":  {"Bull": (0.06, 0.03), "Base": (0.03, 0.025), "Bear": (0.00,  0.02)},
    "utility":    {"Bull": (0.04, 0.025), "Base": (0.02, 0.02), "Bear": (-0.02, 0.015)},
    "default":    {"Bull": (0.12, 0.04), "Base": (0.06, 0.03),  "Bear": (0.00,  0.02)},
}

FY_ENDS = ["2019-12-31", "2020-12-31", "2021-12-31", "2022-12-31", "2023-12-31"]
ENTRY_OFFSET_DAYS = 90
FWD_DAYS_TRADING = 252

DCF_CACHE_DIR = REPO / "data_cache" / "dcf_historical"
DCF_CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ---- sector classification ----
def classify_sector(industry: str) -> str:
    if not industry:
        return "default"
    if "半導體" in industry:
        return "semi"
    if any(k in industry for k in ["電子", "電腦", "通信", "光電", "資訊服務"]):
        return "tech"
    if any(k in industry for k in ["生技", "醫療"]):
        return "biotech"
    if any(k in industry for k in ["食品", "紡織", "運輸", "觀光", "貿易", "百貨", "汽車", "居家"]):
        return "consumer"
    if any(k in industry for k in ["鋼鐵", "塑膠", "化學", "建材", "營造", "橡膠",
                                    "水泥", "玻璃", "造紙", "電機", "電器", "機電"]):
        return "industrial"
    if any(k in industry for k in ["金融", "保險", "銀行", "證券"]):
        return "financial"
    if any(k in industry for k in ["油電", "燃氣", "公用"]):
        return "utility"
    return "default"


# ---- FinMind fetch with disk cache (per stock_id) ----
def _fetch_finmind(dataset: str, stock_id: str, start: str = "2018-01-01") -> pd.DataFrame:
    params = {"dataset": dataset, "data_id": stock_id, "start_date": start}
    if TOKEN:
        params["token"] = TOKEN
    r = requests.get(FINMIND_URL, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("status") != 200:
        raise RuntimeError(f"{dataset}/{stock_id}: {j.get('msg')}")
    return pd.DataFrame(j.get("data", []))


def load_or_fetch_bs(stock_id: str) -> pd.DataFrame:
    p = DCF_CACHE_DIR / f"{stock_id}_bs.parquet"
    if p.exists():
        return pd.read_parquet(p)
    time.sleep(FETCH_SLEEP)
    df = _fetch_finmind("TaiwanStockBalanceSheet", stock_id)
    if not df.empty:
        df.to_parquet(p)
    return df


def load_or_fetch_cf(stock_id: str) -> pd.DataFrame:
    p = DCF_CACHE_DIR / f"{stock_id}_cf.parquet"
    if p.exists():
        return pd.read_parquet(p)
    time.sleep(FETCH_SLEEP)
    df = _fetch_finmind("TaiwanStockCashFlowsStatement", stock_id)
    if not df.empty:
        df.to_parquet(p)
    return df


def load_or_fetch_fs(stock_id: str) -> pd.DataFrame:
    p = DCF_CACHE_DIR / f"{stock_id}_fs.parquet"
    if p.exists():
        return pd.read_parquet(p)
    time.sleep(FETCH_SLEEP)
    df = _fetch_finmind("TaiwanStockFinancialStatements", stock_id)
    if not df.empty:
        df.to_parquet(p)
    return df


def get_value(df: pd.DataFrame, type_name: str) -> float:
    if df.empty:
        return 0.0
    sub = df[df["type"] == type_name]
    return float(sub["value"].iloc[0]) if not sub.empty else 0.0


def compute_beta(stock_px: pd.DataFrame, twii: pd.DataFrame, as_of: pd.Timestamp,
                 window_days: int = 750) -> float:
    s = stock_px[stock_px["date"] <= as_of].sort_values("date").tail(window_days)
    m = twii[twii["date"] <= as_of].sort_values("date").tail(window_days)
    df = s[["date", "Close"]].merge(m[["date", "Close"]], on="date", suffixes=("_s", "_m"))
    df = df[(df["Close_s"] > 0) & (df["Close_m"] > 0)]
    df["r_s"] = df["Close_s"].pct_change()
    df["r_m"] = df["Close_m"].pct_change()
    df = df.replace([np.inf, -np.inf], np.nan).dropna()
    if len(df) < 60:
        return 1.0
    var_m = float(np.var(df["r_m"]))
    if var_m <= 0:
        return 1.0
    return float(np.cov(df["r_s"], df["r_m"])[0, 1] / var_m)


def project_dcf(fcf_base: float, g1: float, g_term: float, wacc: float, years: int = 5) -> float:
    if wacc <= g_term:
        return float("nan")
    pv_explicit = 0.0
    fcf_t = fcf_base
    for t in range(1, years + 1):
        fcf_t = fcf_base * (1 + g1) ** t
        pv_explicit += fcf_t / (1 + wacc) ** t
    fcf_terminal = fcf_t * (1 + g_term)
    tv = fcf_terminal / (wacc - g_term)
    pv_terminal = tv / (1 + wacc) ** years
    return pv_explicit + pv_terminal


def compute_panel_for(stock_id: str, fy_end_str: str,
                      bs_all: pd.DataFrame, fs_all: pd.DataFrame, cf_all: pd.DataFrame,
                      px: pd.DataFrame, twii: pd.DataFrame,
                      sector_key: str) -> dict | None:
    fy_end = pd.Timestamp(fy_end_str)

    bs_a = bs_all[bs_all["date"] == fy_end_str]
    fs_a = fs_all[fs_all["date"] == fy_end_str]
    if bs_a.empty or fs_a.empty:
        return None

    ordinary = get_value(bs_a, "OrdinaryShare")
    shares = ordinary / 10.0 if ordinary > 0 else 0.0
    if shares <= 0:
        return None

    debt_short = get_value(bs_a, "ShorttermBorrowings")
    debt_long = get_value(bs_a, "LongtermBorrowings")
    bonds = get_value(bs_a, "BondsPayable")
    cash = get_value(bs_a, "CashAndCashEquivalents")
    total_debt = debt_short + debt_long + bonds
    net_debt = max(total_debt - cash, 0.0)

    pretax = get_value(fs_a, "PreTaxIncome")
    tax = get_value(fs_a, "TAX")
    etr = tax / pretax if pretax > 0 else DEFAULT_ETR

    fy_year = int(fy_end_str[:4])
    fcf_hist = []
    for yr in range(fy_year - 4, fy_year + 1):
        ye = f"{yr}-12-31"
        cf_y = cf_all[cf_all["date"] == ye]
        cfo = get_value(cf_y, "CashFlowsFromOperatingActivities")
        capex = get_value(cf_y, "AcquisitionOfPropertyPlantAndEquipment")
        if cfo:
            fcf_hist.append(cfo + capex)
    recent = [f for f in fcf_hist[-3:] if f > 0]
    if not recent:
        return None
    fcf_base = float(np.mean(recent))

    stock_px = px[px["stock_id"] == stock_id]
    spot_rows = stock_px[stock_px["date"] <= fy_end].sort_values("date")
    if spot_rows.empty:
        return None
    spot = float(spot_rows.iloc[-1]["Close"])
    if spot <= 0:
        return None
    market_cap = shares * spot

    beta = compute_beta(stock_px, twii, fy_end)

    re = DEFAULT_RF + beta * DEFAULT_ERP
    V = market_cap + total_debt
    we = market_cap / V if V else 1.0
    wd = total_debt / V if V else 0.0
    wacc = we * re + wd * DEFAULT_RD * (1 - etr)

    if wacc - 0.04 < 0.02:
        return None

    scen = SECTOR_SCENARIOS.get(sector_key, SECTOR_SCENARIOS["default"])
    g_term_ceil = max(wacc - 0.04, -0.01)
    mos = {}
    for name in ("Bull", "Base", "Bear"):
        g1, g_term = scen[name]
        g_term = min(g_term, g_term_ceil)
        ev = project_dcf(fcf_base, g1, g_term, wacc)
        if not np.isfinite(ev):
            return None
        equity_val = ev - net_debt
        fv = equity_val / shares
        mos[name] = fv / spot - 1

    entry_target = fy_end + pd.Timedelta(days=ENTRY_OFFSET_DAYS)
    entry_rows = stock_px[stock_px["date"] >= entry_target].sort_values("date")
    if entry_rows.empty:
        return None
    entry_px = float(entry_rows.iloc[0]["Close"])
    if entry_px <= 0:
        return None

    fwd_252 = float(entry_rows.iloc[FWD_DAYS_TRADING]["Close"] / entry_px - 1) \
        if len(entry_rows) > FWD_DAYS_TRADING else np.nan
    fwd_60 = float(entry_rows.iloc[60]["Close"] / entry_px - 1) \
        if len(entry_rows) > 60 else np.nan

    return {
        "stock_id": stock_id,
        "fy_end": fy_end_str,
        "sector_key": sector_key,
        "bull_mos": mos["Bull"],
        "base_mos": mos["Base"],
        "bear_mos": mos["Bear"],
        "wacc": wacc,
        "beta": beta,
        "fcf_base_bn": fcf_base / 1e9,
        "spot_at_fy": spot,
        "market_cap_bn": market_cap / 1e9,
        "net_debt_bn": net_debt / 1e9,
        "entry_px": entry_px,
        "fwd_60d_ret": fwd_60,
        "fwd_252d_ret": fwd_252,
    }


def main():
    logger.info("Loading OHLCV/TWII (offline)...")
    px = pd.read_parquet(REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet")
    px["date"] = pd.to_datetime(px["date"])
    twii = pd.read_parquet(REPO / "data_cache" / "backtest" / "_twii_for_audit.parquet")
    twii["date"] = pd.to_datetime(twii["date"])

    info = pd.read_parquet(REPO / "data_cache" / "tw_stock_info.parquet")
    industry_map = dict(zip(info["stock_id"], info["industry_category"]))

    with open(REPO / "data_cache" / "backtest" / "top300_universe.json", encoding="utf-8") as f:
        universe = json.load(f)

    filtered_universe = []
    for sid in universe:
        ind = industry_map.get(sid, "")
        sector = classify_sector(ind)
        if sector in ("financial", "utility"):
            continue
        filtered_universe.append((sid, sector))
    logger.info("Universe: %d total, %d after excl. fin/utility", len(universe), len(filtered_universe))

    rows = []
    for i, (stock_id, sector_key) in enumerate(filtered_universe, 1):
        try:
            bs_all = load_or_fetch_bs(stock_id)
            fs_all = load_or_fetch_fs(stock_id)
            cf_all = load_or_fetch_cf(stock_id)
        except Exception as e:
            logger.warning("[%d/%d] %s fetch failed: %s", i, len(filtered_universe), stock_id, e)
            continue

        if bs_all.empty or fs_all.empty or cf_all.empty:
            continue

        n_panels = 0
        for fy_end in FY_ENDS:
            try:
                row = compute_panel_for(stock_id, fy_end, bs_all, fs_all, cf_all,
                                        px, twii, sector_key)
                if row is not None:
                    rows.append(row)
                    n_panels += 1
            except Exception as e:
                logger.debug("%s @%s err: %s", stock_id, fy_end, e)
        if i % 20 == 0:
            logger.info("[%d/%d] %s sector=%s panels=%d total_rows=%d",
                        i, len(filtered_universe), stock_id, sector_key, n_panels, len(rows))

    out = pd.DataFrame(rows)
    out_path = REPO / "reports" / "dcf_ic_historical_panel.parquet"
    out.to_parquet(out_path, index=False)
    logger.info("Saved %d rows -> %s", len(out), out_path)

    print("\n=== Panel Summary ===")
    print("By FY:")
    print(out.groupby("fy_end").size().to_string())
    print("\nbase_mos stats by FY:")
    print(out.groupby("fy_end")["base_mos"].describe()[["count", "mean", "50%", "min", "max"]])
    print("\nfwd_252d non-null count by FY:")
    print(out.groupby("fy_end")["fwd_252d_ret"].apply(lambda x: x.notna().sum()).to_string())


if __name__ == "__main__":
    main()
