"""build_quarterly_dcf_panel.py -- 季度 DCF panel for 樣本擴大驗證

Sample: 15 quarters (2019-Q2 ~ 2023-Q4, 略 Q1 避 annual filing lag) × 274 candidates
        ≈ 3000 panels theoretical (扣 護欄/FCF 負/skip 後 ~2500-3000)

每 Q 用 prev year FY-12-31 BS/FS/CF (filing-lag safe);
spot/beta 在 quarter end; forward 60d (~next quarter close) 不重疊 → 獨立 obs。

Reuse data_cache/dcf_historical/{stock_id}_{bs,fs,cf}.parquet (從 yearly run 已 cache)
+ data_cache/backtest/ohlcv_tw.parquet + _twii_for_audit.parquet

Output: reports/dcf_quarterly_panel.parquet
"""
from __future__ import annotations
import json, logging
from pathlib import Path
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DCF_CACHE_DIR = REPO / "data_cache" / "dcf_historical"
OUT = REPO / "reports" / "dcf_quarterly_panel.parquet"

DEFAULT_RF = 0.0155
DEFAULT_ERP = 0.065
DEFAULT_RD = 0.025
DEFAULT_ETR = 0.17
FWD_DAYS = 60

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


def classify_sector(industry: str) -> str:
    if not industry: return "default"
    if "半導體" in industry: return "semi"
    if any(k in industry for k in ["電子","電腦","通信","光電","資訊服務"]): return "tech"
    if any(k in industry for k in ["生技","醫療"]): return "biotech"
    if any(k in industry for k in ["食品","紡織","運輸","觀光","貿易","百貨","汽車","居家"]): return "consumer"
    if any(k in industry for k in ["鋼鐵","塑膠","化學","建材","營造","橡膠","水泥","玻璃","造紙","電機","電器","機電"]): return "industrial"
    if any(k in industry for k in ["金融","保險","銀行","證券"]): return "financial"
    if any(k in industry for k in ["油電","燃氣","公用"]): return "utility"
    return "default"


def get_value(df, t):
    if df.empty: return 0.0
    sub = df[df["type"] == t]
    return float(sub["value"].iloc[0]) if not sub.empty else 0.0


def compute_beta(stock_px, twii, as_of, window=750):
    s = stock_px[stock_px["date"] <= as_of].sort_values("date").tail(window)
    m = twii[twii["date"] <= as_of].sort_values("date").tail(window)
    df = s[["date","Close"]].merge(m[["date","Close"]], on="date", suffixes=("_s","_m"))
    df = df[(df["Close_s"] > 0) & (df["Close_m"] > 0)]
    df["r_s"] = df["Close_s"].pct_change()
    df["r_m"] = df["Close_m"].pct_change()
    df = df.replace([np.inf,-np.inf], np.nan).dropna()
    if len(df) < 60: return 1.0
    var_m = float(np.var(df["r_m"]))
    if var_m <= 0: return 1.0
    return float(np.cov(df["r_s"], df["r_m"])[0,1] / var_m)


def project_dcf(fcf_base, g1, g_term, wacc, years=5):
    if wacc <= g_term: return float("nan")
    pv = 0.0
    fcf_t = fcf_base
    for t in range(1, years+1):
        fcf_t = fcf_base * (1+g1)**t
        pv += fcf_t / (1+wacc)**t
    tv = fcf_t * (1+g_term) / (wacc - g_term)
    return pv + tv / (1+wacc)**years


# 15 quarter snapshots: Q2/Q3/Q4 of each year 2019-2023
QUARTERS = []
for yr in range(2019, 2024):
    for md in [("06","30"), ("09","30"), ("12","31")]:
        QUARTERS.append(f"{yr}-{md[0]}-{md[1]}")


def compute_quarterly_panel(stock_id, q_end_str, bs_all, fs_all, cf_all, px, twii, sector_key):
    q_end = pd.Timestamp(q_end_str)
    # Use prev year FY (filing-lag safe for Q2/Q3/Q4 of year N → FY N-1)
    fy_end_str = f"{q_end.year - 1}-12-31"
    bs_a = bs_all[bs_all["date"] == fy_end_str]
    fs_a = fs_all[fs_all["date"] == fy_end_str]
    if bs_a.empty or fs_a.empty: return None

    ordinary = get_value(bs_a, "OrdinaryShare")
    shares = ordinary / 10.0 if ordinary > 0 else 0.0
    if shares <= 0: return None

    debt_short = get_value(bs_a, "ShorttermBorrowings")
    debt_long  = get_value(bs_a, "LongtermBorrowings")
    bonds      = get_value(bs_a, "BondsPayable")
    cash       = get_value(bs_a, "CashAndCashEquivalents")
    total_debt = debt_short + debt_long + bonds
    net_debt   = max(total_debt - cash, 0.0)

    pretax = get_value(fs_a, "PreTaxIncome")
    tax    = get_value(fs_a, "TAX")
    etr    = tax / pretax if pretax > 0 else DEFAULT_ETR

    # FCF history (5 yr ending at fy_end_str = prev year)
    fy_year = int(fy_end_str[:4])
    fcf_hist = []
    for yr in range(fy_year - 4, fy_year + 1):
        ye = f"{yr}-12-31"
        cf_y = cf_all[cf_all["date"] == ye]
        cfo = get_value(cf_y, "CashFlowsFromOperatingActivities")
        capex = get_value(cf_y, "AcquisitionOfPropertyPlantAndEquipment")
        if cfo: fcf_hist.append(cfo + capex)
    recent = [f for f in fcf_hist[-3:] if f > 0]
    if not recent: return None
    fcf_base = float(np.mean(recent))

    # Spot at q_end (closest trading day <= q_end)
    stock_px = px[px["stock_id"] == stock_id]
    spot_rows = stock_px[stock_px["date"] <= q_end].sort_values("date")
    if spot_rows.empty: return None
    spot = float(spot_rows.iloc[-1]["Close"])
    if spot <= 0: return None
    market_cap = shares * spot

    beta = compute_beta(stock_px, twii, q_end)
    re = DEFAULT_RF + beta * DEFAULT_ERP
    V = market_cap + total_debt
    we = market_cap / V if V else 1.0
    wd = total_debt / V if V else 0.0
    wacc = we * re + wd * DEFAULT_RD * (1 - etr)
    if wacc - 0.04 < 0.02: return None

    scen = SECTOR_SCENARIOS.get(sector_key, SECTOR_SCENARIOS["default"])
    g_term_ceil = max(wacc - 0.04, -0.01)
    mos = {}
    for name in ("Bull","Base","Bear"):
        g1, g_t = scen[name]
        g_t = min(g_t, g_term_ceil)
        ev = project_dcf(fcf_base, g1, g_t, wacc)
        if not np.isfinite(ev): return None
        eq = ev - net_debt
        mos[name] = (eq / shares) / spot - 1

    # Forward 60d return from q_end (next quarter overlap-free)
    fwd_rows = stock_px[stock_px["date"] > q_end].sort_values("date")
    if len(fwd_rows) <= FWD_DAYS: return None
    entry_px = float(fwd_rows.iloc[0]["Close"])
    exit_px  = float(fwd_rows.iloc[FWD_DAYS]["Close"])
    if entry_px <= 0: return None
    fwd_60 = exit_px / entry_px - 1

    return {
        "stock_id": stock_id, "q_end": q_end_str, "fy_used": fy_end_str,
        "sector_key": sector_key, "bull_mos": mos["Bull"], "base_mos": mos["Base"],
        "bear_mos": mos["Bear"], "wacc": wacc, "beta": beta,
        "fcf_base_bn": fcf_base / 1e9, "spot": spot, "fwd_60d_ret": fwd_60,
    }


def main():
    logger.info("Loading OHLCV/TWII/sector info (offline)...")
    px = pd.read_parquet(REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet")
    px["date"] = pd.to_datetime(px["date"])
    twii = pd.read_parquet(REPO / "data_cache" / "backtest" / "_twii_for_audit.parquet")
    twii["date"] = pd.to_datetime(twii["date"])
    info = pd.read_parquet(REPO / "data_cache" / "tw_stock_info.parquet")
    industry_map = dict(zip(info["stock_id"], info["industry_category"]))
    with open(REPO / "data_cache" / "backtest" / "top300_universe.json", encoding="utf-8") as f:
        universe = json.load(f)

    candidates = []
    for sid in universe:
        sector = classify_sector(industry_map.get(sid, ""))
        if sector in ("financial", "utility"): continue
        # Skip stocks without cached BS (couldn't fetch due to quota)
        if not (DCF_CACHE_DIR / f"{sid}_bs.parquet").exists(): continue
        candidates.append((sid, sector))
    logger.info("Candidates with full cache: %d (15 quarters each)", len(candidates))

    rows = []
    for i, (stock_id, sector_key) in enumerate(candidates, 1):
        try:
            bs = pd.read_parquet(DCF_CACHE_DIR / f"{stock_id}_bs.parquet")
            fs = pd.read_parquet(DCF_CACHE_DIR / f"{stock_id}_fs.parquet")
            cf = pd.read_parquet(DCF_CACHE_DIR / f"{stock_id}_cf.parquet")
        except Exception as e:
            logger.debug("%s read cache fail: %s", stock_id, e)
            continue
        if bs.empty or fs.empty or cf.empty: continue

        n_q = 0
        for q_end in QUARTERS:
            try:
                row = compute_quarterly_panel(stock_id, q_end, bs, fs, cf,
                                              px, twii, sector_key)
                if row is not None:
                    rows.append(row)
                    n_q += 1
            except Exception as e:
                logger.debug("%s @%s err: %s", stock_id, q_end, e)
        if i % 30 == 0:
            logger.info("[%d/%d] %s sector=%s q=%d rows=%d",
                        i, len(candidates), stock_id, sector_key, n_q, len(rows))

    out = pd.DataFrame(rows)
    out.to_parquet(OUT, index=False)
    logger.info("Saved %d rows -> %s", len(out), OUT)

    print("\n=== Summary ===")
    print(f"Total quarterly panels: {len(out)}")
    print(f"\nBy quarter (n stocks per Q):")
    print(out.groupby("q_end").size().to_string())
    print(f"\nbase_mos distribution:")
    print(out["base_mos"].describe()[["count","mean","50%","min","max"]])


if __name__ == "__main__":
    main()
