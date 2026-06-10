"""
dcf_calculator.py -- 個股 Two-stage DCF 估值試算 (Bull / Base / Bear)

來源：
  - FinMind raw API: TaiwanStockBalanceSheet / TaiwanStockFinancialStatements /
                     TaiwanStockCashFlowsStatement / TaiwanStockPrice
  - TAIEX index price for β regression

模型：
  Stage 1: 5 年顯式 FCF 預測（成長率 g1 per scenario）
  Stage 2: Terminal value = FCF_yr5 × (1+g_term) / (WACC - g_term)
    EV = Σ FCF_t / (1+WACC)^t + TV / (1+WACC)^5
    Equity Value = EV - 有息負債 + 約當現金
    Per-share fair value = Equity Value / 流通股數

WACC：
  Re = Rf + β × ERP                 (CAPM)
  Rd = TW AA 公司債 benchmark / 用戶覆寫
  WACC = (E/V)·Re + (D/V)·Rd·(1-ETR)

執行：python tools/dcf_calculator.py 2330
       python tools/dcf_calculator.py 2454 --rd 0.020 --erp 0.060

⚠️ 這是 deterministic 估值面板，非 alpha factor。
   敏感度排序：terminal g > Stage-1 g > WACC（見 memory）。
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

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

DEFAULT_RF = 0.0155     # TW 10yr 政府公債殖利率 (2026 Q2 benchmark)
DEFAULT_ERP = 0.065     # Damodaran TW 2024: US mature 5% + country premium 1.5%
DEFAULT_RD = 0.025      # TW AA-rated 公司債 benchmark
DEFAULT_ETR = 0.17      # 台灣 20% 公司稅但大型科技股 R&D credit 後 ~17%

CACHE_DIR = REPO / "data_cache" / "dcf_panels"
CACHE_TTL_DAYS = 30     # 年報只在 Q1 末更新；30 天 TTL 平衡新鮮度 vs FinMind 配額

# 產業分組 g1 預設（2026-05-16 加；舊版一刀切 12/6/0% 對半導體保守對傳產過熱）
# (g1, g_term) per scenario per sector；default 同舊版行為
SECTOR_SCENARIOS = {
    "semi":       {"Bull": (0.18, 0.04), "Base": (0.10, 0.03),  "Bear": (0.02,  0.02)},
    "tech":       {"Bull": (0.14, 0.04), "Base": (0.07, 0.03),  "Bear": (0.00,  0.02)},
    "biotech":    {"Bull": (0.12, 0.03), "Base": (0.06, 0.025), "Bear": (-0.02, 0.02)},
    "consumer":   {"Bull": (0.08, 0.03), "Base": (0.04, 0.025), "Bear": (0.00,  0.02)},
    "industrial": {"Bull": (0.08, 0.03), "Base": (0.04, 0.025), "Bear": (-0.02, 0.02)},
    "financial":  {"Bull": (0.06, 0.03), "Base": (0.03, 0.025), "Bear": (0.00,  0.02)},
    "utility":    {"Bull": (0.04, 0.025), "Base": (0.02, 0.02), "Bear": (-0.02, 0.015)},
    "default":    {"Bull": (0.12, 0.04), "Base": (0.06, 0.03),  "Bear": (0.00,  0.02)},  # 同舊行為
}

# TaiwanStockInfo 對照表 2026-06-10 收斂至 cache_manager.get_tw_stock_info()
# (3 層快取 data_cache/tw_stock_info.csv)；舊自帶 parquet cache + raw requests
# (繞過 FinMindTracker 額度計數) 已移除。


# ============================================================
# FinMind fetch
# ============================================================

def fetch(dataset: str, stock_id: str, start: str = "2020-01-01") -> pd.DataFrame:
    params = {"dataset": dataset, "data_id": stock_id, "start_date": start}
    if TOKEN:
        params["token"] = TOKEN
    r = requests.get(FINMIND_URL, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("status") != 200:
        raise RuntimeError(f"FinMind {dataset}/{stock_id} failed: {j.get('msg')}")
    return pd.DataFrame(j.get("data", []))


def gv(df: pd.DataFrame, type_name: str, date: Optional[str] = None) -> float:
    """從 long-format FinMind 表抽 single value (type, date)，找不到回 0.0"""
    if df.empty:
        return 0.0
    sub = df[df["type"] == type_name]
    if date:
        sub = sub[sub["date"] == date]
    return float(sub["value"].iloc[0]) if not sub.empty else 0.0


# ============================================================
# Beta regression
# ============================================================

def compute_beta(price: pd.DataFrame, taiex: pd.DataFrame, window_days: int = 750) -> float:
    """3-yr daily return beta (vs TAIEX)"""
    p = price[["date", "close"]].copy()
    m = taiex[["date", "close"]].copy()
    p["date"] = pd.to_datetime(p["date"])
    m["date"] = pd.to_datetime(m["date"])
    df = p.merge(m, on="date", suffixes=("_s", "_m")).sort_values("date").tail(window_days)
    # FinMind 偶有 close=0（停牌日），會讓 pct_change 變 inf/-1.0 污染 cov
    df = df[(df["close_s"] > 0) & (df["close_m"] > 0)]
    df["r_s"] = df["close_s"].pct_change()
    df["r_m"] = df["close_m"].pct_change()
    df = df.replace([np.inf, -np.inf], np.nan).dropna()
    if len(df) < 60:
        logger.warning("beta: only %d obs, fallback β=1.0", len(df))
        return 1.0
    var_m = float(np.var(df["r_m"]))
    if var_m <= 0:
        return 1.0
    return float(np.cov(df["r_s"], df["r_m"])[0, 1] / var_m)


# ============================================================
# Snapshot
# ============================================================

@dataclass
class Snapshot:
    stock_id: str
    fy_end: str
    close_price: float
    shares: float
    market_cap: float
    debt_short: float
    debt_long: float
    bonds: float
    cash: float
    total_debt: float = field(init=False)
    net_debt: float = field(init=False)
    pretax: float
    tax: float
    etr: float = field(init=False)
    fcf_history: list = field(default_factory=list)  # last 5 years annual FCF
    fcf_base: float = field(init=False)
    beta: float = 1.0

    def __post_init__(self):
        self.total_debt = self.debt_short + self.debt_long + self.bonds
        self.net_debt = max(self.total_debt - self.cash, 0.0)
        self.etr = self.tax / self.pretax if self.pretax > 0 else DEFAULT_ETR
        # 用 3 年平均 FCF 當 base（去除單年波動）
        recent = [f for f in self.fcf_history[-3:] if f > 0]
        self.fcf_base = float(np.mean(recent)) if recent else 0.0


def _detect_latest_fy(bs: pd.DataFrame) -> Optional[str]:
    """從 balance sheet 取最近完整年報日 (YYYY-12-31)，找不到回 None"""
    if bs.empty:
        return None
    annuals = sorted({d for d in bs["date"].unique() if str(d).endswith("-12-31")})
    return annuals[-1] if annuals else None


def build_snapshot(stock_id: str, fy_end: Optional[str] = None) -> Snapshot:
    bs = fetch("TaiwanStockBalanceSheet", stock_id)
    fs = fetch("TaiwanStockFinancialStatements", stock_id)
    cf = fetch("TaiwanStockCashFlowsStatement", stock_id)
    px = fetch("TaiwanStockPrice", stock_id, start="2022-01-01")
    mkt = fetch("TaiwanStockPrice", "TAIEX", start="2022-01-01")

    if fy_end is None:
        fy_end = _detect_latest_fy(bs)
        if fy_end is None:
            raise RuntimeError(f"{stock_id}: 無年報資料")

    bs_a = bs[bs["date"] == fy_end]
    fs_a = fs[fs["date"] == fy_end]   # cumulative 全年值
    # 流通股數: OrdinaryShare (par 10) / 10
    ordinary = gv(bs_a, "OrdinaryShare")
    shares = ordinary / 10.0 if ordinary > 0 else 0.0
    px["date"] = pd.to_datetime(px["date"])
    px_sorted = px.sort_values("date")
    last_px = float(px_sorted[px_sorted["date"] <= fy_end].iloc[-1]["close"])
    # 用最近 trading day 收盤算市值 (更貼近現在)
    spot_px = float(px_sorted.iloc[-1]["close"])

    # 5 年 FCF history (CFO + CapEx). FinMind CapEx 是負值。
    fcf_hist = []
    for year in range(2020, int(fy_end[:4]) + 1):
        ye = f"{year}-12-31"
        cf_y = cf[cf["date"] == ye]
        cfo = gv(cf_y, "CashFlowsFromOperatingActivities")
        capex = gv(cf_y, "AcquisitionOfPropertyPlantAndEquipment")  # 負值
        if cfo:
            fcf_hist.append(cfo + capex)  # CapEx 已是負 → 加號

    snap = Snapshot(
        stock_id=stock_id,
        fy_end=fy_end,
        close_price=spot_px,
        shares=shares,
        market_cap=shares * spot_px,
        debt_short=gv(bs_a, "ShorttermBorrowings"),
        debt_long=gv(bs_a, "LongtermBorrowings"),
        bonds=gv(bs_a, "BondsPayable"),
        cash=gv(bs_a, "CashAndCashEquivalents"),
        pretax=gv(fs_a, "PreTaxIncome"),
        tax=gv(fs_a, "TAX"),
        fcf_history=fcf_hist,
        beta=compute_beta(px, mkt),
    )
    return snap


# ============================================================
# WACC
# ============================================================

def compute_wacc(snap: Snapshot, rf: float, erp: float, rd: float) -> dict:
    re = rf + snap.beta * erp
    V = snap.market_cap + snap.total_debt
    we = snap.market_cap / V if V else 1.0
    wd = snap.total_debt / V if V else 0.0
    wacc = we * re + wd * rd * (1 - snap.etr)
    return {
        "Re": re, "Rd": rd, "E/V": we, "D/V": wd,
        "ETR": snap.etr, "beta": snap.beta, "rf": rf, "erp": erp,
        "WACC": wacc,
    }


# ============================================================
# DCF projection
# ============================================================

def project_dcf(fcf_base: float, g1: float, g_term: float, wacc: float, years: int = 5) -> dict:
    if wacc <= g_term:
        return {"fair_ev": float("nan"), "pv_explicit": float("nan"), "pv_terminal": float("nan"),
                "fcf_path": [], "error": f"WACC {wacc:.3f} <= g_term {g_term:.3f}"}
    fcf_path = []
    pv_explicit = 0.0
    for t in range(1, years + 1):
        fcf_t = fcf_base * (1 + g1) ** t
        pv = fcf_t / (1 + wacc) ** t
        fcf_path.append((t, fcf_t, pv))
        pv_explicit += pv
    fcf_terminal = fcf_path[-1][1] * (1 + g_term)
    tv = fcf_terminal / (wacc - g_term)
    pv_terminal = tv / (1 + wacc) ** years
    return {
        "fair_ev": pv_explicit + pv_terminal,
        "pv_explicit": pv_explicit,
        "pv_terminal": pv_terminal,
        "fcf_path": fcf_path,
        "terminal_value": tv,
    }


def run_scenarios(snap: Snapshot, wacc: float, sector_key: str = "default") -> pd.DataFrame:
    table = SECTOR_SCENARIOS.get(sector_key, SECTOR_SCENARIOS["default"])
    # 護欄：g_term 必須 ≤ WACC - 4pp 否則 TV 爆炸 (1/小分母)
    g_term_ceiling = max(wacc - 0.04, -0.01)
    rows = []
    for name in ("Bull", "Base", "Bear"):
        g1, g_term = table[name]
        g_term = min(g_term, g_term_ceiling)
        out = project_dcf(snap.fcf_base, g1, g_term, wacc)
        ev = out["fair_ev"]
        equity_val = ev - snap.net_debt
        per_share = equity_val / snap.shares if snap.shares > 0 else float("nan")
        mos = (per_share / snap.close_price - 1) if snap.close_price > 0 else float("nan")
        rows.append({
            "scenario": name,
            "g1": g1, "g_term": g_term,
            "EV_bn": ev / 1e9, "PV_explicit_bn": out["pv_explicit"] / 1e9,
            "PV_terminal_bn": out["pv_terminal"] / 1e9,
            "EquityValue_bn": equity_val / 1e9,
            "FairValue_per_share": per_share,
            "MOS_vs_spot": mos,
        })
    return pd.DataFrame(rows)


# ============================================================
# Disk cache (30 天 TTL)
# ============================================================

def _cache_path(stock_id: str, rf: float, erp: float, rd: float) -> Path:
    """cache key 帶 (rf, erp, rd) 確保非預設假設不會撞 key"""
    key = f"{stock_id}_rf{rf:.4f}_erp{erp:.4f}_rd{rd:.4f}.json"
    return CACHE_DIR / key


def _read_cache(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    age_days = (time.time() - path.stat().st_mtime) / 86400
    if age_days > CACHE_TTL_DAYS:
        logger.debug("dcf cache STALE %s (age %.1fd)", path.name, age_days)
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("dcf cache read failed %s: %s", path.name, e)
        return None


def _write_cache(path: Path, panel: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(panel, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("dcf cache write failed %s: %s", path.name, e)


_STOCK_INFO_MEM: Optional[dict] = None  # session-level in-memory map


def _load_stock_info() -> dict:
    """回 {stock_id: industry_category}。走 cache_manager.get_tw_stock_info() 3 層快取
    (memory -> disk 7天 -> FinMind，失敗回 stale disk)，與全專案共用同一份對照表。"""
    global _STOCK_INFO_MEM
    if _STOCK_INFO_MEM is not None:
        return _STOCK_INFO_MEM
    try:
        import sys
        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from cache_manager import get_tw_stock_info
        df = get_tw_stock_info()
        if df is not None and not df.empty:
            if "date" in df.columns:
                df = df.sort_values("date").drop_duplicates("stock_id", keep="last")
            else:
                df = df.drop_duplicates("stock_id", keep="last")
            _STOCK_INFO_MEM = dict(zip(df["stock_id"], df["industry_category"]))
        else:
            _STOCK_INFO_MEM = {}
    except Exception as e:
        logger.warning("TaiwanStockInfo load failed: %s", e)
        _STOCK_INFO_MEM = {}
    return _STOCK_INFO_MEM


def _classify_sector(industry: str) -> str:
    """將 FinMind industry_category 中文映射到 SECTOR_SCENARIOS key"""
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


def get_sector_for(stock_id: str) -> tuple[str, str]:
    """回 (industry_chinese, sector_key)。失敗回 ('', 'default')"""
    info = _load_stock_info()
    industry = info.get(stock_id, "")
    return industry, _classify_sector(industry)


def _refresh_spot(panel: dict, stock_id: str) -> dict:
    """Cache hit 時拉最新 spot 重算 MOS。fair_value/WACC/FCF 維持 cached
    （那些錨定年報，盤中不變）。只動 spot + market_cap + scenarios.MOS_vs_spot。

    失敗時回 cached panel 不 raise — spot refresh 是 best-effort optimization。
    """
    try:
        start = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
        px = fetch("TaiwanStockPrice", stock_id, start=start)
        if px.empty:
            return panel
        px = px.sort_values("date")
        new_spot = float(px.iloc[-1]["close"])
        if new_spot <= 0 or abs(new_spot - panel["spot"]) < 0.01:
            return panel
        refreshed = dict(panel)
        refreshed["spot"] = new_spot
        refreshed["market_cap_bn"] = panel["shares_b"] * new_spot
        new_scenarios = []
        for s in panel["scenarios"]:
            s_new = dict(s)
            fv = s_new.get("FairValue_per_share")
            if fv is not None and fv == fv:  # NaN-safe
                s_new["MOS_vs_spot"] = fv / new_spot - 1
            new_scenarios.append(s_new)
        refreshed["scenarios"] = new_scenarios
        refreshed["_spot_refreshed"] = True  # debug 訊息，format_panel_text 不顯示
        return refreshed
    except Exception as e:
        logger.warning("spot refresh failed for %s: %s", stock_id, e)
        return panel


# ============================================================
# Public API (供 ai_report.py 等呼叫)
# ============================================================

def compute_panel(stock_id: str, *, rf: float = DEFAULT_RF, erp: float = DEFAULT_ERP,
                  rd: float = DEFAULT_RD, fy_end: Optional[str] = None,
                  use_cache: bool = True) -> dict:
    """End-to-end：拉資料 + 算 WACC + 跑 Bull/Base/Bear。

    回傳結構（給 ai_report context builder 用）:
      {
        'ok': True, 'stock_id': '2330', 'fy_end': '2024-12-31',
        'spot': float, 'shares_b': float, 'market_cap_bn': float,
        'wacc': {'WACC':float, 'Re':float, 'Rd':float, 'beta':float, ...},
        'debt_bn': {'ST':float,'LT':float,'Bond':float,'Total':float,'Cash':float,'Net':float},
        'fcf_history_bn': [(year, fcf_bn), ...],
        'fcf_base_bn': float,
        'scenarios': [{scenario, g1, g_term, fair_value, mos, ev_bn, equity_bn}, ...]
      }

    失敗時回傳 {'ok': False, 'reason': str}

    Cache 行為：只在 fy_end=None (auto-detect) 模式下命中 cache；
    explicit fy_end 走 fresh fetch (debug 用)。命中/寫入只記 DEBUG log。
    """
    # 只 cache auto-detect 模式（ai_report 預設路徑）；explicit fy_end 是 debug 用
    cache_path = _cache_path(stock_id, rf, erp, rd) if (use_cache and fy_end is None) else None
    if cache_path is not None:
        cached = _read_cache(cache_path)
        if cached is not None:
            logger.debug("dcf cache HIT %s", cache_path.name)
            # spot refresh：cache 鎖住 spot 會讓 MOS 落後實際走勢，每次 hit 拉最新收盤
            # 只 1 個 FinMind call（相比 cold 的 5 個），仍保留 80% 配額節省
            if cached.get("ok"):
                cached = _refresh_spot(cached, stock_id)
            return cached

    try:
        snap = build_snapshot(stock_id, fy_end=fy_end)
        if snap.shares <= 0:
            return {"ok": False, "reason": f"無流通股數 (OrdinaryShare=0 @ {snap.fy_end})"}
        if snap.fcf_base <= 0:
            return {"ok": False, "reason": f"近 3 年 FCF 全為負 (history={[round(f/1e9,1) for f in snap.fcf_history]})"}
        industry, sector_key = get_sector_for(stock_id)
        # 金融/公用：經典 DCF 不適用（金融持投資組合 + 保險準備金 / 公用受費率管制 FCF 非自由）
        if sector_key in ("financial", "utility"):
            return {"ok": False, "reason": f"sector={sector_key} ({industry}) DCF 不適用；請改看 P/B / DDM / 殖利率"}
        wacc_info = compute_wacc(snap, rf=rf, erp=erp, rd=rd)
        # 護欄：WACC - g_term 太接近會讓 TV 爆炸 (1/分母)。觀察 1101 案例 β=0.43 → WACC=3.2%
        # 跟 g_term=3% 只差 0.2pp → fair value 失真。強制 g_term ≤ WACC - 4pp
        if wacc_info["WACC"] - 0.04 < 0.02:
            return {"ok": False, "reason": f"WACC={wacc_info['WACC']*100:.2f}% 過低 (β={snap.beta:.2f})，"
                                            "DCF terminal value 不穩定；考慮 Re/Rd 假設後再用"}
        scenarios_df = run_scenarios(snap, wacc=wacc_info["WACC"], sector_key=sector_key)
        panel = {
            "ok": True,
            "stock_id": snap.stock_id,
            "fy_end": snap.fy_end,
            "industry": industry,
            "sector_key": sector_key,
            "spot": snap.close_price,
            "shares_b": snap.shares / 1e9,
            "market_cap_bn": snap.market_cap / 1e9,
            "wacc": wacc_info,
            "debt_bn": {
                "ST": snap.debt_short / 1e9, "LT": snap.debt_long / 1e9,
                "Bond": snap.bonds / 1e9, "Total": snap.total_debt / 1e9,
                "Cash": snap.cash / 1e9, "Net": snap.net_debt / 1e9,
            },
            "fcf_history_bn": [(2020 + i, f / 1e9) for i, f in enumerate(snap.fcf_history)],
            "fcf_base_bn": snap.fcf_base / 1e9,
            "scenarios": scenarios_df.to_dict("records"),
        }
        if cache_path is not None:
            _write_cache(cache_path, panel)
            logger.debug("dcf cache WRITE %s", cache_path.name)
        return panel
    except Exception as e:
        return {"ok": False, "reason": f"{type(e).__name__}: {e}"}


def format_panel_text(panel: dict) -> str:
    """把 compute_panel 結果格式化成 LLM 可讀的緊湊文字 (給 [VALUATION_PANEL] 用)"""
    if not panel.get("ok"):
        return f"N/A ({panel.get('reason', 'unknown')})"
    w = panel["wacc"]
    d = panel["debt_bn"]
    sc = panel["scenarios"]

    lines = []
    sector_key = panel.get("sector_key", "default")
    industry = panel.get("industry", "")
    sector_disp = f"{sector_key} (industry={industry})" if industry else sector_key
    lines.append(f"FY base: {panel['fy_end']} | Sector: {sector_disp} | Spot: {panel['spot']:.1f} | "
                 f"Shares: {panel['shares_b']:.2f}B | MktCap: {panel['market_cap_bn']:,.0f}億")
    lines.append(f"Debt: ST={d['ST']:.0f}億 LT={d['LT']:.0f}億 Bond={d['Bond']:.0f}億 "
                 f"(Total={d['Total']:.0f}億) | Cash={d['Cash']:.0f}億 | NetDebt={d['Net']:.0f}億")
    lines.append(f"WACC: {w['WACC']*100:.2f}% (Re={w['Re']*100:.2f}% β={w['beta']:.2f} | "
                 f"Rd={w['Rd']*100:.2f}% ETR={w['ETR']*100:.1f}% | E/V={w['E/V']*100:.0f}% D/V={w['D/V']*100:.0f}%)")
    fcf_str = " ".join(f"{y}:{v:.0f}" for y, v in panel["fcf_history_bn"][-5:])
    lines.append(f"FCF history (億): {fcf_str} | Base (3yr avg): {panel['fcf_base_bn']:.0f}億")
    lines.append("")
    lines.append("Scenario | g1   | g_term | FairValue | MOS vs Spot")
    for s in sc:
        fv = s["FairValue_per_share"]
        mos = s["MOS_vs_spot"]
        fv_s = f"{fv:>9,.1f}" if fv == fv else "      N/A"  # NaN check
        mos_s = f"{mos*100:+7.1f}%" if mos == mos else "    N/A"
        lines.append(f"{s['scenario']:<8} | {s['g1']*100:>3.0f}% | {s['g_term']*100:>4.0f}%   | {fv_s} | {mos_s}")
    lines.append("")
    lines.append("用法 (給 LLM 的 anchor)：")
    lines.append("  - 上方是 deterministic 兩階段 DCF，**請以此為 thesis 內的 DCF 數字基準**，不要自行腦補")
    lines.append(f"  - g1/g_term 按 sector={sector_key} 套用 (semi 較高, financial/utility 較低)，"
                 "Stage-1 5 年, Stage-2 永續")
    lines.append("  - WACC=10% 等量級在台股大型股是合理區間；MOS>+20% 視為深度低估, <-20% 視為高估")
    lines.append("  - 敏感度排序：terminal g > Stage-1 g > WACC，narrative 中可指出哪個假設最影響結論")
    return "\n".join(lines)


# ============================================================
# CLI / printing
# ============================================================

def print_report(snap: Snapshot, wacc_info: dict, scenarios_df: pd.DataFrame):
    print()
    print(f"=== DCF Report: {snap.stock_id} ===")
    print(f"  Spot close: {snap.close_price:.2f} | Shares: {snap.shares/1e9:.2f} B | "
          f"Market Cap: {snap.market_cap/1e9:,.0f} 億")
    print()
    print(f"--- Balance Sheet snapshot ({snap.fy_end}) ---")
    print(f"  ST borrow: {snap.debt_short/1e9:,.1f} 億 | LT borrow: {snap.debt_long/1e9:,.1f} 億 | "
          f"Bonds: {snap.bonds/1e9:,.1f} 億")
    print(f"  Total interest-bearing debt: {snap.total_debt/1e9:,.1f} 億 | "
          f"Cash: {snap.cash/1e9:,.1f} 億 | Net Debt: {snap.net_debt/1e9:,.1f} 億")
    print()
    print(f"--- WACC components ---")
    print(f"  Beta (3yr daily vs TAIEX): {snap.beta:.3f}")
    print(f"  Rf: {wacc_info['rf']*100:.2f}% | ERP: {wacc_info['erp']*100:.2f}% | "
          f"Re = Rf + β·ERP = {wacc_info['Re']*100:.2f}%")
    print(f"  Rd: {wacc_info['Rd']*100:.2f}% | ETR: {wacc_info['ETR']*100:.2f}%")
    print(f"  E/V = {wacc_info['E/V']*100:.1f}% | D/V = {wacc_info['D/V']*100:.1f}%")
    print(f"  WACC = {wacc_info['WACC']*100:.2f}%")
    print()
    print(f"--- FCF history ---")
    for i, fcf in enumerate(snap.fcf_history):
        yr = 2020 + i
        print(f"  {yr}: {fcf/1e9:,.1f} 億")
    print(f"  Base FCF (3yr avg, latest): {snap.fcf_base/1e9:,.1f} 億")
    print()
    print(f"--- Valuation scenarios ---")
    disp = scenarios_df.copy()
    disp["g1"] = (disp["g1"] * 100).map(lambda x: f"{x:.0f}%")
    disp["g_term"] = (disp["g_term"] * 100).map(lambda x: f"{x:.0f}%")
    disp["EV_bn"] = disp["EV_bn"].map(lambda x: f"{x:,.0f}億")
    disp["EquityValue_bn"] = disp["EquityValue_bn"].map(lambda x: f"{x:,.0f}億")
    disp["FairValue_per_share"] = disp["FairValue_per_share"].map(lambda x: f"{x:,.1f}")
    disp["MOS_vs_spot"] = disp["MOS_vs_spot"].map(lambda x: f"{x*100:+.1f}%")
    print(disp[["scenario", "g1", "g_term", "EV_bn", "EquityValue_bn",
                "FairValue_per_share", "MOS_vs_spot"]].to_string(index=False))
    print()
    print(f"[NOTE] MOS = (fair_value / spot - 1); 正值=低估, 負值=高估")
    print(f"[NOTE] 敏感度: terminal g > Stage-1 g > WACC. 改 g1 1pp = 估值約 ±8-12%")


def main():
    ap = argparse.ArgumentParser(description="TW stock Two-stage DCF calculator")
    ap.add_argument("stock_id", help="台股代號 e.g. 2330")
    ap.add_argument("--fy", default=None, help="最近完整年報日 (預設自動偵測最近 YYYY-12-31)")
    ap.add_argument("--rf", type=float, default=DEFAULT_RF, help=f"無風險利率 (default {DEFAULT_RF})")
    ap.add_argument("--erp", type=float, default=DEFAULT_ERP, help=f"股票風險溢酬 (default {DEFAULT_ERP})")
    ap.add_argument("--rd", type=float, default=DEFAULT_RD, help=f"負債成本 (default {DEFAULT_RD})")
    ap.add_argument("--text", action="store_true", help="輸出 ai_report 用的緊湊文字格式")
    ap.add_argument("--no-cache", action="store_true", help="跳過 disk cache (debug 用)")
    args = ap.parse_args()

    if args.text:
        panel = compute_panel(args.stock_id, rf=args.rf, erp=args.erp, rd=args.rd,
                              fy_end=args.fy, use_cache=not args.no_cache)
        print(format_panel_text(panel))
        return
    snap = build_snapshot(args.stock_id, fy_end=args.fy)
    _industry, sector_key = get_sector_for(args.stock_id)
    wacc_info = compute_wacc(snap, rf=args.rf, erp=args.erp, rd=args.rd)
    scenarios_df = run_scenarios(snap, wacc=wacc_info["WACC"], sector_key=sector_key)
    print_report(snap, wacc_info, scenarios_df)


if __name__ == "__main__":
    main()
