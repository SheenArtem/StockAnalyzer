"""Quick F-Score sanity test for VF-L1a backfilled stocks."""
import os, logging, sys

# Silence noisy loggers
os.environ.setdefault("USE_MOPS", "true")
logging.basicConfig(level=logging.ERROR)
for n in ["FinMind", "cache_manager", "urllib3", "mops_fetcher"]:
    logging.getLogger(n).setLevel(logging.CRITICAL)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from piotroski import calculate_all

STOCKS = ["3685", "3130", "5902", "5383", "5223", "2451", "6278", "3443", "2356", "3702"]

print("[start F-Score sanity test]")
n_ok = n_na = n_err = 0
for sid in STOCKS:
    try:
        r = calculate_all(sid, market_cap=1e10)
        if r and r.get("fscore"):
            fs = r["fscore"]
            f = fs["fscore"]
            comp = fs.get("components", {})
            z_result = r.get("zscore")
            z = z_result["zscore"] if z_result else None
            z_str = f"{z:.2f}" if z is not None else "N/A"
            print(f"  {sid}: F={f}/9 (prof={comp.get('profitability')}/lev={comp.get('leverage')}/eff={comp.get('efficiency')}), Z={z_str}")
            n_ok += 1
        else:
            print(f"  {sid}: N/A (no data)")
            n_na += 1
    except Exception as e:
        print(f"  {sid}: ERR {type(e).__name__}: {str(e)[:100]}")
        n_err += 1

print()
print(f"[done] OK={n_ok}, N/A={n_na}, ERR={n_err}")
