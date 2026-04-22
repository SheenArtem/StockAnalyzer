"""
vf_vf_p3b_extended.py - 擴充 VF-VF 權重 walk-forward
在 bvps bug fix 後 (2026-04-22)，PB/Graham 現在可正常貢獻 valuation_s。
測試是否該提高 valuation 權重；保險起見多探幾個新組合。
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

SCHEMES = {
    'V_live (35/30/18/17/0)':    {'valuation':0.35, 'quality':0.30, 'revenue':0.18, 'technical':0.17, 'smart_money':0.00},
    'V_rev_heavy (30/25/30/15/0) LIVE':  {'valuation':0.30, 'quality':0.25, 'revenue':0.30, 'technical':0.15, 'smart_money':0.00},
    'V_val_40 (40/25/20/15/0)':  {'valuation':0.40, 'quality':0.25, 'revenue':0.20, 'technical':0.15, 'smart_money':0.00},
    'V_val_45 (45/20/20/15/0)':  {'valuation':0.45, 'quality':0.20, 'revenue':0.20, 'technical':0.15, 'smart_money':0.00},
    'V_val_50 (50/20/15/15/0)':  {'valuation':0.50, 'quality':0.20, 'revenue':0.15, 'technical':0.15, 'smart_money':0.00},
    'V_balanced (33/25/25/17/0)':{'valuation':0.33, 'quality':0.25, 'revenue':0.25, 'technical':0.17, 'smart_money':0.00},
    'V_val_rev (40/20/25/15/0)': {'valuation':0.40, 'quality':0.20, 'revenue':0.25, 'technical':0.15, 'smart_money':0.00},
}

def compute(df, w):
    return (df['valuation_s']*w['valuation']
          + df['quality_s']*w['quality']
          + df['revenue_s']*w['revenue']
          + df['technical_s']*w['technical']
          + df['smart_money_s']*w['smart_money'])

def basket_ret(df, col, top_n=50, horizon=60):
    target=f'fwd_{horizon}d'
    out=[]
    for wd,g in df.groupby('week_end_date'):
        sub=g.dropna(subset=[col,target])
        if len(sub)<10: continue
        top=sub.nlargest(top_n, col)
        out.append({'week':wd, 'ret':top[target].mean()})
    return pd.DataFrame(out)

def main():
    snap=pd.read_parquet(ROOT/'data_cache/backtest/trade_journal_value_tw_snapshot.parquet')
    snap['week_end_date']=pd.to_datetime(snap['week_end_date'])
    snap['quarter']=snap['week_end_date'].dt.to_period('Q')
    quarters=sorted(snap['quarter'].unique())
    print(f"Snapshot: {len(snap)} rows, {len(quarters)} quarters ({quarters[0]} ~ {quarters[-1]})")
    print()

    for name, w in SCHEMES.items():
        snap[f'sc_{name}'] = compute(snap, w)

    live_col = 'sc_V_rev_heavy (30/25/30/15/0) LIVE'
    print(f"{'Scheme':<40}{'all_Sh':>10}{'all_IR':>10}{'qSh_mean':>12}{'qWR':>10}{'beats_LIVE':>14}")
    print("-"*96)

    for name, w in SCHEMES.items():
        col=f'sc_{name}'
        b=basket_ret(snap, col).set_index('week')['ret']
        all_sharpe=b.mean()/b.std() if b.std()>0 else np.nan
        all_ic=[]
        for wd,g in snap.groupby('week_end_date'):
            sub=g[[col,'fwd_60d']].dropna()
            if len(sub)<10: continue
            rho,_=stats.spearmanr(sub[col], sub['fwd_60d'])
            if not pd.isna(rho): all_ic.append(rho)
        all_ir = np.array(all_ic).mean()/np.array(all_ic).std() if all_ic else np.nan

        q_sharpes=[]
        q_wins=0
        q_n_valid=0
        beats_live=0
        for q in quarters:
            mask=snap['quarter']==q
            q_snap=snap[mask]
            if len(q_snap)<100: continue
            q_basket=basket_ret(q_snap, col)
            if q_basket.empty: continue
            q_sh = q_basket['ret'].mean()/q_basket['ret'].std() if q_basket['ret'].std()>0 else np.nan
            if pd.notna(q_sh):
                q_sharpes.append(q_sh)
                q_n_valid+=1
                if q_sh>0: q_wins+=1
            q_live_basket = basket_ret(q_snap, live_col)
            if not q_live_basket.empty:
                this_ret = q_basket['ret'].mean()
                live_ret = q_live_basket['ret'].mean()
                if this_ret > live_ret:
                    beats_live+=1

        q_mean_sh = np.mean(q_sharpes) if q_sharpes else np.nan
        q_winrate = q_wins/q_n_valid if q_n_valid else np.nan
        print(f"{name:<40}{all_sharpe:>10.3f}{all_ir:>10.3f}"
              f"{q_mean_sh:>12.3f}{q_winrate:>10.1%}"
              f"{beats_live:>4}/{len(quarters):<10}")

if __name__=='__main__':
    main()
