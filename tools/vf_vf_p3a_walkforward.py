"""
vf_vf_p3a_walkforward.py - VF-VC P3-a 權重 quarterly walk-forward

對關鍵方案做季度滾動穩定性檢驗 (24 slices, 2020Q1 - 2025Q4):
  V_live:      35/30/18/17/0 (當前 live)
  V_rev_heavy: 30/25/30/15/0 (新 revenue 修完後最佳 basket ret)
  V3_val_heavy: 50/20/10/10/10 (最佳 Sharpe, 50% 集中 val)
  V_no_rev:    43/36/0/21/0  (對照: 0 revenue)

判定: 贏 V_live 的季次 >= 60%, 且 Sharpe >= V_live 者為優
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
    'V_live':       {'valuation':0.35, 'quality':0.30, 'revenue':0.18, 'technical':0.17, 'smart_money':0.00},
    'V_rev_heavy':  {'valuation':0.30, 'quality':0.25, 'revenue':0.30, 'technical':0.15, 'smart_money':0.00},
    'V3_val_heavy': {'valuation':0.50, 'quality':0.20, 'revenue':0.10, 'technical':0.10, 'smart_money':0.10},
    'V_no_rev':     {'valuation':0.43, 'quality':0.36, 'revenue':0.00, 'technical':0.21, 'smart_money':0.00},
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
    print(f"Quarters: {len(quarters)}, {quarters[0]} ~ {quarters[-1]}")
    print()

    for name, w in SCHEMES.items():
        snap[f'sc_{name}'] = compute(snap, w)

    # Per-quarter Sharpe + IC
    print(f"{'Scheme':<15}{'all_Sharpe':>12}{'all_IR':>10}{'qWF_mean_Sh':>14}{'qWF_winrate':>14}{'qWF_wins_V_live':>18}")
    print("-"*100)

    # Compute baseline V_live basket per week
    v_live_basket = basket_ret(snap, 'sc_V_live').set_index('week')['ret']

    for name, w in SCHEMES.items():
        col=f'sc_{name}'
        b=basket_ret(snap, col).set_index('week')['ret']

        # All-period metrics
        all_sharpe=b.mean()/b.std() if b.std()>0 else np.nan
        all_ic=[]
        for wd,g in snap.groupby('week_end_date'):
            sub=g[[col,'fwd_60d']].dropna()
            if len(sub)<10: continue
            rho,_=stats.spearmanr(sub[col], sub['fwd_60d'])
            if not pd.isna(rho): all_ic.append(rho)
        all_ir = np.array(all_ic).mean()/np.array(all_ic).std() if all_ic else np.nan

        # Quarterly walk-forward
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
            # vs V_live
            q_live_basket = basket_ret(q_snap, 'sc_V_live')
            if not q_live_basket.empty:
                this_ret = q_basket['ret'].mean()
                live_ret = q_live_basket['ret'].mean()
                if this_ret > live_ret:
                    beats_live+=1

        q_mean_sh = np.mean(q_sharpes) if q_sharpes else np.nan
        q_winrate = q_wins/q_n_valid if q_n_valid else np.nan
        print(f"{name:<15}{all_sharpe:>12.3f}{all_ir:>10.3f}"
              f"{q_mean_sh:>14.3f}{q_winrate:>13.1%}"
              f"{beats_live}/{len(quarters):>15}")

    print()
    print("判讀:")
    print("  all_Sharpe >= V_live 且 qWF_winrate >= 60% 且 beats_live >= 14/24 (58%) = 可 P3-b 落地")
    print("  qWF_winrate < 55% 或 beats_live < 12/24 = 方案穩定性不足")

if __name__=='__main__':
    main()
