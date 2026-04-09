import pandas as pd
import numpy as np
import sys
import io
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from analysis_engine import TechnicalAnalyzer
from tqdm import tqdm

# Fix Windows cp950 encoding for emoji output
if sys.stdout.encoding and sys.stdout.encoding.lower() in ('cp950', 'cp936', 'cp932'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')


class BacktestEngine:
    def __init__(self, df, initial_capital=100000, fee_rate=0.001425, tax_rate=0.003):
        """
        全邏輯回測引擎 (Full-Logic Backtester)
        :param df: 原始日線資料 (需包含足夠的歷史長度以計算指標)
        :param initial_capital: 初始資金
        :param fee_rate: 手續費率 (買賣各收)
        :param tax_rate: 證交稅率 (賣出時收)
        """
        self.df = df.copy()
        self.initial_capital = initial_capital
        self.fee_rate = fee_rate
        self.tax_rate = tax_rate

        self.position = 0
        self.cash = initial_capital
        self.holdings = 0
        self.trades = []
        self.equity_curve = []

        # Pre-calculated scores
        self.df['Trigger_Score'] = np.nan
        self._precalculate_scores()

    # ---------------------------------------------------------------
    # Static / Utility Methods
    # ---------------------------------------------------------------

    @staticmethod
    def _round_to_tick(price):
        """
        台股升降單位 (Taiwan tick size rounding)
        依照台灣證交所升降單位規則，將價格四捨五入到最近的升降單位。

        Rules:
          price <   10  → tick = 0.01
          10 <= price <   50  → tick = 0.05
          50 <= price <  100  → tick = 0.1
         100 <= price <  500  → tick = 0.5
         500 <= price < 1000  → tick = 1.0
               price >= 1000  → tick = 5.0
        """
        if price < 10:
            tick = 0.01
        elif price < 50:
            tick = 0.05
        elif price < 100:
            tick = 0.1
        elif price < 500:
            tick = 0.5
        elif price < 1000:
            tick = 1.0
        else:
            tick = 5.0
        return round(round(price / tick) * tick, 2)

    # ---------------------------------------------------------------
    # Score Pre-Calculation
    # ---------------------------------------------------------------

    def _precalculate_scores(self):
        """
        預先計算歷史每一天的 AI 分數 (Trigger Score)
        這需要一點時間，但能大幅加速後續的參數最佳化測試。
        """
        # 為避免每次都跑完整回測太久，我們這裡做一個折衷：
        # 建立一個 analyzer 實例，並嘗試模擬「當下」的分數。
        # 由於 technical_analysis.py 已經把大部分指標 (MA, KD, MACD) 算好在 df 裡了，
        # analysis_engine._calculate_trigger_score 主要是在做「條件判斷」與「加權」。
        # 因此，我們可以「向量化」或者「逐日呼叫判斷邏輯」。

        # 為了準確性，我們採用「逐日計算」，但利用已算好的指標。
        # 因為 analysis_engine 需要 df_week，我們這裡暫時 mock 或忽略週線分數 (因為回測主要測進出點 Trigger)
        pass

        # 實際運算
        # 我們需要一個 helper 來只算分數而不生成文字 (加速)
        # 這裡直接簡單模擬 analysis_engine 的邏輯，或者重構 analysis_engine。
        # 為了不改動 analysis_engine 太大，我們在這裡由 BacktestEngine 繼承核心邏輯。

        scores = []
        # Optimization: Only iterate indices where we have enough data
        valid_indices = self.df.index[60:]

        # 為了加速，我們直接在這裡實作「快速版計分」
        # 這樣比呼叫 analyzer.run_analysis 快 100 倍

        closes = self.df['Close'].values
        ma20s = self.df['MA20'].values
        ma60s = self.df.get('MA60', self.df['Close']).values
        ks = self.df.get('K', pd.Series(np.zeros(len(self.df)))).values
        ds = self.df.get('D', pd.Series(np.zeros(len(self.df)))).values
        hists = self.df.get('Hist', pd.Series(np.zeros(len(self.df)))).values
        # patterns... (Too complex to vectorise fully without extraction)

        # Loop for complex logic
        # 限制範圍：只算有訊號的那幾天？不行，每天都要檢查。
        analyzer = TechnicalAnalyzer("TEST", pd.DataFrame(), pd.DataFrame())  # Dummy

        print("⏳ 正在預算歷史 AI 分數 (Pre-calculating AI Scores)...")
        # 直接對 df apply?
        # 為了效能，我們簡化部分「形態學」運算，或只在 run() 時期動態算？
        # 使用 apply 逐行呼叫 _calculate_trigger_score (修改版) 是最準的。

        # 讓我們用一個簡單的 Trick:
        # 將 analyzer 的 logic 封裝成一個接受 row 的函數？不行，它需要 prev (昨天)。

        trigger_scores = []
        # Iterate efficiently
        # Turn dataframe to dict records for speed
        records = self.df.to_dict('records')

        for i in range(len(records)):
            if i < 20:
                trigger_scores.append(0)
                continue

            curr = records[i]
            prev = records[i - 1]
            score = 0

            # --- 1. MA ---
            if curr['Close'] > curr.get('MA20', 0):
                score += 1
            else:
                score -= 1

            # --- 2. BIAS ---
            bias = curr.get('BIAS', 0)
            if 0 < bias < 10:
                score += 1
            elif bias > 10:
                score -= 1
            elif bias < -10:
                score += 1

            # --- 3. MACD ---
            if curr.get('Hist', 0) > 0:
                score += 1
            else:
                score -= 1

            # --- 4. KD ---
            if curr.get('K', 50) > curr.get('D', 50):
                score += 1
            else:
                score -= 1

            # --- 5. OBV ---
            if curr.get('OBV', 0) > prev.get('OBV', 0):
                score += 1

            # --- 6. Patterns (Pre-calc in df usually) ---
            # If 'Pattern_Type' column exists
            pat_type = curr.get('Pattern_Type', None)
            pat_name = curr.get('Pattern', '')

            if pat_type == 'Bullish':
                if 'Engulfing' in pat_name or 'Morning' in pat_name:
                    score += 2
                else:
                    score += 1  # Hammer, etc (+0 in main? Let's give it +1 for backtest power)
            elif pat_type == 'Bearish':
                if 'Engulfing' in pat_name:
                    score -= 2
                else:
                    score -= 1

            trigger_scores.append(score)

        self.df['Trigger_Score'] = trigger_scores

    # ---------------------------------------------------------------
    # Core Backtest Run
    # ---------------------------------------------------------------

    def run(self, buy_threshold=3, sell_threshold=-2, slippage=0.001):
        """
        執行快速回測 (Fast Backtest Execution)
        :param buy_threshold: 分數 > 此值 買進
        :param sell_threshold: 分數 < 此值 賣出
        :param slippage: 滑價比例 (Slippage ratio, default 0.1%)
                         買入價格上調、賣出價格下調，並以台股升降單位取整
        """
        # Reset State
        self.position = 0
        self.cash = self.initial_capital
        self.holdings = 0
        self.trades = []
        self.equity_curve = []

        # 使用 numpy 向量加速運算 (比起 iterrows 快 100 倍)
        # 但為了邏輯清晰與包含複雜的持有狀態，我們維持 Loop，但只 Loop 分數

        dates = self.df.index
        closes = self.df['Close'].values
        scores = self.df['Trigger_Score'].values

        for i in range(len(self.df)):
            date = dates[i]
            price = closes[i]
            score = scores[i]

            if np.isnan(score):
                continue

            action = None

            # Sell Logic
            if self.position > 0:
                if score <= sell_threshold:
                    # Apply slippage: sell at lower price
                    exec_price = self._round_to_tick(price * (1 - slippage))

                    revenue = self.holdings * exec_price
                    fee = revenue * self.fee_rate
                    tax = revenue * self.tax_rate
                    net = revenue - fee - tax
                    self.cash += net

                    last_trade = self.trades[-1]
                    last_trade['exit_date'] = date
                    last_trade['exit_price'] = exec_price
                    last_trade['pnl'] = net - last_trade['cost']
                    last_trade['return'] = (last_trade['pnl'] / last_trade['cost']) * 100

                    self.holdings = 0
                    self.position = 0
                    action = 'SELL'

            # Buy Logic
            elif self.position == 0:
                if score >= buy_threshold:
                    # Apply slippage: buy at higher price
                    exec_price = self._round_to_tick(price * (1 + slippage))

                    max_cost = self.cash / (1 + self.fee_rate)
                    shares = int(max_cost // exec_price)
                    if shares > 0:
                        cost = shares * exec_price
                        fee = cost * self.fee_rate
                        self.cash -= (cost + fee)
                        self.holdings = shares
                        self.position = 1

                        self.trades.append({
                            'entry_date': date,
                            'entry_price': exec_price,
                            'shares': shares,
                            'cost': cost + fee,
                            'exit_date': None,
                            'exit_price': None,
                            'pnl': 0,
                            'return': 0
                        })
                        action = 'BUY'

            # Update Equity
            curr_equity = self.cash + (self.holdings * price)
            self.equity_curve.append({'date': date, 'equity': curr_equity, 'action': action})

        return self._generate_report()

    # ---------------------------------------------------------------
    # Optimization
    # ---------------------------------------------------------------

    def optimize(self):
        """
        網格搜索最佳參數 (Grid Search)
        Returns: best_params, best_result
        """
        best_ret = -999
        best_params = {}
        best_res = {}

        # Search Space
        buy_thresholds = [1, 2, 3, 4, 5]
        sell_thresholds = [-1, -2, -3, -4]

        # Vectorized Simulation Simulation?
        # We can implement run() to be very fast since 'Trigger_Score' is ready.

        for b in buy_thresholds:
            for s in sell_thresholds:
                res = self.run(buy_threshold=b, sell_threshold=s)
                if res['total_return'] > best_ret:
                    best_ret = res['total_return']
                    best_params = {'buy': b, 'sell': s}
                    best_res = res

        return best_params, best_res

    # ---------------------------------------------------------------
    # Walk-Forward Optimization
    # ---------------------------------------------------------------

    def walk_forward_optimize(self, is_window=120, oos_window=60):
        """
        走步前進最佳化 (Walk-Forward Optimization)
        將資料切割成滾動視窗，在樣本內（IS）區間做網格搜索，
        再將最佳參數套用到樣本外（OOS）區間，最後彙整 OOS 績效。

        Walk-forward analysis splits data into rolling windows: optimize on
        in-sample (IS) data, then test on out-of-sample (OOS) data. Only OOS
        results are reported, eliminating look-ahead bias.

        :param is_window: 樣本內天數 (in-sample trading days)
        :param oos_window: 樣本外天數 (out-of-sample trading days)
        :return: dict with consolidated OOS results
        """
        total_len = len(self.df)
        if total_len < is_window + oos_window:
            print(f"⚠️ 資料不足：需要 {is_window + oos_window} 天，實際 {total_len} 天")
            return {
                'total_return': 0,
                'win_rate': 0,
                'max_drawdown': 0,
                'windows': [],
                'oos_equity': pd.DataFrame(),
            }

        buy_thresholds = [1, 2, 3, 4, 5]
        sell_thresholds = [-1, -2, -3, -4]

        windows = []
        oos_equity_segments = []
        cumulative_capital = self.initial_capital
        start = 0

        window_count = 0
        # Count total windows for progress display
        tmp_start = 0
        while tmp_start + is_window + oos_window <= total_len:
            window_count += 1
            tmp_start += oos_window

        print(f"📊 走步前進最佳化 (Walk-Forward): {window_count} 個視窗")
        print(f"   IS={is_window} 天, OOS={oos_window} 天, 資料長度={total_len} 天")

        iteration = 0
        while start + is_window + oos_window <= total_len:
            iteration += 1
            is_start = start
            is_end = start + is_window
            oos_start = is_end
            oos_end = min(is_end + oos_window, total_len)

            # --- In-Sample: grid search ---
            is_df = self.df.iloc[is_start:is_end].copy()
            best_ret_is = -999
            best_buy = 3
            best_sell = -2

            for b in buy_thresholds:
                for s in sell_thresholds:
                    is_result = self._run_on_subset(is_df, b, s, cumulative_capital)
                    if is_result['total_return'] > best_ret_is:
                        best_ret_is = is_result['total_return']
                        best_buy = b
                        best_sell = s

            # --- Out-of-Sample: apply best params ---
            oos_df = self.df.iloc[oos_start:oos_end].copy()
            oos_result = self._run_on_subset(oos_df, best_buy, best_sell, cumulative_capital)

            # Track capital growth
            oos_final_equity = cumulative_capital * (1 + oos_result['total_return'] / 100)

            window_info = {
                'window': iteration,
                'is_start': self.df.index[is_start],
                'is_end': self.df.index[is_end - 1],
                'oos_start': self.df.index[oos_start],
                'oos_end': self.df.index[oos_end - 1],
                'best_buy': best_buy,
                'best_sell': best_sell,
                'is_return': best_ret_is,
                'oos_return': oos_result['total_return'],
                'oos_win_rate': oos_result['win_rate'],
                'oos_max_dd': oos_result['max_drawdown'],
                'oos_trades': len(oos_result['trades']) if isinstance(oos_result['trades'], pd.DataFrame) and not oos_result['trades'].empty else 0,
            }
            windows.append(window_info)

            # Collect OOS equity curve with adjusted capital
            if not oos_result['equity_df'].empty:
                eq_segment = oos_result['equity_df'].copy()
                # Scale equity to reflect cumulative capital
                if len(eq_segment) > 0:
                    scale_factor = cumulative_capital / self.initial_capital
                    eq_segment['equity'] = eq_segment['equity'] * scale_factor
                    eq_segment['window'] = iteration
                    oos_equity_segments.append(eq_segment)

            cumulative_capital = oos_final_equity

            print(f"  視窗 {iteration}/{window_count}: "
                  f"IS 報酬={best_ret_is:.1f}%, "
                  f"OOS 報酬={oos_result['total_return']:.1f}%, "
                  f"最佳參數=Buy>{best_buy} Sell<{best_sell}")

            start += oos_window

        # Consolidate results
        if not windows:
            return {
                'total_return': 0, 'win_rate': 0, 'max_drawdown': 0,
                'windows': [], 'oos_equity': pd.DataFrame(),
            }

        total_return = ((cumulative_capital - self.initial_capital) / self.initial_capital) * 100
        avg_win_rate = np.mean([w['oos_win_rate'] for w in windows])

        # Compute max drawdown from concatenated OOS equity
        if oos_equity_segments:
            oos_equity_df = pd.concat(oos_equity_segments)
            eqs = oos_equity_df['equity']
            dd = (eqs - eqs.cummax()) / eqs.cummax()
            max_dd = dd.min() * 100
        else:
            oos_equity_df = pd.DataFrame()
            max_dd = 0

        print(f"\n✅ Walk-Forward 完成: 總報酬={total_return:.2f}%, "
              f"平均勝率={avg_win_rate:.1f}%, 最大回檔={max_dd:.2f}%")

        return {
            'total_return': total_return,
            'win_rate': avg_win_rate,
            'max_drawdown': max_dd,
            'windows': windows,
            'oos_equity': oos_equity_df,
            'final_capital': cumulative_capital,
        }

    def _run_on_subset(self, subset_df, buy_threshold, sell_threshold, capital):
        """
        在子資料集上執行回測 (Run backtest on a data subset)
        內部輔助方法，用於 walk-forward 分段回測。

        :param subset_df: 資料子集 DataFrame
        :param buy_threshold: 買進閾值
        :param sell_threshold: 賣出閾值
        :param capital: 起始資金
        :return: 回測結果 dict
        """
        saved_state = {
            'position': self.position,
            'cash': self.cash,
            'holdings': self.holdings,
            'trades': self.trades[:],
            'equity_curve': self.equity_curve[:],
            'df': self.df,
            'initial_capital': self.initial_capital,
        }

        try:
            self.df = subset_df
            self.initial_capital = capital
            result = self.run(buy_threshold=buy_threshold, sell_threshold=sell_threshold)
            return result
        finally:
            self.df = saved_state['df']
            self.initial_capital = saved_state['initial_capital']
            self.position = saved_state['position']
            self.cash = saved_state['cash']
            self.holdings = saved_state['holdings']
            self.trades = saved_state['trades']
            self.equity_curve = saved_state['equity_curve']

    # ---------------------------------------------------------------
    # Monte Carlo Simulation
    # ---------------------------------------------------------------

    def monte_carlo(self, result, n_simulations=1000):
        """
        蒙地卡羅模擬 (Monte Carlo Simulation)
        隨機打亂交易順序，模擬不同運氣情境下的績效分佈，
        藉此評估策略的穩健性。

        Randomly shuffle trade sequence to simulate different luck scenarios.
        This helps assess strategy robustness independent of trade ordering.

        :param result: run() 回傳的回測結果 dict
        :param n_simulations: 模擬次數 (default 1000)
        :return: dict with distribution statistics
        """
        trades_df = result.get('trades', pd.DataFrame())
        if trades_df.empty or 'return' not in trades_df.columns:
            return {
                'mean_return': 0, 'median_return': 0,
                'p5_return': 0, 'p95_return': 0,
                'mean_dd': 0, 'p5_dd': 0, 'p95_dd': 0,
                'distributions': {'returns': [], 'drawdowns': []},
            }

        # Extract per-trade return ratios (as decimal fractions)
        trade_returns = trades_df['return'].values / 100.0  # Convert from % to ratio

        n_trades = len(trade_returns)
        if n_trades == 0:
            return {
                'mean_return': 0, 'median_return': 0,
                'p5_return': 0, 'p95_return': 0,
                'mean_dd': 0, 'p5_dd': 0, 'p95_dd': 0,
                'distributions': {'returns': [], 'drawdowns': []},
            }

        rng = np.random.default_rng(42)
        sim_returns = np.zeros(n_simulations)
        sim_drawdowns = np.zeros(n_simulations)
        sim_equity_curves = []

        for sim in range(n_simulations):
            # Shuffle trade returns
            shuffled = rng.permutation(trade_returns)

            # Reconstruct equity curve
            equity = np.ones(n_trades + 1)  # Start at 1.0 (normalized)
            for t in range(n_trades):
                equity[t + 1] = equity[t] * (1 + shuffled[t])

            final_return = (equity[-1] - 1.0) * 100  # As percentage
            sim_returns[sim] = final_return

            # Max drawdown
            cummax = np.maximum.accumulate(equity)
            dd = (equity - cummax) / cummax
            sim_drawdowns[sim] = dd.min() * 100  # As percentage (negative)

            sim_equity_curves.append(equity)

        return {
            'mean_return': float(np.mean(sim_returns)),
            'median_return': float(np.median(sim_returns)),
            'p5_return': float(np.percentile(sim_returns, 5)),
            'p95_return': float(np.percentile(sim_returns, 95)),
            'mean_dd': float(np.mean(sim_drawdowns)),
            'p5_dd': float(np.percentile(sim_drawdowns, 5)),
            'p95_dd': float(np.percentile(sim_drawdowns, 95)),
            'distributions': {
                'returns': sim_returns.tolist(),
                'drawdowns': sim_drawdowns.tolist(),
            },
            'n_simulations': n_simulations,
            'n_trades': n_trades,
        }

    # ---------------------------------------------------------------
    # Portfolio Risk Metrics
    # ---------------------------------------------------------------

    def _calculate_risk_metrics(self, equity_series):
        """
        投資組合風險指標 (Portfolio Risk Metrics)
        計算各種風險調整後績效指標，包括 Sharpe / Sortino / Calmar Ratio、
        獲利因子、最大回檔持續天數、平均持有天數與連續勝/敗次數。

        Compute risk-adjusted performance metrics from the equity curve and
        trade history.

        :param equity_series: pd.Series of equity values indexed by date
        :return: dict of risk metrics
        """
        rf = 0.015  # Taiwan 10-year bond rate
        trading_days = 252

        # Daily returns
        daily_returns = equity_series.pct_change().dropna()

        if len(daily_returns) < 2:
            return self._empty_risk_metrics()

        # Annualized return & std
        total_days = len(daily_returns)
        total_return_ratio = equity_series.iloc[-1] / equity_series.iloc[0] - 1
        annualized_return = (1 + total_return_ratio) ** (trading_days / total_days) - 1
        annualized_std = daily_returns.std() * np.sqrt(trading_days)

        # --- Sharpe Ratio ---
        sharpe = (annualized_return - rf) / annualized_std if annualized_std > 0 else 0

        # --- Sortino Ratio ---
        downside_returns = daily_returns[daily_returns < 0]
        downside_std = downside_returns.std() * np.sqrt(trading_days) if len(downside_returns) > 0 else 0
        sortino = (annualized_return - rf) / downside_std if downside_std > 0 else 0

        # --- Max Drawdown ---
        cummax = equity_series.cummax()
        drawdown = (equity_series - cummax) / cummax
        max_dd = drawdown.min()

        # --- Calmar Ratio ---
        calmar = annualized_return / abs(max_dd) if abs(max_dd) > 0 else 0

        # --- Profit Factor ---
        gross_profits = 0
        gross_losses = 0
        if self.trades:
            for t in self.trades:
                if t.get('pnl', 0) > 0:
                    gross_profits += t['pnl']
                elif t.get('pnl', 0) < 0:
                    gross_losses += abs(t['pnl'])
        profit_factor = gross_profits / gross_losses if gross_losses > 0 else float('inf') if gross_profits > 0 else 0

        # --- Max Drawdown Duration (days from peak to recovery) ---
        max_dd_duration = 0
        current_dd_duration = 0
        peak = equity_series.iloc[0]
        for val in equity_series.values:
            if val >= peak:
                peak = val
                current_dd_duration = 0
            else:
                current_dd_duration += 1
                max_dd_duration = max(max_dd_duration, current_dd_duration)

        # --- Average holding period ---
        holding_days = []
        for t in self.trades:
            if t.get('entry_date') is not None and t.get('exit_date') is not None:
                try:
                    entry = pd.Timestamp(t['entry_date'])
                    exit_ = pd.Timestamp(t['exit_date'])
                    holding_days.append((exit_ - entry).days)
                except Exception:
                    pass
        avg_holding = np.mean(holding_days) if holding_days else 0

        # --- Consecutive wins / losses streak ---
        max_consec_wins = 0
        max_consec_losses = 0
        current_wins = 0
        current_losses = 0
        for t in self.trades:
            if t.get('exit_date') is None:
                continue  # Skip open trades
            if t.get('pnl', 0) > 0:
                current_wins += 1
                current_losses = 0
                max_consec_wins = max(max_consec_wins, current_wins)
            elif t.get('pnl', 0) < 0:
                current_losses += 1
                current_wins = 0
                max_consec_losses = max(max_consec_losses, current_losses)
            else:
                current_wins = 0
                current_losses = 0

        return {
            'sharpe_ratio': round(sharpe, 3),
            'sortino_ratio': round(sortino, 3),
            'calmar_ratio': round(calmar, 3),
            'profit_factor': round(profit_factor, 3),
            'max_dd_duration_days': max_dd_duration,
            'avg_holding_days': round(avg_holding, 1),
            'max_consecutive_wins': max_consec_wins,
            'max_consecutive_losses': max_consec_losses,
            'annualized_return': round(annualized_return * 100, 2),
            'annualized_std': round(annualized_std * 100, 2),
        }

    @staticmethod
    def _empty_risk_metrics():
        """
        空白風險指標 (Empty risk metrics placeholder)
        """
        return {
            'sharpe_ratio': 0,
            'sortino_ratio': 0,
            'calmar_ratio': 0,
            'profit_factor': 0,
            'max_dd_duration_days': 0,
            'avg_holding_days': 0,
            'max_consecutive_wins': 0,
            'max_consecutive_losses': 0,
            'annualized_return': 0,
            'annualized_std': 0,
        }

    # ---------------------------------------------------------------
    # Report Generation
    # ---------------------------------------------------------------

    def _generate_report(self):
        """
        生成回測績效報告 (Generate Backtest Performance Report)
        包含基本績效、基準比較、風險指標、增強交易分析與月報酬序列。

        Generates a comprehensive report dict including basic performance,
        benchmark comparison, risk metrics, enhanced trade analysis, and
        monthly return series.
        """
        if not self.trades:
            empty_eq = pd.DataFrame(self.equity_curve).set_index('date') if self.equity_curve else pd.DataFrame()
            return {
                "total_return": 0, "win_rate": 0, "max_drawdown": 0,
                "trades": pd.DataFrame(),
                "equity_df": empty_eq,
                "holding": False,
                "benchmark_return": 0,
                "alpha": 0,
                "risk_metrics": self._empty_risk_metrics(),
                "avg_win": 0, "avg_loss": 0,
                "largest_win": 0, "largest_loss": 0,
                "monthly_returns": pd.Series(dtype=float),
            }

        df_trades = pd.DataFrame(self.trades)
        done_trades = df_trades[df_trades['exit_date'].notna()]

        win_rate = 0
        if not done_trades.empty:
            win_rate = (len(done_trades[done_trades['pnl'] > 0]) / len(done_trades)) * 100

        final_eq = self.equity_curve[-1]['equity']
        ret = ((final_eq - self.initial_capital) / self.initial_capital) * 100

        # Max DD
        eqs_df = pd.DataFrame(self.equity_curve).set_index('date')
        eqs = eqs_df['equity']
        dd = (eqs - eqs.cummax()) / eqs.cummax()
        max_dd = dd.min() * 100

        # ----- Benchmark: buy-and-hold -----
        benchmark_return = 0
        alpha = 0
        try:
            if not eqs_df.empty and 'Close' in self.df.columns:
                first_date = eqs_df.index[0]
                last_date = eqs_df.index[-1]
                # Find closest dates in df
                mask = (self.df.index >= first_date) & (self.df.index <= last_date)
                bm_df = self.df.loc[mask]
                if len(bm_df) >= 2:
                    benchmark_return = ((bm_df['Close'].iloc[-1] / bm_df['Close'].iloc[0]) - 1) * 100
                    alpha = ret - benchmark_return
        except Exception:
            pass

        # ----- Risk Metrics -----
        risk_metrics = self._calculate_risk_metrics(eqs)

        # ----- Enhanced Trade Analysis -----
        avg_win = 0
        avg_loss = 0
        largest_win = 0
        largest_loss = 0

        if not done_trades.empty:
            winners = done_trades[done_trades['pnl'] > 0]
            losers = done_trades[done_trades['pnl'] < 0]

            avg_win = winners['pnl'].mean() if not winners.empty else 0
            avg_loss = losers['pnl'].mean() if not losers.empty else 0
            largest_win = done_trades['pnl'].max()
            largest_loss = done_trades['pnl'].min()

        # ----- Monthly Returns -----
        monthly_returns = pd.Series(dtype=float)
        try:
            if not eqs_df.empty and len(eqs) >= 2:
                # Resample equity to month-end, compute monthly return
                monthly_eq = eqs.resample('ME').last().dropna()
                if len(monthly_eq) >= 2:
                    monthly_returns = monthly_eq.pct_change().dropna() * 100
                    monthly_returns.index = monthly_returns.index.strftime('%Y-%m')
        except Exception:
            pass

        return {
            "total_return": ret,
            "win_rate": win_rate,
            "max_drawdown": max_dd,
            "trades": done_trades,
            "equity_df": eqs_df,
            "holding": self.position == 1,
            # Benchmark
            "benchmark_return": round(benchmark_return, 2),
            "alpha": round(alpha, 2),
            # Risk
            "risk_metrics": risk_metrics,
            # Enhanced Trade Analysis
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "largest_win": round(largest_win, 2),
            "largest_loss": round(largest_loss, 2),
            # Monthly Returns (for heatmap in UI)
            "monthly_returns": monthly_returns,
        }

    # ---------------------------------------------------------------
    # Plotting — Main Equity Curve + Benchmark
    # ---------------------------------------------------------------

    def plot_results(self, result):
        """
        繪製回測績效圖 (Plot Backtest Performance)
        包含策略淨值曲線、買賣標記，以及大盤買入持有基準線。

        Plots the strategy equity curve with buy/sell markers and
        a gray dashed benchmark (buy-and-hold) line for comparison.
        """
        df_eq = result['equity_df']
        if df_eq.empty:
            return go.Figure()

        fig = go.Figure()

        # Strategy equity curve
        fig.add_trace(go.Scatter(
            x=df_eq.index, y=df_eq['equity'],
            mode='lines', name='AI 策略淨值',
            line=dict(color='#00E396', width=2),
        ))

        # Benchmark buy-and-hold line
        try:
            if 'Close' in self.df.columns and not df_eq.empty:
                first_date = df_eq.index[0]
                last_date = df_eq.index[-1]
                mask = (self.df.index >= first_date) & (self.df.index <= last_date)
                bm_df = self.df.loc[mask]
                if len(bm_df) >= 2:
                    starting_capital = df_eq['equity'].iloc[0]
                    normalized_bm = (bm_df['Close'] / bm_df['Close'].iloc[0]) * starting_capital
                    fig.add_trace(go.Scatter(
                        x=normalized_bm.index, y=normalized_bm.values,
                        mode='lines', name='大盤買入持有',
                        line=dict(color='gray', width=1.5, dash='dash'),
                    ))
        except Exception:
            pass

        # Buy/Sell markers
        buys = df_eq[df_eq['action'] == 'BUY']
        sells = df_eq[df_eq['action'] == 'SELL']

        if not buys.empty:
            fig.add_trace(go.Scatter(
                x=buys.index, y=buys['equity'],
                mode='markers', name='Buy',
                marker=dict(symbol='triangle-up', size=10, color='red'),
            ))
        if not sells.empty:
            fig.add_trace(go.Scatter(
                x=sells.index, y=sells['equity'],
                mode='markers', name='Sell',
                marker=dict(symbol='triangle-down', size=10, color='green'),
            ))

        fig.update_layout(
            title='AI 策略回測績效 (3年)',
            xaxis_title='Date', yaxis_title='Equity',
            template='plotly_dark',
            height=400,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        return fig

    # ---------------------------------------------------------------
    # Plotting — Walk-Forward
    # ---------------------------------------------------------------

    def plot_walk_forward(self, wf_result):
        """
        繪製走步前進最佳化績效圖 (Plot Walk-Forward OOS Equity Curve)
        將各 OOS 區段淨值串接成完整曲線，並以垂直虛線標示區段邊界。

        Concatenates OOS equity segments into a single curve with vertical
        dashed lines marking segment boundaries.

        :param wf_result: walk_forward_optimize() 的回傳 dict
        :return: Plotly Figure
        """
        oos_equity = wf_result.get('oos_equity', pd.DataFrame())
        windows = wf_result.get('windows', [])

        fig = go.Figure()

        if oos_equity.empty:
            fig.update_layout(
                title='Walk-Forward: 無 OOS 資料',
                template='plotly_dark', height=400,
            )
            return fig

        # Plot OOS equity curve
        fig.add_trace(go.Scatter(
            x=oos_equity.index, y=oos_equity['equity'],
            mode='lines', name='OOS 淨值',
            line=dict(color='#FF9800', width=2),
        ))

        # Add vertical dashed lines at segment boundaries
        added_boundaries = set()
        for w in windows:
            oos_start = w['oos_start']
            if oos_start not in added_boundaries:
                fig.add_vline(
                    x=oos_start, line_dash='dash',
                    line_color='rgba(255,255,255,0.3)', line_width=1,
                )
                added_boundaries.add(oos_start)

        # Annotations for window parameters
        for w in windows:
            fig.add_annotation(
                x=w['oos_start'],
                y=oos_equity['equity'].max() * 1.02,
                text=f"W{w['window']} B>{w['best_buy']} S<{w['best_sell']}",
                showarrow=False,
                font=dict(size=8, color='rgba(255,255,255,0.5)'),
                yshift=10,
            )

        fig.update_layout(
            title=f"Walk-Forward OOS 績效 (總報酬: {wf_result['total_return']:.2f}%)",
            xaxis_title='Date', yaxis_title='Equity',
            template='plotly_dark',
            height=400,
            margin=dict(l=20, r=20, t=50, b=20),
        )
        return fig

    # ---------------------------------------------------------------
    # Plotting — Monte Carlo
    # ---------------------------------------------------------------

    def plot_monte_carlo(self, mc_result):
        """
        繪製蒙地卡羅模擬分佈圖 (Plot Monte Carlo Simulation Distribution)
        直方圖顯示最終報酬分佈，並標示第 5 與第 95 百分位線。

        Histogram of simulated final returns with 5th/95th percentile lines.

        :param mc_result: monte_carlo() 的回傳 dict
        :return: Plotly Figure
        """
        returns = mc_result.get('distributions', {}).get('returns', [])

        fig = go.Figure()

        if not returns:
            fig.update_layout(
                title='Monte Carlo: 無模擬資料',
                template='plotly_dark', height=400,
            )
            return fig

        returns_arr = np.array(returns)

        # Histogram
        fig.add_trace(go.Histogram(
            x=returns_arr,
            nbinsx=50,
            name='模擬報酬分佈',
            marker_color='#26A69A',
            opacity=0.75,
        ))

        # Percentile lines
        p5 = mc_result.get('p5_return', 0)
        p95 = mc_result.get('p95_return', 0)
        mean_ret = mc_result.get('mean_return', 0)
        median_ret = mc_result.get('median_return', 0)

        fig.add_vline(x=p5, line_dash='dash', line_color='#EF5350', line_width=2,
                      annotation_text=f'P5: {p5:.1f}%', annotation_position='top left',
                      annotation_font_color='#EF5350')
        fig.add_vline(x=p95, line_dash='dash', line_color='#66BB6A', line_width=2,
                      annotation_text=f'P95: {p95:.1f}%', annotation_position='top right',
                      annotation_font_color='#66BB6A')
        fig.add_vline(x=mean_ret, line_dash='solid', line_color='#FFA726', line_width=2,
                      annotation_text=f'Mean: {mean_ret:.1f}%', annotation_position='top',
                      annotation_font_color='#FFA726')

        n_sim = mc_result.get('n_simulations', 0)
        n_trades = mc_result.get('n_trades', 0)

        fig.update_layout(
            title=f'Monte Carlo 模擬 ({n_sim} 次, {n_trades} 筆交易)',
            xaxis_title='最終報酬 (%)',
            yaxis_title='次數',
            template='plotly_dark',
            height=400,
            margin=dict(l=20, r=20, t=50, b=20),
            bargap=0.05,
        )
        return fig

    # ---------------------------------------------------------------
    # Pyramiding Backtest (分批進場回測)
    # ---------------------------------------------------------------

    def run_pyramid(self, buy_threshold=3, sell_threshold=-2, slippage=0.001,
                    max_positions=3, position_pct=None):
        """
        金字塔式分批進場回測 (Pyramid Position Backtest)
        支援分批建倉：信號持續正向時逐步加碼，最多 max_positions 批。

        :param buy_threshold: 買進分數閾值
        :param sell_threshold: 賣出分數閾值 (全部出場)
        :param slippage: 滑價比例
        :param max_positions: 最大分批次數 (預設 3 批)
        :param position_pct: 每批資金比例 list，預設均分
        :return: 回測結果 dict
        """
        if position_pct is None:
            position_pct = [1.0 / max_positions] * max_positions

        # Reset State
        cash = self.initial_capital
        entries = []  # List of {'date', 'price', 'shares', 'cost'}
        total_shares = 0
        trades_completed = []
        equity_curve = []

        dates = self.df.index
        closes = self.df['Close'].values
        scores = self.df['Trigger_Score'].values

        for i in range(len(self.df)):
            date = dates[i]
            price = closes[i]
            score = scores[i]

            if np.isnan(score):
                continue

            action = None
            n_entries = len(entries)

            # === SELL: 全部出場 ===
            if n_entries > 0 and score <= sell_threshold:
                exec_price = self._round_to_tick(price * (1 - slippage))
                revenue = total_shares * exec_price
                fee = revenue * self.fee_rate
                tax = revenue * self.tax_rate
                net = revenue - fee - tax

                total_cost = sum(e['cost'] for e in entries)
                avg_entry = sum(e['price'] * e['shares'] for e in entries) / total_shares

                trades_completed.append({
                    'entry_date': entries[0]['date'],
                    'exit_date': date,
                    'entry_price': avg_entry,
                    'exit_price': exec_price,
                    'shares': total_shares,
                    'batches': n_entries,
                    'cost': total_cost,
                    'pnl': net - total_cost,
                    'return': ((net - total_cost) / total_cost) * 100
                })

                cash += net
                entries = []
                total_shares = 0
                action = 'SELL'

            # === BUY: 分批加碼 ===
            elif n_entries < max_positions and score >= buy_threshold:
                # 計算本批可用資金
                batch_idx = n_entries
                if batch_idx < len(position_pct):
                    alloc_pct = position_pct[batch_idx]
                else:
                    alloc_pct = position_pct[-1]

                alloc_cash = self.initial_capital * alloc_pct
                available = min(alloc_cash, cash)

                exec_price = self._round_to_tick(price * (1 + slippage))
                max_buy = available / (1 + self.fee_rate)
                shares = int(max_buy // exec_price)

                if shares > 0:
                    cost = shares * exec_price
                    fee_paid = cost * self.fee_rate
                    cash -= (cost + fee_paid)
                    total_shares += shares

                    entries.append({
                        'date': date,
                        'price': exec_price,
                        'shares': shares,
                        'cost': cost + fee_paid
                    })
                    action = f'BUY_{batch_idx + 1}'

            # Equity
            curr_equity = cash + (total_shares * price)
            equity_curve.append({'date': date, 'equity': curr_equity, 'action': action})

        # Close open position at last price
        if entries and total_shares > 0:
            last_price = closes[-1]
            total_cost = sum(e['cost'] for e in entries)
            avg_entry = sum(e['price'] * e['shares'] for e in entries) / total_shares
            revenue = total_shares * last_price
            fee = revenue * self.fee_rate
            tax = revenue * self.tax_rate
            net = revenue - fee - tax
            trades_completed.append({
                'entry_date': entries[0]['date'],
                'exit_date': dates[-1],
                'entry_price': avg_entry,
                'exit_price': last_price,
                'shares': total_shares,
                'batches': len(entries),
                'cost': total_cost,
                'pnl': net - total_cost,
                'return': ((net - total_cost) / total_cost) * 100
            })

        # Generate report
        df_trades = pd.DataFrame(trades_completed)
        eqs = pd.DataFrame(equity_curve).set_index('date')

        final_eq = equity_curve[-1]['equity'] if equity_curve else self.initial_capital
        total_return = ((final_eq - self.initial_capital) / self.initial_capital) * 100

        win_rate = 0
        avg_batches = 0
        if not df_trades.empty:
            win_rate = (len(df_trades[df_trades['pnl'] > 0]) / len(df_trades)) * 100
            avg_batches = df_trades['batches'].mean()

        max_dd = 0
        if not eqs.empty:
            eq_series = eqs['equity']
            dd = (eq_series - eq_series.cummax()) / eq_series.cummax()
            max_dd = dd.min() * 100

        return {
            'total_return': total_return,
            'win_rate': win_rate,
            'max_drawdown': max_dd,
            'trades': df_trades,
            'equity_df': eqs,
            'avg_batches': avg_batches,
            'max_positions': max_positions,
            'holding': len(entries) > 0
        }

    def plot_pyramid_results(self, result):
        """
        繪製金字塔回測績效圖 (Pyramid Backtest Plot)
        """
        df_eq = result['equity_df']
        if df_eq.empty:
            return go.Figure()

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_eq.index, y=df_eq['equity'],
            mode='lines', name='Equity',
            line=dict(color='#00E396', width=2)
        ))

        # Mark buy batches with different colors
        colors_buy = ['#FF4444', '#FF8800', '#FFDD00']
        for batch_num in range(1, result.get('max_positions', 3) + 1):
            batch_key = f'BUY_{batch_num}'
            batch_pts = df_eq[df_eq['action'] == batch_key]
            if not batch_pts.empty:
                color = colors_buy[batch_num - 1] if batch_num <= len(colors_buy) else '#FFFFFF'
                fig.add_trace(go.Scatter(
                    x=batch_pts.index, y=batch_pts['equity'],
                    mode='markers', name=f'Buy #{batch_num}',
                    marker=dict(symbol='triangle-up', size=10, color=color)
                ))

        sells = df_eq[df_eq['action'] == 'SELL']
        if not sells.empty:
            fig.add_trace(go.Scatter(
                x=sells.index, y=sells['equity'],
                mode='markers', name='Sell All',
                marker=dict(symbol='triangle-down', size=12, color='#00FF00')
            ))

        avg_b = result.get('avg_batches', 0)
        fig.update_layout(
            title=f'Pyramid Backtest (avg {avg_b:.1f} batches/trade)',
            xaxis_title='Date', yaxis_title='Equity',
            template='plotly_dark', height=400,
            margin=dict(l=20, r=20, t=50, b=20)
        )
        return fig
