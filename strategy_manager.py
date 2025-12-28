import json
import os

class StrategyManager:
    def __init__(self, config_file='strategy_config.json'):
        self.config_file = config_file
        self.strategies = self._load_from_file()

    def _load_from_file(self):
        if not os.path.exists(self.config_file):
            return {}
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

    def save_strategy(self, ticker, buy_threshold, sell_threshold):
        """
        儲存特定股票的策略參數
        """
        # 正規化 ticker (去除非數字若需要，這裡假設 ticker 是穩定的 id)
        self.strategies[ticker] = {
            'buy_threshold': int(buy_threshold),
            'sell_threshold': int(sell_threshold)
        }
        self._save_to_file()

    def load_strategy(self, ticker):
        """
        讀取特定股票的策略參數
        Returns: dict {'buy_threshold': val, 'sell_threshold': val} or None
        """
        return self.strategies.get(ticker)

    def _save_to_file(self):
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.strategies, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving strategy config: {e}")
