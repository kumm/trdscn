from backtest import Signal
from chart_db import Candle


class Analyzer:
    def collect_signals(self, candles: list[Candle], last_only: bool = False) -> set[Signal]:
        pass

    def new_algorithm(self):
        pass
