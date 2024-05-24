import numpy
import talib

from analysis import TrendLineExplorer
from analyzer import Analyzer
from backtest import Signal
from chart_db import Candle
from chart_geometry import Geometry, Line, Point
from chart_image import ChartImage
from matcher_bounce import BounceSignal


class BounceAnalyzer(Analyzer):
    def collect_signals(self, candles: list[Candle], last_only: bool = False) -> set[Signal]:
        result = set()
        geometry = Geometry(False)
        trend_line_explorer = TrendLineExplorer(candles, geometry)
        # print(symbol)
        tls_huge = trend_line_explorer.explore(88, 44)
        tls_long = trend_line_explorer.explore(44, 22)
        tls_mid = trend_line_explorer.explore(20, 10)
        tls_short = trend_line_explorer.explore(10, 5)

        atr = talib.ATR(
            high=numpy.array([c.high for c in candles]),
            low=numpy.array([c.low for c in candles]),
            close=numpy.array([c.close for c in candles]),
        )

        for i, c in enumerate(candles):
            ctls_huge = {
                'high': [f for f in filter(lambda l: l.stop.x <= i, tls_huge['high'])],
                'low': [f for f in filter(lambda l: l.stop.x <= i, tls_huge['low'])],
            }
            ctls_long = {
                'high': [f for f in filter(lambda l: l.stop.x <= i, tls_long['high'])],
                'low': [f for f in filter(lambda l: l.stop.x <= i, tls_long['low'])],
            }
            ctls_mid = {
                'high': [f for f in filter(lambda l: l.stop.x <= i, tls_mid['high'])],
                'low': [f for f in filter(lambda l: l.stop.x <= i, tls_mid['low'])],
            }
            ctls_short = {
                'high': [f for f in filter(lambda l: l.stop.x <= i, tls_short['high'])],
                'low': [f for f in filter(lambda l: l.stop.x <= i, tls_short['low'])],
            }
            lines = []
            lines = lines + ctls_huge['high'][-3:] + ctls_huge['low'][-3:]
            lines = lines + ctls_long['high'][-10:] + ctls_long['low'][-10:]
            # lines = lines + ctls_mid['high'][-2:] + ctls_mid['low'][-2:]
            # lines = lines + ctls_short['high'][-2:] + ctls_short['low'][-2:]
            for l in lines:
                y = l.y(i)
                c = [c.close for c in candles[0:i + 1]]
                rising = c[-7] < y - 3 * atr[i]
                falling = c[-7] > y + 3 * atr[i]
                collision_high = rising and c[-2] < c[-1] < y + 0.1 * atr[i] and candles[i].high > y - 0.5 * atr[i]
                collision_low = falling and c[-2] > c[-1] > y - 0.1 * atr[i] and candles[i].low < y + 0.5 * atr[i]
                if collision_high:
                    result.add(BounceSignal(i, l, 1))
                if collision_low:
                    result.add(BounceSignal(i, l, -1))

        return result

    def new_algorithm(self):
        return super().new_algorithm()
