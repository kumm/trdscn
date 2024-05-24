from typing import Optional

from talipp.indicators import ATR
from talipp.ohlcv import OHLCV

from analysis_i import Matcher, LineCollector, TimeBasedPivotDetector
from backtest import Signal
from chart_db import Candle
from chart_geometry import Line, Point
from chart_image import ChartImage


class BounceSignal(Signal):
    x: int
    trend_line: Line
    direction: int

    def __init__(self, x, trend_line, direction):
        self.x = x
        self.trend_line = trend_line
        self.direction = direction

    def get_completion_bar_index(self) -> int:
        return self.x

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(frozenset(self.__dict__.items()))

    def show(self, chart_image: ChartImage):
        chart_image.add_line(
            Line(self.trend_line.start, Point(self.x, self.trend_line.y(self.x)), self.trend_line.geometry),
            "solid", "#0e0e0e", 1
        )
        chart_image.add_enter_trade(self.x, self.direction == -1)


class BounceMatcher(Matcher):
    collectors: list[tuple[LineCollector, LineCollector, TimeBasedPivotDetector]]
    candles: list[Candle]
    atr: ATR

    def __init__(self):
        self.collectors = [
            (LineCollector(), LineCollector(), TimeBasedPivotDetector(before=88, after=44, use_bodies=True)),
            (LineCollector(), LineCollector(), TimeBasedPivotDetector(before=44, after=22, use_bodies=True)),
        ]
        self.candles = []
        self.atr = ATR(14)

    def match(self, candle: Candle, bar_index: int) -> Optional[BounceSignal]:
        for hlc, llc, pd in self.collectors:
            pp = pd.detect(candle, bar_index)
            if pp is not None:
                pnt, pos = pp
                if pos == 1:
                    hlc.add(pnt)
                if pos == -1:
                    llc.add(pnt)

        self.candles.append(candle)
        self.atr.add(OHLCV(open=candle.open, close=candle.close, high=candle.high, low=candle.low))
        atr = self.atr[-1]
        lines = []
        lines = lines + self.collectors[0][0].trend_lines[-3:]
        lines = lines + self.collectors[0][1].trend_lines[-3:]
        lines = lines + self.collectors[1][0].trend_lines[-10:]
        lines = lines + self.collectors[1][1].trend_lines[-10:]
        for l in lines:
            y = l.y(bar_index)
            c = [c.close for c in self.candles]
            rising = c[-20] < y - 10 * atr
            falling = c[-20] > y + 10 * atr
            collision_high = rising and c[-2] < c[-1] < y + 0.1 * atr and candle.high > y - 0.5 * atr
            collision_low = falling and c[-2] > c[-1] > y - 0.1 * atr and candle.low < y + 0.5 * atr
            if collision_high:
                return BounceSignal(bar_index, l, 1)
            if collision_low:
                return BounceSignal(bar_index, l, -1)

        return None
