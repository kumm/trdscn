from typing import Optional

from talipp.indicators import ATR
from talipp.ohlcv import OHLCV

from analysis_i import Matcher
from backtest import Signal
from chart_db import Candle
from chart_geometry import Point, Line, Geometry
from chart_image import ChartImage


class RocSignal(Signal):
    base: Point
    dip: Point
    trigger: Point
    atr: float
    geometry = Geometry(False)

    def __init__(self, base, dip, trigger, atr):
        self.base = base
        self.dip = dip
        self.trigger = trigger
        self.atr = atr

    def get_completion_bar_index(self) -> int:
        return self.trigger.x

    def get_begin_bar_index(self) -> int:
        return self.base.x

    def show(self, chart_image: ChartImage):
        chart_image.add_line(
            line = Line(start=self.base, stop=self.dip, geometry=self.geometry),
            color="#0000FF",
            dash='solid'
        )
        chart_image.add_line(
            line = Line(start=self.dip, stop=self.trigger, geometry=self.geometry),
            color="#0000FF",
            dash='solid'
        )


class RocMatcher(Matcher):
    candles: list[Candle]
    roc: list[float]
    atr: ATR
    len = 14
    rate_threshold = 0.2
    atr_len = 10

    def __init__(self):
        self.candles = []
        self.roc = []
        self.atr = ATR(self.atr_len)

    def match(self, candle: Candle, bar_index: int) -> Optional[RocSignal]:
        self.candles.insert(0, candle)
        self.atr.add(OHLCV(open=candle.open, close=candle.close, high=candle.high, low=candle.low))
        if len(self.candles) < self.len + 2:
            return None
        self.candles = self.candles[0:self.len + 2]
        self.roc.insert(0, self.candles[0].close / self.candles[self.len].close - 1)
        if len(self.roc) < 2:
            return None
        self.roc = self.roc[0:2]

        if self.roc[1] < -self.rate_threshold and self.roc[0] > self.roc[1]:
            return RocSignal(
                base=Point(x=bar_index - self.len - 1, y=self.candles[self.len + 1].close),
                dip=Point(x=bar_index - 1, y=self.candles[1].close),
                trigger=Point(x=bar_index, y=self.candles[0].close),
                atr=self.atr[-1]
            )
        else:
            return None
