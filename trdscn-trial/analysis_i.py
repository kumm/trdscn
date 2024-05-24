from typing import Optional

from talipp.indicators import SMA

from backtest import Signal
from chart_db import Candle
from chart_geometry import Point, Line, Geometry


class Matcher:
    def match(self, candle: Candle, bar_index: int) -> Optional[Signal]:
        pass

    def scan(self, candles: list[Candle], limit: int = 0):
        signals = set()
        for i, c in enumerate(candles):
            signal = self.match(c, i)
            if (limit == 0 or i > len(candles) - limit - 1) and signal is not None and signal not in signals:
                signals.add(signal)
        return signals


class LineCollector:
    last: Optional[Point]
    lines: list[Line]
    geometry: Geometry

    def __init__(self):
        self.last = None
        self.geometry = Geometry(False)
        self.trend_lines = []

    def add(self, point: Point):
        if self.last is not None:
            line = Line(start=self.last, stop=point, geometry=self.geometry)
            self.trend_lines.append(line)
        self.last = point


class TimeBasedPivotDetector:
    before: int
    after: int
    use_bodies: bool
    candles: list[Candle]

    def __init__(self, before: int, after: int, use_bodies: bool = True):
        self.before = before
        self.after = after
        self.use_bodies = use_bodies
        self.candles = []

    def detect(self, candle: Candle, bar_ndx: int):
        self.candles.append(candle)
        if len(self.candles) <= self.before + self.after:
            return None

        a = self.candles[self.before]
        before = self.candles[0:self.before]
        after = self.candles[-self.after:]
        u_b = self.use_bodies
        high = a.max(u_b)
        low = a.min(u_b)
        x = bar_ndx - self.after
        self.candles.pop(0)
        if max([c.max(u_b) for c in before]) < high > max([c.max(u_b) for c in after]):
            return Point(x=x, y=high), 1
        if min([c.min(u_b) for c in before]) > low < min([c.min(u_b) for c in after]):
            return Point(x=x, y=low), -1


class MoveBasedPivotDetector:
    factor: float
    use_bodies: bool
    count: int
    flag: str
    highest_high: float
    lowest_low: float
    last_candle: Optional[Candle]
    move_sma: SMA

    def __init__(self, factor: float, move_sma_len: int = 90, use_bodies: bool = True):
        self.use_bodies = use_bodies
        self.factor = factor
        self.count = 0
        self.flag = "first"
        self.highest_high = 0
        self.lowest_low = 0
        self.last_candle = None
        self.move_sma = SMA(move_sma_len)

    def __calc_move(self, c: Candle):
        last = self.last_candle if self.last_candle is not None else c
        tr = max(c.high - c.low, abs(c.high - last.close), abs(c.low - last.close))
        hl2 = (c.high + c.low) / 2
        self.move_sma.add(tr / hl2)
        self.last_candle = c
        return self.move_sma[-1]

    def detect(self, c: Candle, bar_ndx: int):
        high = c.max(self.use_bodies)
        low = c.min(self.use_bodies)
        self.count += 1
        x = bar_ndx - self.count
        if low == 0 or high == 0:
            return None
        cm = self.__calc_move(c)
        if cm is None:
            return None
        move = cm * self.factor
        if self.highest_high == 0 and self.lowest_low == 0:
            self.highest_high = high
            self.lowest_low = low
        result = None
        if self.flag == "first":
            if (self.highest_high - low) / self.highest_high > move:
                self.flag = "top"
                result = Point(x=x, y=self.highest_high), 1
                self.highest_high = high
                self.count = 0
            elif (high - self.lowest_low) / self.lowest_low > move:
                self.flag = "base"
                result = Point(x=x, y=self.lowest_low), -1
                self.lowest_low = low
                self.count = 0
        elif self.flag == "base":
            self.lowest_low = min(low, self.lowest_low)
            if (self.highest_high - low) / self.highest_high > move:
                self.flag = "top"
                result = Point(x=x, y=self.highest_high), 1
                self.highest_high = high
                self.lowest_low = low
                self.count = 0
            if high > self.highest_high:
                self.highest_high = high
                self.lowest_low = low
                self.count = 0
        elif self.flag == "top":
            self.highest_high = max(high, self.highest_high)
            if (high - self.lowest_low) / self.lowest_low > move:
                self.flag = "base"
                result = Point(x=x, y=self.lowest_low), -1
                self.lowest_low = low
                self.highest_high = high
                self.count = 0
            if low < self.lowest_low:
                self.highest_high = high
                self.lowest_low = low
                self.count = 0
        return result
