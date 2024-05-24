from datetime import datetime
from typing import NamedTuple, Optional

from talipp.indicators import SMA


class Candle(NamedTuple):
    open: float
    close: float
    high: float
    low: float
    volume: int
    date: datetime

    def min(self, use_body: bool = False):
        return self.low if not use_body else min(self.open, self.close)

    def max(self, use_body: bool = False):
        return self.high if not use_body else max(self.open, self.close)


class Signal:
    def get_completion_bar_index(self) -> int:
        pass

    def show(self, chart_image: ChartImage):
        pass


class Matcher:
    def match(self, candle: Candle) -> Signal:
        pass


class CollisionMatcher(Matcher):


    def match(self, candle: Candle) -> Signal:


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

    def detect(self, candle: Candle):
        self.candles.append(candle)
        if len(self.candles) <= self.before + self.after:
            return None

        a = self.candles[self.before]
        before = self.candles[0:self.before]
        after = self.candles[-self.after:]
        u_b = self.use_bodies
        high = a.max(u_b)
        low = a.min(u_b)
        self.candles.pop(0)
        if max([c.max(u_b) for c in before]) < high > max([c.max(u_b) for c in after]):
            return high, 1
        if min([c.min(u_b) for c in before]) > low < min([c.min(u_b) for c in after]):
            return low, -1


class MoveBasedPivotDetector:
    factor: float
    use_bodies: bool
    candles: list[Candle]
    count: int
    flag: str
    highest_high: float
    lowest_low: float
    last_candle: Optional[Candle]
    move_sma: SMA

    def __init__(self, factor: float, move_sma_len: int = 90, use_bodies: bool = True):
        self.candles = []
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

    def detect(self, c: Candle):
        high = c.max(self.use_bodies)
        low = c.min(self.use_bodies)
        if low == 0 or high == 0:
            return None
        if self.count == 0:
            self.highest_high = high
            self.lowest_low = low
        self.count += 1
        move = self.__calc_move(c) * self.factor
        if self.flag == "first":
            if (self.highest_high - low) / self.highest_high > move:
                self.flag = "top"
                self.highest_high = high
                return high, 1
            elif (high - self.lowest_low) / self.lowest_low > move:
                self.flag = "base"
                self.lowest_low = low
                return low, -1
        elif self.flag == "base":
            self.lowest_low = min(low, self.lowest_low)
            if (self.highest_high - low) / self.highest_high > move:
                self.flag = "top"
                self.highest_high = high
                self.lowest_low = low
                return high, 1
            if high > self.highest_high:
                self.highest_high = high
                self.lowest_low = low
        elif self.flag == "top":
            self.highest_high = max(high, self.highest_high)
            if (high - self.lowest_low) / self.lowest_low > move:
                self.flag = "base"
                self.lowest_low = low
                self.highest_high = high
                return low, -1
            if low < self.lowest_low:
                self.highest_high = high
                self.lowest_low = low
        return None
