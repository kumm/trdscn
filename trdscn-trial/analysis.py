import numpy
import talib

from chart_db import Candle
from chart_geometry import Point, Geometry, Line


def find_pivots(before: int, after: int, values: list[float], direction: int):
    pivot_points = []
    counter = 0
    if len(values) < before + 1:
        return pivot_points
    current_max = values[before]
    for i in range(before, len(values)):
        pivot_range = values[i - before + 1:i + 1]
        new_max = max(pivot_range) if direction > 0 else min(pivot_range)
        if current_max == new_max:
            counter += 1
        else:
            if values[i] == new_max:
                counter = 0
                current_max = new_max
        if counter == after:
            pivot_points.append(Point(x=i - after, y=current_max))
    return pivot_points


class TrendLineExplorer:
    candles: list[Candle]
    geometry: Geometry

    def __init__(self, candles, geometry):
        self.candles = candles
        self.geometry = geometry

    def explore(self, pivot_before, pivot_after, use_bodies: bool = False):
        pivot_highs = find_pivots(
            pivot_before, pivot_after, [(max(c.open, c.close) if use_bodies else c.high) for c in self.candles], 1
        )
        pivot_lows = find_pivots(
            pivot_before, pivot_after, [(min(c.open, c.close) if use_bodies else c.low) for c in self.candles], -1
        )
        return {
            'low': self.__create_trend_lines(pivot_lows),
            'high': self.__create_trend_lines(pivot_highs),
        }

    def __create_trend_lines(self, pivot_points: list[Point]):
        trend_lines = []
        for i in range(1, len(pivot_points)):
            a = pivot_points[i - 1]
            b = pivot_points[i]
            trend_lines.append(Line(start=a, stop=b, geometry=self.geometry))
        return trend_lines


def __calc_move(candles, sma_length):
    norm_tr = []
    b = candles[0] if len(candles) > 0 else None
    for a in candles:
        tr = max(a.high - a.low, abs(a.high - b.close), abs(a.low - b.close))
        hl2 = (a.high + a.low) / 2
        norm_tr.append(tr / hl2)
        b = a

    return talib.SMA(numpy.array(norm_tr), timeperiod=sma_length)


def find_levels(candles: list[Candle], factor, move_sma_len=90, use_bodies=True):
    result = []
    first_pos = ''
    count = 0
    flag = "first"
    fc = candles[0]
    highest_high = fc.max(use_bodies)
    lowest_low = fc.min(use_bodies)
    moves = __calc_move(candles, move_sma_len)
    for i, c in enumerate(candles):
        count += 1
        high = c.max(use_bodies)
        low = c.min(use_bodies)
        if low == 0 or high == 0:
            continue
        move = moves[i] * factor
        if flag == "first":
            if (highest_high - low) / highest_high > move:
                flag = "top"
                first_pos = 'high'
                result.append(Point(x=i, y=highest_high))
                highest_high = high
                count = 0
            elif (high - lowest_low) / lowest_low > move:
                flag = "base"
                first_pos = 'low'
                result.append(Point(x=i, y=lowest_low))
                lowest_low = low
                count = 0
        elif flag == "base":
            lowest_low = min(low, lowest_low)
            if (highest_high - low) / highest_high > move:
                flag = "top"
                result.append(Point(x=i - count, y=highest_high))
                highest_high = high
                lowest_low = low
                count = 0
            if high > highest_high:
                highest_high = high
                lowest_low = low
                count = 0
        elif flag == "top":
            highest_high = max(high, highest_high)
            if (high - lowest_low) / lowest_low > move:
                flag = "base"
                result.append(Point(x=i - count, y=lowest_low))
                lowest_low = low
                highest_high = high
                count = 0
            if low < lowest_low:
                highest_high = high
                lowest_low = low
                count = 0
    return first_pos, result
