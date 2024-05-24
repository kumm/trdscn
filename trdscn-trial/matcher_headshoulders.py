import logging
from typing import Optional

from talipp.indicators import SMA

from analysis_i import Matcher, MoveBasedPivotDetector
from backtest import Signal
from chart_db import Candle
from chart_geometry import Point, Line, Geometry
from chart_image import ChartImage


class HsSignal(Signal):
    start: Point
    l_sr: Point
    l_nck: Point
    head: Point
    r_nck: Point
    r_sr: Point
    stop: Point
    neck_line: Line
    inverse: bool

    def __init__(self, start, l_sr, l_nck, head, r_nck, r_sr, stop, neck_line, inverse=False):
        self.start = start
        self.l_sr = l_sr
        self.l_nck = l_nck
        self.head = head
        self.r_nck = r_nck
        self.r_sr = r_sr
        self.stop = stop
        self.neck_line = neck_line
        self.inverse = inverse

    def as_polygon(self):
        return [self.start, self.l_sr, self.l_nck, self.head, self.r_nck, self.r_sr, self.stop]

    def get_completion_bar_index(self) -> int:
        return self.stop.x

    def show(self, chart_image: ChartImage):
        chart_image.add_polygon(
            self.as_polygon(),
            "solid", "#0e0e0e", 2
        )

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(frozenset(self.__dict__.items()))


class HeadshouldersMatcher(Matcher):
    pivots: list[tuple[MoveBasedPivotDetector, list[Point]]]
    candles: list[Candle]
    geometry: Geometry

    def __init__(self):
        self.pivots = [
            (MoveBasedPivotDetector(1.1, use_bodies=False), SMA(14), []),
            (MoveBasedPivotDetector(1.3, use_bodies=False), SMA(24), []),
            (MoveBasedPivotDetector(2, use_bodies=False), SMA(50), []),
            (MoveBasedPivotDetector(2.5, use_bodies=False), SMA(90), []),
            (MoveBasedPivotDetector(3, use_bodies=False), SMA(100), []),
            (MoveBasedPivotDetector(4, use_bodies=False), SMA(120), []),
            (MoveBasedPivotDetector(5, use_bodies=False), SMA(150), []),
            (MoveBasedPivotDetector(6, use_bodies=False), SMA(200), []),
        ]
        self.geometry = Geometry(False)
        self.candles = []
        self.logger = logging.getLogger("matcher_headshoulders")

    def match(self, candle: Candle, bar_index: int) -> Optional[HsSignal]:
        self.candles.append(candle)
        signals = []
        for pd, ma, pps in self.pivots:
            ma.add(candle.close)
            pp = pd.detect(candle, bar_index)
            if pp is not None:
                pps.append(pp)
                self.logger.debug(f"{bar_index} - New {'high' if pp[1]>0 else 'low'} pivot: {pp[0].y} on {self.candles[pp[0].x].date.date()}")
                # print(pp, self.candles[pp[0].x])
            if len(pps) >= 6:
                points = [pp[0] for pp in pps[-6:]]
                signal = self.__match_hs(points, pps[-6][1] == 1)
                if signal is not None and self.__check_trend(ma, signal):
                    signals.append(signal)
        return None if len(signals) == 0 else signals[-1]

    def __check_trend(self, ma: SMA, signal: HsSignal):
        if signal.inverse:
            return signal.l_sr.y < ma[signal.l_sr.x] and signal.r_sr.y < ma[signal.r_sr.x]
        else:
            return signal.l_sr.y > ma[signal.l_sr.x] and signal.r_sr.y > ma[signal.r_sr.x]

    def __match_hs(self, points: list[Point], invert: bool) -> Optional[HsSignal]:
        if invert:
            inverse_candles = [Candle(
                open=-c.open, close=-c.close, high=-c.low, low=-c.high, date=c.date, volume=c.volume
            ) for c in self.candles]
            inverse_points = [Point(x=p.x, y=-p.y) for p in points]
            signal = self.__hs_pattern_detector(inverse_points, inverse_candles)
            return None if signal is None else self.__invert_signal(signal)
        else:
            return self.__hs_pattern_detector(points, self.candles)

    def __hs_pattern_detector(self, points: list[Point], candles: list[Candle]):
        bfr, l_sr, l_nck, head, r_nck, r_sr = points
        if not (l_sr.y < head.y > r_sr.y and bfr.y < l_nck.y and bfr.y < r_nck.y):
            # print("abs h failed")
            return None

        neck_line = Line(start=l_nck, stop=r_nck, geometry=self.geometry)
        # if neck_line.y(bfr.x) < bfr.y:
        #     return None
        head_h = head.y - neck_line.y(head.x)
        l_sr_h = l_sr.y - neck_line.y(l_sr.x)
        r_sr_h = r_sr.y - neck_line.y(r_sr.x)

        if r_sr_h > l_sr_h * 1.2 or l_sr_h > head_h * 0.85:
            # print("rel h failed")
            return None


        start_x = self.__find_under(neck_line, candles, l_sr.x - 1, max(0, bfr.x - 1))
        stop_x = self.__find_under(neck_line, candles, r_sr.x + 1, len(candles))
        if stop_x is not None and stop_x != len(candles) - 1:
            # print("EEE ", candles[stop_x], candles[-1])
            return None

        # print(start_x, stop_x)

        if start_x is None or stop_x is None:
            # print("start-stop failed", start_x, stop_x)
            return None

        head_w = r_nck.x - l_nck.x
        l_sr_w = l_nck.x - start_x
        r_sr_w = stop_x - r_nck.x

        if max([c.max(use_body=True) for c in candles[r_sr.x:stop_x + 1]]) > r_sr.y:
            # print("right shoulder violated")
            return None

        if (head.x-l_sr.x)/(stop_x-start_x) < 0.15 or (r_sr.x-head.x)/(stop_x-start_x) < 0.15:
            print("head <-> shoulders space violation")
            return None

        if l_sr_w < 3 or r_sr_w < 3:
            # print("narrow shoulder")
            return None

        # print(head_w, l_sr_w, r_sr_w)
        if not 0.25 < l_sr_w / r_sr_w < 1 / 0.25:
            # print("sr w failed")
            return None

        if head_w < max(l_sr_w, r_sr_w) * 0.5:
            # print("head w failed")
            return None

        if abs(head_h / neck_line.y(stop_x)) < 0.05:
            # print("head h failed")
            return None

        return HsSignal(
            start=Point(start_x, neck_line.y(start_x)),
            l_sr=l_sr, l_nck=l_nck, head=head, r_nck=r_nck, r_sr=r_sr,
            stop=Point(stop_x, neck_line.y(stop_x)),
            neck_line=neck_line
        )

    @staticmethod
    def __find_under(line: Line, candles: list[Candle], start: int, stop: int):
        for i in range(start, stop, 1 if stop > start else -1):
            c = candles[i]
            if c.close < line.y(i):
                return i

    @staticmethod
    def __invert_signal(signal: HsSignal) -> HsSignal:
        return HsSignal(
            start=Point(signal.start.x, -signal.start.y),
            l_sr=Point(signal.l_sr.x, -signal.l_sr.y),
            l_nck=Point(signal.l_nck.x, -signal.l_nck.y),
            head=Point(signal.head.x, -signal.head.y),
            r_nck=Point(signal.r_nck.x, -signal.r_nck.y),
            r_sr=Point(signal.r_sr.x, -signal.r_sr.y),
            stop=Point(signal.stop.x, -signal.stop.y),
            neck_line=Line(Point(signal.neck_line.start.x, -signal.neck_line.start.y),
                           Point(signal.neck_line.stop.x, -signal.neck_line.stop.y),
                           signal.neck_line.geometry),
            inverse=True
        )
