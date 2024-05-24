from analysis import find_levels
from analyzer import Analyzer
from backtest import Signal
from chart_db import Candle
from chart_geometry import Geometry, Line, Point
from chart_image import ChartImage
from matcher_headshoulders import HsSignal

geometry = Geometry(False)


def find_under(line: Line, candles: list[Candle], start: int, stop: int):
    for i in range(start, stop, 1 if stop > start else -1):
        c = candles[i]
        if c.close < line.y(i):
            return i




def hs_pattern_detector(points: list[Point], candles: list[Candle]):
    bfr, l_sr, l_nck, head, r_nck, r_sr = points
    if not (l_sr.y < head.y > r_sr.y and bfr.y < l_nck.y < r_nck.y):
        # print("abs h failed")
        return None

    neck_line = Line(start=l_nck, stop=r_nck, geometry=geometry)
    # if neck_line.y(bfr.x) < bfr.y:
    #     return None
    head_h = head.y - neck_line.y(head.x)
    l_sr_h = l_sr.y - neck_line.y(l_sr.x)
    r_sr_h = r_sr.y - neck_line.y(r_sr.x)


    if r_sr_h > l_sr_h * 1.2 or l_sr_h > head_h * 0.85:
        # print("rel h failed")
        return None

    # start_x = find_cross_x(neck_line, candles[bfr.x:l_sr.x:-1], bfr.x, 1)
    # stop_x = find_cross_x(neck_line, candles[r_sr.x:], r_sr.x, -1)
    start_x = find_under(neck_line, candles, l_sr.x - 1, bfr.x)
    stop_x = find_under(neck_line, candles, r_sr.x, len(candles) - 1)

    # print(start_x, stop_x)

    if start_x is None or stop_x is None:
        # print("start-stop failed")
        return None

    head_w = r_nck.x - l_nck.x
    l_sr_w = l_nck.x - start_x
    r_sr_w = stop_x - r_nck.x

    if max([c.max(use_body=True) for c in candles[r_sr.x:stop_x + 1]]) > r_sr.y:
        return None

    if l_sr_w < 3 or r_sr_w < 3:
        return None

    # print(head_w, l_sr_w, r_sr_w)
    if not 0.25 < l_sr_w / r_sr_w < 1 / 0.25:
        # print("sr w failed")
        return None

    if head_w < max(l_sr_w, r_sr_w) * 0.5:
        # print("head w failed")
        return None

    if abs(head_h / neck_line.y(stop_x)) < 0.1:
        return None

    return HsSignal(
        start=Point(start_x, neck_line.y(start_x)),
        l_sr=l_sr, l_nck=l_nck, head=head, r_nck=r_nck, r_sr=r_sr,
        stop=Point(stop_x, neck_line.y(stop_x)),
        neck_line=neck_line
    )


daily_factors = [1.5, 2, 3, 4.5, 6]
daily_chart_styles = {
    1.5: dict(dash='solid', color='#121212', width=1),
    2: dict(dash='solid', color='#991212', width=2),
    3: dict(dash='solid', color='#CC1212', width=3),
    4.5: dict(dash='solid', color='#FF1212', width=4),
    6: dict(dash='solid', color='#FF1212', width=6),
}


def detect_hs(candles, last_only):
    result = {}
    for factor in daily_factors:
        result[factor] = find_all_hs(candles, factor, last_only)
    return result


def find_all_hs(candles: list[Candle], factor: float, last_only=True):
    first_pos, points = find_levels(candles, factor, 200, use_bodies=True)
    even_low = first_pos == 'low'
    if last_only:
        if (len(points) - 6) % 2 == 1:
            even_low = not even_low
        points = points[-6:]
    result = traverse_points(candles, points, even_low)
    even_low = not even_low
    inverse_candles = [Candle(
        open=-c.open, close=-c.close, high=-c.high, low=-c.low, date=c.date, volume=c.volume
    ) for c in candles]
    inverse_points = [Point(x=p.x, y=-p.y) for p in points]
    inverse_result = traverse_points(inverse_candles, inverse_points, even_low)
    for r in inverse_result:
        result.append(invert_signal(r))
    return result


def invert_signal(signal: HsSignal):
    return HsSignal(
        start=Point(signal.start.x, -signal.start.y),
        l_sr=Point(signal.l_sr.x, -signal.l_sr.y),
        l_nck=Point(signal.l_nck.x, -signal.l_nck.y),
        head=Point(signal.head.x, -signal.head.y),
        r_nck=Point(signal.r_nck.x, -signal.r_nck.y),
        r_sr=Point(signal.r_sr.x, -signal.r_sr.y),
        stop=Point(signal.stop.x, -signal.stop.y),
        neck_line=Line(Point(signal.neck_line.start.x, -signal.neck_line.start.y), Point(signal.neck_line.stop.x, -signal.neck_line.stop.y),
                       signal.neck_line.geometry)
    )


def traverse_points(candles: list[Candle], points: list[Point], even_low: bool):
    result = []
    for i, p in enumerate(points[:-5]):
        if i % 2 == (0 if even_low else 1):
            hs = hs_pattern_detector(points[i:i + 6], candles)
            if hs is not None:
                result.append(hs)
    return result


def collect_hs_signals(candles: list[Candle], last_only):
    hs_by_factors = detect_hs(candles, last_only)
    signals: set[HsSignal] = set()
    for factor, hs_list in hs_by_factors.items():
        for hs in hs_list:
            signals.add(hs)
    return signals


