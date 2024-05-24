import math
from typing import NamedTuple


class Point(NamedTuple):
    x: int
    y: float


class Geometry:
    log: bool

    def __init__(self, log):
        self.log = log

    def calc_slope(self, source: Point, target: Point):
        if self.log:
            return (math.log(target.y) - math.log(source.y)) / (target.x - source.x)
        else:
            return (target.y - source.y) / (target.x - source.x)

    def transpose_point_x(self, point: Point, slope: float, d_x: int):
        if d_x == 0:
            return point
        new_y = point.y * math.exp(d_x * slope) if self.log else point.y + d_x * slope
        new_x = point.x + d_x
        return Point(new_x, new_y)


class Line:
    start: Point
    stop: Point
    slope: float
    geometry: Geometry

    def __init__(self, start: Point, stop: Point, geometry: Geometry):
        self.start = start
        self.stop = stop
        self.slope = geometry.calc_slope(start, stop)
        self.geometry = geometry

    def is_rising(self):
        return self.slope > 0

    def y(self, at_x):
        d_x = at_x - self.start.x
        return self.geometry.transpose_point_x(self.start, self.slope, d_x).y

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.start == other.start and self.stop == other.stop

    def __hash__(self):
        return hash((self.start, self.stop))
