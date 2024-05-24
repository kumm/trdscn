import chart_db
from analyzer import Analyzer
from backtest import Algorithm, Strategy, Signal
from chart_db import Candle
from chart_geometry import Line, Point, Geometry
from chart_image import ChartImage
from hs_detector import detect_hs, daily_chart_styles, HsSignal, collect_hs_signals


def show_chart(symbol, last_only, max_age):
    candles = chart_db.load_daily_candles(symbol, 900)
    hs_by_factors = detect_hs(candles, last_only)
    chart_image = ChartImage(symbol, candles, 900)
    for factor, hs_list in hs_by_factors.items():
        style = daily_chart_styles[factor]
        for hs in hs_list:
            if max_age is None or hs.stop.x >= len(candles) - 1 - max_age:
                chart_image.add_polygon(hs.as_polygon(), style['dash'], style['color'], style['width'])

    if len(chart_image.shapes) > 0:
        chart_image.show()


class HsAlgorithm(Algorithm):
    strategy: Strategy
    neck_line: Line
    target: float
    direction: int
    sgnl: HsSignal
    rs_h: float
    head_h: float
    last_bar_index: int

    def __init__(self):
        self.behaviour = None

    def signal(self, strategy: Strategy, signal: HsSignal):
        self.strategy = strategy
        self.sgnl = signal
        self.neck_line = signal.neck_line
        head_h = signal.head.y - self.neck_line.y(signal.head.x)
        self.rs_h = abs(signal.r_sr.y - self.neck_line.y(signal.r_sr.x))
        self.target = signal.stop.y - head_h
        self.direction = -1 if head_h > 0 else 1
        self.head_h = head_h
        strategy.entry('HS', self.direction)
        self.last_bar_index = 0
        self.behaviour = self.normal_behave

    def bar_close(self, bar_index: int, candle: Candle) -> bool:
        self.last_bar_index = bar_index
        return self.behaviour(bar_index, candle)

    def normal_behave(self, bar_index: int, candle: Candle):
        target = self.sgnl.stop.y - self.head_h
        edge = candle.high if self.direction > 0 else candle.low
        if self.direction * (edge - target) >= 0:
            self.strategy.close('HS')
            return False
        if self.direction * (self.neck_line.y(bar_index) - candle.close) > self.rs_h / 10:
            self.behaviour = self.backup_behave
            return True
        return True

    def backup_behave(self, bar_index: int, candle: Candle):
        if self.direction * (candle.close - self.strategy.get_position_avg_price()) >= 0:
            self.strategy.close('HS')
            return False
        if self.direction * ((self.sgnl.head.y + self.sgnl.r_sr.y) / 2 - candle.close) >= 0:
            self.strategy.close('HS')
            return False
        return True

    def show(self, chart_image: ChartImage):
        self.sgnl.show(chart_image)
        chart_image.add_line(
            Line(Point(self.sgnl.stop.x, self.target),
                 Point(self.last_bar_index, self.target),
                 Geometry(False)
                 ), "solid", "#000000")

    # for i, s in enumerate(["NASDAQ:NVDA", "NASDAQ:AMAT", "NASDAQ:AKAM", "NASDAQ:CDW", "NASDAQ:EQIX", "NASDAQ:FITB"]):


# backtest_hs("NASDAQ:ADBE", 900)

# for i, s in enumerate(chart_db.list_symbols()):
#     print(s['sort'])
#     show_chart(s['sort'], True, 1)


class HsAnalyzer(Analyzer):
    def collect_signals(self, candles: list[Candle], last_only: bool = False) -> set[Signal]:
        return collect_hs_signals(candles, last_only)

    def new_algorithm(self):
        return HsAlgorithm()
