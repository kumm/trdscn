import plotly.graph_objects as go

from chart_db import Candle
from chart_geometry import Line, Point


class ChartImage:
    title: str
    offset: int
    limit: int
    candles: list[Candle]
    fig: go.Figure
    shapes: list[dict]
    annotations: list[go.layout.Annotation]
    avg_candle_size: float

    def __init__(self, title, candles, offset = None, limit = None):
        self.title = title
        self.candles = candles
        self.shapes = []
        self.annotations = []
        self.offset = 0 if offset is None else offset
        self.limit = len(candles) if limit is None else limit
        self.__init_figure()

    def __init_figure(self):
        visible_candles = self.candles[self.offset:self.offset + self.limit]
        candle_sizes = [c.high - c.low for c in visible_candles]
        self.avg_candle_size = sum(candle_sizes) / len(visible_candles)
        self.fig = go.Figure(
            data=[go.Candlestick(x=[c.date for c in visible_candles],
                                 open=[c.open for c in visible_candles],
                                 high=[c.high for c in visible_candles],
                                 low=[c.low for c in visible_candles],
                                 close=[c.close for c in visible_candles])])
        self.fig.update_layout(
            xaxis_rangeslider_visible=False if self.limit < 1000 else True,
            title={'text': self.title}
        )

    def add_vector(self, line: Line, dash: str, color: str, width: int = 1):
        first_x =self.offset
        last_x = len(self.candles) - 1
        x0 = line.start.x if line.start.x >= first_x else first_x
        self.shapes.append(dict(
            x0=self.candles[x0].date, y0=line.y(x0),
            x1=self.candles[last_x].date, y1=line.y(last_x),
            xref='x', yref='y',
            type='line', line={'width': width, 'dash': dash, 'color': color}
        ))

    def add_line(self, line: Line, dash: str, color: str, width: int = 1):
        first_x =self.offset
        x0 = line.start.x if line.start.x >= first_x else first_x
        self.shapes.append(dict(
            x0=self.candles[x0].date, y0=line.y(x0),
            x1=self.candles[line.stop.x].date, y1=line.stop.y,
            xref='x', yref='y',
            type='line', line={'width': width, 'dash': dash, 'color': color}
        ))

    def add_level(self, y: float, dash: str, color: str, width: int = 1):
        self.shapes.append(dict(
            x0=0, y0=y,
            x1=1, y1=y,
            xref='paper', yref='y',
            type='line', line={'width': width, 'dash': dash, 'color': color}
        ))

    def add_polygon(self, points: list[Point], dash: str, color: str, width: int = 1):
        for i, p1 in enumerate(points):
            p2 = points[i + 1 if i < len(points) - 1 else 0]
            self.shapes.append(dict(
                x0=self.candles[p1.x].date, y0=p1.y,
                x1=self.candles[p2.x].date, y1=p2.y,
                xref='x', yref='y',
                type='line', line={'width': width, 'dash': dash, 'color': color}
            ))

    def __add_vert_arrow(self, p: Point, up: bool, size: float, color: str, head, width):
        x = self.candles[p.x].date
        self.annotations.append(go.layout.Annotation(dict(
            x=x, y=p.y, xref="x", yref="y",
            text="",
            showarrow=True,
            axref="x", ayref='y',
            ax=x,
            ay=p.y - size if up else p.y + size,
            arrowhead=head,
            arrowwidth=width,
            arrowcolor=color
        )))

    def add_enter_trade(self, bar_ndx: int, up: bool):
        c = self.candles[bar_ndx]
        y = c.low if up else c.high
        self.__add_vert_arrow(
            Point(bar_ndx, y), up, self.avg_candle_size, "#00FF00" if up else "#FF0000", 1, 4
        )

    def add_exit_trade(self, bar_ndx: int, up: bool):
        y = self.candles[bar_ndx].high if up else self.candles[bar_ndx].low
        self.__add_vert_arrow(
            Point(bar_ndx, y), not up, self.avg_candle_size, "#00FF00" if up else "#FF0000", 1, 4
        )

    def __update(self):
        self.fig.update_layout(shapes=self.shapes, annotations=self.annotations)

    def show(self):
        self.__update()
        self.fig.show()

    def show_if_not_empty(self):
        if len(self.shapes) > 0:
            self.__update()
            self.fig.show()

    def write(self, filename):
        self.__update()
        self.fig.write_image(filename)
