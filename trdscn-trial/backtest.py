import logging
from typing import Optional

from chart_db import Candle
from chart_image import ChartImage


class Signal:
    def get_completion_bar_index(self) -> int:
        pass

    def get_begin_bar_index(self) -> int:
        pass

    def show(self, chart_image: ChartImage):
        pass


class Algorithm:
    def signal(self, strategy, signal: Signal):
        pass

    def bar_close(self, bar_index: int, candle: Candle) -> bool:
        pass

    def show(self, chart: ChartImage):
        pass


class Trade:
    qty: float
    price: float
    exit_price: Optional[float]
    open_bar_ndx: int
    close_bar_ndx: int

    def __init__(self, open_bar_ndx, qty, price):
        self.qty = qty
        self.price = price
        self.exit_price = None
        self.open_bar_ndx = open_bar_ndx

    def size(self):
        return self.qty * self.price

    def close(self, close_bar_ndx, price):
        self.exit_price = price
        self.close_bar_ndx = close_bar_ndx

    def get_closed_profit(self):
        return 0 if self.is_open() else self.qty * (self.exit_price - self.price)

    def get_open_position(self, current_price):
        return (self.qty * (current_price - self.price)) if self.is_open() else 0

    def is_open(self):
        return self.exit_price is None


class Session:
    capital: int
    position_size: float
    position_avg_price: float
    trades: dict[str, Trade]
    max_drawdown: float
    max_runup: float
    position_qty: float
    sum_closed_profit: float
    open_commands: list[tuple]
    close_commands: list[str]
    client_counter: int
    last_close: float

    def __init__(self):
        self.capital = 1000
        self.position_size = 0.0
        self.position_avg_price = 0.0
        self.trades = {}
        self.max_drawdown = 0.0
        self.max_runup = 0.0
        self.position_qty = 0.0
        self.sum_closed_profit = 0.0
        self.open_commands = []
        self.close_commands = []
        self.client_counter = 0
        self.last_close = 0.0
        self.logger = logging.getLogger("backtest")

    def entry(self, id: str, direction: int, size: int = 0):
        if size == 0:
            size = self.capital
        self.open_commands.append((id, direction * size))

    def __update_position(self):
        sum_size = 0
        sum_qty = 0
        for p in self.trades.values():
            if p.is_open():
                sum_size += p.size()
                sum_qty += p.qty
        self.position_size = sum_size
        self.position_avg_price = sum_size / sum_qty if sum_qty != 0 else None
        self.position_qty = sum_qty

    def close(self, id: str):
        self.close_commands.append(id)

    def on_candle_close(self, close_price: float):
        self.last_close = close_price
        # self.max_drawdown = max(self.max_drawdown, (self.position_avg_price - close_price) * self.position_qty)
        # self.max_runup = max(self.max_runup, (close_price - self.position_avg_price) * self.position_qty)

    def on_candle(self, bar_index: int, candle: Candle):
        for c in self.open_commands:
            id, size = c
            qty = size / candle.open
            self.trades[id] = Trade(open_bar_ndx=bar_index, qty=qty, price=candle.open)
            self.logger.debug(f"Enter {'long' if qty > 0 else 'short'} {abs(qty)}@{candle.open} on {candle.date}")
        for id in self.close_commands:
            qty = self.trades[id].qty
            self.trades[id].close(bar_index, candle.open)
            self.logger.debug(f"Exit {'long' if qty > 0 else 'short'} {abs(qty)}@{candle.open} on {candle.date}")
        self.open_commands.clear()
        self.close_commands.clear()
        self.__update_position()
        self.last_close = candle.close

    def get_closed_profit(self):
        return sum([p.get_closed_profit() for p in self.trades.values()])

    def get_open_position(self):
        return sum([p.get_open_position(self.last_close) for p in self.trades.values()])

    def get_win_count(self):
        return sum([1 if p.get_closed_profit() > 0 else 0 for p in self.trades.values()])

    def get_closed_trades_count(self):
        return sum([0 if p.is_open() else 1 for p in self.trades.values()])

    def run_algorithm(self, candles, algorithm: Algorithm, signal: Signal):
        self.client_counter += 1
        algorithm.signal(Strategy(self, self.client_counter), signal)
        for i in range(signal.get_completion_bar_index() + 1, len(candles) - 1):
            candle = candles[i]
            self.on_candle(i, candle)
            if not algorithm.bar_close(i, candle):
                self.on_candle(i + 1, candles[i + 1])
                return

    def show_trades(self, chart_image: ChartImage, candles: list[Candle]):
        for t in self.trades.values():
            chart_image.add_enter_trade(t.open_bar_ndx, t.qty > 0)
            if not t.is_open():
                chart_image.add_exit_trade(t.close_bar_ndx, t.qty > 0)


class Strategy:
    session: Session
    client_id: int

    def __init__(self, session, client_id):
        self.session = session
        self.client_id = client_id

    def entry(self, id: str, direction: int, size: int = 0):
        self.session.entry(f"{self.client_id}#{id}", direction, size)

    def close(self, id: str):
        self.session.close(f"{self.client_id}#{id}")

    def get_position_avg_price(self):
        return self.session.position_avg_price

    def get_position_size(self):
        return self.session.position_size


def backtest(candles: list[Candle], signals: set[Signal], algorithm: Algorithm, chart_image: ChartImage):
    session = Session()
    for sgn in signals:
        session.run_algorithm(candles, algorithm, sgn)
        if chart_image is not None:
            algorithm.show(chart_image)

    if chart_image is not None:
        session.show_trades(chart_image, candles)

    return session
