from concurrent.futures import ProcessPoolExecutor

import chart_db
from analysis_i import Matcher
from chart_image import ChartImage
from matcher_bounce import BounceMatcher
from matcher_headshoulders import HeadshouldersMatcher


def show_signals(symbol, matcher: Matcher, limit: int = 0):
    print(symbol)
    candles = chart_db.load_daily_candles(symbol, 900)
    chart_image = ChartImage(symbol, candles, 900)
    signals = matcher.scan(candles, limit)
    for s in signals:
        s.show(chart_image)

    chart_image.show_if_not_empty()


def show_signals_multi(symbols, matcher_factory, limit: int = 0):
    with ProcessPoolExecutor(max_workers=10) as executor:
        for s in symbols:
            executor.submit(show_signals, s, matcher_factory(), limit)

mag8 = ["NASDAQ:NVDA", "NASDAQ:TSLA", "NASDAQ:AAPL", "NASDAQ:MSFT", "NASDAQ:AMZN", "NASDAQ:GOOGL", "NASDAQ:META",
        "NASDAQ:NFLX"]

show_signals_multi([s['sort'] for s in chart_db.list_symbols()], HeadshouldersMatcher, 7)
# show_signals_multi([s['sort'] for s in chart_db.list_symbols()], BounceMatcher, 60)
# show_signals_multi(mag8, HeadshouldersMatcher)
# show_signals("NASDAQ:NVDA", HeadshouldersMatcher())
# show_signals("NASDAQ:TSLA", HeadshouldersMatcher())


# show_signals("NYSE:BAC", BounceMatcher())
# show_signals("NYSE:DAL", BounceMatcher())
