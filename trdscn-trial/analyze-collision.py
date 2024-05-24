import numpy
import talib

import chart_db
from analysis import TrendLineExplorer
from chart_geometry import Geometry
from chart_image import ChartImage


# tls_short = trend_line_explorer.explore(10, 5)

# print([candles[i.x].date.strftime("%Y-%m-%d") for i in pivot_highs])
# print([candles[i.x].date.strftime("%Y-%m-%d") for i in pivot_lows])

def analyze(symbol):
    candles = chart_db.load_daily_candles(symbol)
    #    candles = candles[0:-8]
    #    print(f"Date {candles[-1].date}")
    print(len(candles))
    geometry = Geometry(False)
    trend_line_explorer = TrendLineExplorer(candles, geometry)
    # print(symbol)
    tls_huge = trend_line_explorer.explore(88, 44)
    tls_long = trend_line_explorer.explore(44, 22)
    tls_mid = trend_line_explorer.explore(20, 10)
    tls_short = trend_line_explorer.explore(10, 5)
    lines = []
    lines = lines + tls_huge['high'][-5:] + tls_huge['low'][-5:]
    lines = lines + tls_long['high'][-10:] + tls_long['low'][-10:]
    # lines = lines + tls_mid['high'][-2:] + tls_mid['low'][-2:]
    # lines = lines + tls_short['high'][-2:] + tls_short['low'][-2:]
    last_candle_ndx = len(candles) - 1
    close_ndarr = numpy.array([c.close for c in candles])
    atr = talib.ATR(
        high=numpy.array([c.high for c in candles]),
        low=numpy.array([c.low for c in candles]),
        close=close_ndarr,
    )[-1]
    #    rsi = talib.RSI(close_ndarr)[-1]
    show_lines(symbol, lines, candles)
    for l in lines:
        y = l.y(last_candle_ndx)
        c = [c.close for c in candles]
        rising = c[-7] < y - 3 * atr
        falling = c[-7] > y + 3 * atr
        # print(rising,falling, y, y+0.1*atr, candles[-1].high - y - 0.5 * atr)
        collision_high = rising and c[-2] < c[-1] < y + 0.1 * atr and candles[-1].high > y - 0.5 * atr
        collision_low = falling and c[-2] > c[-1] > y - 0.1 * atr and candles[-1].low < y + 0.5 * atr
        if collision_low or collision_high:
            print(f"Prepare for collision on {symbol}")
            chart_image = ChartImage(symbol, candles, 300)
            chart_image.add_vector(l, 'solid', '#12DE12', 1)
            # chart_image.show()
            break


def show_lines(symbol, lines, candles):
    chart_image = ChartImage(symbol, candles, 900)
    for l in lines:
        chart_image.add_vector(l, 'solid', '#12DE12', 1)
    chart_image.show()


# for smbl in database["symbol"].find({'active': True}):
#    analyze(smbl['_id'])

analyze('NYSE:DG')

# chart_image = ChartImage(candles, 300)
# for l in tls['high'][-3:]:
#     chart_image.add_line(l, 'solid', '#12DE12', 1)
#     print(candles[l.start.x])
#     print(l)
# for l in tls['low'][-3:]:
#     chart_image.add_line(l, 'solid', '#DE1212', 1)
# chart_image.show()
