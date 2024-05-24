import numpy
import talib
from dotenv import dotenv_values
from pymongo import MongoClient

from analysis import find_levels
from chart_db import load_weekly_candles, load_daily_candles
from chart_image import ChartImage

config = dotenv_values(".env")
mongodb_client = MongoClient(config["MONGODB_URI"])
database = mongodb_client[config["MONGODB_DBNAME"]]

# tls_short = trend_line_explorer.explore(10, 5)

# print([candles[i.x].date.strftime("%Y-%m-%d") for i in pivot_highs])
# print([candles[i.x].date.strftime("%Y-%m-%d") for i in pivot_lows])

level_table = [
    (10000, 10),
    (1000, 8),
    (100, 6),
    (50, 4),
    # (25, 2)
]


def analyze(symbol):
    candles = load_weekly_candles(database, symbol)[:-4]
    levels = []
    for l, f in level_table:
        first_pos, points = find_levels(candles[-l:], f)
        for p in points:
            levels.append(p.y)

    candles = load_daily_candles(database, symbol)
    show_levels(symbol, levels, candles)
    close_ndarr = numpy.array([c.close for c in candles])
    atr = talib.ATR(
        high=numpy.array([c.high for c in candles]),
        low=numpy.array([c.low for c in candles]),
        close=close_ndarr,
    )[-1]
    for l in levels:
        y = l
        c = [c.close for c in candles]
        rising = c[-6] < y - 4 * atr
        falling = c[-6] > y + 4 * atr
        print(y, candles[-1].low, atr, rising, falling)
        collision_high = rising and c[-2] < c[-1] < y and candles[-1].high > y - 1.5*atr
        collision_low = falling and c[-2] > c[-1] > y and candles[-1].low < y + 1.5*atr
        if collision_low or collision_high:
            print(f"Prepare for collision on {symbol}")
            chart_image = ChartImage(symbol, candles, 300)
            chart_image.add_level(l, 'solid', '#12DE12', 1)
            chart_image.show()
            break


def show_levels(symbol, levels, candles):
    chart_image = ChartImage(symbol, candles, 300)
    for l in levels:
        chart_image.add_level(l, 'solid', '#12DE12', 1)
    chart_image.show()


#for smbl in database["symbol"].find({'active': True}):
#    analyze(smbl['_id'])

analyze('NYSE:LUV')
# analyze('NASDAQ:NVDA')

# chart_image = ChartImage(candles, 300)
# for l in tls['high'][-3:]:
#     chart_image.add_line(l, 'solid', '#12DE12', 1)
#     print(candles[l.start.x])
#     print(l)
# for l in tls['low'][-3:]:
#     chart_image.add_line(l, 'solid', '#DE1212', 1)
# chart_image.show()
mongodb_client.close()
