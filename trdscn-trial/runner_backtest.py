import logging.config
import yaml
from concurrent.futures import ProcessPoolExecutor

import logging.config
from concurrent.futures import ProcessPoolExecutor

import chart_db
from analysis_i import Matcher
from analyzer_headshoulders import HsAlgorithm
from backtest import backtest
from chart_image import ChartImage
from matcher_headshoulders import HeadshouldersMatcher


def backtest_run(symbol, limit, show_chart, matcher: Matcher, algo_factory):
    candles = chart_db.load_daily_candles(symbol, limit)
    signals = matcher.scan(candles, limit)
    chart_image = ChartImage(symbol, candles, len(candles)) if show_chart else None
    algo = algo_factory()
    session = backtest(candles, signals, algo, chart_image)
    trades_count = session.get_closed_trades_count()
    print(f"{symbol} - Profit: {session.get_closed_profit()}; "
          f"Total trades: {trades_count}; "
          f"Profitable: {100 * session.get_win_count() / trades_count if trades_count != 0 else 0}% "
          f"Open position: {session.get_open_position()}")
    if chart_image is not None:
        chart_image.show_if_not_empty()
    return session


def backtest_run_multi(symbol_ids, show_chart, matcher: Matcher, algo_factory):
    futures = []
    with ProcessPoolExecutor(max_workers=10) as executor:
        for i in symbol_ids:
            futures.append(executor.submit(backtest_run, i, 900, show_chart, matcher, algo_factory))

    sum_profit = 0
    sum_trades = 0
    sum_win = 0
    sum_open = 0
    for future in futures:
        session = future.result()
        sum_profit += session.get_closed_profit()
        sum_trades += session.get_closed_trades_count()
        sum_win += session.get_win_count()
        sum_open += session.get_open_position()

    print(f"\n----\nProfit: {sum_profit}; "
          f"Total trades: {sum_trades}; "
          f"Profitable: {100 * sum_win / sum_trades if sum_trades != 0 else 0}% "
          f"Open position: {sum_open}")


# with open('./logging_debug.yaml', 'r') as stream:
#     logging.config.dictConfig(yaml.load(stream, Loader=yaml.FullLoader))

mag8 = ["NASDAQ:NVDA", "NASDAQ:TSLA", "NASDAQ:AAPL", "NASDAQ:MSFT", "NASDAQ:AMZN", "NASDAQ:GOOGL", "NASDAQ:META",
        "NASDAQ:NFLX"]

# backtest_run_multi([s['sort'] for s in chart_db.list_symbols("NASDAQ:")], True, HeadshouldersMatcher(), HsAlgorithm)
backtest_run_multi(mag8, True, HeadshouldersMatcher(), HsAlgorithm)

# backtest_run("NASDAQ:GOOGL", 900, True, HeadshouldersMatcher(), HsAlgorithm)
