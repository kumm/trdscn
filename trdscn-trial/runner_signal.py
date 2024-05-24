from concurrent.futures import ProcessPoolExecutor

import chart_db
from analyzer import Analyzer
from analyzer_bounce import BounceAnalyzer
from analyzer_headshoulders import HsAnalyzer
from chart_image import ChartImage


def show_signals(symbol, analyzer: Analyzer, recent_candles: int = 0):
    print(symbol)
    candles = chart_db.load_daily_candles(symbol, 900)
    chart_image = ChartImage(symbol, candles, 900)
    for s in analyzer.collect_signals(candles, recent_candles > 0):
        if recent_candles == 0 or s.get_completion_bar_index() >= len(candles) - 1 - recent_candles:
            s.show(chart_image)
    chart_image.show_if_not_empty()


def show_signals_multi(symbols, analyzer_factory, recent_candles: int = 0):
    with ProcessPoolExecutor(max_workers=10) as executor:
        for s in symbols:
            executor.submit(show_signals, s, analyzer_factory(), recent_candles)


#show_signals_multi([s['sort'] for s in chart_db.list_symbols()], HsAnalyzer,2)
# show_signals("NYSE:DG", BounceAnalyzer())
show_signals("NASDAQ:TSLA", HsAnalyzer())
