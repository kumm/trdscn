from datetime import datetime

import fmpsdk
import pymongo
from dotenv import dotenv_values
from fmpsdk.url_methods import __return_json_v3
from pymongo import MongoClient

config = dotenv_values(".env")
mongodb_client = MongoClient(config["MONGODB_URI"])
database = mongodb_client[config["MONGODB_DBNAME"]]

stock_exchanges = ['NASDAQ', 'NYSE']


def map_chart_item(i, symbol_id):
    return {
        '_id': symbol_id + '@' + i['date'],
        'open': i['open'],
        'close': i['close'],
        'low': i['low'],
        'high': i['high'],
        'volume': i['volume'],
        'symbol_id': symbol_id,
        'date': datetime.strptime(i['date'], "%Y-%m-%d")
    }


def init_chart(symbol_id):
    chart = fmpsdk.historical_price_full(
        apikey=config["FMP_API_KEY"],
        symbol=symbol_id.split(":")[1],
        from_date="1970-01-01",
    )
    database["chart_daily"].delete_many({'symbol_id': symbol_id})
    database["chart_daily"].insert_many([map_chart_item(i, symbol_id) for i in chart])
    database["symbol"].update_one({'_id': symbol_id}, {'$set': {'last_init': datetime.now()}})


def append_to_chart(exchange, rec):
    date = datetime.fromtimestamp(rec['timestamp'])
    symbol_id = f"{exchange}:{rec['symbol']}"
    id = f"{symbol_id}@{date.strftime('%Y-%m-%d')}"
    database["chart_daily"].update_one(
        filter={'_id': id},
        update={'$set': {
            'open': rec['open'],
            'close': rec['price'],
            'low': rec['dayLow'],
            'high': rec['dayHigh'],
            'volume': rec['volume'],
            'symbol_id': symbol_id,
            'date': date
        }},
        upsert=True
    )
    database["symbol"].update_one({'_id': symbol_id}, {'$set': {'last_append': datetime.now()}})


symbols_by_exchange = {}
all_symbol = [
    s for s in database["symbol"].find({
        'active': True,
        'exchange': {'$in': stock_exchanges}
    }).sort('last_init', pymongo.ASCENDING)
]

for smbl in all_symbol:
    exchange_ = smbl['exchange']
    if symbols_by_exchange.get(exchange_) is None:
        symbols_by_exchange[exchange_] = []
    symbols_by_exchange[exchange_].append(smbl['symbol'])

for exchange, symbols in symbols_by_exchange.items():
    print(f"Loading exchange data for {exchange} ...")
    query_vars = {"apikey": config["FMP_API_KEY"]}
    data = __return_json_v3(path=f"symbol/{exchange}", query_vars=query_vars)
    m = {k['symbol']: k for k in data}
    for symbol in symbols:
        if m.get(symbol) is not None:
            append_to_chart(exchange, m[symbol])

for smbl in all_symbol[0:100]:
    print(f"Loading full chart for {smbl['_id']} ...")
    init_chart(smbl['_id'])

mongodb_client.close()
