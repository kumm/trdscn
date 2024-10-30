import os
import typing
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key, Attr
from dotenv import load_dotenv
from fmpsdk.url_methods import __return_json_v3, __validate_series_type

load_dotenv()
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['DDB_TABLE_NAME'])

def historical_price_full(
        apikey: str,
        symbol: typing.Union[str, typing.List],
        time_series: int = None,
        series_type: str = None,
        from_date: str = None,
        to_date: str = None,
) -> typing.Optional[typing.List[typing.Dict]]:
    if type(symbol) is list:
        symbol = ",".join(symbol)
    path = f"historical-price-full/{symbol.replace('.','-')}"
    query_vars = {
        "apikey": apikey,
    }
    if time_series:
        query_vars["timeseries"] = time_series
    if series_type:
        query_vars["serietype"] = __validate_series_type(series_type)
    if from_date:
        query_vars["from"] = from_date
    if to_date:
        query_vars["to"] = to_date

    res = __return_json_v3(path=path, query_vars=query_vars)

    if type(res) is list:
        return []
    else:
        return res.get("historicalStockList", res.get('historical', None))


def init_chart(symbol_id, from_timestamp):
    from_date = datetime.fromtimestamp(from_timestamp / 1000).strftime('%Y-%m-%d')
    chart = historical_price_full(
        apikey=os.environ["FMP_API_KEY"],
        symbol=symbol_id.split(":")[1],
        from_date=from_date,
    )
    conflict = {}
    with table.batch_writer() as batch:
        prev = None
        for i in chart:
            if prev is None or i['date'] != prev['date']:
                batch.put_item(Item=dict(
                    hash=f"DAILY:{symbol_id}",
                    sort=i['date'],
                    open=str(i['open']),
                    close=str(i['close']),
                    low=str(i['low']),
                    high=str(i['high']),
                    volume=str(i['volume']),
                ))
            else:
                if conflict.get(i['date']) is None:
                    conflict[i['date']] = []
                conflict[i['date']].append(prev)
                conflict[i['date']].append(i)
                print(f"conflict DAILY:{symbol_id} sort {prev['date']}, ochl {prev['open']} {prev['close']} {prev['high']} {prev['low']}, vol {prev['volume']}")
                print(f"conflict DAILY:{symbol_id} sort {i['date']}, ochl {i['open']} {i['close']} {i['high']} {i['low']}, vol {i['volume']}")
            prev = i
    table.update_item(
        Key={'hash': 'SYMBOL', 'sort': symbol_id},
        UpdateExpression="set last_init = :last",
        ExpressionAttributeValues={':last': int(datetime.utcnow().timestamp() * 1000)}
    )

# def merge_conflicts(conflicts, symbol):
#     for date,l in conflicts:
#         table.update_item(
#             Key={'hash': symbol, 'sort': date},
#             UpdateExpression="set open = :o, close = :c, high = :h, low = :l",
#             ExpressionAttributeValues={
#                 ':o' : max(map(lambda r: r['open'])),
#                 ':c' : max(prev['close'], i['close']),
#                 ':h' : max(prev['high'], i['high'], prev['open'], i['open'], prev['close'], i['close'], prev['low'], i['low']),
#                 ':l' : min(prev['low'], i['low']) if min(prev['low'], i['low']) > 0 else max(prev['low'], i['low']),
#             }
#         )
#

def append_to_chart(exchange, rec):
    date = datetime.fromtimestamp(rec['timestamp'])
    symbol_id = f"{exchange}:{rec['symbol']}"
    strdate = date.strftime('%Y-%m-%d')
    table.put_item(
        Item=dict(
            hash=f"DAILY:{symbol_id}",
            sort=strdate,
            open=str(rec['open']),
            close=str(rec['price']),
            low=str(rec['dayLow']),
            high=str(rec['dayHigh']),
            volume=str(rec['volume']),
        )
    )
    table.update_item(
        Key={'hash': 'SYMBOL', 'sort': symbol_id},
        UpdateExpression="SET last_append = :last, market_cap = :market_cap, avg_vol = :avg_vol, loader = :loader",
        ExpressionAttributeValues={
            ':last': int(rec['timestamp'] * 1000),
            ':market_cap': rec['marketCap'],
            ':avg_vol': rec['avgVolume'],
            ':loader': 'FMP'
        }
    )
    print(f"Appended to {symbol_id} on {strdate}")



def get_symbols():
    all_symbol = table.query(
        KeyConditionExpression=Key('hash').eq('SYMBOL') & Key('sort').begins_with('NASDAQ'),
        FilterExpression=Attr('active').eq(True)
    )['Items']
    all_symbol.extend(
        table.query(
            KeyConditionExpression=Key('hash').eq('SYMBOL') & Key('sort').begins_with('NYSE'),
            FilterExpression=Attr('active').eq(True)
        )['Items']
    )
    all_symbol.extend(
        table.query(
            KeyConditionExpression=Key('hash').eq('SYMBOL') & Key('sort').begins_with('AMEX'),
            FilterExpression=Attr('active').eq(True)
        )['Items']
    )
    return all_symbol


def load_exchanges(symbols):
    symbols_by_exchange = {}

    for smbl in symbols:
        exchange_ = smbl['exchange']
        if symbols_by_exchange.get(exchange_) is None:
            symbols_by_exchange[exchange_] = []
        symbols_by_exchange[exchange_].append(smbl['symbol'])

    for exchange, symbols in symbols_by_exchange.items():
        print(f"Loading exchange data for {exchange} ...")
        query_vars = {"apikey": os.environ["FMP_API_KEY"]}
        data = __return_json_v3(path=f"symbol/{exchange}", query_vars=query_vars)
        m = {k['symbol'].replace('-', '.'): k for k in data}
        for symbol in symbols:
            if m.get(symbol) is not None:
                append_to_chart(exchange, m[symbol])

def map_last_init(rec):
    l = rec.get('last_init', 0)
    return 0 if l is None else int(l)

def load_history(symbols, limit):
    sorted_symbols = symbols.copy()
    sorted_symbols.sort(key=lambda r: map_last_init(r))
    for smbl in sorted_symbols[0:limit]:
        print(f"Loading full chart for {smbl['sort']} ...")
        init_chart(smbl['sort'], map_last_init(smbl))


def lambda_handler(event, context):
    all_symbol = get_symbols()
    load_exchanges(all_symbol)
    load_history(all_symbol, int(os.environ['CHART_LOAD_LIMIT']))


if __name__ == '__main__':
    all_symbol = get_symbols()
    load_exchanges(all_symbol[0:2])
