import boto3
import csv

from boto3.dynamodb.conditions import Key, Attr
from dotenv import dotenv_values
from fmpsdk.url_methods import __return_json_v3, __validate_series_type

config = dotenv_values(".env")
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(config['DDB_TABLE_NAME'])

group = "SPX"
csv_file = 'SP500.csv'
csv_ticker_col = 0
exchanges = ['NYSE', 'NASDAQ', 'AMEX']

# [
#     {
#         "symbol": "SRDX",
#         "name": "Surmodics, Inc.",
#         "price": 22.1,
#         "changesPercentage": 0.7752,
#         "change": 0.17,
#         "dayLow": 21.76,
#         "dayHigh": 22.3,
#         "yearHigh": 45.85,
#         "yearLow": 21.61,
#         "marketCap": 312162500,
#         "priceAvg50": 30.0696,
#         "priceAvg200": 33.5478,
#         "exchange": "NASDAQ",
#         "volume": 51517,
#         "avgVolume": 72111,
#         "open": 21.76,
#         "previousClose": 21.93,
#         "eps": -2.23,
#         "pe": -9.91,
#         "earningsAnnouncement": "2023-04-25T12:30:00.000+0000",
#         "sharesOutstanding": 14125000,
#         "timestamp": 1677789927
#     }
# ]

total = 0
found = set()


def import_symbol(exchange, rec, batch_writer):
    ticker = rec['symbol'].replace('-', '.')
    symbol_id = exchange + ":" + ticker
    global symbols
    if symbol_id in symbols:
        print(f"- updating symbol: {symbol_id}")
        table.update_item(
            Key={'hash': 'SYMBOL', 'sort': symbol_id},
            UpdateExpression="ADD groups :group SET market_cap = :market_cap, avg_vol = :avg_vol, active = :active",
            ExpressionAttributeValues={
                ":group": {group},
                ":active": True,
                ':market_cap': rec['marketCap'],
                ':avg_vol': rec['avgVolume'],
            }
        )
    else:
        print(f"- adding symbol: {symbol_id}")
        batch_writer.put_item(Item={
            'hash': 'SYMBOL',
            'sort': symbol_id,
            'exchange': exchange,
            'symbol': ticker,
            'name': rec['name'],
            'active': True,
            'market_cap': rec['marketCap'],
            'avg_vol': rec['avgVolume'],
            'loader': 'FMP',
            'groups': {group}
        })
    found.add(rec['symbol'])


def process_exchange(exchange, tickers, batch_writer):
    global total
    print(f"Downloading exchange data {exchange} ...")
    query_vars = {"apikey": config["FMP_API_KEY"]}
    data = __return_json_v3(path=f"symbol/{exchange}", query_vars=query_vars)
    for sym in data:
        if sym['symbol'] in tickers:
            import_symbol(exchange, sym, batch_writer)
            total = total + 1


def load_symbols():
    all_symbol = table.query(
        KeyConditionExpression=Key('hash').eq('SYMBOL'),
        FilterExpression=Attr('active').eq(True)
    )['Items']
    return {i['sort'] for i in all_symbol}


with open(csv_file, newline='') as csvfile:
    rows = csv.reader(csvfile, delimiter=';', quotechar='"')
    tickers = {r[csv_ticker_col].replace('.', '-') for r in rows}

print(f"Importing {len(tickers)} to group '{group}' ...")

symbols = load_symbols()
with table.batch_writer() as batch:
    for exchange in exchanges:
        process_exchange(exchange, tickers, batch)

print(f"Imported {total} tickers")
missing = tickers - found
if len(missing) > 0:
    print(f"Not found some tickers: {missing}")
