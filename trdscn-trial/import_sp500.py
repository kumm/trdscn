import csv

import fmpsdk
from dotenv import dotenv_values
from pymongo import MongoClient

config = dotenv_values(".env")
mongodb_client = MongoClient(config["MONGODB_URI"])
database = mongodb_client[config["MONGODB_DBNAME"]]

exchange_map = {
    k['symbol']: k['exchangeShortName'] for k in fmpsdk.symbols_list(config["FMP_API_KEY"])
}


def map_csv_row(row):
    name = row[0]
    symbol = row[1].replace('.', '-')
    exchange = exchange_map[symbol]
    return {
        '_id': f"{exchange}:{symbol}",
        'name': name,
        'symbol': symbol,
        'exchange': exchange,
        'active': True
    }


with open('SP-500-Companies-List.csv', newline='') as csvfile:
    rows = csv.reader(csvfile, delimiter=',', quotechar='"')
    database["symbol"].insert_many([map_csv_row(row) for row in rows])

mongodb_client.close()
