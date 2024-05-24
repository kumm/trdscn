import json
import base64
from datetime import datetime

import fmpsdk
from dotenv import dotenv_values
from pymongo import MongoClient

config = dotenv_values(".env")
mongodb_client = MongoClient(config["MONGODB_URI"])
database = mongodb_client[config["MONGODB_DBNAME"]]

base64.b64encode()

# Company Valuation Methods
#symbol: str = "AAPL"
# print(f"Company Profile: {fmpsdk.company_profile(apikey=apikey, symbol=symbol)}")

# chart = fmpsdk.historical_price_full(
#     apikey=config["FMP_API_KEY"],
#     symbol=symbol,
#     from_date="1970-01-01",
#     #    to_date="2023-10-30"
# )
#
# print(json.dumps(chart, indent=3))


# database["chart_daily"].insert_many([map_chart_item(i, "NASDAQ:AAPL") for i in chart])


# path = f"symbol/NASDAQ"
# query_vars = {"apikey": config["FMP_API_KEY"]}
# ndq = __return_json_v3(path=path, query_vars=query_vars)

# def map_symbol(s):
#     return {
#         '_id': s['exchange'] + ':' + s['symbol'],
#         'symbol': s['symbol'],
#         'exchange': s['exchange'],
#         'name': s['name']
#     }


# print(json.dumps(ndq, indent=3))

# print(fmpsdk.symbols_list(apikey))


# database["symbol"].insert_many(list(map(map_symbol, ndq)))
# database["symbol"].delete_many({})

# database["symbol"].insert_one({
#     '_id':"NASDAQ:AAPL",
#     'exchange':'NASDAQ',
#     'symbol':'AAPL',
#     'name':'Apple',
#     'active':True
# })

# database["chart_daily"].create_index("symbol_id")

print(fmpsdk.market_hours(config["FMP_API_KEY"]))

mongodb_client.close()
