import calendar

import boto3
from dotenv import dotenv_values
from pymongo import MongoClient

config = dotenv_values(".env")
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(config['DDB_TABLE_NAME'])

mongodb_client = MongoClient(config["MONGODB_URI"])
database = mongodb_client[config["MONGODB_DBNAME"]]

with table.batch_writer() as batch:
    # for s in database["symbol"].find():
    #     item = s
    #     item['hash'] = 'SYMBOL'
    #     item['sort'] = s['_id']
    #     item['last_append'] = calendar.timegm(s['last_append'].utctimetuple()) if s.get('last_append') else None
    #     item['last_init'] = calendar.timegm(s['last_init'].utctimetuple()) if s.get('last_init') else None
    #     del item['_id']
    #     batch.put_item(
    #         Item=item
    #     )

    for i in database["chart_daily"].find():
        batch.put_item(
            Item={
                'hash': f"DAILY:{i['symbol_id']}",
                'sort': i['date'].strftime('%Y-%m-%d'),
                'volume': str(i['volume']),
                'open': str(i['open']),
                'close': str(i['close']),
                'high': str(i['high']),
                'low': str(i['low']),
            }
        )

mongodb_client.close()
