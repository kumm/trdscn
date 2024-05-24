import os
from datetime import datetime

import boto3
import pytz
from binance.um_futures import UMFutures
from boto3.dynamodb.conditions import Key, Attr
from dotenv import load_dotenv

load_dotenv()
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['DDB_TABLE_NAME'])

um_futures_client = UMFutures()


def parse_timestamp(timestamp):
    return datetime.fromtimestamp(int(timestamp / 1000), tz=pytz.utc)


def put_candles(symbol_id, candles):
    with table.batch_writer() as batch:
        for c in candles:
            batch.put_item(Item=map_candle(symbol_id, c))


def map_candle(symbol_id, rec):
    return dict(
        hash=f"DAILY:{symbol_id}",
        sort=parse_timestamp(rec[0]).strftime('%Y-%m-%d'),
        open=rec[1],
        close=rec[4],
        low=rec[3],
        high=rec[2],
        volume=rec[5]
    )


def load():
    all_symbol = table.query(
        KeyConditionExpression=Key('hash').eq('SYMBOL') & Key('sort').begins_with('BNNCUMF'),
        FilterExpression=Attr('active').eq(True)
    )['Items']

    for smbl in all_symbol:
        start_time = smbl['last_append'] if smbl.get('last_append') is not None else 0
        print(f"Loading {smbl['sort']} "
              f"from {parse_timestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}...")
        candles = um_futures_client.klines(symbol=smbl['symbol'], interval="1d", limit=1500, startTime=start_time)
        put_candles(smbl['sort'], candles)
        table.update_item(
            Key={'hash': smbl['hash'], 'sort': smbl['sort']},
            UpdateExpression="set last_append = :last",
            ExpressionAttributeValues={':last': candles[-1][0]}
        )


def lambda_handler(event, context):
    load()


if __name__ == '__main__':
    # print(um_futures_client.exchange_info())
    load()
