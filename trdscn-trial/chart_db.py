import os
from datetime import datetime
from typing import NamedTuple

import boto3
from boto3.dynamodb.conditions import Key, Attr
from dotenv import load_dotenv

load_dotenv()
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['DDB_TABLE_NAME'])


class Candle(NamedTuple):
    open: float
    close: float
    high: float
    low: float
    volume: int
    date: datetime

    def min(self, use_body: bool = False):
        return self.low if not use_body else min(self.open, self.close)

    def max(self, use_body: bool = False):
        return self.high if not use_body else max(self.open, self.close)


def load_daily_candles(symbol_id: str, limit: int = 0):
    condition = Key('hash').eq(f'DAILY:{symbol_id}')
    response = table.query(KeyConditionExpression=condition, ScanIndexForward=False)
    items = response['Items']
    while limit == 0 or len(items) < limit:
        if response.get('LastEvaluatedKey'):
            response = table.query(
                KeyConditionExpression=condition,
                ExclusiveStartKey=response['LastEvaluatedKey'],
                ScanIndexForward=False
            )
            items += response['Items']
        else:
            break
    if limit != 0 and len(items) > limit:
        items = items[:limit]
    items.reverse()
    return [
        Candle(
            open=float(row['open']),
            close=float(row['close']),
            high=float(row['high']),
            low=float(row['low']),
            volume=row['volume'],
            date=datetime.strptime(row['sort'], "%Y-%m-%d")
        )
        for row in items
    ]


def load_weekly_candles(symbol_id: str, limit: int = 0):
    candles = load_daily_candles(symbol_id, limit * 7)
    return __aggregate_candles(candles, lambda date: date.isocalendar()[1])


def __aggregate_candles(candles, interval_func):
    chart = []
    last_interval = None
    interval_open = None
    interval_high = -1.0
    interval_low = float("inf")
    interval_close = None
    interval_volume = 0
    interval_date = None
    for row in candles:
        interval = interval_func(row.date)
        if last_interval is None:
            last_interval = interval
            interval_date = row.date
        if interval != last_interval:
            last_interval = interval
            chart.append(
                Candle(open=interval_open, close=interval_close, high=interval_high, low=interval_low,
                       volume=interval_volume, date=interval_date)
            )
            interval_open = None
            interval_high = -1.0
            interval_low = float("inf")
            interval_volume = 0
        if interval_open is None:
            interval_open = row.open
            interval_date = row.date
        if row['high'] > interval_high:
            interval_high = row.high
        if row['low'] < interval_low:
            interval_low = row.low
        interval_close = row.close
        interval_volume += row.volume

    # STILL OPEN Candle:
    if last_interval is not None:
        chart.append(
            Candle(open=interval_open, close=interval_close, high=interval_high, low=interval_low,
                   volume=interval_volume, date=interval_date)
        )

    return chart


def list_symbols(begins_with: str = None):
    condition = Key('hash').eq('SYMBOL')
    if begins_with is not None:
        condition = condition & Key('sort').begins_with(begins_with)
    return table.query(KeyConditionExpression=condition, FilterExpression=Attr('active').eq(True))['Items']


def list_symbols_for_group(group: str = None):
    condition = Key('hash').eq('SYMBOL')
    return table.query(
        KeyConditionExpression=condition,
        FilterExpression=Attr('active').eq(True) & Attr('groups').contains(group)
    )['Items']
