# -*- coding: utf-8 -*-
"""
Created on Tue Oct 4 2022
@author: Jackson Luo
"""

import requests
import time
import sys
import pandas as pd
import calendar
from datetime import datetime, timedelta


def parse_timeframe(timeframe):
    amount = int(timeframe[0:-1])
    unit = timeframe[-1]
    if 'y' == unit:
        scale = 60 * 60 * 24 * 365
    elif 'M' == unit:
        scale = 60 * 60 * 24 * 30
    elif 'w' == unit:
        scale = 60 * 60 * 24 * 7
    elif 'd' == unit:
        scale = 60 * 60 * 24
    elif 'h' == unit:
        scale = 60 * 60
    elif 'm' == unit:
        scale = 60
    elif 's' == unit:
        scale = 1
    else:
        raise ValueError('timeframe unit {} is not supported'.format(unit))
    
    return amount * scale


def get_unix_ms_from_date(date):
    return int(calendar.timegm(date.timetuple()) * 1000 + date.microsecond / 1000)


def get_oi(symbol, period, from_timestamp, end_timestamp, limit):
    r = requests.get("https://fapi.binance.com/futures/data/openInterestHist",
                     params={
                         "symbol": symbol,
                         "period": period,
                         "limit": limit,
                         "startTime": from_timestamp,
                         "endTime": end_timestamp
                     })

    if r.status_code != 200:
        print('somethings wrong!', r.status_code)
        print('sleeping for 10s... will retry')
        time.sleep(10)
        get_oi(symbol, period, from_timestamp)

    return r.json()


def trim(df, from_date, to_date):
    return df[
        (df['timestamp'] >= get_unix_ms_from_date(from_date)) &
        (df['timestamp'] <= get_unix_ms_from_date(to_date))
    ]


def fetch_binance_oi(symbol, timeframe, from_date, to_date, limit=500):
    print(f'{symbol[0]} Open Interest {timeframe[0]} start fetching')
    timeframe_duration_in_seconds = parse_timeframe(timeframe[0])
    timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
    timedelta = limit * timeframe_duration_in_ms
    start_timestamp = get_unix_ms_from_date(from_date)
    end_timestamp = get_unix_ms_from_date(to_date)
    temp_start_timestamp = end_timestamp - timedelta
    temp_end_timestamp = end_timestamp
    df = pd.DataFrame()

    while True:
        try:
            trades = get_oi(symbol, timeframe, temp_start_timestamp, temp_end_timestamp, limit)

            temp_end_timestamp = trades[0]['timestamp']
            temp_start_timestamp = temp_end_timestamp - timedelta

            print(f'fetched {len(trades)} trades from {datetime.utcfromtimestamp(trades[0]["timestamp"] / 1000.0)} to {datetime.utcfromtimestamp(trades[-1]["timestamp"] / 1000.0)}')

            df = pd.concat([pd.DataFrame(trades), df])

            # dont exceed request limits
            time.sleep(0.5)
            
            if trades[0]['timestamp'] < start_timestamp:
                break
        except Exception as e:
            print(e)
            print('somethings wrong....... sleeping for 15s')
            time.sleep(15)

    df.drop_duplicates(subset='timestamp', inplace=True)
    df = trim(df, from_date, to_date)

    filename = f'oi_data.csv'
    df.to_csv(filename, index=False)

    print(f'{filename} file created!')


if __name__ == "__main__":
    if len(sys.argv) < 4:
        raise Exception('arguments format: <symbol> <timeframe> <start_date> <end_date>')

    symbol = sys.argv[1],
    timeframe = sys.argv[2],
    from_date = datetime.strptime(sys.argv[3], '%m/%d/%Y')
    to_date = datetime.strptime(sys.argv[4], '%m/%d/%Y') + timedelta(days=1) - timedelta(microseconds=1)

    fetch_binance_oi(symbol, timeframe, from_date, to_date)
