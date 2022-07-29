import pandas as pd
import datetime
import ccxt
from tqdm import tqdm

def listToDataframe(dataUnprocess):
    data_df = pd.DataFrame(dataUnprocess)
    data_df.columns = ['TimeStamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    data_df['Datetime'] = data_df['TimeStamp'].apply(lambda x: datetime.datetime.fromtimestamp(x//1000).strftime("%Y-%m-%d %H:%M:%S"))
    data_df['Datetime'] = pd.to_datetime(data_df['Datetime']) - datetime.timedelta(hours=8) #轉為UTC+0的時區
    # data_df['Date'] = data_df['Datetime'].apply(lambda x: str(x)[:10])
    # data_df['Time'] = data_df['Datetime'].apply(lambda x: str(x)[11:])
    # data_df['Time'] = data_df['Time'].apply(lambda x: str(x)[:5])
    data_df = data_df.set_index('Datetime')
    # data_df = data_df.drop(columns=['TimeStamp', 'Date', 'Time'])
    data_df = data_df[['Open', 'High', 'Low', 'Close', 'Volume']]
    return data_df

def retry_fetch_ohlcv(exchange, max_retries, symbol, timeframe, since, limit):
    num_retries = 0
    try:
        num_retries += 1
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
        # print('Fetched', len(ohlcv), symbol, 'candles from', exchange.iso8601 (ohlcv[0][0]), 'to', exchange.iso8601 (ohlcv[-1][0]))
        return ohlcv
    except Exception:
        if num_retries > max_retries:
            raise  # Exception('Failed to fetch', timeframe, symbol, 'OHLCV in', max_retries, 'attempts')

def scrape_ohlcv(exchange, max_retries, symbol, timeframe, since, limit):
    earliest_timestamp = exchange.milliseconds()
    timeframe_duration_in_seconds = exchange.parse_timeframe(timeframe)
    timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
    timedelta = limit * timeframe_duration_in_ms
    all_ohlcv = []
    while True:
        fetch_since = earliest_timestamp - timedelta
        ohlcv = retry_fetch_ohlcv(exchange, max_retries, symbol, timeframe, fetch_since, limit)
        if not ohlcv:
            print(symbol, "is empty.")
            return None
        # if we have reached the beginning of history
        if ohlcv[0][0] >= earliest_timestamp:
            break
        earliest_timestamp = ohlcv[0][0]
        all_ohlcv = ohlcv + all_ohlcv
        print(len(all_ohlcv), symbol, 'candles in total from', exchange.iso8601(all_ohlcv[0][0]), 'to', exchange.iso8601(all_ohlcv[-1][0]))
        # if we have reached the checkpoint
        if fetch_since < since:
            break
    return all_ohlcv

def scrape_symbol(exchange_id, max_retries, symbol, timeframe, since, limit, data_type):
    # instantiate the exchange by id
    exchange = getattr(ccxt, exchange_id)({
        'enableRateLimit': True,  # required by the Manual
        'options': {
            'defaultType': data_type
        }
    })
    # convert since from string to milliseconds integer if needed
    if isinstance(since, str):
        since = exchange.parse8601(since)
    # preload all markets from the exchange
    exchange.load_markets()
    # fetch all candles
    ohlcv = scrape_ohlcv(exchange, max_retries, symbol, timeframe, since, limit)
    # convert list to dataframe
    ohlcv = listToDataframe(ohlcv) if ohlcv else None
    # remove rows with duplicate indices
    return ohlcv[~ohlcv.index.duplicated(keep='first')] if ohlcv else None


def scrape_multiple_symbols(exchange_id, max_retries, symbols, timeframe, since, limit, data_type):
    # instantiate the exchange by id
    if data_type == None:
        exchange = getattr(ccxt, exchange_id)()
    elif data_type == 'future':
        exchange = getattr(ccxt, exchange_id)({
            'enableRateLimit': True,  # required by the Manual
            'options': {
                'defaultType': data_type
            }
        })
    # convert since from string to milliseconds integer if needed
    if isinstance(since, str):
        since = exchange.parse8601(since)
    # prepare the dict store all symbols ohlcv
    klines_dict = {}
    for symbol in tqdm(symbols):
        print(symbol)
        # fetch all candles
        ohlcv = scrape_ohlcv(exchange, max_retries, symbol, timeframe, since, limit)
        # convert list to dataframe
        temp_data_df = listToDataframe(ohlcv) if ohlcv else None
        if temp_data_df is not None:
            # remove rows with duplicate indices
            temp_data_df = temp_data_df[~temp_data_df.index.duplicated(keep='first')]
            # add to the dict
            klines_dict[symbol] = temp_data_df
    # return klines_dict
    return klines_dict

if __name__ == '__main__':
    trading_pairs = ['KEEPUSDT', 'ETHUSDT']
    klines_dict = scrape_multiple_symbols("binance", 3, trading_pairs, "4h", '2022-01-01 00:00:00Z', 100, 'spot')
