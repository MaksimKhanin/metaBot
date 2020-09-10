import MetaTrader5 as mt5
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta


def extract_bars(connector, symbol, timeframe, date_from, date_to):

    ticks_frame = pd.DataFrame(connector.perform_action(mt5.copy_rates_range,
                                                        symbol,
                                                        timeframe,
                                                        date_from,
                                                        date_to))



    ticks_frame['time'] = pd.to_datetime(ticks_frame['time'], unit='s')
    return ticks_frame


def get_timesnp(connector, validSymbols,
                date_from, date_to,
                index_df, pricetypes=["open", "high", "low", "close", "tick_volume"],
                timeframe=mt5.TIMEFRAME_D1):

    date_from = date_from + timedelta(hours=connector.timedelta)
    date_to = date_to + timedelta(hours=connector.timedelta)

    for each_sym in tqdm(validSymbols):
        try:
            candles = extract_bars(connector, each_sym, timeframe, date_from, date_to).set_index("time")[pricetypes]
            candles.columns = list(map(lambda x: each_sym + "_" + x, candles.columns))
            index_df = index_df.join(candles)
        except:
            print("downdloading ", each_sym, " candles returned empty data")
            continue
    return index_df


def extract_ar(ds, shift, new_column=None):
    if new_column == None:
        new_column = 'ar_' + ds.name + '_' + str(shift)
    new_series = ds - ds.shift(shift)
    new_series.name = new_column
    return new_series

def extract_mean(ds, per, new_column=None):
    if new_column == None:
        new_column = 'mean_' + ds.name + '_' + str(per)
    new_series = ds.rolling(min_periods=per, window=per).mean()
    new_series.name = new_column
    return new_series

def extract_stdev(ds, per, new_column=None):
    if new_column == None:
        new_column = 'stdev_' + ds.name + '_' + str(per)
    new_series = ds.rolling(min_periods=per, window=per).std()
    new_series.name = new_column
    return new_series

def extract_csum(ds, per, new_column=None):
    if new_column == None:
        new_column = 'csum_' + ds.name + '_' + str(per)
    new_series = ds.rolling(min_periods=per, window=per).sum()
    new_series.name = new_column
    return new_series


def extract_max(ds, per, new_column=None):
    if new_column == None:
        new_column = 'max_' + ds.name + '_' + str(per)
    new_series = ds.rolling(min_periods=per, window=per).max()
    new_series.name = new_column
    return new_series


def extract_min(ds, per, new_column=None):
    if new_column == None:
        new_column = 'min_' + ds.name + '_' + str(per)
    new_series = ds.rolling(min_periods=per, window=per).min()
    new_series.name = new_column
    return new_series

def extract_rsi(ds, per, new_column=None):

    if new_column == None:
        new_column = 'RSI_' + str(ds.name) + "_" + str(per)

    delta = ds - ds.shift(1)
    dUp, dDown = delta.copy(), delta.copy()
    dUp[dUp < 0] = 0
    dDown[dDown > 0] = 0

    RolUp = dUp.rolling(min_periods=per, window=per).mean()
    RolDown = dDown.rolling(min_periods=per, window=per).mean().abs()

    RS = RolUp / RolDown
    rsi = 100.0 - (100.0 / (1.0 + RS))
    rsi.name = new_column
    return rsi

def extract_macd(ds, periods=[12,26,9], new_column=None):
    if new_column == None:
        new_column = 'MACD_' + str(ds.name)

    exp1 = ds.ewm(span=periods[0], adjust=False).mean()
    exp2 = ds.ewm(span=periods[1], adjust=False).mean()
    macd = exp1-exp2
    exp3 = macd.ewm(span=periods[2], adjust=False).mean()
    d_macd = macd - exp3
    d_macd.name = new_column
    return d_macd

def extract_TP(close, high, low, new_column=None):
    if new_column == None:
        new_column = 'TypicalPrice'
    new_series = (close + high + low) /3
    new_series.name = new_column
    return new_series

def CCI(TP, per, new_column=None):
    if new_column == None:
        new_column = 'CCI_' + str(per)
    new_series = TP - TP.rolling(min_periods=per, window=per).mean() / (0.015 * TP.rolling(min_periods=per, window=per))
    new_series.name = new_column
    return new_series

def EWMA(ds, per, new_column=None):
    if new_column == None:
        new_column = 'ewma_' + ds.name + '_' + str(per)
    new_series = ds.rolling(min_periods=per, window=per).min()
    new_series.name = new_column
    return new_series

def extract_windowMean(df, columns, column_prefixes=[""], column_postfixes=[""], periods=[6, 12, 24, 120]):

    for feature in columns:
        for prefix in column_prefixes:
            for postfix in column_postfixes:
                columnName = prefix + feature + postfix
                for each_per in periods:
                    try:
                        df = df.join(extract_mean(df, columnName, each_per, new_column=None))
                    except:
                        print("An error occured exctracting " + str(each_per) + " mean from ", columnName)
                        continue
    return df


def extract_windowStdev(df, columns, column_prefixes=[""], column_postfixes=[""], periods=[6, 12, 24, 120]):

    for feature in columns:
        for prefix in column_prefixes:
            for postfix in column_postfixes:
                columnName = prefix + feature + postfix
                for each_per in periods:
                    try:
                        stdev = extract_stdev(df, columnName, each_per, new_column=None)
                        stdev = stdev.replace(0, 0.000001)
                        df = df.join(stdev)
                    except:
                        print("An error occured exctracting " + str(each_per) + " stdev from ", columnName)
                        continue
    return df


def extract_windowCumulativeSum(df, columns, column_prefixes=[""], column_postfixes=[""], periods=[6, 12, 24, 120]):

    for feature in columns:
        for prefix in column_prefixes:
            for postfix in column_postfixes:
                columnName = prefix + feature + postfix
                for each_per in periods:
                    try:
                        diff = pd.DataFrame(df[columnName] - df[columnName].shift(1))
                        df = df.join(extract_csum(diff, columnName, each_per, new_column=None))
                    except:
                        print("An error occured exctracting " + str(each_per) + " csum from ", columnName)
                        continue
    return df

