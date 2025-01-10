"""This package has two useful functions:
1. ticker_finder helps you find the right pair name to use in the ohlcv_data_download function. See examples at the bottom after function definition.
2. ohlcv_data_download downloads candlestick data for a given pair and time period. It also converts the quote currency to USDT. See examples at the bottom after function definition."""

import requests
import pandas as pd
import numpy as np
import ccxt
from datetime import datetime, timedelta, timezone
import time
import asyncio


def match_finder(search_key, search_dictionary):
    """Supporting function. Finds closest match in a dictionary to a given key"""
    try:
        return search_dictionary[search_key]
    except:
        return search_dictionary[
            min(list(search_dictionary.keys()), key=lambda x: abs(x - search_key))
        ]
    
def ticker_finder(exchange, base_token):
    """finds all the pairs that have base_token as the base currency - use token symbol e.g. BTC, not the name e.g. Bitcoin
    supported values for exchange parameter: binance, upbit, bithumb, coinbaseprime, huobi, okx, gateio, bybit, kucoin, mexc, bitget"""

    exchange = eval(f"ccxt.{exchange}()")
    exchange.enableRateLimit = True
    exchange.load_markets()
    pairs = []
    for item in exchange.symbols:
        if '1000' not in item: #this is to deal with bybit pairs that have 1000 in the name
            if base_token == item[: item.find("/")]:
                pairs.append(item)
        else: #if 1000 is in the pair name, just do a string search - this approach is not perfect because looking for T will also match with BTC, MATIC etc.
            if base_token in item[: item.find("/")]:
                pairs.append(item)

    print('\nFollowing are the matching pairs we found on this exchange.\nPlease compare the listed base and quote volumes with the ones on the exchange website to verify the correct pair name to use in the ohlcv_data_download function.\nThis is especially relevant for differentiating between spot and futures.\n')

    for pair in pairs:
        try:
            pair_data = exchange.fetch_ticker(pair)
            print(pair, '\n', 'Base volume:', pair_data['baseVolume'], '\n', 'Quote volume:', pair_data['quoteVolume'], '\n')
        except:
            continue

def ohlcv_data_download(
    exchange,
    pair,
    period_start,
    period_end,
    candle_duration_seconds_data_download,
    candle_duration_seconds_result_output,
):
    """use the pair name from the ticker_finder function output.
    supported values for exchange parameter: binance, upbit, bithumb, coinbaseprime, huobi, okx, gateio, bybit, kucoin, mexc, bitget
    Time period format should be like this '2023-02-22 00:00:00+00:00' and should be in UTC timezone
    The bigger the input candlestick, the more information we lose and the faster the code runs. Only column impacted by using larger inputs is for average price.
    candle_duration_seconds_data_download options can be found in exchange_dict below
    candle_duration_seconds_result_output should be equal or larger than candle_duration_seconds_data_download in terms of seconds
    """

    candle_duration_to_seconds = {
        "1s": 1,
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "1d": 3600 * 24,
        "1w": 3600 * 24 * 7,
        "1M": 3600 * 24 * 30,
        "30d": 3600 * 24 * 30,
    }

    if (
        candle_duration_seconds_result_output
        < candle_duration_to_seconds[candle_duration_seconds_data_download]
    ):
        raise Exception(
            "candle_duration_seconds_result_output should be equal or larger than candle_duration_seconds_data_download in terms of seconds"
        )

    exchange_dict = {
        "binance": {
            "name": "Binance",
            "max_data_points": 500,
            "intervals": {
                "1s": 1,
                "1m": 60,
                "5m": 300,
                "15m": 900,
                "30m": 1800,
                "1h": 3600,
                "1d": 3600 * 24,
                "1w": 3600 * 24 * 7,
                "1M": 3600 * 24 * 30,
            },
        },
        "upbit": {
            "name": "Upbit",
            "max_data_points": 200,
            "intervals": {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "1d": 3600 * 24},
        },
        "bithumb": {
            "name": "Bithumb",
            "max_data_points": 500,
            "intervals": {"30m": 1800, "1h": 3600},
        },
        "coinbase": {
            "name": "Coinbase",
            "max_data_points": 100,
            "intervals": {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 3600 * 24},
        },
        "huobi": {
            "name": "Huobi",
            "max_data_points": 100,
            "intervals": {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "1d": 3600 * 24, "1w": 3600 * 24 * 7, "1M": 3600 * 24 * 30},
        },
        "okx": {
            "name": "OKX",
            "max_data_points": 100,
            "intervals": {
                "1s": 1,
                "1m": 60,
                "5m": 300,
                "15m": 900,
                "30m": 1800,
                "1h": 3600,
                "1d": 3600 * 24,
                "1w": 3600 * 24 * 7,
                "1M": 3600 * 24 * 30,
            },
        },
        "gate": {
            "name": "Gate.io",
            "max_data_points": 500,
            "intervals": {"5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "1d": 3600 * 24, "1w": 3600 * 24 * 7, "30d": 3600 * 24 * 30},
        },
        "bybit": {
            "name": "Bybit",
            "max_data_points": 150,
            "intervals": {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "1d": 3600 * 24, "1w": 3600 * 24 * 7, "1M": 3600 * 24 * 30},
        },
        "kucoin": {
            "name": "Kucoin",
            "max_data_points": 1000,
            "intervals": {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "1d": 3600 * 24, "1w": 3600 * 24 * 7, "1M": 3600 * 24 * 30},
        },
        "mexc": {
            "name": "MEXC",
            "max_data_points": 100,
            "intervals": {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "1d": 3600 * 24, "1M": 3600 * 24 * 30},
        },
        "bitget": {
            "name": "Bitget",
            "max_data_points": 200,
            "intervals": {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "1d": 3600 * 24, "1w": 3600 * 24 * 7, "1M": 3600 * 24 * 30},
        },
    }

    try:
        exchange_name = exchange_dict[exchange]["name"]
    except:
        raise Exception('this exchange is not supported')
    
    max_data_points = exchange_dict[exchange]["max_data_points"]

    try:
        intervals = exchange_dict[exchange]["intervals"][
            candle_duration_seconds_data_download
        ]
    except:
        raise Exception(
            f"this time interval ({candle_duration_seconds_data_download}) is not supported"
        )

    exchange = eval(f"ccxt.{exchange}()")
    exchange.enableRateLimit = True
    exchange.load_markets()

    base = pair[: pair.find("/")]
    quote = pair[
        pair.find("/") + 1 :
    ]  # if quote is not USDT, we have to convert it to USDT

    #for futures pairs, there is usually a colon in the pair name so get the right quote name e.g. EDU/USDT:USDT needs to be changed to EDU/USDT
    if ":" in quote:
        quote = quote[:quote.find(':')]

    period_start_timestamp = datetime.fromisoformat(period_start).replace(tzinfo=timezone.utc).timestamp() - 1
    period_end_timestamp = datetime.fromisoformat(period_end).replace(tzinfo=timezone.utc).timestamp() + 1

    # collect OHLCV data for the pair
    data_list = []
    increment = 0
    while True:
        data = exchange.fetchOHLCV(
            pair,
            timeframe=candle_duration_seconds_data_download,
            since=int(
                (
                    period_start_timestamp
                    + max_data_points
                    * increment
                    * candle_duration_to_seconds[candle_duration_seconds_data_download]
                )
                * 1000
            ),
        )
        data_list += data

        if len(data) == 0:
            break

        if (
            data[-1][0] / 1000 > period_end_timestamp
        ):  # collected all data up to the end of the period
            break

        increment += 1
        time.sleep(0.5)

    data_list = [
        i
        for i in data_list
        if i[0] / 1000 <= period_end_timestamp and i[0] / 1000 >= period_start_timestamp
    ]

    df_list = []
    for itm in data_list:
        df_list.append(
            [
                exchange_name,
                pair,
                base,
                quote,
                datetime.fromtimestamp(itm[0] / 1000, tz=timezone.utc),
                (itm[1] + itm[4]) / 2,
                itm[1],
                itm[4],
                itm[2],
                itm[3],
                itm[5],
            ]
        )

    vol_data = pd.DataFrame(
        df_list,
        columns=[
            "exchange_name",
            "pair",
            "base",
            "quote",
            "timestamp",
            "average_price",
            "open",
            "close",
            "high",
            "low",
            "volume_base",
        ],
    )
    vol_data = vol_data.drop_duplicates().reset_index(drop=True)

    if vol_data["volume_base"].sum() == 0:  # to get rid of pairs that dont exist
        print("no volume data for this time period -", pair)
        raise Exception

    quote_list = []
    increment = 0
    if quote != "USDT":  # convert quote to USDT
        try:
            while True:
                data = exchange.fetchOHLCV(
                    f"{quote}/USDT",
                    timeframe=candle_duration_seconds_data_download,
                    since=int(
                        (
                            period_start_timestamp
                            + max_data_points
                            * increment
                            * candle_duration_to_seconds[
                                candle_duration_seconds_data_download
                            ]
                        )
                        * 1000
                    ),
                )
                quote_list += data

                if len(data) == 0:
                    break

                if data[-1][0] / 1000 > period_end_timestamp:
                    break

                increment += 1

            quote_list = [
                i
                for i in quote_list
                if i[0] / 1000 <= period_end_timestamp
                and i[0] / 1000 >= period_start_timestamp
            ]
            quote_dict = {
                datetime.fromtimestamp(itm[0] / 1000, tz=timezone.utc): (itm[1] + itm[4]) / 2
                for itm in quote_list
            }

            vol_data["quote_currency_price_usdt"] = vol_data["timestamp"].apply(
                lambda x: match_finder(x, quote_dict)
            )
            vol_data["average_price_usdt"] = (
                vol_data["average_price"] * vol_data["quote_currency_price_usdt"]
            )
            vol_data["open_usdt"] = (
                vol_data["open"] * vol_data["quote_currency_price_usdt"]
            )
            vol_data["close_usdt"] = (
                vol_data["close"] * vol_data["quote_currency_price_usdt"]
            )
            vol_data["high_usdt"] = (
                vol_data["high"] * vol_data["quote_currency_price_usdt"]
            )
            vol_data["low_usdt"] = (
                vol_data["low"] * vol_data["quote_currency_price_usdt"]
            )

        except (
            Exception
        ) as e:  # when the pair isnt found and you have to find the opposite pair
            print(e)
            try:
                while True:
                    data = exchange.fetchOHLCV(
                        f"USDT/{quote}",
                        timeframe=candle_duration_seconds_data_download,
                        since=int(
                            (
                                period_start_timestamp
                                + max_data_points
                                * increment
                                * candle_duration_to_seconds[
                                    candle_duration_seconds_data_download
                                ]
                            )
                            * 1000
                        ),
                    )
                    quote_list += data

                    if len(data) == 0:
                        break

                    if data[-1][0] / 1000 > period_end_timestamp:
                        break

                    increment += 1

                quote_list = [
                    i
                    for i in quote_list
                    if i[0] / 1000 <= period_end_timestamp
                    and i[0] / 1000 >= period_start_timestamp
                ]
                quote_dict = {
                    datetime.fromtimestamp(itm[0] / 1000, tz=timezone.utc): (itm[1] + itm[4]) / 2
                    for itm in quote_list
                }

                vol_data["quote_currency_price_usdt"] = vol_data["timestamp"].apply(
                    lambda x: match_finder(x, quote_dict)
                )
                vol_data["quote_currency_price_usdt"] = (
                    1 / vol_data["quote_currency_price_usdt"]
                )  # because we found the reverse pair
                vol_data["average_price_usdt"] = (
                    vol_data["average_price"] * vol_data["quote_currency_price_usdt"]
                )
                vol_data["open_usdt"] = (
                    vol_data["open"] * vol_data["quote_currency_price_usdt"]
                )
                vol_data["close_usdt"] = (
                    vol_data["close"] * vol_data["quote_currency_price_usdt"]
                )
                vol_data["high_usdt"] = (
                    vol_data["high"] * vol_data["quote_currency_price_usdt"]
                )
                vol_data["low_usdt"] = (
                    vol_data["low"] * vol_data["quote_currency_price_usdt"]
                )

            except Exception as e:  # cant find the opposite pair either
                print(e)
                print(
                    "Unable to find candle data for the quote currency in terms of USDT. We will convert the quote currency to USDT using price data from Coingecko at current prices. This might make the USDT equivalent values relatively less accurate."
                )
                quote_dict = {}
                
                while True:
                    usdt_price = requests.get("https://api.coingecko.com/api/v3/coins/tether?localization=false&tickers=true&market_data=true&community_data=false&developer_data=false&sparkline=false")

                    if usdt_price.status_code == 200:
                        usdt_price = 1 / usdt_price.json()["market_data"]["current_price"].get(quote.lower(), None)
                        break

                    else:
                        print(usdt_price.status_code)
                        time.sleep(5)

                vol_data["quote_currency_price_usdt"] = usdt_price

                vol_data["average_price_usdt"] = (
                    vol_data["average_price"] * vol_data["quote_currency_price_usdt"]
                )
                vol_data["open_usdt"] = (
                    vol_data["open"] * vol_data["quote_currency_price_usdt"]
                )
                vol_data["close_usdt"] = (
                    vol_data["close"] * vol_data["quote_currency_price_usdt"]
                )
                vol_data["high_usdt"] = (
                    vol_data["high"] * vol_data["quote_currency_price_usdt"]
                )
                vol_data["low_usdt"] = (
                    vol_data["low"] * vol_data["quote_currency_price_usdt"]
                )

    elif quote == "USDT":
        vol_data["quote_currency_price_usdt"] = 1
        vol_data["average_price_usdt"] = (
            vol_data["average_price"] * vol_data["quote_currency_price_usdt"]
        )
        vol_data["open_usdt"] = vol_data["open"] * vol_data["quote_currency_price_usdt"]
        vol_data["close_usdt"] = (
            vol_data["close"] * vol_data["quote_currency_price_usdt"]
        )
        vol_data["high_usdt"] = vol_data["high"] * vol_data["quote_currency_price_usdt"]
        vol_data["low_usdt"] = vol_data["low"] * vol_data["quote_currency_price_usdt"]

    # # converting USDT to USD at daily intervals
    # while True:
    #     try:
    #         usdt_price = requests.get(
    #             f"https://api.coingecko.com/api/v3/coins/tether/market_chart?vs_currency=usd&days=max&interval=daily"
    #         )
    #         if usdt_price.status_code == 200:
    #             usdt_price = usdt_price.json()["prices"]
    #             break
    #         else:
    #             print(usdt_price.status_code)
    #             raise Exception
    #     except:
    #         time.sleep(5)

    # usdt_price_dict = {
    #     datetime.utcfromtimestamp(itm[0] / 1000): itm[1] for itm in usdt_price
    # }

    # vol_data["usdt_currency_price_usd"] = pd.to_datetime(
    #     vol_data["timestamp"].dt.strftime("%Y-%m-%d 00:00:00")
    # ).apply(lambda x: match_finder(x, usdt_price_dict))

    # vol_data["average_price_usd"] = (
    #     vol_data["average_price_usdt"] * vol_data["usdt_currency_price_usd"]
    # )
    # vol_data["open_usd"] = vol_data["open_usdt"] * vol_data["usdt_currency_price_usd"]
    # vol_data["close_usd"] = vol_data["close_usdt"] * vol_data["usdt_currency_price_usd"]
    # vol_data["high_usd"] = vol_data["high_usdt"] * vol_data["usdt_currency_price_usd"]
    # vol_data["low_usd"] = vol_data["low_usdt"] * vol_data["usdt_currency_price_usd"]

    vol_data['volume_usdt'] = vol_data['volume_base'] * vol_data['average_price_usdt']
    #vol_data["volume_usd"] = vol_data["volume_base"] * vol_data["average_price_usd"]

    # grouping input data according to candle_duration_seconds_result_output happens at the end to avoid rounding errors
    vol_data_by_time = vol_data.groupby(
        pd.Grouper(
            key="timestamp",
            axis=0,
            freq=f"{candle_duration_seconds_result_output}s",
            closed="left",
            label="left",
        )
    ).agg(
        {
            "exchange_name": "last",
            "pair": "last",
            "base": "last",
            "quote": "last",
            "average_price": "mean",
            "open": "first",
            "close": "last",
            "high": "max",
            "low": "min",
            "volume_base": lambda x: x.sum(min_count=1),
            "quote_currency_price_usdt": "mean",
            "average_price_usdt": "mean",
            #"usdt_currency_price_usd": "mean",
            "open_usdt": "first",
            "close_usdt": "last",
            "high_usdt": "max",
            "low_usdt": "min",
            "volume_usdt": lambda x: x.sum(min_count=1),
        }
    )
    vol_data_by_time["timestamp"] = vol_data_by_time.index
    vol_data_by_time['timestamp'] = vol_data_by_time['timestamp'].dt.tz_localize(None)
    vol_data_by_time = vol_data_by_time.reset_index(drop=True)

    vol_data_by_time = vol_data_by_time.fillna(
        {
            "exchange_name": exchange_name,
            "pair": pair,
            "base": base,
            "quote": quote,
            "average_price": np.nan,
            "open": np.nan,
            "close": np.nan,
            "high": np.nan,
            "low": np.nan,
            "volume_base": 0,
            "quote_currency_price_usdt": np.nan,
            "average_price_usdt": np.nan,
            #"usdt_currency_price_usd": np.nan,
            "open_usdt": np.nan,
            "close_usdt": np.nan,
            "high_usdt": np.nan,
            "low_usdt": np.nan,
            "volume_usdt": np.nan,
        }
    )

    print("Number of rows:", len(vol_data_by_time))
    print("Number of rows with NaN values:", len(vol_data_by_time[vol_data_by_time.isna().any(axis=1)]))

    return vol_data_by_time


#EXAMPLES
#ticker_finder('bitget', 'ROOT')

#print(ohlcv_data_download('upbit', 'BTC/KRW', '2024-05-01 00:00:00+00:00', '2024-05-31 00:00:00+00:00', '1h', 3600).tail(5).values)
