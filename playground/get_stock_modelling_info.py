# !/usr/bin/env python
import csv
import json
from urllib.request import urlopen

import certifi
import pandas as pd


def get_jsonparsed_data(url: str):
    """
    Receive the content of ``url``, parse it as JSON and return the object.

    Parameters
    ----------
    url : str

    Returns
    -------
    dict
    """
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)


def convert_raw_data_to_csv(raw_data: list, file_name: str) -> None:
    keys = raw_data[0].keys()

    with open(f'{file_name}.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(raw_data)


def merge_rsi_column_into_market_price_csv(market_price_file_name: str, rsi_file_name: str, column_name: str) -> None:
    # Load each file into a pandas dataframe
    data1 = pd.read_csv(f'{rsi_file_name}.csv', sep=',', parse_dates=False)
    data2 = pd.read_csv(f'{market_price_file_name}.csv', sep=',', parse_dates=False)

    # Now add 'rsi' from data1 to data2
    data2[column_name] = data1[column_name]

    # Save it back to csv
    data2.to_csv(f'{market_price_file_name}.csv')


if __name__ == "__main__":
    stock_symbol, market_price_file_name, rsi_file_name = "PFE", "pfizer", "pfizer_rsi"
    # get urls
    market_price_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{stock_symbol}?from=2020-01-02&to=2022-04-08&apikey=68d5eead0acf09e389ee50dfa856eb88"
    rsi_url = f"https://financialmodelingprep.com/api/v3/technical_indicator/daily/{stock_symbol}?from=2020-01-02&to=2022-04-08&type=rsi&apikey=68d5eead0acf09e389ee50dfa856eb88"
    # pull data
    raw_market_price_data = get_jsonparsed_data(market_price_url).get('historical')
    raw_rsi_data = get_jsonparsed_data(rsi_url)
    # save data into csv files
    convert_raw_data_to_csv(raw_market_price_data, market_price_file_name)
    convert_raw_data_to_csv(raw_rsi_data, rsi_file_name)
    # copy rsi column into market price csv
    merge_rsi_column_into_market_price_csv(market_price_file_name, rsi_file_name, 'rsi')
