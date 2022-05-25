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


def merge_rsi_column_into_market_price_csv(market_price_file_name: str, rsi_file_name: str, column_name: str, combined_file_name: str) -> None:
    # Load each file into a pandas dataframe
    data1 = pd.read_csv(f'{rsi_file_name}.csv', sep=',', parse_dates=False)
    data2 = pd.read_csv(f'{market_price_file_name}.csv', sep=',', parse_dates=False)

    # Now add 'rsi' from data1 to data2
    data2[column_name] = data1[column_name]

    # Save it back to csv with new file name
    data2.to_csv(f'{combined_file_name}.csv')


if __name__ == "__main__":

    """
    Please get your personal API key from https://site.financialmodelingprep.com/developer/docs 

    Notes:

         This script will fail with error message 'HTTP Error 403' since the market_price_url(historical-price-full) endpoint used to be free 
         when we started the project, but the API provider changed access.

         If you would like to replicate, please check how to purchase this API access at https://financialmodelingprep.com/developer/docs/pricing. 

         We provide the results generated from this python script and stored them in .csv files with all six stock data in our github repo. 
         (https://github.com/yyan17/SIADS-591/tree/main/data).  

    """

    stock_info = {

        'pfizer': 'PFE',
        'novavax': 'NVAX',
        'moderna': 'MRNA',
        'jnj': 'JNJ',
        'astra-zeneca': 'AZN',
        'biontech': 'BNTX'
    }

    for stock_name, stock_symbol in stock_info.items():
        market_price_file_name = '_'.join([stock_name, 'market_price'])
        rsi_file_name = '_'.join([stock_name, 'rsi'])
        # get urls
        market_price_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{stock_symbol}?from=2020-01-02&to=2022-04-08&apikey=<your_API_key>"
        rsi_url = f"https://financialmodelingprep.com/api/v3/technical_indicator/daily/{stock_symbol}?from=2020-01-02&to=2022-04-08&type=rsi&apikey=<your_API_key>"
        # pull data
        raw_market_price_data = get_jsonparsed_data(market_price_url).get('historical')
        raw_rsi_data = get_jsonparsed_data(rsi_url)
        # save data into csv files
        convert_raw_data_to_csv(raw_market_price_data, market_price_file_name)
        convert_raw_data_to_csv(raw_rsi_data, rsi_file_name)
        # copy rsi column into market price csv
        merge_rsi_column_into_market_price_csv(market_price_file_name, rsi_file_name, 'rsi', stock_name)
