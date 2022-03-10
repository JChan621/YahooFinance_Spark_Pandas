# -*- coding: utf-8 -*-

"""

Created on March 10 2022 
Created by Fourier

Disclaimer: This is for personal interest and has no connections with Yahoo Finance or any stock provider.
Use of the data is at your own risk. 
I assume no responsibility for, and offer no warranties or representations regarding, the accuracy, reliability, completeness or timeliness of any of the content.

#To add: labels of the plots 
"""
import datetime

import requests
import pandas as pd
#import matplotlib.pyplot as plt #for future update

def retrieve_ticker_history(ticker : str):
    #ticker = 'NVDA'
    url = 'https://query1.finance.yahoo.com/v8/finance/chart/' + ticker.upper() + '?formatted=true&crumb=FIOmmg93x1j&lang=en-US&region=US&includeAdjustedClose=true&interval=1d&period1=916963200&period2=1646870400&events=capitalGain%7Cdiv%7Csplit&useYfid=true&corsDomain=finance.yahoo.com'
    web_content = requests.get(url, headers = {'User-agent': 'Mozilla/5.0'})
    ticker_info = web_content.json()
    if ticker_info['chart']['error']:
        print('Ticker not found.')
        return
    ticker_df = pd.DataFrame(ticker_info['chart']['result'][0]['indicators']['quote'][0])
    ticker_df = ticker_df.round(2)
    ticker_df['Timestamp'] = ticker_info['chart']['result'][0]['timestamp']
    ticker_df['Date'] = ticker_df['Timestamp'].apply(lambda d: datetime.datetime.utcfromtimestamp(d).strftime('%Y-%m-%d'))
    ticker_df = ticker_df.set_index('Date')
    ticker_df['close'].iloc[-500:].plot(figsize=(10,6))
    ticker_df['close'].rolling(window = 250).mean().iloc[-500:].plot()
    ticker_df['close'].rolling(window = 50).mean().iloc[-500:].plot()
    ticker_df['close'].rolling(window = 20).mean().iloc[-500:].plot()
    print("Currently trading at: " + str(ticker_info['chart']['result'][0]['meta']["regularMarketPrice"]))
if __name__ == '__main__':
    ticker = input('Enter a ticker: ')
    retrieve_ticker_history(ticker)
