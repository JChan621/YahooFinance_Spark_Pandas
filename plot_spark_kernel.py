# -*- coding: utf-8 -*-

"""
Created on March 10 2022 
Created by Fourier
Disclaimer: This is for personal interest and has no connections with Yahoo Finance or any stock provider.
Use of the data is at your own risk. 
I assume no responsibility for, and offer no warranties or representations regarding, the accuracy, reliability, completeness or timeliness of any of the content.
#To add: labels of the plots 
"""

import time
import requests
#import pandas as pd #unused
#from pyspark import SQLContext #current script done within pyspark kernel
#from pyspark import SparkContext #current script done within pyspark kernel
#from pyspark.sql import SparkSession #current script done within pyspark kernel
from pyspark.sql  import functions as func
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

#MASTER = 'local'
#APP_NAME = 'test'
def retrieve_ticker_history(ticker : str):
    curr_time = str(int(time.time()))
    url = 'https://query1.finance.yahoo.com/v8/finance/chart/' + \
          ticker.upper() + \
          '?formatted=true&crumb=FIOmmg93x1j&lang=en-US&region=US&includeAdjustedClose=true&interval=1d&period1=916963200&period2=' + \
          curr_time + \
          '&events=capitalGain%7Cdiv%7Csplit&useYfid=true&corsDomain=finance.yahoo.com'
    web_content = requests.get(url, headers = {'User-agent': 'Mozilla/5.0'})
    ticker_info = web_content.json()
    if ticker_info['chart']['error']:
        print('Ticker not found.')
        return
    ticker_quote = ticker_info['chart']['result'][0]['indicators']['quote'][0]
    ticker_quote['Timestamp'] = ticker_info['chart']['result'][0]['timestamp']
    schema = []
    df_input = []
    for prop in ticker_quote:
        ticker_quote[prop].pop()#Last record could be an after market record at weekends due to how yahoo finance works. Just forget it atm.
        schema.append(prop)
        df_input.append(ticker_quote[prop])
    #sc = SparkContext(MASTER, APP_NAME)
    #spark = SparkSession(sc)
    #sqlContext = SQLContext(sc)
    ticker_df = sqlContext.createDataFrame(zip(*df_input), schema=schema)
    ticker_df = ticker_df.withColumn('Date', func.from_unixtime('Timestamp').cast(DateType()))
    days = lambda i: i * 86400
    windowSpec_20 = Window.orderBy(func.col("Timestamp").cast('long')).rangeBetween(-days(30), 0) #Taking 30 days as a 20 trading days moving average
    ticker_df = ticker_df.withColumn('20_days_average', func.avg("close").over(windowSpec_20))
    windowSpec_250 = Window.orderBy(func.col("Timestamp").cast('long')).rangeBetween(-days(365), 0) #Taking 365 days as a 250 trading days moving average
    ticker_df = ticker_df.withColumn('250_days_average', func.avg("close").over(windowSpec_250))
    display(ticker_df.tail(500),1)#Doesn't work outside spark kernel notebook

ticker = 'aapl'
retrieve_ticker_history(ticker)
