#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 19:37:55 2021

@author: marcowong
"""

from utils import postgres_utils as pu
import requests
import datetime
import pandas as pd


def get_daily_fx_data(date='2021-01-01', to_cur='USD,GBP'):
    
    access_key = 'a54e40ed95bd3a96d2abd09e3a85c462'
    url = f'http://api.exchangeratesapi.io/v1/{date}?access_key={access_key}&symbols={to_cur}'
    response = requests.post(url)
    data = response.json()
    
    return data


def get_date_list(start_date, end_date):
    date_list = [i.date().strftime('%Y-%m-%d') for i in list(pd.date_range(start=start_date, end=end_date))]
    db_dates = pu.query('select distinct date from public.daily_fx_rate order by date')['date']
    db_dates = [date.strftime('%Y-%m-%d') for date in db_dates]
    date_list = list(set(date_list).difference(set(db_dates)))
    date_list.sort()
    return date_list


def get_historic_fx_data(date_list):
    
    if len(date_list) == 0:
        return pd.DataFrame()
    
    data = []
    for d in date_list:
        data.append(get_daily_fx_data(date=d))
    df = pd.DataFrame(data)
    
    # transform data into required format
    df = df[['date', 'base', 'rates']]
    # parse dictionary into separate columns
    df = pd.concat([df, df['rates'].apply(pd.Series)], axis=1)
    df = df.drop(columns=['rates'])
    
    # transform df to be GBP base
    df['GBPToEUR'] = 1/df['GBP']
    df['USDToEUR'] = 1/df['USD']
    df['GBPToUSD'] = df['GBPToEUR']/df['USDToEUR']
    
    # transform df from wide to long
    df = df[['date', 'GBPToEUR', 'GBPToUSD']].rename(columns={'GBPToEUR': 'EUR', 'GBPToUSD': 'USD'})
    df = pd.melt(df, id_vars=['date'], value_vars=['EUR', 'USD'])
    df['source_currency'] = 'GBP'
    df = df.rename(columns={'variable': 'target_currency', 'value': 'rate'})
    df = df.round(4)
    
    if df.shape[0] > 0:
        cols = list(pu.query('select * from public.daily_fx_rate limit 0').columns)
        df = df[cols]
        pu.db_insert(df, 'daily_fx_rate')
    
    return df


if __name__ == '__main__':
    
    start_date = '2020-01-01'
    end_date = datetime.datetime.today()
    
    date_list = get_date_list(start_date=start_date, end_date=end_date)
    data = get_historic_fx_data(date_list=date_list)

    pu.query('call create_monthly_rate() ')
    pu.query('call create_fixed_rate() ')
    
