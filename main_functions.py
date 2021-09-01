# -*- coding: utf-8 -*-
"""
Created on Wed Sep  1 12:25:16 2021

@author: luiznbastos
"""
import pandas as pd
from datetime import datetime
from yahooquery import Ticker
import numpy as np


def sharpe_ratio_analysis(current_portfolio_on_history, risk_free_return):

  asset_price_reference = lambda x: np.median(x[-11:-1])
  asset_return = lambda x: [(price - asset_price_reference(x))/asset_price_reference(x) for price in x]
  excess_return = lambda x: [x[0]-x[1] for x in zip(asset_return(x), [risk_free_return]*len(x))]
  sharpe_ratio = lambda x: np.mean(excess_return(x)) / np.std(excess_return(x))
  excess_return_std = lambda x: np.std(excess_return(x))
  avg_excess_return = lambda x: np.mean(excess_return(x))

  sharpe = current_portfolio_on_history.apply(sharpe_ratio,raw=True)
  sharpe = pd.DataFrame(sharpe).reset_index().drop("index",axis=1)

  risk = current_portfolio_on_history.apply(excess_return_std,raw=True)
  risk = pd.DataFrame(risk).reset_index().drop("index",axis=1)

  asset_return = current_portfolio_on_history.apply(avg_excess_return,raw=True)
  asset_return = pd.DataFrame(asset_return).reset_index().drop("index",axis=1)

  risk_return = pd.merge(sharpe,asset_return,how='outer',left_index=True,right_index=True)
  risk_return = pd.merge(risk_return,risk ,how='outer',left_index=True,right_index=True)
  risk_return.columns = ["Sharpe Ratio", " Average Excess Return", "Excess Return Std"]
  risk_return.index = [datetime.date(datetime.today())]
  
  return risk_return


def get_ibov(start_date, end_date, current_portfolio_on_history):
    ibov = Ticker("^BVSP")
    ibov_history = ibov.history(start='2018-08-03',end='2021-08-02')
    ibov_history.drop(ibov_history.columns.difference(['close']), 1, inplace=True)

    ibov_history.index = ibov_history.index.droplevel(0)
    ibov_history.rename(columns={"close":"IBOV"}, inplace=True)
    portfolio_ibov_dataframe = pd.merge(
                                  ibov_history,
                                  current_portfolio_on_history,
                                  how='outer',
                                  left_index=True,
                                  right_index=True
                                  ).fillna(method="bfill").fillna(method='ffill')

    return portfolio_ibov_dataframe

def get_current_moment_status(position_history, price_history, total_investment_over_time):
  current_position = position_history.head(1)
  current_price = price_history.head(1)
  current_total_invested = total_investment_over_time.head(1)[["Total"]].values[0][0]
  current_investment_per_stock = total_investment_over_time.head(1).drop("Total", axis=1)
  current_stock_proportion = current_investment_per_stock.apply(lambda stock_investment: stock_investment/current_total_invested)
  current_stock_proportion_dict = {column:list(value)[0] for column,value in current_stock_proportion.iteritems()}

  return current_position, current_price, current_investment_per_stock, current_stock_proportion, current_stock_proportion_dict


def get_current_portfolio_on_history(price_history, current_position):
  current_position_on_price_history = price_history.copy()
  current_pos_columns = current_position_on_price_history.columns.values
  current_pos_indexes = current_position_on_price_history.index.values
  for cols in current_pos_columns:
    for idx in current_pos_indexes:
      current_position_on_price_history.at[idx,cols] = current_position[cols].values[0]
  current_portfolio_on_history = current_position_on_price_history*price_history.fillna(method="bfill").fillna(method='ffill')
  current_portfolio_on_history = pd.DataFrame(current_portfolio_on_history.fillna(value=0).apply(lambda x: sum(x), axis=1))
  current_portfolio_on_history.columns = ["Current Portfolio Historical Value"]
  
  return current_portfolio_on_history


def get_history(transactions, start_date, end_date):

  wallet = {}
  for stock in transactions.keys():
    wallet[stock] = Ticker(stock)

  price_history, dividend_history, splits_history = create_wallet_history(wallet, start_date, end_date)
  split_happened = splits_history.sum(axis=1).apply(lambda x:x>0)
  split_happened.rename("split_happened_on_date",inplace=True)
  splits_history = pd.merge(splits_history, split_happened, left_index=True, right_index=True,how='outer')
  splits_history.sort_index(ascending=False, inplace=True)

  position_history = price_history.copy()
  analysis_dates = price_history.index.values
  splited_stocks = splits_history.columns.values
  split_dates = splits_history[splits_history["split_happened_on_date"] == True].index.values

  for stock in transactions:
    current_stock_position = 0
    for date in analysis_dates:
      isoformated_date = date.isoformat()
      if (stock in splited_stocks and date in split_dates):
        split_multiplier = splits_history.loc[date, stock]
        split_multiplier = 1 if split_multiplier==0 else split_multiplier
        current_stock_position = current_stock_position#*split_multiplier
      if isoformated_date in transactions[stock]:
        current_stock_position = transactions[stock][isoformated_date] + current_stock_position 
      position_history.at[date,stock] = current_stock_position

  total_investment_over_time = position_history*price_history
  total_investment_over_time.sort_index(ascending=False, inplace=True)
  total_investment_over_time = pd.merge(total_investment_over_time,total_investment_over_time.sum(axis=1).rename("Total",inplace=True), left_index=True, right_index=True,how='outer')

  position_history.sort_index(ascending=False, inplace=True)
  price_history.sort_index(ascending=False, inplace=True)
  dividend_history.sort_index(ascending=False, inplace=True)

  return price_history, position_history, total_investment_over_time, dividend_history


def create_wallet_history(wallet_stocks, start_date, end_date):
  wallet_history = pd.DataFrame()
  wallet_dividends = pd.DataFrame()
  wallet_splits =  pd.DataFrame()
  
  for stock, stock_info in wallet_stocks.items():
    stock_history_dataframe = stock_info.history(start=start_date,end=end_date)
    stock_history_dataframe.reset_index(level=0, drop=True, inplace=True)


    if (wallet_history.empty):
      wallet_history = stock_history_dataframe[['close']]
      wallet_history.columns.values[-1] = stock

      wallet_dividends = stock_history_dataframe[['dividends']]
      wallet_dividends.columns.values[-1] = stock   

      wallet_splits = stock_history_dataframe[['splits']]
      wallet_splits.columns.values[-1] = stock   

    else:
      stock_history_dataframe = stock_info.history(start=start_date,end=end_date)
      stock_history_dataframe.reset_index(level=0, drop=True, inplace=True)

      new_stock = stock_history_dataframe[['close']]
      new_stock.columns.values[-1] = stock

      wallet_history = pd.merge(
                          wallet_history,
                          new_stock,
                          how='outer',
                          left_index=True,
                          right_index=True
                          ).fillna(method="bfill").fillna(method='ffill')
      
      if 'dividends' in stock_history_dataframe.columns.values:
        if wallet_dividends.empty:
          wallet_dividends = stock_history_dataframe[['dividends']]
          wallet_dividends.columns.values[-1] = stock
        else:      
          new_dividend = stock_history_dataframe[['dividends']]
          new_dividend.columns.values[-1] = stock

          wallet_dividends = pd.merge(
                              wallet_dividends,
                              new_dividend,
                              how='outer',
                              left_index=True,
                              right_index=True
                              ).fillna(method="bfill").fillna(method='ffill')
      
      if 'splits' in stock_history_dataframe.columns.values:
        if (wallet_splits.empty):
          wallet_splits = stock_history_dataframe[['splits']]
          wallet_splits.columns.values[-1] = stock
        else:
          new_split = stock_history_dataframe[['splits']]
          new_split.columns.values[-1] = stock

          wallet_splits = pd.merge(
                              wallet_splits,
                              new_split,
                              how='outer',
                              left_index=True,
                              right_index=True
                              ).fillna(method="bfill").fillna(method='ffill')

  return wallet_history, wallet_dividends, wallet_splits
