"""
Script for updating the instrument and price tables.
"""

# Imports
import config as cf
import utils as ut

import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None  # default = 'warn'

import time
import os

# Starting the server
os.system("mysql.server start")

# Connections
print('\n[INFO] Connecting to Endpoints ...')
time.sleep(1)
conn = ut.connect_db()
cursor = conn.cursor()
api = ut.connect_api()


# Updating Instruments & Downloading Price History
time.sleep(1)
print('\n[INFO] Retreiving tickers from Alpaca API ...')
assets = api.list_assets()
print('[INFO] Retreiving tickers from Database ...')
tickers = ut.get_all_tickers(conn)

yesterday = str(ut.get_yesterday())

time.sleep(1)
print('\n[INFO] Comparing Alpaca tickers against Database ...')
time.sleep(1)
new_tickers_alpaca = ut.get_new_tickers(assets, tickers, conn)

if new_tickers_alpaca.empty:
  print('|---------------| No new tickers have been identified. Instruments are up to date. |---------------|')
  new_tickers = []
else:
  exchanges = new_tickers_alpaca['exchange_id'].to_list()

  tmp = []
  for exchange in exchanges:
    tmp.append(ut.get_exchange_id(exchange, conn))

  new_tickers_alpaca.drop('exchange_id', axis = 1, inplace = True)
  new_tickers_alpaca.insert(0, 'exchange_id', tmp)

  new_tickers = sorted(ut.cast_to_yf(new_tickers_alpaca['ticker'].to_list())) # cast tickers to YF format

  ## Starting Corporate Information download from Yahoo Finance
  time.sleep(1)
  print('\n[INFO] Downloading corporate information from Yahoo Finance API ...')
  time.sleep(1)
  splits = ut.determine_splits(new_tickers, cf.BATCH_SIZE)
  split_arr = np.array_split(new_tickers, splits)
  new_tickers_info = pd.DataFrame()

  count = 1
  for array in split_arr:
    print("|---------------| Split {} of {} |---------------|".format(count, splits))
    tmp = ut.get_corporate_info(array, 25, cf.KOI)
    new_tickers_info = new_tickers_info.append(tmp)
    count += 1
  print('|---------------| Successfully downloaded corporate information. |---------------|')

  # does not work if absolutely no information could be downloaded
  try:
    info_count = len(new_tickers_info[new_tickers_info['sector'] != 'NaN'])
    print('[INFO] Could retrieve additional information on {} out of {} tickers ({:.2f}%).'.format(info_count, len(new_tickers), info_count / len(new_tickers) * 100))

    new_tickers = ut.cast_to_alpaca(new_tickers)
    tmp = ut.cast_to_alpaca(new_tickers_info['symbol'].tolist()) # cast tickers back to Alpaca format
    new_tickers_info.drop('symbol', axis = 1, inplace = True)
    new_tickers_info.insert(0, 'ticker', tmp)

    new_tickers_df = pd.merge(new_tickers_alpaca, new_tickers_info, on = 'ticker', how = 'outer')
    new_tickers_df = new_tickers_df.sort_values(by = ['ticker'])
    new_tickers_df = new_tickers_df.replace({np.nan: None})

    time.sleep(1)
    print('\n[INFO] Writing to instrument table ...')
    for row in new_tickers_df.itertuples(index = False):
      cursor.execute("INSERT INTO instrument (exchange_id, ticker, name, sector, industry, country, website) VALUES (%s, %s, %s, %s, %s, %s, %s)", row)
    conn.commit()
    time.sleep(1)
    print("|---------------| Successfully added new instruments. |---------------|")
  except Exception:
    print('[INFO] Could not retrieve any additional information.')

  ## Price History for new tickers
  new_tickers_df = ut.get_tickers_from_list(new_tickers, conn) # read in new tickers with their ids

  ## Starting Price download from Yahoo Finance
  time.sleep(1)
  print('\n[INFO] Downloading price history from Yahoo Finance API ...')
  time.sleep(1)
  splits = ut.determine_splits(new_tickers, cf.BATCH_SIZE)
  new_tickers_list = [new_tickers_df.loc[i : i + cf.BATCH_SIZE - 1, : ] for i in range(0, len(new_tickers_df), cf.BATCH_SIZE)]
  new_excepts = []

  count = 1
  for batch in new_tickers_list:
    print("|---------------| Split {} of {} |---------------|".format(count, splits))
    tmp = ut.write_historical_prices_yf(batch, conn, cursor, None, yesterday)
    new_excepts.append(tmp)
    count += 1

  new_excepts = ut.flatten_exceptions(new_excepts)
  new_alpaca = pd.DataFrame()

  ## Starting Price download from Alpaca
  time.sleep(1)
  print('\n[INFO] Downloading price history from Alpaca API ...')
  time.sleep(1)
  for ticker in new_excepts:
    new_alpaca = new_alpaca.append(new_tickers_df[new_tickers_df['ticker'] == ticker])
  ut.write_historical_prices_alpaca(new_alpaca, api, conn, cursor, None, yesterday)

  time.sleep(1)
  print('|---------------| Successfully added price history for new tickers. |---------------|')


# Updating Prices
time.sleep(1)
print('\n[INFO] Fetching data for existing tickers in Database ...')
time.sleep(1)

yesterday_dt = ut.get_yesterday()

tickers_df = ut.get_update_info_from_list(conn, cursor)
tickers_df = tickers_df[~tickers_df['ticker'].isin(new_tickers)] # filter out newly added tickers that are already up to date
tickers_df = tickers_df[tickers_df['last_date'] != yesterday_dt] # filter out those tickers whose last_price date is equal to yesterday's date

if tickers_df.empty:
  print('|---------------| Prices for existing tickers are already up to date. |---------------|')
else:
  print('[INFO] Updating prices for {} tickers ...'.format(len(tickers_df)))
  yahoo_tickers_df = tickers_df[tickers_df['vendor_id'] == ut.get_vendor_id('Yahoo Finance', conn)]
  yahoo_tickers_df.drop('vendor_id', axis = 1, inplace = True)

  ## Casting tickers to yf format
  tmp = ut.cast_to_yf(yahoo_tickers_df['ticker'].tolist())
  yahoo_tickers_df.drop('ticker', axis = 1, inplace = True)
  yahoo_tickers_df.insert(0, 'ticker', tmp)

  alpaca_tickers_df = tickers_df[tickers_df['vendor_id'] == ut.get_vendor_id('Alpaca', conn)]
  alpaca_tickers_df.drop('vendor_id', axis = 1, inplace = True)

  yf = ut.get_vendor_id('Yahoo Finance', conn)
  alp = ut.get_vendor_id('Alpaca', conn)

  ## Starting Price download from Yahoo Finance
  time.sleep(1)
  print('\n[INFO] Downloading new price data from Yahoo Finance API ...')
  time.sleep(1)
  splits = ut.determine_splits(yahoo_tickers_df['ticker'].to_list(), cf.BATCH_SIZE)
  yahoo_tickers_list = [yahoo_tickers_df.loc[i : i + cf.BATCH_SIZE - 1, : ] for i in range(0, len(yahoo_tickers_df), cf.BATCH_SIZE)]

  count = 1
  for batch in yahoo_tickers_list:
    print("|---------------| Split {} of {} |---------------|".format(count, splits))
    ut.update_historical_prices_yf(batch, yf, conn, cursor, yesterday)
    count += 1

  ## Starting Price download from Alpaca
  time.sleep(1)
  print('\n[INFO] Downloading new price data from Alpaca API ...')
  time.sleep(1)
  ut.update_historical_prices_alpaca(alpaca_tickers_df, alp, api, conn, cursor, yesterday)

  time.sleep(1)
  print('|---------------| Successfully downloaded new price data for existing tickers. |---------------|')


# Ending Script
time.sleep(1)
print('\n[INFO] Closing connection to Database ...')
time.sleep(1)
cursor.close()
conn.close()

# Stopping the server
os.system("mysql.server stop")
