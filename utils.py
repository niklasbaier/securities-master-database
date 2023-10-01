"""
Holds all function definitions for the backend.
"""

import config as cf

import pymysql.cursors
import alpaca_trade_api as tradeapi
import yfinance as yf

import pandas as pd
from pandas_datareader import data as pdr
yf.pdr_override() # let pandas_datareader override yfinance for faster downloads (use pdr prefix instead of yf)

from tqdm.auto import tqdm # to output nice progress bars for the data download
import lxml.html as lh
import requests
import datetime
import time
import sys




# MySQL Server
def connect_db():
  """
  Connect to a MySQL database
  :return: connection object
  """
  conn = pymysql.connect(
    host = cf.DB_HOST,
    user = cf.DB_USER,
    password = cf.DB_PASSWORD,
    database = cf.DB_DATABASE)
  print("|---------------| Successfully connected to {}. |---------------|".format(cf.DB_DATABASE))

  return conn


def get_all_exchanges(conn):
  """
  Queries the database for every existing exchange and returns their acronym as a list
  :param conn: MySQL database connection
  :return: list of all exchange acronyms
  """
  return pd.read_sql('SELECT acronym FROM exchange', conn)['acronym'].to_list()


def get_all_tickers(conn):
  """
  Queries the database for every existing ticker and returns them as a list
  :param conn: MySQL database connection
  :return: list of all tickers
  """
  return pd.read_sql('SELECT ticker FROM instrument', conn)['ticker'].to_list()


def get_exchange_id(acronym, conn):
  """
  Given a MySQL database connection, query for the exchange id of a specified exchange
  :param acronym: acronym of exchange that id needs to be retrieved for
  :param conn: MySQL database connection
  :return: exchange id
  """
  return pd.read_sql("SELECT id FROM exchange WHERE acronym = '{}'".format(acronym), conn).astype(int).iloc[0]['id']


def get_exchange_tickers(acronym, conn):
  """
  Given a MySQL database connection, query for the tickers of a specified exchange
  :param acronym: acronym of exchange that tickers needs to be retrieved for
  :param conn: MySQL database connection
  :return: dataframe of tickers with their corresponding ids
  """
  exchange_id = get_exchange_id(acronym, conn)
  return pd.read_sql("SELECT ticker, id FROM instrument WHERE exchange_id = '{}'".format(exchange_id), conn)


def get_ohlcv_df(ticker, conn):
  """
  Query the database for OHLCV data of a specified ticker
  :param ticker: ticker for which OHLCV data should be returned
  :param conn: MySQL database connection
  :return: dataframe with OHLCV data
  """
  price = 'adj_close_price' # default: Yahoo Finance
  vendor = get_vendor_id_ticker(ticker, conn)

  if vendor == 2:
    price = 'close_price' # Alpaca

  sql = """
    SELECT p.price_date, p.open_price, p.high_price, p.low_price, p.{}, p.volume
    FROM instrument AS ins
    INNER JOIN price AS p
    ON p.ticker_id = ins.id
    WHERE ins.ticker = '{}'
    ORDER BY p.price_date ASC;""".format(price, ticker)

  tmp = pd.read_sql(sql, conn, index_col = 'price_date')

  if tmp.empty:
    print('No data for {} in database.'.format(ticker))
    sys.exit(1)

  return tmp


def get_price_df(ticker, conn):
  """
  Query the database for (adj) closing prices of a specified ticker
  :param ticker: ticker for which prices should be returned
  :param conn: MySQL database connection
  :return: dataframe with closing prices
  """
  price = 'adj_close_price' # default: Yahoo Finance
  vendor = get_vendor_id_ticker(ticker, conn)

  if vendor == 2:
    price = 'close_price' # Alpaca

  sql = """
    SELECT p.price_date, p.{}
    FROM instrument AS ins
    INNER JOIN price AS p
    ON p.ticker_id = ins.id
    WHERE ins.ticker = '{}'
    ORDER BY p.price_date ASC;""".format(price, ticker)

  tmp = pd.read_sql(sql, conn, index_col = 'price_date')

  if tmp.empty:
    print('No data for {} in database.'.format(ticker))
    sys.exit(1)

  return tmp


def get_ticker_info_from_list(ticker_list, conn):
  """
  Returns a dataframe with instrument information for a given list of tickers
  :param index: list of tickers
  :param conn: MySQL database connection
  :return: dataframe of instrument information
  """
  ticker_df = pd.DataFrame()

  for i in range(0, len(ticker_list)):
    try:
      sql = """
        SELECT ex.acronym, ins.ticker, ins.name, ins.sector, ins.industry, ins.country, ins.website
        FROM exchange AS ex
        INNER JOIN instrument AS ins
        ON ex.id = ins.exchange_id
        WHERE ins.ticker = '{}';""".format(ticker_list[i])
      tmp = pd.read_sql(sql, conn)
      ticker_df = ticker_df.append(tmp, ignore_index = True)
    except Exception as e:
      continue

  return ticker_df


def get_tickers_from_list(tickers, conn):
  """
  Queries the database for a given list of tickers and returns a dataframe of their symbols and ids
  :param tickers: list of tickers to be queried for
  :conn: MySQL database connection
  :return: dataframe of tickers and ids
  """
  ticker_df = pd.DataFrame()
  for ticker in tickers:
    try:
      tmp = pd.read_sql("SELECT ticker, id FROM instrument WHERE ticker = '{}'".format(ticker), conn)
      ticker_df = ticker_df.append(tmp, ignore_index = True)
    except Exception as e:
      continue

  return ticker_df


def get_vendor_id(name, conn):
  """
  Given a MySQL database connection, query for the vendor id of a specified vendor
  :param name: name of vendor that id needs to be retrieved for
  :param conn: MySQL database connection
  :return: vendor id
  """
  return pd.read_sql("SELECT id FROM vendor WHERE name = '{}'".format(name), conn).astype(int).iloc[0]['id']


def get_vendor_id_ticker(ticker, conn):
  """
  Given a MySQL database connection, query for the vendor id of a specified ticker
  :param ticker: ticker for which vendor id should be returned
  :conn: MySQL database connection
  :return: vendor id
  """
  sql = """
    SELECT DISTINCT (p.vendor_id)
    FROM instrument AS ins
    INNER JOIN price AS p
    ON p.ticker_id = ins.id
    WHERE ins.ticker = '{}';""".format(ticker)

  return pd.read_sql(sql, conn).astype(int).iloc[0]['vendor_id']




# Alpaca API
def cast_to_alpaca(exchange):
  """
  Cast tickers from Yahoo Finance format to Alpaca format
  :param exchange: list of tickers to be casted
  :return: list of tickers in Alpaca format
  """
  for i in range(0, len(exchange)):
    # Strings are immutable, we therefore need new assignment
    exchange[i] = exchange[i].replace('-UN', '.U')
    exchange[i] = exchange[i].replace('-', '.')

  return exchange


def connect_api():
  """
  Connect to Alpaca API
  :return: api object
  """
  api = tradeapi.REST(cf.API_KEY, cf.API_SECRET, cf.API_URL)
  print("|---------------| Successfully connected to Alpaca API. |---------------|")

  return api


def populate_exchange_list(acronym, assets):
  """
  Query the active and tradeable asset universe for tickers of a specific exchange
  :param acronym: acronym of exchange that the assets should be queried for
  :param assets: list of asset objects to be queried
  :return: list of tickers listed on the given exchange
  """
  exchange = []

  for asset in assets:
    try:
      if asset.status == 'active' and asset.tradable and asset.exchange == acronym:
        exchange.append(asset.symbol)
    except Exception as e:
      continue

  return exchange


def populate_exchange_df(acronym, assets, conn):
  """
  Query the active and tradeable asset universe for ticker data of a specific exchange
  :param acronym: acronym of exchange that the assets should be queried for
  :param assets: list of asset objects to be queried
  :param conn: MySQL database connection
  :return: dataframe of tickers with their corresponding exchange_id, symbol, and name of the given exchange
  """
  exchange_df = pd.DataFrame(columns = ['exchange_id', 'ticker', 'name'])

  for asset in assets:
    try:
      if asset.status == 'active' and asset.tradable and asset.exchange == acronym:
        tmp = {'exchange_id' : get_exchange_id(acronym, conn), 'ticker' : asset.symbol, 'name' : asset.name}
        exchange_df = exchange_df.append(tmp, ignore_index = True)
    except Exception as e:
      continue

  return exchange_df


def update_historical_prices_alpaca(tickers_df, vendor, api, conn, cursor, end_date):
  """
  Retrieves historical prices in a given timeframe for a set of tickers from Alpaca and writes them directly to the database
  :param tickers_df: dataframe of tickers for which prices should be written
  :param vendor: vendor id
  :param api: Alpaca api object
  :param conn: MySQL database connection
  :param cursor: MySQL database cursor for query execution
  :param end_date: latest price date
  """
  for ticker in tqdm(tickers_df.itertuples()):
    try:
      next_date = str(ticker.last_date + datetime.timedelta(days = 1)) + 'T00:00:00-05:00' # adhering to the strict alpaca date format
      end_date = end_date + 'T00:00:00-05:00' # adhering to the strict alpaca date format
      tmp = api.get_barset(ticker.ticker, 'day', limit = 1000, start = next_date, end = end_date).df

      for row in tmp.itertuples():
        values = [vendor, ticker.id] + list(row)
        sql = "INSERT INTO price (vendor_id, ticker_id, price_date, open_price, high_price, low_price, close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, tuple(values))
    except Exception as e:
      print('Failed to update {}.'.format(ticker.ticker))
      continue

  conn.commit()
  print("|---------------| Data successfully written to database. |---------------|")
  return


def write_historical_prices_alpaca(tickers, api, conn, cursor, start_date, end_date):
  """
  Retrieves historical prices for a set of tickers from Alpaca and writes them directly to the database
  :param tickers: list of tickers for which prices should be written
  :param api: Alpaca api object
  :param conn: MySQL database connection
  :param cursor: MySQL database cursor for query execution
  :param start_date: earliest price date
  :param end_date: latest price date
  """
  ticker_index = dict(tickers.to_dict('split')['data'])
  ticker_list = list(ticker_index.keys())
  vendor = get_vendor_id('Alpaca', conn)

  for ticker in tqdm(ticker_list):
    try:
      tmp = api.get_barset(ticker, 'day', limit = 1000, start = start_date, end = end_date).df
      tmp.drop(tmp.tail(1).index, inplace = True) # drop row for 'today' as long as end_date is not working

      for row in tmp.itertuples():
        values = [vendor, ticker_index[ticker]] + list(row)
        sql = "INSERT INTO price (vendor_id, ticker_id, price_date, open_price, high_price, low_price, close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, tuple(values))
    except Exception as e:
      continue

  conn.commit()
  print("|---------------| Data successfully written to database. |---------------|")
  return




# Yahoo Finance API
def cast_to_yf(exchange):
  """
  Cast tickers from Alpaca format to Yahoo Finance format
  :param exchange: list of tickers to be casted
  :return: list of tickers in Yahoo Finance format
  """
  for i in range(0, len(exchange)):
    # Strings are immutable, we therefore need new assignment
    exchange[i] = exchange[i].replace('.U', '-UN')
    exchange[i] = exchange[i].replace('.', '-')

  return exchange


def determine_splits(exchange, batch_size):
  """
  Determines the amount of splits for a list, given a batch_size
  :param exchange: list for which splits should be determined
  :param batch_size: max. amount of tickers to be included in one batch
  :return: amount of splits
  """
  return -(-len(exchange) // batch_size)


def flatten_exceptions(excepts):
  """
  Flattens a list with 2 levels.
  :param excepts: list of lists to be flattened
  :return: flattened list
  """
  return [[item for sublist in excepts for item in sublist]][0]


def get_corporate_info(exchange, batch_size, koi):
  """
  Wrapper function to start a batch-wise download of corporate information for a list of tickers
  :param exchange: list of tickers to get corporate information for
  :param batch_size: max. amount of tickers to be included in one batch
  :koi: which dictionary items to add to the return dataframe
  :return: dataframe that holds key corporate information of all tickers
  """
  n_batches = determine_splits(exchange, batch_size)
  exchange_df = pd.DataFrame()

  print("Downloading data in {} batches of size {}.".format(n_batches, batch_size))
  for i in range(0, n_batches * batch_size, batch_size):
    exchange_batch = get_info_batch(i, i + batch_size, exchange, koi)
    exchange_df = exchange_df.append(exchange_batch)
    time.sleep(10)

  return exchange_df


def get_info_batch(start_idx, end_idx, exchange, koi):
  """
  Batch-wise download of key corporate information for a list of tickers
  :param start_idx: defines window of tickers within the passed list
  :param end_idx: defines window of tickers within the passed list
  :param exchange: list of tickers to get corporate information for
  :koi: which dictionary items to add to the return dataframe
  :return: dataframe that holds key corporate information of all tickers
  """
  exchange_batch = pd.DataFrame()

  for ticker in tqdm(exchange[start_idx:end_idx]):
    try:
      tmp = yf.Ticker(ticker).info
      data = {key: tmp[key] for key in koi}
      exchange_batch = exchange_batch.append(data, ignore_index = True)
    except Exception as e:
      continue

  return exchange_batch


def update_historical_prices_yf(tickers_df, vendor, conn, cursor, end_date):
  """
  Retrieves historical prices in a given timeframe for a set of tickers from Yahoo Finance and writes them directly to the database
  :param tickers_df: dataframe of tickers for which prices should be written
  :param vendor: vendor id
  :param conn: MySQL database connection
  :param cursor: MySQL database cursor for query execution
  :param end_date: latest price date
  """
  for ticker in tqdm(tickers_df.itertuples()):
    try:
      next_date = str(ticker.last_date + datetime.timedelta(days = 1))
      tmp = pdr.get_data_yahoo(ticker.ticker, start = next_date, end = end_date, progress = False)
      tmp = tmp.iloc[1:, :] # drop row for last price_date

      for row in tmp.itertuples():
        values = [vendor, ticker.id] + list(row)
        sql = "INSERT INTO price (vendor_id, ticker_id, price_date, open_price, high_price, low_price, close_price, adj_close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, tuple(values))
    except Exception as e:
      print("Failed to update {}.".format(ticker.ticker))
      continue

  conn.commit()
  print("|---------------| Data successfully written to database. |---------------|")
  return


def write_historical_prices_yf(tickers, conn, cursor, start_date, end_date):
  """
  Retrieves historical prices for a set of tickers from Yahoo Finance and writes them directly to the database
  :param tickers: list of tickers for which prices should be written
  :param conn: MySQL database connection
  :param cursor: MySQL database cursor for query execution
  :param start_date: earliest price date
  :param end_date: latest price date
  :return: list of tickers that no data could be retrieved for, casted to Alpaca format
  """
  ticker_index = dict(tickers.to_dict('split')['data'])
  ticker_list = cast_to_yf(list(ticker_index.keys()))
  vendor = get_vendor_id('Yahoo Finance', conn)
  excepts = []

  for ticker in tqdm(ticker_list):
    try:
      tmp = pdr.get_data_yahoo(ticker, start = start_date, end = end_date, progress = False)

      if tmp.empty:
        excepts.append(ticker) # tickers that no exception was raised but no data was found
        continue

      for row in tmp.itertuples():
        values = [vendor, ticker_index[ticker]] + list(row)
        sql = "INSERT INTO price (vendor_id, ticker_id, price_date, open_price, high_price, low_price, close_price, adj_close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, tuple(values))
    except Exception as e:
      excepts.append(ticker) # tickers for which an exception was thrown
      continue

  conn.commit()
  print("|---------------| Data successfully written to database. |---------------|")
  return cast_to_alpaca(excepts)




# Updating
def get_new_tickers(assets, tickers, conn):
  """
  Compares an asset universe from the Alpaca API against a list of tickers and returns a dataframe of new tickers to be added to the database
  :param assets: list of asset objects to be queried
  :param tickers: list of tickers to compare against
  :param conn: MySQL database connection
  :return: dataframe of all newly identified tickers with their corresponding exchange, symbol and name
  """
  new_tickers_df = pd.DataFrame()

  for asset in assets:
    try:
      if asset.status == 'active' and asset.tradable and asset.exchange in get_all_exchanges(conn) and asset.symbol not in tickers:
        data = []
        data.append([asset.exchange, asset.symbol, asset.name])

        tmp = pd.DataFrame(data, columns = ['exchange_id', 'ticker', 'name'])
        new_tickers_df = new_tickers_df.append(tmp, ignore_index = True)
        print('Identified a new instrument: {}, {} ({})'.format(asset.symbol, asset.name, asset.exchange))
    except Exception as e:
      continue

  return new_tickers_df


def get_update_info_from_list(conn, cursor):
  """
  Gets latest price_date for every ticker in the database and returns it together with some other information
  :param conn: MySQL database connection
  :param cursor: MySQL database cursor for query execution
  :return: dataframe of tickers with their corresponding ids, vendor_ids and last price_dates
  """
  cursor.execute("SET sql_mode = (SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));") # to use group by with an aggregated column

  sql = """
    SELECT j.ticker, j.id, j.vendor_id, MAX(j.price_date) last_date
    FROM
      (SELECT ins.ticker, ins.id, p.vendor_id, p.price_date
      FROM instrument AS ins
      INNER JOIN price AS p
      ON ins.id = p.ticker_id) AS j
    GROUP BY j.id;"""

  return pd.read_sql(sql, conn)


def get_yesterday():
  """
  Determines 'yesterday', i.e. the last past weekday to today's date
  :return: 'yesterday' in datetime format
  """
  today = datetime.date.today()
  # yesterday = today - datetime.timedelta(days = 1)
  yesterday = today

  if yesterday.weekday() == 5:
    # yesterday was Saturday -> go back to Friday
    yesterday -= datetime.timedelta(days = 1)
  elif yesterday.weekday() == 6:
    # yesterday was Sunday -> go back to Friday
    yesterday -= datetime.timedelta(days = 2)

  return yesterday




# Interacting
def compare_tickers(tickers, conn):
  """
  Takes in a list of tickers, queries the database for their closing prices and returns a dataframe with all values
  :param tickers: list of tickers for which closing prices should be returned
  :param conn: MySQL database connection
  :return: dataframe with closing prices for the ticker list
  """
  tickers_df = pd.DataFrame()

  for ticker in tickers:
    tmp = get_price_df(ticker, conn)
    if tmp.columns[0] == 'adj_close_price':
      # ticker is a Yahoo ticker
      tmp = tmp.rename(columns = {'adj_close_price' : ticker})
    else:
      # ticker is an Alpaca ticker
      tmp = tmp.rename(columns = {'close_price' : ticker})
    tickers_df = pd.concat([tickers_df, tmp], axis = 1)

  return tickers_df.sort_values(by = ['price_date'], ascending = True)


def get_SP500_constituents():
  """
  Scrapes the Wikipedia article on S&P500 constituents and returns a list of their tickers
  :return: list of S&P500 tickers
  """
  url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
  page = requests.get(url) # handles the contents of the website
  doc = lh.fromstring(page.content) # stores website contents

  # data is stored in a table <tr>..</tr>
  tr_elements = doc.xpath('//tr')
  cols = []

  # for each row, store each first element (header) and an empty list
  for t in tr_elements[0]:
    name = t.text_content()
    cols.append((name, []))

  # actual data is stored on the second row onwards
  for j in range(1, len(tr_elements)):
    T = tr_elements[j] # T is j'th row

    # If row is not of size 9 (hard-coded), the //tr data is not from our table
    if len(T) != 9:
      break

    i = 0 # column index

    # iterate through each row element
    for t in T.iterchildren():
      data = t.text_content()
      cols[i][1].append(data) # append data to empty list of i'th column
      i += 1 # next column

  col_names = {title:column for (title, column) in cols}
  df = pd.DataFrame(col_names)
  tickers = df['Symbol\n'].str.replace('\n', '').to_list()

  return tickers

