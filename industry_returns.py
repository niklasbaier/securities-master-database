import pickle
import datetime
import pymysql.cursors

import pandas as pd
pd.options.mode.chained_assignment = None  # default = 'warn'

from tqdm import tqdm

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'your_password_here'
DB_DATABASE = 'securities_master'

conn = pymysql.connect(
  host = DB_HOST,
  user = DB_USER,
  password = DB_PASSWORD,
  database = DB_DATABASE)
print("|---------------| Successfully connected to {}. |---------------|".format(DB_DATABASE))

industries = pd.read_sql("SELECT DISTINCT(industry) FROM instrument;", conn)
industries.dropna(inplace = True)
industries.sort_values(by = 'industry', inplace = True)
industries = industries.iloc[1:]
industries = list(industries.industry)

industry_returns = pd.DataFrame()

for industry in tqdm(industries):
  sql = """
    SELECT ins.id, p.price_date, p.adj_close_price
    FROM instrument AS ins
    INNER JOIN price AS p
    ON ins.id = p.ticker_id
    WHERE industry = '{}';""".format(industry)

  tmp = pd.read_sql(sql, conn)
  tmp['simple_return'] = tmp.adj_close_price.pct_change()

  for i in range(1, len(tmp.id)):
    if tmp.id.at[i] != tmp.id.at[i-1]:
      tmp.simple_return.at[i] = None

  if tmp.empty:
    industry_returns[industry] = 0
  else:
    industry_returns[industry] = tmp.groupby('price_date').mean().iloc[:,-1]

today = datetime.date.today()
pickle.dump(industry_returns, open('industry_returns/' + str(today) + '.p', 'wb'))
print("|---------------| Successfully took a dump. |---------------|")
