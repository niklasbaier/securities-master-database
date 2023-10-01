import datetime
import pickle
import time
import sys
import os

from pi_utils import *

# Load in industry returns
os.system("mysql.server start")
# count = int(sys.argv[1])
count = 10

today = datetime.date.today()

if not os.path.exists('industry_returns/' + str(today) + '.p'):
  print("Calculating industry returns ...")
  os.system('python industry_returns.py')

industry_returns = pickle.load(open('industry_returns/' + str(today) + '.p', 'rb'))


# Determine top and flop industries
tops = (industry_returns.iloc[-1,:].sort_values(ascending = False).iloc[:count] * 100).round(2)
flops = (industry_returns.iloc[-1,:].sort_values().iloc[:count] * 100).round(2)

tops_hl = "\nTop " + str(count) + " Industries" + ":\n"
tops_str = tops.to_string()
flops_hl = "\nFlop " + str(count) + " Industries" + ":\n"
flops_str = flops.to_string()

print(tops_hl, "\033[92m" + tops_str + "\033[0m", flops_hl, "\033[93m" + flops_str + "\033[0m")

os.system("mysql.server stop")
print("\n")


# Send mail
sender = "your_mail_here"
password = "your_password_here"
# reciever = ["other_mail_here", "other_mail_here"]
reciever = "other_mail_here"

subject = "Daily Industry Winners & Losers, " + str(today)
body = tops_hl + tops_str + "\n" + flops_hl + flops_str
sendMail(sender, password, reciever, subject, body)
