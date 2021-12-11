import json
import datetime
import numpy as np
import pandas as pd

#----------------------

stocks = json.load(open('stocks.json'))

sdate_str = '2019-01-01'
edate_str = '2021-12-01'

sdate_dt = datetime.datetime.strptime(sdate_str, '%Y-%m-%d')
edate_dt = datetime.datetime.strptime(edate_str, '%Y-%m-%d')

date_range = pd.date_range(sdate_dt, edate_dt - datetime.timedelta(days=1), freq='d')
bdate_range = pd.bdate_range(sdate_dt, edate_dt - datetime.timedelta(days=1))
weekends_dt = [c for c in date_range.difference(bdate_range)]

holidays_str = ['2019-01-01','2019-01-21','2019-02-18','2019-04-19','2019-05-27','2019-07-04','2019-09-02','2019-11-28','2019-12-25',
    '2020-01-01','2020-01-20','2020-02-17','2020-04-10','2020-05-25','2020-07-03','2020-09-07','2020-11-26','2020-12-25',
    '2021-01-01','2021-01-18','2021-02-15','2021-04-02','2021-05-31','2021-07-05','2021-09-06','2021-11-25']
holidays_dt = [datetime.datetime.strptime(c, '%Y-%m-%d') for c in holidays_str]

nonWorkingDays = weekends_dt + holidays_dt
nonWorkingDays.sort()
workingDays = [day for day in date_range if day not in nonWorkingDays]

method = 1

mapNonWork = {}
for d in nonWorkingDays:

    if method == 1:
        availableDates = [date for date in workingDays if date > d]
    else:
        availableDates = [date for date in workingDays if date < d]

    try:
        mapNonWork[d.strftime('%Y-%m-%d')] = min(availableDates, key=lambda x : abs(x - d)).strftime('%Y-%m-%d')
    except ValueError:
        continue

for k, v in mapNonWork.items():
    print(f'{k}: {v}')

#----------------------

#tweets = pd.read_csv('data/sentiment/tweets_sentiment.csv', converters={'Date':lambda x: x[:10]})
# tweets

# ID,Stock,Date,User_Name,User_ID,User_Followers,User_Verified,User_Lists,Likes,Retweets,Replies,URL,Text,Cleaned,sentiment,weighted_sentiment
