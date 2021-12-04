import os
import tqdm
import pandas as pd
from numpy import np
from datetime import datetime

#----------------------

def tweetData():

    path = '/Users/tanishbafna/Downloads/data'
    folders = [i for i in os.listdir(path) if not i.startswith('.')]

    arr = []

    for folder in folders:
        files = [i for i in os.listdir(f'{path}/{folder}/progress') if i.endswith('.csv')]
        for file in tqdm.tqdm(files):
            df = pd.read_csv(f'{path}/{folder}/progress/{file}', converters={'ID':int, 'User_ID':int, 'User_Followers':int, 'User_Verified':int, 'User_Lists':int, 'Likes':int, 'Retweets':int, 'Replies':int})
            df.drop(columns=['Unnamed: 0'], inplace=True)
            arr.append(df)

    tweets_df = pd.concat(arr, ignore_index=True)
    tweets_df.drop_duplicates(subset=['ID', 'Stock'], ignore_index=True, inplace=True)
    tweets_df.to_csv('data/tweets_raw.csv', index=False)

    print(f'Scraped {len(tweets_df.index)} Tweets')

    for stock in tweets_df['Stock'].unique():
        print(f"{stock}: {len(tweets_df[tweets_df['Stock']==stock])}")

def newsData():

    df_company = pd.read_csv('data/temp/company_news.csv')
    df_financial = pd.read_csv('data/temp/financial_news.csv')

    rename_company = {
        'datetime':'Date',
        'headline': 'Title',
        'id': 'ID',
        'related': 'Stock',
        'source': 'Publisher',
        'summary': 'Description',
        'url': 'URL'
    }

    df_company.rename(columns=rename_company, inplace=True)
    df_company['Author'] = df_company['Publisher']
    df_company = df_company[['ID','Stock','Date','Publisher','Title','Author','Description','URL']]

    df_company['Date'] = df_company.apply(lambda row: datetime.utcfromtimestamp(row['Date']).strftime('%Y-%m-%d'), axis=1)
    df_financial['Date'] = df_financial.apply(lambda row: row['Date'][:10], axis=1)    

    final_df = pd.concat([df_company, df_financial], ignore_index=True) 
    final_df.drop_duplicates(subset=['ID','Stock','Date'], inplace=True)
    final_df.drop_duplicates(subset=['Stock','Date','URL'], inplace=True)

    final_df['Description'] = final_df.apply(lambda row: row['Title'] if row['Description'] is np.NaN else row['Description'])

    final_df.to_csv('data/news_raw.csv', index=False)
    print(f'Scraped {len(final_df.index)} Articles')

    for stock in final_df['Stock'].unique():
        print(f"{stock}: {len(final_df[final_df['Stock']==stock])}")

newsData()