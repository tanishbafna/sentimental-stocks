import pandas as pd
import tqdm
import os

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
tweets_df.to_csv('data/tweets_raw.csv')
print(f'Scraped {len(tweets_df.index)} Tweets')

for stock in tweets_df['Stock'].unique():
    print(f"{stock}: {len(tweets_df[tweets_df['Stock']==stock])}")
