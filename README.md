# sentimental-stocks

We investigate the accuracy of the Linear Regression and Random Forest models using financial fundamentals and qualitative information (public sentiment extracted from Twitter and corporate news) in predicting short-term stock price movements. Our data ranges over nearly 3 years — 2019 to 2021 — for 15 selected stocks of the S\&P500 Index. The conclusive analysis of our paper is able to produce a high R-square value () and a low Maximum Absolute Percentage Error () using the Random Forest model for a 15-day prediction.

------------
<br>

## Dependencies

To run the pipeline or any of the components, you first need to clone this repository to your local machine and install the necessary requirements using the command (from within the directory):

`pip3 install -r requirements.txt`

<br>

------------
<br>

## Running the Code

We advise you to run the `pipeline.ipynb` notebook on Google Colab. To ensure a complete run, upload the following CSV files to the Colab directory first:

1. `data/quantitative/financial_signals.csv`
2. `data/quantitative/price_signals.csv`
3. `data/qualitative/tweets_raw.csv`
4. `data/qualitative/news_raw.csv`

<br>

For running only the ML models (from the `Linear Regression`) section, upload only the following CSV files to the Colab directory:

1. `data/compiled/ml_dataset_3.csv`
2. `data/compiled/ml_dataset_7.csv`
3. `data/compiled/ml_dataset_15.csv`

<br>

------------
<br>

## Directory Tree

- `pipeline.ipynb` (Main Project Pipeline)

- `components/` (Includes various code blocks that finally form the Pipeline)
    - `stocks` (Extracting Financial Data)
    - `helper` (Helper Functions)
    - `temp_data` (Raw/Temporary Data)
    - `sentiments` (Sentiment Analysis)

- `data/`
    - `compiled` (Final ML Datasets)
    - `qualitative` (Raw scraped tweets and articles)
    - `quantitative` (Financial Datasets)
    - `sentiments` (Post Sent-Analysis Datasets)

- `scraper/`
    - `twitter.py` (Scrapes Tweets)
    - `news_source1.py` (Scrapes Financial News)
    - `news_source2.py` (Scrapes Company News)
    - `merge_news.py` (Merges Company and Financial Data)

- `results/` (Graphs and Tables)
    
- `stocks.json` (The 15 stocks we analyze)

<br>

------------
<br>

Developers: Tanish Bafna, Aditya Sarkar, Pritish Dugar