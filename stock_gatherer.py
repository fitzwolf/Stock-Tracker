import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

def get_sp500_tickers():
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    sp500_stocks = table[0]['Symbol'].tolist()
    sp500_stocks = [stock.replace('.', '-') for stock in sp500_stocks]
    return sp500_stocks

sp500_stocks = get_sp500_tickers()  # get all S&P 500 stocks
pd.DataFrame(sp500_stocks, columns=['Tickers']).to_csv('ticker_list.csv', index=False)  # Save the ticker list to a CSV file

end_date = datetime.now()
start_date = end_date - timedelta(days=90)

data = []

for stock in sp500_stocks:
    try:
        csv_path = os.path.join('data', f'{stock}.csv')

        if os.path.exists(csv_path):  # Check if the CSV file for the stock exists
            df = pd.read_csv(csv_path, index_col=0, parse_dates=True)  # Load the data from the CSV file
            yf_ticker = yf.Ticker(stock)
            company_name = yf_ticker.info['longName']
            df.index = df.index.tz_localize(None)  # remove timezone information from df
        else:
            yf_ticker = yf.Ticker(stock)
            df = yf_ticker.history(start=start_date, end=end_date)
            df.to_csv(csv_path)  # Save the data for the stock in a separate CSV file
            company_name = yf_ticker.info['longName']
            df.index = df.index.tz_localize(None)  # remove timezone information from df

        idx = pd.date_range(start=start_date, end=end_date)  # create a full date range
        df = df.reindex(idx, method='ffill')  # reindex the df with the full date range, forward filling any missing data

        # Forward-fill the first day if it is null
        if pd.isnull(df.iloc[0]['Close']):
            df.iloc[0] = df.iloc[1]

        initial_price = df.iloc[0]['Close']
        final_price = df.iloc[-1]['Close']
        return_90_days = ((final_price - initial_price) / initial_price) * 100
        print(f"90 day return for {stock} is {return_90_days}")
        row = [stock, company_name] + list(df['Close']) + [return_90_days]

        if len(row) == 94:  # Adjusted row length
            data.append(row)
        else:
            print(f"Skipping {stock} because it has less than 90 days of data")

    except Exception as e:
        print(f"An error occurred while downloading data for {stock}: {str(e)}")

columns = ['Ticker', 'Company Name'] + list(df.index.strftime('%Y-%m-%d')) + ['Percent Growth']  # Adjusted range
df_stocks = pd.DataFrame(data, columns=columns)

# Sort the DataFrame by the absolute value of the 'Percent Growth' column in descending order
df_stocks['abs_growth'] = df_stocks['Percent Growth'].abs()
df_stocks = df_stocks.sort_values(by='abs_growth', ascending=False)

# Select the top 50 stocks
df_stocks = df_stocks.iloc[:50]

df_stocks.to_csv('sp500_top50_data.csv', index=False)
