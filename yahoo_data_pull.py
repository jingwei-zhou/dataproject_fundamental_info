import io
import pandas as pd
tickers = pd.read_csv(io.StringIO('''
AMAT
MA
ASML
AMD
LYV
WM
CL
HOOD
GLOB
MPWR
CVX
CTSH
VST
LRCX
SPOT
TXN
STM
NVDA
DVA
NXPI
KLAC
ADI
VRSN
SMAR
TENB
DAVA
WRBY
DUOL
LSCC
PUBM
'''), header=None)

# step3

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

# Set pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# List of User-Agents to rotate
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1"
]

# Function to convert HTML table to DataFrame
def html_table_to_dataframe(html_content):
    table = html_content.find('table')

    # Extract table headers
    headers = [header.text for header in table.find('thead').find_all('th')]

    # Extract table rows
    rows = []
    for row in table.find('tbody').find_all('tr'):
        rows.append([cell.text for cell in row.find_all('td')])

    # Create DataFrame
    df = pd.DataFrame(rows, columns=headers)
    return df

# Initialize an empty DataFrame to store results
stock_fund_data = pd.DataFrame(columns=['ticker', 'Current year revenue', 'Next year revenue', 'Current year eps', 'Next year eps'])

# List of tickers to scrape (example list)
# tickers = ['AAPL', 'GOOGL', 'MSFT']  # Add your list of tickers here

# Loop through each ticker and scrape data
n = 0
for ticker in tickers:
    try:
        # Rotate User-Agent header
        headers = {"User-Agent": random.choice(user_agents)}

        url = f"https://finance.yahoo.com/quote/{ticker}/analysis"
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        html_content_revenue = soup.find('section', {'data-testid': 'revenueEstimate'})  # this is the key to find the data test id name !!!!
        html_content_earnings = soup.find('section', {'data-testid': 'earningsEstimate'})

        # Convert the HTML table to a DataFrame
        revenue_df = html_table_to_dataframe(html_content_revenue)
        earnings_df = html_table_to_dataframe(html_content_earnings)
        print(ticker)

        # Append the data to the DataFrame
        stock_fund_data.loc[n] = [ticker, revenue_df.iloc[1, 3], revenue_df.iloc[1, 4], earnings_df.iloc[1, 3], earnings_df.iloc[1, 4]]
        n += 1

        # Random delay between requests
        time.sleep(random.uniform(0.5, 5.0))

    except Exception as e:
        print(f"Error processing ticker {ticker}: {e}")
        continue

# Display the resulting DataFrame
##print(stock_fund_data)

# Optionally, save the DataFrame to a CSV file
#stock_fund_data.to_csv("stock_fund_data.csv", index=False)
print(stock_fund_data.shape)
