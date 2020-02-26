from datetime import datetime
from concurrent import futures
import argparse
import pandas as pd
from pandas import DataFrame
import pandas_datareader.data as web
import os

parser = argparse.ArgumentParser(description="grep stock price")
parser.add_argument('--symbols', '-s', type=str, required=True, help="tickers symbols for the stock")
parser.add_argument('--collected-years', '-y', type=int, default = 1, help="dimensionality of hidden layers (default: 64)")

all_args = parser.parse_args()
symbols = all_args.symbols
year = all_args.collected_years

# turn the tickers into a list and feed into the function
symbols = [str(item) for item in symbols.split(',')]

folder = r'./fabianStocks/'
if not os.path.exists(folder):
    os.makedirs(folder)

def download_stock(stock):
    try:
        print(stock)
        stock_df = web.DataReader(stock, 'yahoo', start_time, now_time)
        stock_df['Name'] = stock
        output_name = stock + '_data.csv'
        stock_df.to_csv(folder + output_name)
    except:
        bad_names.append(stock)
        print('bad: %s' %(stock))

if __name__ == '__main__':
    # symbols = ['TSLA']

    bad_names = [] # placeholder
    '''
    use concurrent.futures module's ThreadPoolExecutor to speed up the downloads by doing them in parallel as opposoed to sequentially
    '''

    # set the maximum thread number
    max_workers = 50
    workers = min(max_workers, len(symbols))

    now_time = datetime.now()
    start_time = datetime(now_time.year - year, now_time.month , now_time.day)    

    with futures.ThreadPoolExecutor(workers) as executor:
        res = executor.map(download_stock, symbols)

    '''
    save failed queries to a text file
    '''
    if len(bad_names)>0:
        with open(folder+'failed_queries.txt', 'w') as outfile:
            for name in bad_names:
                outfile.write(name+'\n')
        
    #get market cap for all tickers
    market_cap = web.get_quote_yahoo(symbols)['marketCap']
    df = pd.DataFrame({'Name': market_cap.index, "MarketCap": market_cap.values})
    df.to_csv("marketcap.csv", index=False)
