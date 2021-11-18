import pymongo
import datetime as dt
import pandas as pd
import yfinance as yf

class MongoInteractor:
    """
    Module to connect to the local mongo client and interact with the relevant databases.
    """
    def __init__(self, config) -> None:
        self.config = config
        self.client = None
        self.data_db = None
        self.strategy_db = None
        self.data_collection = None
        self.support_collection = None
        self.strategy_collection = None

    def create_connections(self):
        """
        Connects to the mongo client and the relevant databases.
        """
        self.client = pymongo.MongoClient()
        self.data_db = self.client[self.config['data_db']]
        self.strategy_db = self.client[self.config['strategy_db']]
        self.data_collection = self.data_db[self.config['data_collection']]
        self.support_collection = self.data_db[self.config['support_collection']]
        self.strategy_collection = self.strategy_db[self.config['strategy_collection']]
        return

    def destroy_connections(self):
        """
        Closes the mongo client connection.
        """
        self.client.close()
        return

    def fetch_data(self, stock_name, start_date=None, end_date=None):
        """
        Fetch data from the local mongo database for a stock in the start_date - end_date interval.
        If start_date or end_date are None, all the data available in that direction is fetched.
        """
        mongo_query = {
            'instrument_name': f'EQTSTK_{stock_name}_XXXXXXXXX_XX_0'
        }
        date_query_dict = {}
        if not start_date == None:
            date_query_dict['$gte'] = start_date
        if not end_date == None:
            date_query_dict['$lte'] = end_date
        if not date_query_dict == {}:
            mongo_query['date'] = date_query_dict
        data = pd.DataFrame(self.data_collection.find(mongo_query))
        return data
    
    def save_trades(self, trades_list, doc_name):
        """
        Save the trades in MongoDB.
        """
        if not trades_list == []:
            self.strategy_collection.insert_one({'_id': doc_name, 'trades': trades_list})
        return

class YahooDataFetcher:
    """
    Module to fetch data from Yahoo Finance.
    """
    def __init__(self) -> None:
        pass

    def fetch_data(self, stock_name, start_date=None, end_date=None):
        data = yf.download(tickers=stock_name, start=start_date, end=end_date)
        return data

def create_train_test_split( combined_stock_df, train_period, test_period):
    """
    Module to create train-test splits based on the train_period and test_period parameter.
    """
    max_len = len(combined_stock_df)
    split_df_list = []
    i = 0
    while i < max_len - test_period:
        split_df = combined_stock_df[i:i+train_period+test_period]
        split_df.reset_index(inplace=True, drop=True)
        split_df_list.append(split_df)
        i += train_period
    return split_df_list

def get_transaction_costs(buy_price, sell_price, quantity):
    return 0