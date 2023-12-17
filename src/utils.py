import pymongo
import datetime as dt
import pandas as pd
import yfinance as yf
import json

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
        mongo_params = self.config
        self.data_db = self.client[mongo_params['data_db']]
        self.strategy_db = self.client[mongo_params['strategy_db']]
        self.data_collection = self.data_db[mongo_params['data_collection']]
        self.support_collection = self.data_db[mongo_params['support_collection']]
        self.strategy_collection = self.strategy_db[mongo_params['strategy_collection']]
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

    def save_data(self, data_list):
        if not data_list == []:
            self.data_collection.insert_many(data_list)
        else:
            raise Exception("data_list is empty.")
        return

class YahooDataFetcher:
    """
    Module to fetch data from Yahoo Finance.
    """
    def __init__(self) -> None:
        pass

    def fetch_data(self, stock_name, start_date=None, end_date=None, period='1D'):
        data = yf.download(tickers=stock_name, start=start_date, end=end_date, period=period)
        return data

    def format_data(self, stock_name, sector, data):
        if data.empty:
            raise Exception("data is empty")
        data_list = []
        for idx, row in data.iterrows():
            dict_ = {}
            dict_["date"] = idx
            dict_["instrument_name"] = f"EQTSTK_{stock_name}_XXXXXXXXX_XX_0"
            dict_["underlying"] = stock_name
            dict_["asset_type"] = "EQT"
            dict_["security_type"] = "STK"
            dict_["expiry"] = dt.datetime(1970, 1, 1)
            dict_["strike"] = 0
            dict_["open"] = row["Open"]
            dict_["high"] = row["High"]
            dict_["low"] = row["Low"]
            dict_["close"] = row["Close"]
            dict_["adj_close"] = row["Adj Close"]
            dict_["volume"] = row["Volume"]
            dict_["freq"] = "1D"
            dict_["sector"] = sector
            dict_["name"] = stock_name
            dict_["_id"] = f"EQTSTK_{stock_name}_XXXXXXXXX_XX_0|1D|{str(idx).split(' ')[0]}"
            data_list.append(dict_)
        return data_list

    # def push_data_to_mongo(self, data_list):
    #     mongo_db = MongoInteractor()
    #     return

def create_train_test_split(combined_stock_df, train_period, test_period):
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

def update_database(config_dict, sectors_dict, start_date, end_date):
    yahoo_data = YahooDataFetcher()
    mongo_db = MongoInteractor(config_dict)
    mongo_db.create_connections()
    issue_list = []
    for sector, stock_list in sectors_dict.items():
        for stock in stock_list:
            try:
                data = yahoo_data.fetch_data(stock, start_date, end_date, period='1D')
                data_list = yahoo_data.format_data(stock, sector, data)
                mongo_db.save_data(data_list)
                print(f"Done with : {sector} | {stock}")
            except Exception as error:
                print(error, f"{sector} | {stock}")
                issue_list.append((sector, stock))
    mongo_db.destroy_connections()
    print(issue_list)
    return

if __name__ == "__main__":
    with open('config.json') as jfile:
        config_dict = json.load(jfile)
    
    with open('sectors.json') as jfile:
        sectors_dict = json.load(jfile)

    start_date = "2021-11-13"
    end_date = "2023-12-01"

    update_database(config_dict, sectors_dict, start_date, end_date)