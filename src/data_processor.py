import json
import pandas as pd


class DataProcessor:
    """
    Module to - 
    -> obtain data from the local MongoDB or from Yahoo Finance.
    -> match the dates for both the stocks
    -> combine the data for the 2 stocks
    """
    def __init__(self, mongo_interactor) -> None:
        self.mongo_interactor = mongo_interactor

    def perform_date_matching(self, df_1, df_2):
        """
        This function matches the dates in the 2 dataframes. Dates present in one and absent in the other dataframe are omitted.
        """
        dates_1, dates_2 = df_1['date'], df_2['date']
        common_dates = list(set.intersection(set(dates_1), set(dates_2)))
        df_1 = df_1[df_1['date'].isin(common_dates)]
        df_2 = df_2[df_2['date'].isin(common_dates)]
        df_1.reset_index(inplace=True, drop=True)
        df_2.reset_index(inplace=True, drop=True)
        return df_1, df_2

    def get_data(self, stock_1, stock_2, start_date=None, end_date=None):
        """
        This function - 
        1) Fetches data for the 2 stocks from MongoDB.
        2) Performs date matching.
        3) Creates a new single dataframe with relevant data of both the stocks.
        """
        stock_1_data = self.mongo_interactor.fetch_data(stock_1, start_date, end_date)
        stock_2_data = self.mongo_interactor.fetch_data(stock_2, start_date, end_date)
        stock_1_data, stock_2_data = self.perform_date_matching(stock_1_data, stock_2_data)
        combined_stock_df = pd.DataFrame()
        combined_stock_df['date'] = stock_1_data['date']
        combined_stock_df['next_date'] = stock_1_data['date'].shift(-1)
        combined_stock_df['underlying_1'] = stock_1_data['underlying']
        combined_stock_df['underlying_2'] = stock_2_data['underlying']
        combined_stock_df['close_1'] = stock_1_data['close']
        combined_stock_df['close_2'] = stock_2_data['close']
        combined_stock_df['next_open_1'] = stock_1_data['open'].shift(-1)
        combined_stock_df['next_open_2'] = stock_2_data['open'].shift(-1)
        combined_stock_df['sector'] = stock_1_data['sector']
        combined_stock_df.dropna(inplace=True)
        combined_stock_df.reset_index(inplace=True, drop=True)
        return combined_stock_df


