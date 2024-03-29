import json
import pandas as pd
import os
import datetime as dt
import numpy as np
from statsmodels.tsa.stattools import adfuller
from statsmodels.regression.linear_model import OLS
import statsmodels.api as sm

from itertools import combinations
from math import floor
from multiprocessing import Pool

from data_processor import DataProcessor
from results import ResultsCalculator
from utils import MongoInteractor, get_transaction_costs, create_train_test_split

import warnings
warnings.filterwarnings("ignore")

class Strategy:
    def __init__(self, config, sector, stock_list) -> None:
        self.config = config
        self.stock_list = stock_list
        self.sector_name = sector
        self.mongo_interactor = None
        self.data_processor = None

        # Indicator Variables
        self.in_trade = False
        self.position = ""
        self.trade_dict = {}
        self.cumulative_pnl = 0
        self.trades_list = []
        self.create_connections()
    
    def create_connections(self):
        """
        Creates and connects to the helper modules.
        """
        self.mongo_interactor = MongoInteractor(self.config['database_parameters']['mongo'])
        self.mongo_interactor.create_connections()
        self.data_processor = DataProcessor(self.mongo_interactor)
        return

    def fit_regression_model(self, combined_stock_df, train_split_idx):
        """
        Fits a regression model and calculates the price spread using the hedge ratio (coefficient estimated by the regression model).
        """
        X = combined_stock_df[:train_split_idx]['close_2']
        Y = combined_stock_df[:train_split_idx]['close_1']
        X_1 = sm.add_constant(X)
        regression_model = OLS(Y, X_1)
        model_results = regression_model.fit()
        coefficient = model_results.params.values[1]
        combined_stock_df['price_spread'] = combined_stock_df['close_1'] - coefficient * combined_stock_df['close_2']
        combined_stock_df['hedge_ratio'] = coefficient
        return combined_stock_df

    def perform_adfuller_test(self, combined_stock_df):
        """
        Performs the Augmented Dickey Fuller test on the price spread.
        """
        adf_test_result = adfuller(combined_stock_df['price_spread'])
        return self.analyze_adf_test(adf_test_result)

    def analyze_adf_test(self, adf_test_result):
        """
        Validates the results of the Augmented Dickey Fuller test and gives the go ahead for trading if the test is successful (i.e. price spread is stationary).
        """
    #     return adf_test_result[1] < 0.05 and (adf_test_result[0] < min(adf_test_result[4].values()))
        return adf_test_result[1] < 0.05 and adf_test_result[0] <= adf_test_result[4]['5%']
    
    def calculate_signals(self, data, mean_period, sd_factor_1, sd_factor_2):
        """
        Calculates the rolling mean, rolling standard deviation, upper band and lower bands using the given parameters.
        Generates the signals for buying and selling the spread.
        """
        data['mean'] = data['price_spread'].rolling(window=mean_period).mean()
        data['std'] = data['price_spread'].rolling(window=mean_period).std()
        data['upper_band'] = data['mean'] + sd_factor_1 * data['std']
        data['lower_band'] = data['mean'] - sd_factor_1 * data['std']

        # Higher band can be used as a level of doubling down or stop loss. Yet to be tested.
        data['upper_band_2'] = data['mean'] + sd_factor_2 * data['std']
        data['lower_band_2'] = data['mean'] - sd_factor_2 * data['std']
        
        # sd_factor_1
        data['upper_band_breach_1'] = np.where((data['price_spread'] >= data['upper_band']) & (data['price_spread'].shift(1) < data['upper_band'].shift(1)), 1, 0) # Short the spread
        data['upper_band_backtrack_1'] = np.where((data['price_spread'] < data['upper_band']) & (data['price_spread'].shift(1) >= data['upper_band'].shift(1)), 1, 0) # Short the spread
        data['lower_band_breach_1'] = np.where((data['price_spread'] <= data['lower_band']) & (data['price_spread'].shift(1) > data['lower_band'].shift(1)), 1, 0) # Long the spread
        data['lower_band_backtrack_1'] = np.where((data['price_spread'] > data['lower_band']) & (data['price_spread'].shift(1) <= data['lower_band'].shift(1)), 1, 0) # Long the spread
        
        # sd_factor_2
        data['upper_band_breach_2'] = np.where((data['price_spread'] >= data['upper_band_2']) & (data['price_spread'].shift(1) < data['upper_band_2'].shift(1)), 1, 0) # Short the spread
        data['upper_band_backtrack_2'] = np.where((data['price_spread'] < data['upper_band_2']) & (data['price_spread'].shift(1) >= data['upper_band_2'].shift(1)), 1, 0) # Short the spread
        data['lower_band_breach_2'] = np.where((data['price_spread'] <= data['lower_band_2']) & (data['price_spread'].shift(1) > data['lower_band_2'].shift(1)), 1, 0) # Long the spread
        data['lower_band_backtrack_2'] = np.where((data['price_spread'] > data['lower_band_2']) & (data['price_spread'].shift(1) <= data['lower_band_2'].shift(1)), 1, 0) # Long the spread
        
        # Mean breach
        data['mean_breach_from_above'] = np.where(data['price_spread'] <= data['mean'], 1, 0)
        data['mean_breach_from_below'] = np.where(data['price_spread'] >= data['mean'], 1, 0)
        return data

    def trade_pairs(self, stock_1, stock_2, train_period, test_period, mean_period, std_factor_1, std_factor_2, capital_per_trade):
        """
        Master function to trade a particular pair. 
        """
        start_date = self.config['date_parameters']['start_date'] if not self.config['date_parameters']['start_date'] == "" else None
        end_date = self.config['date_parameters']['end_date'] if not self.config['date_parameters']['end_date'] == "" else None
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
        end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')
        
        # Obtain the aggregated data for the 2 stocks.
        combined_stock_df = self.data_processor.get_data(stock_1, stock_2, start_date=start_date, end_date=end_date)
        if len(combined_stock_df) < train_period + test_period:
            # Not sufficient data available.
            return []
        # Create train-test splits for the data.
        stock_df_list = create_train_test_split(combined_stock_df, train_period, test_period)
        all_trades_list = []
        for stock_df in stock_df_list:
            # Fit the regression model for the train data
            stock_df = self.fit_regression_model(stock_df, train_split_idx=train_period)
            # print(stock_df)
            if self.perform_adfuller_test(stock_df):
                # Augmented Dickey Fuller test passed.
                stock_df = self.calculate_signals(stock_df, mean_period, std_factor_1, std_factor_2)
                test_df = stock_df[-test_period:]
                test_df.reset_index(inplace=True, drop=True)
                # Simulate trade execution and log the execution info.
                trades = self.generate_trades(test_df, capital_per_trade=capital_per_trade)
                all_trades_list.extend(trades)
        # Save the trades in MongoDB.
        self.mongo_interactor.save_trades(all_trades_list, doc_name=f'{self.sector_name}|{stock_1}|{stock_2}')
        return all_trades_list

    def generate_trades(self, data_df, capital_per_trade, entry='backtrack'):
        """
        Simulates the trade execution and logs execution info.
        """
        self.trades_list = []
        for _, row in data_df.iterrows():
            if self.in_trade:
                if (self.position == 'Long' and row['mean_breach_from_below'] == 1) or (self.position == 'Short' and row['mean_breach_from_above'] == 1):
                    # Exit Signal obtained.
                    self.process_exit_signal(row, capital_per_trade)
                else:
                    # Record the current mark-to-market and the cumulative pnl.
                    self.update_cumulative_pnl(row)
            else:
                if row[f'lower_band_{entry}_1'] == 1:
                    self.in_trade = True
                    self.position = 'Long'
                elif row[f'upper_band_{entry}_1'] == 1:
                    self.in_trade = True
                    self.position = 'Short'
                if self.in_trade:
                    # Entry Signal obtained.
                    self.process_entry_signal(row, capital_per_trade)
        
        # if self.in_trade:
        #     # If open position exists at the end of the backtest, exit that position.
        #     self.process_exit_signal(row, capital_per_trade)

        # Reset all indicator variables.
        self.reset_indicators()
        return self.trades_list
    
    def update_cumulative_pnl(self, row):
        long_price, short_price = (row['close_1'], row['close_2']) if self.position == 'Long' else (row['close_2'], row['close_1'])
        gain_till_date = (long_price - self.trade_dict['Long_Entry_Price']) * self.trade_dict['Long_Quantity'] + \
            (self.trade_dict['Short_Entry_Price'] - short_price) * self.trade_dict['Short_Quantity']
        self.trade_dict['MtM_dict'][row['date'].strftime('%Y-%m-%d')] = gain_till_date - self.cumulative_pnl
        self.cumulative_pnl = gain_till_date
        return

    def process_entry_signal(self, row, capital_per_trade):
        trade_dict = {}
        trade_dict['Position'] = self.position
        trade_dict['Entry_Date'] = row['next_date']
        trade_dict['Long_Stock'], trade_dict['Short_Stock'] = (row['underlying_1'], row['underlying_2']) if self.position == 'Long' else (row['underlying_2'], row['underlying_1']) 
        trade_dict['Long_Entry_Price'], trade_dict['Short_Entry_Price'] = (row['next_open_1'], row['next_open_2']) if self.position == 'Long' else (row['next_open_2'], row['next_open_1'])
        if not (trade_dict['Long_Entry_Price'] > 0 and trade_dict['Short_Entry_Price'] > 0):
            # Issue in the data.
            self.reset_indicators()
            return
        # Split the capital based on the hedge_ratio.
        capital_split_factor = abs(row['hedge_ratio']) + 1
        capital_stock_1 = capital_per_trade / capital_split_factor
        capital_stock_2 = capital_per_trade - capital_stock_1
        trade_dict['Long_Quantity'] = floor((capital_stock_1 if self.position == 'Long' else capital_stock_2) / trade_dict['Long_Entry_Price'])
        trade_dict['Short_Quantity'] = floor((capital_stock_1 if self.position == 'Short' else capital_stock_2) / trade_dict['Short_Entry_Price'])
        trade_dict['MtM_dict'] = {}
        self.trade_dict = trade_dict
        return

    def process_exit_signal(self, row, capital_per_trade):
        self.trade_dict['Exit_Date'] = row['next_date']
        self.trade_dict['Long_Exit_Price'], self.trade_dict['Short_Exit_Price'] = (row['next_open_1'], row['next_open_2']) if self.position == 'Long' else (row['next_open_2'], row['next_open_1'])
        self.trade_dict['Long_Points'] = self.trade_dict['Long_Exit_Price'] - self.trade_dict['Long_Entry_Price']
        self.trade_dict['Long_PnL'] = self.trade_dict['Long_Points'] * self.trade_dict['Long_Quantity']
        self.trade_dict['Short_Points'] = self.trade_dict['Short_Entry_Price'] - self.trade_dict['Short_Exit_Price']
        self.trade_dict['Short_PnL'] = self.trade_dict['Short_Points'] * self.trade_dict['Short_Quantity']
        self.trade_dict['Net_Points'] = self.trade_dict['Long_Points'] + self.trade_dict['Short_Points']
        transaction_costs = get_transaction_costs(self.trade_dict['Long_Entry_Price'], self.trade_dict['Long_Exit_Price'], self.trade_dict['Long_Quantity']) + \
            get_transaction_costs(self.trade_dict['Short_Exit_Price'], self.trade_dict['Short_Entry_Price'], self.trade_dict['Short_Quantity'])
        self.trade_dict['Trade_PnL'] = self.trade_dict['Long_PnL'] + self.trade_dict['Short_PnL'] - transaction_costs
        self.trade_dict['Trade_Return'] = 100 * self.trade_dict['Trade_PnL'] / capital_per_trade
        self.trade_dict['Trade_Duration'] = (self.trade_dict['Exit_Date'] - self.trade_dict['Entry_Date']).total_seconds()/3600/24
        self.trade_dict['Sector'] = self.sector_name
        self.trade_dict['Hedge_Ratio'] = row['hedge_ratio']
        self.trade_dict['Stock_Pair'] = f"{row['underlying_1']}|{row['underlying_2']}"
        
        # Update PnL.
        self.update_cumulative_pnl(row)

        self.trades_list.append(self.trade_dict)
        
        # Reset the indicator variables.
        self.reset_indicators()
        return
    
    def reset_indicators(self):
        self.in_trade = False
        self.position = ""
        self.cumulative_pnl = 0
        self.trade_dict = {}
        return

    def trade_sector(self):
        """
        Creates all possible pairs in a given sector and runs the strategy on each pair.
        """
        print(self.sector_name)
        stock_combinations = list(combinations(self.stock_list, 2))
        complete_trades_list = []
        train_period = self.config['strategy_parameters']['train_period']
        test_period = self.config['strategy_parameters']['test_period']
        mean_period = self.config['strategy_parameters']['mean_period']
        std_factor_1 = self.config['strategy_parameters']['std_factor_1']
        std_factor_2 = self.config['strategy_parameters']['std_factor_2']
        capital_per_trade = self.config['capital_parameters']['capital_per_trade']
        for stock_pair in stock_combinations:
            stock_1, stock_2 = stock_pair
            stock_pair_trades = self.trade_pairs(stock_1, stock_2, train_period, test_period, mean_period, std_factor_1, std_factor_2, capital_per_trade)
            complete_trades_list.extend(stock_pair_trades)
            print(stock_pair)
        self.destroy_connections()
        return complete_trades_list

    def destroy_connections(self):
        """
        Disconnecting from relevant modules.
        """
        self.mongo_interactor.destroy_connections()
        return

def run_strategy_for_sector(config, sector, stock_list):
    """
    Runs the strategy for a given sector.
    """
    strat = Strategy(config, sector, stock_list)
    trades = strat.trade_sector()
    return trades

if __name__ == '__main__':
    # Config
    with open('config.json') as jfile:
        config = json.load(jfile)
    
    # Sets the strategy collection to the name of the current simulation.
    config['database_parameters']['mongo']['strategy_collection'] = config['simulation_name']
    config['results_path'] += config['simulation_name']
    try:
        os.makedirs(config['results_path'])
    except:
        print('Folder already exists.')

    # Sectors. 
    with open('sectors.json') as jfile:
        sectors_dict = json.load(jfile)

    if not config['sectors'] == []:
        if config['sectors'] == ['All']:
            sectors_dict = {key: val for key, val in sectors_dict.items()}
        else:   
            sectors_dict = {key: val for key, val in sectors_dict.items() if key in config['sectors']}
    else:
        raise Exception("Sectors dictionary is empty.")

    # print(sectors_dict)
    pool = Pool(6)
    sectors_res = {sector: pool.apply_async(run_strategy_for_sector, args=(config, sector, stock_list)) for sector, stock_list in sectors_dict.items()}
    sector_trades = {key: res.get() for key, res in sectors_res.items()}

    results_config = {
        'start_date': config['date_parameters']['start_date'],
        'end_date': config['date_parameters']['end_date'],
        'capital': config['capital_parameters']['total_capital']
    }
    results_calculator = ResultsCalculator(config=results_config)

    # Calculate sector wise results.
    for sector, trades in sector_trades.items():
        results_calculator.calculate_results(trades, config['results_path'], sector)

    # Generate the results for all sectors combined.
    combined_sector_trades = sum(list(sector_trades.values()), [])
    results_calculator.calculate_results(combined_sector_trades, config['results_path'], "Combined")

