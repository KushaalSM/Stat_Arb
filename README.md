# Stat_Arb

## Disclaimer : 
1) '*' before a functionality of a module implies that functionality is in a state of development and isn't frozen. I will be trying out different ideas and implementations for these starred functionalities.
2) '**' before a functionality of a module implies that functionality isn't completed yet. Some aspects of the functionality described are yet to be implemented.

## Description
This is a backtest engine for a Statistical Arbitrage strategy. In particular, I am developing this for the constituents of S&P 500.
The different modules created are - 
1) data_processor : This module involves - 
    1. Obtaining data from local database or yahoo finance.
    2. Matching the dates for the 2 stocks.
    3. Aggregating the data for 2 stock pairs.

2) strategy : This module involves - 
    1. Calculating the hedge ratio for each train-test period.
    2. Performing Augmented Dickey Fuller test to check for stationarity of the price spread.
    3. Calculate the buy-sell signals based on a rolling mean and rolling standard deviations.
    4. Trading the signals and logging the execution information.

3) results : This module involves - 
    1. Calculating the trade related metrics such as hit_rate, avg_win_to_avg_loss and so on.
    2. Calculating Capital related metrics such as return, volatility, sharpe and so on. (**)

4) utils : This is a combination of miscellaneous tools used by the other modules. The current toolbox contains - 
    1. MongoInteractor : Connecting to the local mongo database, fetching data, saving trades and so on.
    2. YahooDataFetcher : Fetching data from Yahoo Finance. (*)
    3. Transaction Costs : Calculating transaction costs for different asset classes. (*)
    4. Train_Test_Split : Creating train-test splits of the input data based on the given split parameters.


# Results
## Iteration 1: 
    Sector : Information Technology
    
    Parameters : {
        "train_period": 150,
        "test_period": 100,
        "mean_period": 45,
        "std_factor_1": 2,
        "std_factor_2": 3,
        "capital_per_trade": 10000
    }

