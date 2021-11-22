# Stat_Arb

## Disclaimer : 
    a) '*' before a functionality of a module implies that functionality is in a state of development and isn't frozen. I will be trying out different ideas and implementations for these starred functionalities.
    b) '**' before a functionality of a module implies that functionality isn't completed yet. Some aspects of the functionality described are yet to be implemented.

## Description
This is a backtest engine for a Statistical Arbitrage strategy. In particular, I am developing this for the constituents of S&P 500.
The different modules created are - 
1) data_processor : 
This module involves - 
    i) Obtaining data from local database or yahoo finance.
    ii) Matching the dates for the 2 stocks.
    iii) Aggregating the data for 2 stock pairs.

2) strategy : 
This module involves - 
    i) Calculating the hedge ratio for each train-test period.
    ii) Performing Augmented Dickey Fuller test to check for stationarity of the price spread.
    iii) Calculate the buy-sell signals based on a rolling mean and rolling standard deviations.
    iv) Trading the signals and logging the execution information.

3) results :
This module involves - 
    i) Calculating the trade related metrics such as hit_rate, avg_win_to_avg_loss and so on.
    ii) Calculating Capital related metrics such as return, volatility, sharpe and so on.

4) utils : 
This is a combination of miscellaneous tools used by the other modules. The current toolbox contains - 
    i) MongoInteractor : Connecting to the local mongo database, fetching data, saving trades and so on.
    ii) YahooDataFetcher : Fetching data from Yahoo Finance.
    iii) Transaction Costs : Calculating transaction costs for different asset classes.
    iv) Train_Test_Split : Creating train-test splits of the input data based on the given split parameters.


## Results of iterations tried to follow soon!
