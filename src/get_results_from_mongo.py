import pymongo
import pandas as pd
import datetime as dt
import os

from collections import defaultdict

from results import ResultsCalculator

if __name__ == '__main__':
    results_config = {
        "start_date": "2010-01-01",
        "end_date": "2021-11-12",
        'capital': 100000000
    }

    simulation_name = 'Iter_3_Inf_Tech'
    results_path = f'results/{simulation_name}'
    try:
        os.makedirs(results_path)
    except:
        pass

    results_calc = ResultsCalculator(results_config)

    client = pymongo.MongoClient()
    collection = client['Stat_Arb'][simulation_name]

    pair_trades_list = list(collection.find())

    trades_dict = defaultdict(list)

    for pair_doc in pair_trades_list:
        sector, stock_1, stock_2 = pair_doc['_id'].split('|')
        trades_dict[sector].extend(pair_doc['trades'])

    all_trades_list = []
    for key, val in trades_dict.items():
        # results_calc.calculate_results(val, results_path, key)
        all_trades_list.extend(val)

    results_calc.calculate_results(all_trades_list, results_path, 'Combined')
    




