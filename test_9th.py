import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import backtrader as bt
import pyfolio as pf
import datetime as datetime
import sklearn

from scipy.optimize import minimize
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt import black_litterman, risk_models
from pypfopt.black_litterman import BlackLittermanModel


# Initial cash & commission
startcash = 2000000
commission = 0.003
risk_free_rate = 0

# Period for stats calculation (Covariance, expected returns, etc.)
count_period = 200

# Backtest's period
from_date = datetime.datetime(2014, 1, 1)
to_date = datetime.datetime(2019, 12, 31)

# Universe of asset
Universe = ['BID', 'BVH', 'CTD', 'CTG', 'EIB', 'FPT', 'GAS', 'HPG', 'MBB',
            'MSN', 'MWG', 'PNJ', 'REE', 'SBT', 'SSI', 'STB', 'VCB', 'VIC', 'VNM']

# Load data
data = pd.read_csv('./datas/pivotedData.csv')
data['Date'] = pd.to_datetime(data['Date'])
data = data[['Date']].join(data[Universe])  # Missing data 2016-01-03
data = data.dropna()

cerebro = bt.Cerebro()
for asset in Universe:
    data_feed = pd.DataFrame()
    data_feed['close'] = data[asset]
    data_feed['open'] = data_feed['close']
    data_feed['high'] = data_feed['close']
    data_feed['low'] = data_feed['close']
    data_feed = data_feed.set_index(data['Date'])
    data_feed.dropna()

    backtrader_data_add = bt.feeds.PandasData(
        dataname=data_feed, fromdate=from_date, todate=to_date)
    cerebro.adddata(backtrader_data_add, name=asset)

cerebro.broker.setcash(startcash)
cerebro.broker.setcommission(commission)
# cerebro.addanalyzer(bt.analyzers.PyFolio)


def equally_weighted(number_of_assets):
    weight = 1./number_of_assets
    return weight/2


class equally_weighted_strategies(bt.Strategy):
    def __init__(self):
        self.count_period = count_period
        self.counter = 0
        self.day_counter = 0

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def next(self):
        to_buy = Universe
        to_sell = []
        if self.counter < self.count_period:
            self.counter += 1
        else:
            if self.day_counter % 5 == 0:
                for asset in to_buy:
                    self.order_target_percent(
                        asset, target=equally_weighted(number_of_assets=len(Universe)))
            self.day_counter += 1

    def notify_order(self, order):
        if order.status == order.Margin:
            print('Not enough cash')
        if order.status == order.Rejected:
            print('Order is rejected')
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'Stock: %s, BUY EXECUTED, Price: %.2f, size: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.data._name,
                     order.executed.price,
                     order.executed.size,
                     order.executed.value,
                     order.executed.comm))

            else:  # Sell
                self.log(
                    'Stock: %s, SELL EXECUTED, Price: %.2f, size: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.data._name,
                     order.executed.price,
                     order.executed.size,
                     order.executed.value,
                     order.executed.comm))


cerebro.addstrategy(equally_weighted_strategies)
results_ew = cerebro.run()
# Print out the final result
print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
