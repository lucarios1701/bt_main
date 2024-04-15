from __future__ import absolute_import, division, print_function, unicode_literals

import backtrader as bt
import os
import datetime
import sys
import pyodbc
import pandas as pd
import pyfolio as pf

from backtrader import num2date
from pypfopt.efficient_frontier import EfficientFrontier


# Create Strategy
class TestDataBricks(bt.Strategy):
    def __init__(self):
        self.t = 0
        self.ticker_list = [
            "FPT",
            "HPG",
            "MBB",
            "MSN",
            "MWG",
            "STB",
            "TCB",
            "VHM",
            "VPB",
            "VRE",
        ]

        self.buy_value = dict.fromkeys(self.ticker_list, 0)
        self.holding = []

        # Add indicator with each data
        self.sma_dict = dict()
        for data in self.datas:
            self.sma_dict["sma_20_" + data._name] = bt.indicators.SMA(data, period=20)
            self.sma_dict["sma_50_" + data._name] = bt.indicators.SMA(data, period=50)
            self.sma_dict["sma_200_" + data._name] = bt.indicators.SMA(data, period=200)

    def log(self, txt, dt=None):
        """Logging function fot this strategy"""
        dt = dt or self.datas[0].datetime.date(0)
        print("%s, %s" % (dt.isoformat(), txt))

    def MPT_minvol(self, df):
        daily_returns = df.pct_change(1).dropna(how="all")
        mu = daily_returns.mean()
        Sigma = daily_returns.cov() * 252
        ef = EfficientFrontier(mu, Sigma)
        weights = ef.min_volatility()
        clean_weights = ef.clean_weights()
        return clean_weights

    def next(self):
        _to_buy = []
        for data in self.datas:
            if (
                (data.close[0] > self.sma_dict["sma_20_" + data._name][0])
                and (
                    self.sma_dict["sma_20_" + data._name][0]
                    > self.sma_dict["sma_50_" + data._name][0]
                )
                and (
                    self.sma_dict["sma_50_" + data._name][0]
                    > self.sma_dict["sma_200_" + data._name][0]
                )
            ):
                _to_buy.append(data._name)

        _to_sell_all = list(set(self.holding) - set(_to_buy))
        _on_hold = list(set(self.holding) - set(_to_sell_all))

        print(
            num2date(self.datas[0].datetime[0]),
            "====",
            _to_buy,
            "=====",
            _to_sell_all,
            "====",
            _on_hold,
            "====",
            self.broker.get_value(),
        )

        if self.t % 2 == 0:
            # No stock to buy
            if not _to_buy:
                print("No stock to buy! - Close all position")
                for ticker in self.ticker_list:
                    self.order_target_percent(data=ticker, target=0)
            else:
                for ticker in self.buy_value.keys():
                    if ticker in _to_buy:
                        self.buy_value[ticker] = (
                            1 / len(_to_buy) * self.broker.get_value() * 0.95
                        )
                    else:
                        self.buy_value[ticker] = 0

                    order_list = dict(
                        sorted(self.buy_value.items(), key=lambda item: item[1])
                    )

                _pending = []
                for stock in order_list.keys():
                    if stock in self.holding:
                        self.order_target_value(data=stock, target=order_list[stock])
                    else:
                        if order_list[stock] != 0:
                            _pending.append(stock)

                    # make sure pending add to the latest stock
                    if stock == list(order_list.keys())[-1]:
                        for pending_stock in _pending:
                            self.order_target_value(
                                data=pending_stock, target=order_list[pending_stock]
                            )

            self.holding = _to_buy
        self.t += 1

    def notify_order(self, order):
        if order.status == order.Margin:
            print("Not enough cash")
        if order.status == order.Rejected:
            print("Order is rejected")
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    "Stock: %s, BUY EXECUTED, Price: %.2f, size: %.2f, Cost: %.2f, Comm %.2f"
                    % (
                        order.data._name,
                        order.executed.price,
                        order.executed.size,
                        order.executed.value,
                        order.executed.comm,
                    )
                )

            else:  # Sell
                self.log(
                    "Stock: %s, SELL EXECUTED, Price: %.2f, size: %.2f, Cost: %.2f, Comm %.2f"
                    % (
                        order.data._name,
                        order.executed.price,
                        order.executed.size,
                        order.executed.value,
                        order.executed.comm,
                    )
                )


if __name__ == "__main__":
    # Cerebro
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(5 * 10e8)
    cerebro.broker.set_shortcash(False)

    cerebro.addanalyzer(bt.analyzers.PyFolio)

    # Stickers
    tickers = ("FPT", "HPG", "MBB", "MSN", "MWG", "STB", "TCB", "VHM", "VPB", "VRE")
    fromdate = datetime.date(2019, 1, 1)
    # fromdate = datetime.date(2023, 11, 21)
    todate = datetime.datetime.now().date()

    # Bricks
    conn = pyodbc.connect("DSN=PIT_AzureDataBricks_DSN", autocommit=True)
    sql = """
        select TradingDate, Ticker, OpenPrice, ClosePrice, HighestPrice, LowestPrice, TotalVolume
        from silver.prod_finapi.hose_stock
        where Ticker in {} 
        and TradingDate between ? and ?
        order by TradingDate, Ticker
    """.format(
        tickers
    )

    df = pd.read_sql(sql, conn, params=(fromdate, todate), index_col="TradingDate")

    for ticker in tickers:
        datafeed = df[df["Ticker"] == ticker]
        datafeed.drop("Ticker", axis=1, inplace=True)
        datafeed.index = pd.to_datetime(datafeed.index)

        # Create a Data Feed
        data = bt.feeds.PandasData(
            dataname=datafeed,
            # Do not pass values before this date
            fromdate=fromdate,
            # Do not pass values before this date
            todate=todate,
            open=0,
            close=1,
            high=2,
            low=3,
            volume=4,
            name=ticker,
        )

        cerebro.adddata(data)

    cerebro.addstrategy(TestDataBricks)
    results = cerebro.run()
    returns, positions, transactions, gross_lev = results[
        0
    ].analyzers.pyfolio.get_pf_items()

    returns.to_csv("./backtesting_results/returns.csv")
    positions.to_csv("./backtesting_results/positions.csv")
    transactions.to_csv("./backtesting_results/transactions.csv")

    print("Final Portfolio Value: %.2f" % cerebro.broker.getvalue())
