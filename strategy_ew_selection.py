from __future__ import absolute_import, division, print_function, unicode_literals

import backtrader as bt
import os
import datetime
import sys
import pyodbc
import pandas as pd

from backtrader import num2date


# Create Strategy
class TestDataBricks(bt.Strategy):
    def __init__(self):
        self.dataclose = self.datas[0].close
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
        self.in_position = list()

    def log(self, txt, dt=None):
        """Logging function fot this strategy"""
        dt = dt or self.datas[0].datetime.date(0)
        print("%s, %s" % (dt.isoformat(), txt))

    def next(self):
        close_df = {}

        for data in self.datas:
            close_df[data._name] = data.close.get(0, 30)  # 15 data points

        _df = pd.DataFrame(close_df)
        if _df.empty is False:
            week = _df.pct_change(5).T[29].sort_values().index[0:5].to_list()
            month = _df.pct_change(24).T[29].sort_values().index[0:5].to_list()

            target_stock = list(set(week) & set(month))

            for ticker in self.buy_value.keys():
                if ticker in target_stock:
                    self.buy_value[ticker] = (
                        1 / len(target_stock) * self.broker.get_value() * 0.95
                    )
                else:
                    self.buy_value[ticker] = 0

            if self.t % 2 == 0:
                order_list = dict(
                    sorted(self.buy_value.items(), key=lambda item: item[1])
                )

                print("==================================================xxxx")
                print(
                    num2date(self.datas[0].datetime[0]),
                    "======",
                    1 / len(target_stock) * self.broker.get_value(),
                    "=======",
                    self.broker.get_value(),
                    "==========",
                    target_stock,
                )

                print(order_list)
                print(self.in_position)
                print("==================================================yyyy")

                _pending = []
                for stock in order_list.keys():
                    if stock in self.in_position:
                        self.order_target_value(data=stock, target=order_list[stock])
                    else:
                        if order_list[stock] != 0:
                            _pending.append(stock)

                    if stock == list(order_list.keys())[-1]:
                        for pending_stock in _pending:
                            self.order_target_value(
                                data=pending_stock, target=order_list[pending_stock]
                            )

                self.in_position = {x: y for x, y in order_list.items() if y != 0}

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

    # Stickers
    tickers = ("FPT", "HPG", "MBB", "MSN", "MWG", "STB", "TCB", "VHM", "VPB", "VRE")
    fromdate = datetime.date(2023, 11, 21)
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

    print("Final Portfolio Value: %.2f" % cerebro.broker.getvalue())
