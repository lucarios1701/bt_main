from __future__ import absolute_import, division, print_function, unicode_literals

import backtrader as bt
import os
import datetime
import sys
import pyodbc
import pandas as pd
import pyfolio as pf

from backtrader import num2date


# Create Strategy
class TestDataBricks(bt.Strategy):
    def __init__(self):
        self.t = 0
        self.ticker_list = [
            "ACB",
            "BCM",
            "BID",
            "BVH",
            "CTG",
            "FPT",
            "GAS",
            "GVR",
            "HDB",
            "HPG",
            "MBB",
            "MSN",
            "MWG",
            "PLX",
            "POW",
            "SAB",
            "SHB",
            "SSB",
            "SSI",
            "STB",
            "TCB",
            "TPB",
            "VCB",
            "VHM",
            "VIB",
            "VIC",
            "VJC",
            "VNM",
            "VPB",
            "VRE",
        ]
        self.holding = None
        self.to_buy = None

    def log(self, txt, dt=None):
        """Logging function fot this strategy"""
        dt = dt or self.datas[0].datetime.date(0)
        print("%s, %s" % (dt.isoformat(), txt))

    def next(self):
        close_df = {}

        for data in self.datas:
            if data._name == "SHB":
                print(data.close.get(0, 1))
            close_df[data._name] = data.close.get(0, 5)  # 5 data points

        print(close_df)
        _df = pd.DataFrame(close_df)

        if _df.empty is False:
            if self.t % 5 == 0:
                self.to_buy = (
                    _df.pct_change(4).T.sort_values(4, ascending=False).iloc[1].name
                )
                if self.holding != self.to_buy:
                    if self.holding:
                        self.order_target_value(self.holding, target=0)

                    self.order_target_value(
                        self.to_buy, target=self.broker.get_value() * 0.95
                    )
                    self.holding = self.to_buy

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
    tickers = (
        "ACB",
        "BCM",
        "BID",
        "BVH",
        "CTG",
        "FPT",
        "GAS",
        "GVR",
        "HDB",
        "HPG",
        "MBB",
        "MSN",
        "MWG",
        "PLX",
        "POW",
        "SAB",
        "SHB",
        "SSB",
        "SSI",
        "STB",
        "TCB",
        "TPB",
        "VCB",
        "VHM",
        "VIB",
        "VIC",
        "VJC",
        "VNM",
        "VPB",
        "VRE",
    )
    fromdate = datetime.date(2021, 1, 1)
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
