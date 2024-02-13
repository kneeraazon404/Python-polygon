from scanners.services.scanner import Scanner
import pandas as pd

from scanners.models.ticker_sc import Ticker
from scanners.services.timeframes_sc import TimeFrames
from scanners.services.utils_sc import get_plotpath

from datetime import datetime as pydatetime, date as pydate, timedelta as pytimedelta

import asyncio
import logging
import requests
import time as pytime
import os

from scanners.services.ticker_w_details_sc import (
    TickerSelector,
)


def find_first_monday(row):
    d = pydatetime(row.year, row.month, 7)
    offset = -d.weekday() #weekday = 0 means monday

    return (d + pytimedelta(offset)).strftime("%Y-%m-%d")

def find_third_monday(year, month):
    d = pydatetime(year, int(month), 21)
    offset = -d.weekday() #weekday = 0 means monday
    return d + pytimedelta(offset)

def get_first_and_third_mondays():

    return

from scanners.services.utils_sc import datechunks
logger = logging.getLogger(__name__)  # TODO: Add colorama on the logging comments
class LargeCapBacksideScanner(Scanner):
    # COMMENT IN THE TIMEFRAMES YOU WANT FOR YOUR STRATEGY & ALSO TO PRINT ...
    # BY DEFAULT -> Even if commented out, script will still pull the 1 min data & daily data ...
    timeframe = TimeFrames.set_tfs([
        # '1 min',
        # '2 min',
        # '3 min',
        # '5 min',
        # '10 min',
        # '15 min',
        # '30 min',
        # '1 hour',
        # '4 hour',
        '1 day'
    ])  # To change timeframe variables for all instances

    _filters = [  # you can ignore this section for now, does nothing ... but I left it in here for now ...
    ]

    _gapperslist = []

    category = "largecap"
    strategy_name = "LargeCapBacksideScanner"  # this is just a folder that gets created in /backtests/findings to help organize printouts
    plot_path = get_plotpath(category=category, scanner="LargeCapBacksideScanner")  # This get's the path to plot all the charts/excel sheet print out in

    first_monday_df = None

    async def run(self):

        # TODO: Check if file is open ...
        # Get the working time list ...

        start_time = pytime.time()
        start = (pd.to_datetime(self.startdate)-pd.to_timedelta(365*.75,"D")).strftime("%Y-%m-%d")

        drange = pd.date_range(start, self.enddate)
        first_monday_df = pd.DataFrame(index=drange)
        first_monday_df["Date"] = first_monday_df.index
        first_monday_df["Date"] = first_monday_df["Date"].apply(find_first_monday)
        first_monday_df = first_monday_df.drop_duplicates()
        self.first_monday_df = first_monday_df

        tickers = await TickerSelector(self._filters, start, self.enddate).get_tickers()  # TODO: Find out what this is ...
        if len(tickers) == 0:
            logger.info(f"No tickers to run {self.__class__.__name__} on.")
            return

        with requests.Session() as session:
            await asyncio.gather(
                *[asyncio.create_task(self.run_per_ticker(session, ticker)) for ticker in tickers]
            )

        # Exporting the file ...
        if not self._gapperspd.empty:
            try:
                df = self._gapperspd.copy()
                df = df.sort_values(["Ticker", "Date"])
                df.index = range(0, len(df))

                result = df.groupby("Date", as_index=False).first()

                result = pd.DataFrame(columns=self._gapperspd.columns.to_list())

                df["Rank 3 Mo Push %"] = df.groupby("Date")["3mo push %"].rank("dense", ascending=False)
                df["Rank 6 Mo Push %"] = df.groupby("Date")["6mo push %"].rank("dense", ascending=False)
                df["Rank 9 Mo Push %"] = df.groupby("Date")["9mo push %"].rank("dense", ascending=False)

                result3mo = df.loc[df["Rank 3 Mo Push %"] <=10].sort_values(["Date", "Rank 3 Mo Push %"])
                result3mo = result3mo.loc[:, ["Date", "Ticker", "Volume", "Rank 3 Mo Push %"]]

                result6mo = df.loc[df["Rank 6 Mo Push %"] <=10].sort_values(["Date", "Rank 6 Mo Push %"])
                result6mo = result6mo.loc[:, ["Date", "Ticker", "Volume", "Rank 6 Mo Push %"]]

                result9mo = df.loc[df["Rank 9 Mo Push %"] <=10].sort_values(["Date", "Rank 9 Mo Push %"])
                result9mo = result9mo.loc[:, ["Date", "Ticker", "Volume", "Rank 9 Mo Push %"]]

                # # result = pd.concat([result, df.loc[df["Rank 6 Mo Push %"] <=10].sort_values(["Date", "Rank 6 Mo Push %"])])
                # # result = pd.concat([result, df.loc[df["Rank 9 Mo Push %"] <=10].sort_values(["Date", "Rank 9 Mo Push %"])])
                #
                # result = result.sort_values("Date")
                # result = result.loc[:,["Date", "Ticker", "Volume", "Rank 3 Mo Push %", "Rank 6 Mo Push %", "Rank 9 Mo Push %"]]


                if not os.path.isdir(f"{self.plot_path}"):  # is path directory even created?
                    os.makedirs(f"{self.plot_path}", mode=777)
                result3mo.to_csv(f"{self.plot_path}{self.strategy_name}_result3mo_{self.startdate}_to_{self.enddate}.csv", index=False)
                result6mo.to_csv(f"{self.plot_path}{self.strategy_name}_result6mo_{self.startdate}_to_{self.enddate}.csv", index=False)
                result9mo.to_csv(f"{self.plot_path}{self.strategy_name}_result9mo_{self.startdate}_to_{self.enddate}.csv", index=False)
                logger.info(
                    f"Exported {len(result3mo)+len(result6mo)+len(result9mo)} Results to CSV in {int(pytime.time() - start_time)} Seconds"
                )
            except AssertionError:
                print()
        else:
            logger.info(
                f"No Results in {int(pytime.time() - start_time)} Seconds"
            )
        return

    async def run_per_ticker(self, session, ticker: Ticker):

        # GAP ... pm gap > 80%
        # PM VOL ... 100K
        try:
            df = ticker.dfs_dict['1 day']  # check if it actually pulled any data for that ticker ...
        except KeyError:
            return

        # some of the datapull contain extra columns "a" and "op", this just checks if either exists and removes then bc they're not needed
        try:
            if "a" in df:
                df = df.drop("a", axis=1)
            if "op" in df:
                df = df.drop("op", axis=1)
        except:
            print()

        # if ticker.symbol == "ABVC":  # DEBUG — I just leave these here to help debug
        #     print()  # DEBUG
        if ticker.market_cap == None:
            return

        if ticker.market_cap < 2_000_000_000:
            return

        if ticker.symbol == "AR":  # DEBUG
            print()  # DEBUG
        df = df.drop_duplicates()
        df['Datetime'] = df.index  # copy the datetime index into a column
        day_df = df.copy()

        day_df = day_df.loc[day_df["High"] > 1]
        day_df.insert(loc=1, column="9mo behind", value=day_df["Low"].shift(189))
        day_df.insert(loc=1, column="6mo behind", value=day_df["Low"].shift(126))
        day_df.insert(loc=1, column="3mo behind", value=day_df["Low"].shift(63))

        day_df = day_df.dropna(subset=["9mo behind", "6mo behind", "3mo behind"])

        day_df.insert(loc=1, column="9mo push %", value= (day_df["High"]-day_df["9mo behind"])/day_df["9mo behind"])
        day_df.insert(loc=1, column="6mo push %", value= (day_df["High"]-day_df["6mo behind"])/day_df["6mo behind"])
        day_df.insert(loc=1, column="3mo push %", value= (day_df["High"]-day_df["3mo behind"])/day_df["3mo behind"])

        day_df.insert(loc=0, column="Date", value=df['Datetime'].dt.strftime("%Y-%m-%d"))
        day_df.index.name = None

        joined = day_df.merge(self.first_monday_df, how="right", left_on="Date", right_on="Date")
        joined = joined.dropna()

        if not joined.empty:
            if not self._columns:
                self._columns = joined.columns.to_list()

            joined.insert(loc=1, column="Ticker", value=ticker.symbol)
            joined["Date"] = joined["Date"].astype(str)
            self._gapperspd = pd.concat([self._gapperspd, joined])

        # scan_months = day_df['Datetime'].dt.strftime("%Y-%m")
        # scan_months = scan_months.drop_duplicates().to_list()
        #
        # for i in self.first_monday_df.to_list():
        #     try:
        #         r = day_df.loc[[i]]
        #         # r = day_df.loc[['2022-01-03']]
        #         self._gapperslist.extend(r.values.tolist())
        #         if not self._columns:
        #             self._columns = r.columns.to_list()
        #     except:
        #         pass
        #
        # return


        # day_df.loc[first_mondays]
        #
        # push_df = df.between_time('11:00', '16:30')  #
        # push_df.insert(loc=1,
        #              column="VolumeSum",
        #              value=push_df.loc[:, 'Volume'].groupby(pd.Grouper(freq='1D')).transform(lambda x: x.rolling('1D').sum())
        #              )
        #
        # push_df.insert(loc=0, column="Date", value=push_df['Datetime'].dt.date)
        # push_df.index.rename("Index", inplace=True)
        #
        # if push_df.empty:
        #     return
        #
        # push_df["11 AM Price"] = push_df["Low"].iloc[0]
        # push_df["Aftn Push %"] = round((push_df["High"] - push_df["11 AM Price"]) / push_df["11 AM Price"], 2)
        #
        # # IF YOU CHANGE THE RESULT TO LOWER, the results will pick up the lower number even tho it may also be 0.8
        # # intraday, push, rate of change
        #
        # day_df = df.between_time('09:30', '16:30')
        # highs = day_df.groupby([day_df['Datetime'].dt.date])['High'].max()
        # macro_df = highs.to_frame()
        # sums = day_df.groupby([day_df['Datetime'].dt.date])['Volume'].sum()
        # macro_df = macro_df.join(sums)
        #
        # day_df_filtered = pd.DataFrame(columns=macro_df.columns.tolist())
        # conditions = [
        #     macro_df.loc[(macro_df['High'] > 0.5) & (macro_df['Volume'] > 100_000)],
        #     ]
        #
        # for condition in conditions:
        #     if not condition.empty:
        #         day_df_filtered = pd.concat([day_df_filtered, condition])
        #
        # if day_df_filtered.empty:
        #     return
        #
        # push_df = pd.merge(push_df, day_df_filtered, left_on="Date", right_index=True)
        #
        # results = {
        #     "38% Gap": push_df[(push_df["Aftn Push %"] > 0.2)],
        # }
        #
        # for resultkey in results:
        #     result = results[resultkey]
        #     if not result.empty:
        #         result = result.groupby("Date", as_index=False).first()
        #         result = result.loc[:, ["Date", "Datetime", "VolumeSum", "11 AM Price", "Aftn Push %"]]
        #         result.insert(loc=1, column="Ticker", value=ticker.symbol)
        #         result = result.rename(columns={"Datetime": "Scan Time"})
        #         result["Scan Time"] = result["Scan Time"].dt.strftime("%H:%M:%S %p")
        #         result["Date"] = result["Date"].astype(str)
        #         result.insert(loc=3, column="Scan Type", value=resultkey)
        #         self._gapperslist.extend(result.values.tolist())
        #         if not self._columns:
        #             self._columns = result.columns.to_list()
        # return