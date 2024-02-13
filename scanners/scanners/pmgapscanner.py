from scanners.services.scanner import Scanner
import pandas as pd

from scanners.models.ticker_sc import Ticker
from scanners.services.timeframes_sc import TimeFrames
from scanners.services.utils_sc import get_plotpath

class PMGapScanner(Scanner):
    # COMMENT IN THE TIMEFRAMES YOU WANT FOR YOUR STRATEGY & ALSO TO PRINT ...
    # BY DEFAULT -> Even if commented out, script will still pull the 1 min data & daily data ...
    timeframe = TimeFrames.set_tfs([
        '1 min',
        # '2 min',
        # '3 min',
        # '5 min',
        # '10 min',
        # '15 min',
        # '30 min',
        # '1 hour',
        # '4 hour',
        # '1 day'
    ])  # To change timeframe variables for all instances

    _filters = [  # you can ignore this section for now, does nothing ... but I left it in here for now ...
    ]

    _gapperslist = []

    category = "pm_gaps"
    strategy_name = "PMGapScanner"  # this is just a folder that gets created in /backtests/findings to help organize printouts
    plot_path = get_plotpath(category=category, scanner="PMGapScanner")  # This get's the path to plot all the charts/excel sheet print out in

    async def run_per_ticker(self, session, ticker: Ticker):

        # GAP ... pm gap > 80%
        # PM VOL ... 100K
        try:
            df = ticker.dfs_dict['1 min']  # check if it actually pulled any data for that ticker ...
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
        # if ticker.symbol == "AIM":  # DEBUG
        #     print()  # DEBUG
        df = df.drop_duplicates()
        df['Datetime'] = df.index  # copy the datetime index into a column
        pm_df = df.between_time('04:00', '09:29')  #
        try:
            pm_df.insert(loc=1,
                     column="VolumeSum",
                     value=pm_df.loc[:, 'Volume'].groupby(pd.Grouper(freq='1D')).transform(lambda x: x.rolling('1D').sum())
                     )
        except:
            print()
        pm_df.insert(loc=0, column="Date", value=pm_df['Datetime'].dt.date)
        pm_df.index.rename("Index", inplace=True)

        day_df = df.between_time('09:30', '16:00')
        closes = day_df.groupby([day_df['Datetime'].dt.date])['Close'].last()
        prev_closes = closes.shift(1).rename("PrevClose")
        prev_closes = prev_closes.to_frame()

        joined_df = pd.merge(pm_df, prev_closes, how="left", left_on="Date", right_index=True)
        joined_df = joined_df.dropna()
        joined_df["Gap %"] = round((joined_df["High"] - joined_df["PrevClose"]) / joined_df["PrevClose"], 2)

        # IF YOU CHANGE THE RESULT TO LOWER, the results will pick up the lower number even tho it may also be 0.8
        # intraday, push, rate of change

        results = {
            "78% Gap": joined_df[(joined_df["Gap %"] > 0.10) & (joined_df["VolumeSum"] > 100_000)],
            # "58% Gap": joined_df[(joined_df["Gap %"] > 0.58) & (joined_df["VolumeSum"] > 100_000)],
            # "38% Gap": joined_df[(joined_df["Gap %"] > 0.38) & (joined_df["VolumeSum"] > 100_000)],
        }
        for resultkey in results:
            result = results[resultkey]
            if not result.empty:
                result = result.groupby("Date", as_index=False).first()
                result = result.loc[:, ["Date", "Datetime", "VolumeSum", "PrevClose", "Gap %"]]
                result.insert(loc=1, column="Ticker", value=ticker.symbol)
                result = result.rename(columns={"Datetime": "Scan Time"})
                result["Scan Time"] = result["Scan Time"].dt.strftime("%H:%M:%S %p")
                result["Date"] = result["Date"].astype(str)
                result.insert(loc=3, column="Scan Type", value=resultkey)
                self._gapperslist.extend(result.values.tolist())
                if not self._columns:
                    self._columns = result.columns.to_list()

        return