from scanners.services.scanner import Scanner
import pandas as pd

from scanners.models.ticker_sc import Ticker
from scanners.services.timeframes_sc import TimeFrames
from scanners.services.utils_sc import get_plotpath

class PushPercScanner(Scanner):
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

    category = "percpush"
    strategy_name = "PushPercScanner"  # this is just a folder that gets created in /backtests/findings to help organize printouts
    plot_path = get_plotpath(category=category, scanner="PushPercScanner")  # This get's the path to plot all the charts/excel sheet print out in

    async def run_per_ticker(self, session, ticker: Ticker):

        # if ticker.symbol == "TRVN":  # DEBUG
        #     print()  # DEBUG

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

        # if ticker.symbol == "ANY":  # DEBUG — I just leave these here to help debug
        #     print()  # DEBUG


        df = df.drop_duplicates()
        df['Datetime'] = df.index  # copy the datetime index into a column
        push_df = df.between_time('09:30', '16:00')  #
        push_df.insert(loc=1,
                     column="VolumeSum",
                     value=push_df.loc[:, 'Volume'].groupby(pd.Grouper(freq='1D')).transform(lambda x: x.rolling('1D').sum())
                     )

        push_df.insert(loc=0, column="Date", value=push_df['Datetime'].dt.date)
        push_df.index.rename("Index", inplace=True)

        day_df = df.between_time('09:30', '16:00')
        closes = day_df.groupby([day_df['Datetime'].dt.date])['Close'].last()
        prev_closes = closes.shift(1).rename("PrevClose")
        prev_closes = prev_closes.to_frame()

        day_close = day_df.groupby([day_df['Datetime'].dt.date])['Close'].last()
        day_close = day_close.to_frame()


        joined_df = pd.merge(push_df, prev_closes, how="left", left_on="Date", right_index=True)
        joined_df = joined_df.merge(day_close, how="left", left_on="Date", right_index=True)

        joined_df = joined_df.dropna()
        joined_df["Gap %"] = round((joined_df["High"] - joined_df["PrevClose"]) / joined_df["PrevClose"], 2)

        joined_df["PercHoldingUp"] = round((joined_df["Close_y"] - joined_df["High"]) / joined_df["High"], 2)

        macro_df = closes.to_frame()
        sums = day_df.groupby([day_df['Datetime'].dt.date])['Volume'].sum()
        macro_df = macro_df.join(sums)

        day_df_filtered = pd.DataFrame(columns=macro_df.columns.tolist())
        conditions = [
            macro_df.loc[(macro_df['Close'] > 0.5) & (macro_df['Volume'] > 100_000)],
            # macro_df.loc[(3 <= macro_df['Close']) & (macro_df['Close'] < 4) & (macro_df['Volume'] > 10_000_000)],
            # macro_df.loc[(4 <= macro_df['Close']) & (macro_df['Close'] < 5) & (macro_df['Volume'] > 8_000_000)],
            # macro_df.loc[(5 <= macro_df['Close']) & (macro_df['Close'] < 7) & (macro_df['Volume'] > 6_000_000)],
            # macro_df.loc[(7 <= macro_df['Close']) & (macro_df['Close'] < 8) & (macro_df['Volume'] > 4_000_000)],
            # macro_df.loc[(8 <= macro_df['Close']) & (macro_df['Close'] < 20) & (macro_df['Volume'] > 2_000_000)],
            # macro_df.loc[(macro_df['Close']) >= 20],
            ]

        for condition in conditions:
            if not condition.empty:
                day_df_filtered = pd.concat([day_df_filtered, condition])

        if day_df_filtered.empty:
            return

        joined_df = pd.merge(joined_df, day_df_filtered, left_on="Date", right_index=True)

        # IF YOU CHANGE THE RESULT TO LOWER, the results will pick up the lower number even tho it may also be 0.8
        # intraday, push, rate of change

        results = {
            # "78% Gap": joined_df[(joined_df["Gap %"] > 0.78) & (joined_df["VolumeSum"] > 100_000)],
            # "58% Gap": joined_df[(joined_df["Gap %"] > 0.58) & (joined_df["VolumeSum"] > 100_000)],
            "20% Gap": joined_df[(joined_df["Gap %"] > 0.14) & (joined_df["VolumeSum"] > 1_000_000) & (joined_df["Close_x"] > 0.5)],
            # "38% Gap": joined_df[(joined_df["Gap %"] > 0.38) & (joined_df["VolumeSum"] > 100_000)],
        }

        for resultkey in results:
            result = results[resultkey]
            if not result.empty:
                result = result.groupby("Date", as_index=False).first()
                result = result.loc[:, ["Date", "Datetime", "VolumeSum", "PrevClose", "Gap %", "PercHoldingUp", "High", "Volume_y", "Close_x"]]
                result.insert(loc=1, column="Ticker", value=ticker.symbol)
                result = result.rename(columns={"Datetime": "Scan Time"})
                result["Scan Time"] = result["Scan Time"].dt.strftime("%H:%M:%S %p")
                result["Date"] = result["Date"].astype(str)
                result.insert(loc=3, column="Scan Type", value=resultkey)
                self._gapperslist.extend(result.values.tolist())
                if not self._columns:
                    self._columns = result.columns.to_list()

        return