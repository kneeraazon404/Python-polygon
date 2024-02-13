from scanners.services.scanner import Scanner
import pandas as pd

from scanners.models.ticker_sc import Ticker
from scanners.services.timeframes_sc import TimeFrames
from scanners.services.utils_sc import get_plotpath

class ScalpPara(Scanner):
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

    category = "parab"
    strategy_name = "ParabScanner"  # this is just a folder that gets created in /backtests/findings to help organize printouts
    plot_path = get_plotpath(category=category, scanner="ParabScanner")  # This get's the path to plot all the charts/excel sheet print out in

    async def run_per_ticker(self, session, ticker: Ticker):

        # GAP ... pm gap > 80%
        # PM VOL ... 100K

        # TEST CASE
        if ticker.symbol == "ACON":
            print()

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

        df = df.drop_duplicates()
        df['Datetime'] = df.index  # copy the datetime index into a column
        para_df = df.between_time('09:30', '16:00')  #TODO: Change to 1400

        if para_df.empty:
            return

        para_df.insert(loc=1,
                     column="VolumeSum",
                     value=para_df.loc[:, 'Volume'].groupby(pd.Grouper(freq='1D')).transform(lambda x: x.rolling('1D').sum())
                     )

        para_df.insert(loc=0, column="Date", value=para_df['Datetime'].dt.date)
        para_df.index.rename("Index", inplace=True)

        day_df = df.between_time('09:30', '16:00')
        closes = day_df.groupby([day_df['Datetime'].dt.date])['Close'].last()
        prev_closes = closes.shift(1).rename("PrevClose")
        prev_closes = prev_closes.to_frame()

        joined_df = pd.merge(para_df, prev_closes, how="left", left_on="Date", right_index=True)
        joined_df = joined_df.dropna()
        joined_df["% from Prev Close"] = round((joined_df["High"] - joined_df["PrevClose"]) / joined_df["PrevClose"], 2)

        para_df = joined_df.copy()

        para_df["Shift 2 Min"] = para_df.groupby(pd.Grouper(freq='1D'))["Low"].shift(2)
        # para_df["Shift 10 Min"] = para_df.groupby(pd.Grouper(freq='1D'))["Low"].shift(10)
        # para_df["Shift 15 Min"] = para_df.groupby(pd.Grouper(freq='1D'))["Low"].shift(15)

        para_df["Change 2 Minute"] = round((para_df["High"] - para_df["Shift 2 Min"]) / para_df["Shift 2 Min"], 2)
        # para_df["Change 10 Minute"] = round((para_df["High"] - para_df["Shift 10 Min"]) / para_df["Shift 10 Min"], 2)
        # para_df["Change 15 Minute"] = round((para_df["High"] - para_df["Shift 15 Min"]) / para_df["Shift 15 Min"], 2)


        # common_filters = {
        #     "Price": 1.25,
        #     "Volume Today": 250_000,
        #     "Change from the Close": .2,
        #     "Count": 10,
        #     "Minutes after Open": 330,
        #
        # }
        #
        # params = {
        #     "Para (15min)": {
        #         "Change X Minute": .18,
        #         "Count": 2,
        #     },
        #     "Para (10min)": {
        #         "Change X Minute": .16,
        #         "Count": 2,
        #     },
        #     "Para (12min)": {
        #         "Change X Minute": .15,
        #         "Count": 2,
        #     }
        # }

        para_df = para_df[(para_df["% from Prev Close"] > 0.3) & (para_df["VolumeSum"] > 250_000)
                          & (para_df["Close"] >= 1) & (para_df["Close"] <= 35)]

        prelims = {
            # "Para (15min)": para_df[(para_df["Change 15 Minute"] > 0.12)],
            # "Para (10min)": para_df[(para_df["Change 10 Minute"] > 0.10)],
            "Para (2min)": para_df[(para_df["Change 2 Minute"] > 0.08)],
        }

        results = pd.DataFrame(columns=para_df.columns)

        for resultkey in prelims:
            prelim = prelims[resultkey]

            if not prelim.empty:
                if ((resultkey == "Para (15min)" and len(prelim) >= 2) or
                    (resultkey == "Para (10min)" and len(prelim) >= 2) or
                    (resultkey == "Para (2min)" and len(prelim) >= 2)
                ):
                    prelim = prelim.groupby("Date", as_index=False).nth(2-1)
                    prelim.insert(loc=1, column="Scan Type", value=resultkey)
                    prelim.insert(loc=3, column="Count (Alerts)", value=2)
                    results = pd.concat([results, prelim])

        if not results.empty:
            results = results.sort_values("Scan Type", ascending=False)
            results = results.sort_values("Datetime", ascending=True)
            results.insert(loc=1, column="Ticker", value=ticker.symbol)
            results = results.rename(columns={"Datetime": "Scan Time"})
            results["Scan Time"] = results["Scan Time"].dt.strftime("%H:%M:%S %p")
            results["Date"] = results["Date"].astype(str)
            results["VolumeSum"] = results["VolumeSum"].astype(int)
            results = results[["Date", "Ticker", "Scan Time", "Scan Type", "Close", "% from Prev Close", "VolumeSum", "Change 2 Minute", "Change 10 Minute", "Change 15 Minute", "Count (Alerts)"]]  # TODO add "Count" & % Change from Open

            result = results.iloc[[0]]
            self._gapperslist.extend(result.values.tolist())
            if not self._columns:
                self._columns = result.columns.to_list()

        return