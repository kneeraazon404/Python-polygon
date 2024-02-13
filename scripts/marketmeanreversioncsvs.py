from abc import ABC, abstractmethod
from typing import Any, Dict, List, Mapping, Optional

import pandas as pd

# functions
def clean_df(df):
    df["Datetime"] = df.index
    df.insert(loc=0, column="Date", value=df['Datetime'].dt.date)
    return df


def get_pclose(df, checktime):
    day_df = df
    closes = day_df.groupby([day_df['Datetime'].dt.date])['Close'].last()
    prev_closes = closes.shift(1).rename("PrevClose")
    prev_closes = prev_closes.to_frame()

    joined_df = pd.merge(df, prev_closes, how="left", left_on="Date", right_index=True)
    joined_df = joined_df.dropna()
    # creating a column of the first

    joined_df = joined_df.between_time(checktime, checktime)
    joined_df["% from Prev Close"] = round((joined_df["Close"] - joined_df["PrevClose"]) / joined_df["PrevClose"], 4)

    joined_df.index = joined_df["Date"]
    return joined_df


def get_rankings(es, vix, euros):
    es.df[f"{es.name} Rank"] = es.df.apply(lambda row: 1 if row["% from Prev Close"] < 0.0 else 0, axis=1)
    es.df = es.df.rename(columns={"PrevClose": f"{es.name} PrevClose"})
    es.df = es.df.rename(columns={"% from Prev Close": f"{es.name} % from Prev Close"})

    vix.df[f"{vix.name } Rank"] = vix.df.apply(lambda row: 1 if row["% from Prev Close"] > 0.0 else 0, axis=1)  # positive
    vix.df = vix.df.rename(columns={"PrevClose": f"{vix.name} PrevClose"})
    vix.df = vix.df.rename(columns={"% from Prev Close": f"{vix.name} % from Prev Close"})

    for i in range(0, len(euros)):
        euros[i].df["Rank"] = euros[i].df.apply(lambda row: 1 if row["% from Prev Close"] < -0.0 else 0, axis=1)

    eurocols = []
    euro_rankings = euros[0].df[["Rank", "PrevClose", "% from Prev Close"]]
    euro_rankings = euro_rankings.rename(columns={"Rank": f"{euros[0].name} Rank",
                                                  "PrevClose": f"{euros[0].name} PrevClose",
                                                  "% from Prev Close": f"{euros[0].name} % from Prev Close"})
    eurocols.extend([f"{euros[0].name} Rank"])

    for i in range(1, len(euros)):
        next_eurorank = euros[i].df[["Rank", "PrevClose", "% from Prev Close"]]
        next_eurorank = next_eurorank.rename(columns={"Rank": f"{euros[i].name} Rank",
                                                      "PrevClose": f"{euros[i].name} PrevClose",
                                                      "% from Prev Close": f"{euros[i].name} % from Prev Close"})
        eurocols.extend([f"{euros[i].name} Rank"])
        euro_rankings = pd.merge(euro_rankings, next_eurorank, how="inner", left_index=True, right_index=True)

    euro_rankings["EURO Rank %"] = euro_rankings[eurocols].sum(axis=1)/len(euro_rankings[eurocols].columns)
    euro_rankings["EURO Rank"] = euro_rankings.apply(lambda row: 1 if row["EURO Rank %"] > 0.5 else 0, axis=1)

    results_df = pd.merge(es.df[[f"{es.name} Rank", f"{es.name} PrevClose", f"{es.name} % from Prev Close"]],
                          vix.df[[f"{vix.name} Rank", f"{vix.name} PrevClose", f"{vix.name} % from Prev Close"]],
                          how="inner", left_index=True, right_index=True)

    results_df = pd.merge(results_df, euro_rankings, how="inner", left_index=True, right_index=True)
    results_df["Market Rank"] = results_df[["EURO Rank", f"{es.name} Rank", f"{vix.name} Rank"]].sum(axis=1)

    return results_df


class Asset(ABC):
    name: str
    file: str
    header_names: List[str]
    _checktime: str

    df: None  # pandas dataframe

    def run(self):
        df = pd.read_csv("files/MarketMeanReversion/data/"+self.file,
                         names=self.header_names)
        df.index = pd.to_datetime(df.index)  # TODO: check timeconversion ...
        df = clean_df(df)
        print(f"{self.name} read successfully ...")
        self.df = get_pclose(df, self._checktime)  # adds columns for previous close and % close

    def __init__(self):
        self.name = self.__class__.__name__
        self.run()


class ES(Asset):
    file = "ES_continuous_adjusted_1min.txt"
    header_names = ["Open", "High", "Low", "Close", "Volume"]
    _checktime = "08:15"


class VIX(Asset):
    file = "VIX_1min.txt"
    header_names = ["Open", "High", "Low", "Close"]
    _checktime = "08:10"


class CAC40(Asset):
    file = "CAC40_1min.txt"
    header_names = ["Open", "High", "Low", "Close"]
    _checktime = "15:10"


class DAX(Asset):
    file = "ES_continuous_adjusted_1min.txt"
    header_names = ["Open", "High", "Low", "Close", "Volume"]
    _checktime = "09:10"


class ESTX50(Asset):
    file = "ESTX50_1min.txt"
    header_names = ["Open", "High", "Low", "Close"]
    _checktime = "15:10"


class N100(Asset):
    file = "N100_1min.txt"
    header_names = ["Open", "High", "Low", "Close"]
    _checktime = "15:10"


class UKX(Asset):
    file = "UKX_1min.txt"
    header_names = ["Open", "High", "Low", "Close", "Volume"]
    _checktime = "8:10"


# Main
df = get_rankings(es=ES(), vix=VIX(), euros=[CAC40(), DAX(), ESTX50(), N100(), UKX()])
df.to_csv("files/MarketMeanReversion/MarketMeanReversionOutput.csv")
print("done.")