import pandas as pd

from abc import ABC, abstractmethod
from typing import List, Optional, Union, TypeVar
from scanners.models.ticker_sc import Ticker
from scanners.services.timeframes_sc import TimeFrames


PandasDataFrame = TypeVar('pandas.core.frame.DataFrame')


class Indicators:
    # data:

    def run(self, data: Union[dict, PandasDataFrame], **kwargs):
        if type(data) == pd.core.frame.DataFrame:
            data = self.run_per_ticker(data, *kwargs)
        elif type(data) == dict:  # if dictionary:
            for frame in data:
                data[frame] = self.run_per_ticker(data[frame], frame, **kwargs)

        return data

    @abstractmethod
    def run_per_ticker(self, df, frame, **kwargs):
        pass


class CandleRange(Indicators):
    def __init__(self, data: PandasDataFrame) -> None:
        self.data = self.run(data=data)

    def run_per_ticker(self, df, frame, **kwargs) -> PandasDataFrame:
        df['Candle Range'] = df['High'] - df['Low']
        return df


class ATR(Indicators):
    def __init__(self, data: PandasDataFrame, n: int = 14) -> None:
        self.n = n
        self.data = self.run(data=data)

    def wwma(self, values, n):
        return values.ewm(alpha=1/n, adjust=False).mean()

    def run_per_ticker(self, df, frame, **kwargs) -> PandasDataFrame:
        """ this functions outputs a series for the data frame """
        data = df.copy()
        high = data['High']
        low = data['Low']
        close = data['Close']
        data['tr0'] = abs(high - low)
        data['tr1'] = abs(high - close.shift())
        data['tr2'] = abs(low - close.shift())
        tr = data[['tr0', 'tr1', 'tr2']].max(axis=1)
        atr = self.wwma(tr, self.n)
        df[f"ATR {self.n}"] = atr
        return df


class ATR_plus_Candle(Indicators):
    def __init__(self, data: PandasDataFrame, atr_label: str, candle_part: str) -> None:
        self.atr_label = atr_label
        self.candle_part = candle_part
        self.data = self.run(data=data)

    def run_per_ticker(self, df, frame, **kwargs) -> PandasDataFrame:
        if frame in ['1 day 1 year']:
            return df
        df[f"{self.atr_label} + {self.candle_part}"] = df[self.candle_part] + df[self.atr_label]
        return df


class VWAP_Intraday(Indicators):
    def __init__(self, data: PandasDataFrame) -> None:
        self.data = self.run(data=data)

    def run_per_ticker(self, df, frame, **kwargs) -> PandasDataFrame:
        if frame in ['1 day 1 year']:
            return df

        volume_sum = df['Volume'].groupby(pd.Grouper(freq='1D')).transform(lambda x: x.rolling('1D').sum())
        volume_vwap_sum = (df['Volume'] * df['VWAP']).groupby(pd.Grouper(freq='1D')).transform(lambda x: x.rolling('1D').sum())
        # vwap = volume_vwap_sum / volume_sum
        df['VWAP'] = volume_vwap_sum / volume_sum
        df['Candle Range'] = df['High'] - df['Low']
        return df


