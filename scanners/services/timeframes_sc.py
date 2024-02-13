import numpy
import matplotlib

matplotlib.use('agg')
import matplotlib.pyplot as plt
from matplotlib import gridspec
import matplotlib.ticker as plticker
import math

import pandas as pd

from typing import List, TypeVar, Union
PandasDataFrame = TypeVar('pandas.core.frame.DataFrame')


def get_pm_and_ah_span(timeframe, dfloc):

    pm_df = dfloc
    ah_df = dfloc

    if timeframe == '1 hour 1 month':
        pm_df = pm_df.between_time('00:00', '08:29')
        ah_df = ah_df.between_time('16:00', '22:59')
    elif timeframe == '4 hour 3 months':
        pm_df = pm_df.between_time('00:00', '05:29')
        ah_df = ah_df.between_time('16:00', '19:59')
    else:  # 30 minute intervals
        pm_df = pm_df.between_time('00:00', '09:29')
        ah_df = ah_df.between_time('16:00', '23:59')

    pm_max_df = pm_df.groupby(pd.Grouper(key='Datetime', freq='D')).max()
    pm_min_df = pm_df.groupby(pd.Grouper(key='Datetime', freq='D')).min()
    pm_mask_data = [pm_min_df['MPL Number Map'], pm_max_df['MPL Number Map']]
    pm_mask = pd.concat(pm_mask_data, axis=1, keys=['Start', 'End']).dropna()

    ah_max_df = ah_df.groupby(pd.Grouper(key='Datetime', freq='D')).max()
    ah_min_df = ah_df.groupby(pd.Grouper(key='Datetime', freq='D')).min()
    ah_mask_data = [ah_min_df['MPL Number Map'], ah_max_df['MPL Number Map']]
    ah_mask = pd.concat(ah_mask_data, axis=1, keys=['Start', 'End']).dropna()
    return pm_mask, ah_mask


def convert_1min_to_2min(x):
    try:
        return x.apply(lambda ts: ts.replace(minute=math.floor(ts.minute / 2) * 2))
    except ValueError:
        return x.apply(lambda ts: ts.replace(hour=ts.hour + 1, minute=0))


def convert_1min_to_3min(x):
    try:
        return x.apply(lambda ts: ts.replace(minute=math.floor(ts.minute / 3) * 3))
    except ValueError:
        return x.apply(lambda ts: ts.replace(hour=ts.hour + 1, minute=0))


def convert_1min_to_5min(x):
    try:
        return x.apply(lambda ts: ts.replace(minute=math.floor(ts.minute / 5) * 5))
    except ValueError:
        return x.apply(lambda ts: ts.replace(hour=ts.floor + 1, minute=0))


def convert_1min_to_10min(x):
    try:
        return x.apply(lambda ts: ts.replace(minute=math.floor(ts.minute / 10) * 10))
    except ValueError:
        return x.apply(lambda ts: ts.replace(hour=ts.hour + 1, minute=0))


def convert_1min_to_15min(x):
    try:
        return x.apply(lambda ts: ts.replace(minute=math.floor(ts.minute / 15) * 15))
    except ValueError:
        return x.apply(lambda ts: ts.replace(hour=ts.hour + 1, minute=0))


def convert_1min_to_30min(x):
    try:
        return x.apply(lambda ts: ts.replace(minute=math.floor(ts.minute / 30) * 30))
    except ValueError:
        return x.apply(lambda ts: ts.replace(hour=ts.hour + 1, minute=0))


def convert_1min_to_60min(x):
    try:
        return x.apply(lambda ts: ts.replace(minute=math.floor(ts.minute / 60) * 60))
    except ValueError:
        return x.apply(lambda ts: ts.replace(hour=ts.hour + 1, minute=0))


def convert_1min_to_240min(x):
    try:
        return x.apply(lambda ts: ts.replace(minute=math.floor(ts.minute / 240) * 240))
    except ValueError:
        return x.apply(lambda ts: ts.replace(hour=ts.hour + 1, minute=0))

class TimeFrames:

    start_date = ""
    end_date = ""

    # STATES
    user_selected_timeframes: List[str] = [
        '1 min',
        '2 min',
        '3 min',
        '5 min',
        '10 min',
        '15 min',
        '30 min',
        '1 hour',
        '4 hour',
        '1 day'
    ]

    _mandatory_timeframes: List[str] = [
        # '1 min',
        # '1 day'
    ]

    active_timeframes_params: dict = {}

    supporting_timeframes_params: dict = {
        '1 min +- 3 days': {
            'mpl_datetime_fmt': '%H:%M',
            'mult_and_timespan': (1, 'minute', 3, 3),  # 3 trading days in 3 days
            'chart_bounds': ['04:00', '20:00', '1Min'],
        }
    }

    all_timeframes_params: dict = {  # TODO: See about adding this to the switcher, or creating subclass
        '1 min': {
            'mpl_datetime_fmt': '%H:%M',
            'mult_and_timespan': (1, 'minute', 1),  # 1 trading days in 1 day
            'chart_bounds': ['04:00', '20:00', '1Min'],
        },
            # 'agg_bars_minus_days_for_url': 4,
            # 'agg_bars_plus_days_for_url': 3,
        '2 min': {
            'mpl_datetime_fmt': '%H:%M',
            'mult_and_timespan': (2, 'minute', 2),  # 2 trading days in 2 days
            'chart_bounds': ['04:00', '20:00', '2Min'],
            'tf_convert_lambda_func': convert_1min_to_2min,

            # 'agg_bars_minus_days_for_url': 2,  # this should match the # days in label: for ex "2 min 2 days"
        },
        '3 min': {
            'mpl_datetime_fmt': '%H:%M',
            'mult_and_timespan': (3, 'minute', 2),  # 2 trading days in 2 days
            'chart_bounds': ['04:00', '20:00', '3Min'],
            'tf_convert_lambda_func': convert_1min_to_3min,

            # 'agg_bars_minus_days_for_url': 2,  # this should match the # days in label: for ex "2 min 2 days"
        },
        '5 min': {
            'mpl_datetime_fmt': '%H:%M',
            'mult_and_timespan': (5, 'minute', 3),  # 3 trading days in 3 days
            'chart_bounds': ['04:00', '20:00', '5Min'],
            'tf_convert_lambda_func': convert_1min_to_5min,

            # 'agg_bars_minus_days_for_url': 3,  # this should match the # days in label: for ex "2 min 2 days"
        },
        '10 min': {
            'mpl_datetime_fmt': '%H',
            'mult_and_timespan': (10, 'minute', 5),  # 5 trading days in 1 week
            'chart_bounds': ['04:00', '20:00', '10Min'],
            'tf_convert_lambda_func': convert_1min_to_10min,

            # 'agg_bars_minus_days_for_url': 5,  # this should match the # days in label: for ex "2 min 2 days"
        },
        '15 min': {
            'mpl_datetime_fmt': '%H:%M',
            'mult_and_timespan': (15, 'minute', 5),  # 5 trading days in 1 week
            'chart_bounds': ['04:00', '20:00', '15Min'],
            'tf_convert_lambda_func': convert_1min_to_15min,

            # 'agg_bars_minus_days_for_url': 5,  # this should match the # days in label: for ex "2 min 2 days"
        },
        '30 min': {
            'mpl_datetime_fmt': '%m/%d',
            'mult_and_timespan': (30, 'minute', 10),  # 10 trading days in 2 weeks
            'chart_bounds': ['04:00', '20:00', '30Min'],
            'tf_convert_lambda_func': convert_1min_to_30min,

            # 'agg_bars_minus_days_for_url': 10,  # this should match the # days in label: for ex "2 min 2 days"

        },
        '1 hour': {
            'mpl_datetime_fmt': '%b-%d',
            'mult_and_timespan': (1, 'hour', 23),  # 23 trading days in 1 month
            'chart_bounds': ['04:00', '20:00', '1H'],
            'tf_convert_lambda_func': convert_1min_to_60min,
            # 'agg_bars_minus_days_for_url': 23,  # this should match the # days in label: for ex "2 min 2 days"

        },
        '4 hour': {
            'mpl_datetime_fmt': '%b-%d',
            'mult_and_timespan': (1, 'hour', 69),  # 69 trading days in 3 months
            'chart_bounds': ['04:00', '20:00', '4H'],
            'tf_convert_lambda_func': convert_1min_to_240min,


            # 'agg_bars_minus_days_for_url': 69,  # this should match the # days in label: for ex "2 min 2 days"

        },
        '1 day': {
            'mpl_datetime_fmt': '%b-%d',
            'mult_and_timespan': (1, 'day', 253),  # 253 trading days in 1 year
            # 'chart_bounds': ['04:00', '20:00', '1Min'],

            # 253 trading days ...
            # 'agg_bars_minus_days_for_url': 253,  # this should match the # days in label: for ex "2 min 2 days"
        }
    }

    # INIT
    def __init__(self) -> None:
        pass

    # METHODS

    @classmethod
    def get_all_tfs_params(cls) -> dict:
        """ return dict: cls.timeframes_params_all """
        return cls.all_timeframes_params

    @classmethod
    def get_active_tfs_params(cls) -> dict:
        """ return dict: cls.timeframes_params """
        return cls.active_timeframes_params

    @classmethod
    def _set_active_tfs_params(cls, frame):
        """ copy over user selected and mandatory timeframes into the active timeframe params"""
        cls.active_timeframes_params[frame] = cls.get_all_tfs_params()[frame]
        for mandatory_frame in cls._mandatory_timeframes:
            if mandatory_frame not in cls.active_timeframes_params.keys():
                cls.active_timeframes_params[mandatory_frame] = cls.all_timeframes_params[mandatory_frame]
        # cls.active_timeframes_params = {**cls.active_timeframes_params, **cls.supporting_timeframes_params}  # append timeframes_params_supporting
        return

    @classmethod
    def set_tfs(cls, timeframes_user_selected):
        """ Set the timeframes_user_selected List[str] """
        cls.user_selected_timeframes = timeframes_user_selected
        for frame in timeframes_user_selected:
            cls._set_active_tfs_params(frame)
        return cls

    @classmethod
    def get_tfs(cls) -> List[str]:
        """ Get user selected timeframes -> List[str] """
        return cls.user_selected_timeframes

    @classmethod
    def get_mult_and_ts(cls, frame: str):
        if frame in cls.active_timeframes_params.keys():
            return cls.active_timeframes_params[frame]['mult_and_timespan']
        else:
            return cls.all_timeframes_params[frame]['mult_and_timespan']

    @classmethod
    def get_tf_freq(cls, frame: str):
        return cls.all_timeframes_params[frame]['mult_and_timespan'][0]

    @classmethod
    def get_convert_lambda_func(cls, frame: str):
        return cls.all_timeframes_params[frame]['tf_convert_lambda_func']

    @classmethod
    def get_mpl_fmt(cls, frame: str) -> str:
        """
        Returns a string "%b-%d" that's used by MPL Finance Library to convert MPL Numbers to proper datetime format
        """
        return cls.all_timeframes_params[frame]['mpl_datetime_fmt']

    @classmethod
    def reindex_bounds(cls, data: Union[dict, PandasDataFrame], frame=None) -> Union[dict, PandasDataFrame]:
        """
        This function re-indexes a dict of dataframes or a single dataframe based on the
        Timeframe().timeframes_params_all['chart_bounds'] parametters.

        AND it adds the MPL index number (the charting library, mpl finance doesn't actually plot dates, instead it
        assigns a number (we call MPL Number) to each datetime value, spaces it out, plots it, and then it has another
        internal function to re-index it back to datetime for the charts) ... unpacked a lot there, I know haha ...

        Since polygon will just ignore missing data, this will show halts separated into fixed timeframes

        if inputting a dictionary like "dfs_dict" no need to populate "frame" variable
        if inputting a single pandas dataframe, need to specify which timeframe to use ...

        ** Returns: whatever was passed through data ... either dict or pandas dataframe
        """

        def reindex(df: PandasDataFrame = data, frame: str = frame) -> PandasDataFrame:
            if not frame in ['1 min +- 3 days', '1 day 1 year']:
                valid_dates = numpy.unique(df.index.strftime('%Y-%m-%d'))
                fr, to, freq = cls.get_all_tfs_params()[frame]["chart_bounds"]
                datetimes_idx = pd.date_range(start=df.between_time(fr, to).index[0],
                                               end=df.between_time(fr, to).index[-1],
                                               freq=freq)
                df = df.reindex(datetimes_idx)
                df = df.tz_convert('US/Eastern')
                df = df.between_time(fr, to)
                df = df[df.index.strftime('%Y-%m-%d').isin(valid_dates)]
                df.loc[:, 'Datetime'] = df.index
                df.loc[:, 'MPL Number Map'] = range(0, len(df))

            return df

        if type(data) == PandasDataFrame:   # if input is a dataframe just pass it into the function
            assert frame is not None
            data = reindex()
        elif type(data) == dict:  # if input is a dictionary, loop thru all the frames ...
            for frame in data:
                data[frame] = reindex(df=data[frame], frame=frame)
        return data


# TIME FRAME DEPENDENT METHODS
def get_proper_x_loc(timeframe, df):

    numdays = len(numpy.unique(df.index.date))

    if timeframe == '1 min today':  # 30 minute intervals
        rslt_df = df[df['Datetime'].dt.minute == 00].append(
            df[df['Datetime'].dt.minute == 30])
        return plticker.FixedLocator(rslt_df['MPL Number Map'].tolist())

    elif timeframe == '2 min 2 days':  # 1 hour intervals
        rslt_df = df[df['Datetime'].dt.minute == 00]
        return plticker.FixedLocator(rslt_df['MPL Number Map'].tolist())

    elif timeframe == '3 min 2 days':  # 1 hour intervals
        rslt_df = df[df['Datetime'].dt.minute == 00]
        return plticker.FixedLocator(rslt_df['MPL Number Map'].tolist())

    elif timeframe == '5 min 3 days':  # 1 hour intervals
        rslt_df = df[df['Datetime'].dt.minute == 00]
        return plticker.FixedLocator(rslt_df['MPL Number Map'].tolist())

    elif timeframe == '10 min 1 week':  # 1 hour intervals
        rslt_df = df[df['Datetime'].dt.minute == 00]
        return plticker.FixedLocator(rslt_df['MPL Number Map'].tolist())

    elif timeframe == '15 min 1 week':  # 1 hour intervals
        rslt_df = df[(df['Datetime'].dt.minute == 00) & (df['Datetime'].dt.hour == 4)
                        ].append(
            df[(df['Datetime'].dt.minute == 00) & (df['Datetime'].dt.hour == 6)]
        ).append(
            df[(df['Datetime'].dt.minute == 00) & (df['Datetime'].dt.hour == 8)]
        ).append(
            df[(df['Datetime'].dt.minute == 00) & (df['Datetime'].dt.hour == 10)]
        ).append(
            df[(df['Datetime'].dt.minute == 00) & (df['Datetime'].dt.hour == 12)]
        ).append(
            df[(df['Datetime'].dt.minute == 00) & (df['Datetime'].dt.hour == 14)]
        ).append(
            df[(df['Datetime'].dt.minute == 00) & (df['Datetime'].dt.hour == 16)]
        ).append(
            df[(df['Datetime'].dt.minute == 00) & (df['Datetime'].dt.hour == 18)]
        )
        return plticker.FixedLocator(rslt_df['MPL Number Map'].tolist())

    elif timeframe == '30 min 2 weeks':  # 30 minute intervals
        rslt_df = df[(df['Datetime'].dt.minute == 00) & (df['Datetime'].dt.hour == 4)]
        return plticker.FixedLocator(rslt_df['MPL Number Map'].tolist())

    elif timeframe == '1 hour 1 month':  # 30 minute intervals
        return plticker.MaxNLocator(nbins=numdays)

    elif timeframe == '4 hour 3 months':  # 30 minute intervals
        return plticker.MaxNLocator(nbins=numdays/5+1)

    else:
        return plticker.MaxNLocator(nbins=25)

