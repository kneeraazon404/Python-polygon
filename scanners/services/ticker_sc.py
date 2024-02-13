# This file also houses a lot of the scanning functions ...

import asyncio
import logging
import time as pytime
import traceback
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import urljoin

import httpx
import pandas as pd

import scanners.settings_sc as settings
from scanners.models.ticker_sc import Ticker
from scanners.services.timeframes_sc import TimeFrames

from scanners.services.market_data_sc import clean_mkt_agg_data
# from ..services.utils import MarketDatetime, MaxRetryError, retry_request
from scanners.services.utils_sc import MaxRetryError, retry_request




logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DateTickerFilter(ABC):
    """
    Class that holds a function to loops thru TickerDates and filters by its characteristics ...
    """
    @abstractmethod
    async def filter(self, ticker_list: List[Ticker]):
        pass


class NoFilter(DateTickerFilter):
    """
    Returns the TickerDate List without filtering it
    """
    def __init__(self):
        pass

    async def filter(self, ticker_list: List[Ticker]):
        return ticker_list


# class MarketCapFilter(DateTickerFilter):
#     def __init__(self, min_market_cap: int, max_market_cap: int):
#         self.min_market_cap = min_market_cap
#         self.max_market_cap = max_market_cap
#
#     async def filter(self, ticker_list: List[DateTicker]):
#         return [
#             ticker
#             for ticker in ticker_list
#             if self.min_market_cap <= ticker.market_cap <= self.max_market_cap
#         ]


# TODO: Create Variation of this ... for previously closed
# def _get_premarket_last_price(ticker: Dict[str, Any]) -> float:
#     dt = MarketDatetime.from_timestamp(ticker["updated"] / 1e9)
#     if dt.time() <= time(9, 30, 0):
#         curr_value = ticker["lastTrade"]["p"]
#     else:
#         curr_value = ticker["day"]["o"]
#     return curr_value


# def _get_premarket_gap(ticker: Dict[str, Any]):
#     last_price = _get_premarket_last_price(ticker)
#     prev_close = ticker["prevDay"]["c"]
#     if prev_close == 0:
#         return 0
#     return (last_price - prev_close) / prev_close


# Get Aggregate Tickers ...

# class SnapshotFilter(DateTickerFilter):
#     def __init__(
#         self,
#         min_price: float,
#         min_gap: float,
#         min_volume: float,
#     ):
#         self.min_price = min_price
#         self.min_gap = min_gap
#         self.min_volume = min_volume
#
#     async def filter(self, ticker_list: List[DateTicker]):
#         async with httpx.AsyncClient() as client:
#             response = await retry_request(
#                 client,
#                 "GET",
#                 urljoin(
#                     settings.POLYGON_BASE_URL,
#                     "/v2/snapshot/locale/us/markets/stocks/tickers",
#                 ),
#                 params={
#                     "apiKey": settings.POLYGON_API_KEY,
#                 },
#             )
#             response.raise_for_status()
#             new_tickers = [
#                 ticker["ticker"]
#                 for ticker in response.json()["tickers"]
#             #     if _get_premarket_last_price(ticker) >= self.min_price
#             #     and _get_premarket_gap(ticker) >= self.min_gap
#             #     and ticker["day"]["v"] >= self.min_volume
#              ] # REPLACE BY AGGREGATES
#         return [ticker for ticker in ticker_list if ticker.symbol in new_tickers]


class PullDatesTickersCSV():
    def __init__(self):
        self.df = pd.read_csv(settings.POLYGON_BASE_URL).dropna(axis=0)



# REQUIRED_DETAILS = ["market_cap"]
# REQUIRED_DETAILS = []

# async def _get_ticker_details(
#     client: httpx.AsyncClient, day1: str, ticker: str
# ) -> Optional[DateTicker]:
#     url = urljoin(settings.POLYGON_BASE_URL, f"v3/reference/tickers/{ticker}")
#     try:
#         response = await retry_request(
#             client,
#             "GET",
#             url,
#             params={"apiKey": settings.POLYGON_API_KEY},
#         )
#         response.raise_for_status()
#     except (MaxRetryError, httpx.HTTPStatusError) as exc:
#         logger.error(f"Failed to get ticker details for {ticker}")
#         return None
#     data = response.json()["results"]
#     if not all([field in data for field in REQUIRED_DETAILS]):
#         return None
#     return DateTicker(**data)


# DAILY BARS
# async def _get_dateticker_daily_df(
#     client: httpx.AsyncClient, dateticker: Ticker,
# ) -> Optional[Ticker]:
#
#     day1, symbol = dateticker.day1, dateticker.symbol
#
#     frdate = (datetime.strptime(day1, "%Y-%m-%d") - datetime_orig.timedelta(days=365)).strftime('%Y-%m-%d')  # one year behind
#     todate = (datetime.strptime(day1, "%Y-%m-%d") + datetime_orig.timedelta(days=10)).strftime('%Y-%m-%d')
#
#     url = urljoin(settings.POLYGON_BASE_URL, f"v2/aggs/ticker/{symbol}/range/1/day/{frdate}/{todate}")
#     try:
#         response = await retry_request(
#             client,
#             "GET",
#             url,
#             params={
#                 "adjusted": "false",
#                 "sort": "asc",
#                 "limit": "50000",
#                 "apiKey": settings.POLYGON_API_KEY},
#         )
#         response.raise_for_status()
#     except (MaxRetryError, httpx.HTTPStatusError) as exc:
#         logger.error(f"Failed to get ticker details for {day1, symbol}")
#         return None
#     data = response.json()["results"]
#     # if not all([field in data for field in REQUIRED_DETAILS]):
#     #     return None
#
#     daily_df = clean_mkt_agg_data(data)
#     dateticker.daily_df = daily_df
#     return dateticker
#
#
# async def _get_datetickers_daily_df(
#     client: httpx.AsyncClient, datetickers: List[Ticker]
# ) -> List[Ticker]:
#     new_datetickers = await asyncio.gather(
#         *[
#             asyncio.create_task(_get_dateticker_daily_df(client, dateticker))
#             for dateticker in datetickers
#         ]
#     )
#     return [dateticker for dateticker in new_datetickers if dateticker is not None]

def get_agg_bars_timeframe_url(frame, _start_date, _end_date):
    mult_ts = TimeFrames.get_mult_and_ts(frame)
    multiplier = mult_ts[0]
    timespan = mult_ts[1]


    return f"range/{multiplier}/{timespan}/{_start_date}/{_end_date}"


# INTRADAY BARS
_failed_tickers_aggbars_cache = []


async def _get_ticker_aggbars(
    client: httpx.AsyncClient, ticker: Ticker, frame: str, startdate: str, enddate: str
) -> Optional[Ticker]:

    symbol = ticker.symbol
    range_url = get_agg_bars_timeframe_url(frame, startdate, enddate)
    url = urljoin(settings.POLYGON_BASE_URL, f"v2/aggs/ticker/{symbol}/{range_url}")

    try:
        response = await retry_request(
            client,
            "GET",
            url,
            params={
                "apiKey": settings.POLYGON_API_KEY,
                "limit": 50000}
        )
        response.raise_for_status()
    except (MaxRetryError, httpx.HTTPStatusError) as exc:
        logger.error(f"Failed to get ticker details for {symbol}")
        return None
    try:
        data = response.json()["results"]
        chart_df = clean_mkt_agg_data(data)
        # chart_df['Datetime'] = chart_df.index
        ticker.dfs_dict[frame] = chart_df
    except:
        _failed_tickers_aggbars_cache.extend([symbol])
        # print(f"error collecting | _get_ticker_aggbars: {symbol}")
    # if not all([field in data for field in REQUIRED_DETAILS]):
    #     return None
    return ticker


async def _get_tickers_aggbars(
    client: httpx.AsyncClient, tickers: List[Ticker], frame: str, startdate: str, enddate: str
) -> List[Ticker]:
    new_tickers = await asyncio.gather(
        *[
            asyncio.create_task(_get_ticker_aggbars(client, ticker, frame, startdate, enddate))
            for ticker in tickers
        ]
    )
    return [ticker for ticker in new_tickers if ticker is not None]


async def _get_ticker_details(
    client: httpx.AsyncClient, symbol: str
) -> Optional[Ticker]:


    # day1, symbol = dateticker[0], dateticker[1]

    # scan_time = datetime.strptime(f"{day1} {dateticker[2]}", "%Y-%m-%d %X %p").strftime("%I:%M")
    # url = urljoin(settings.POLYGON_BASE_URL, f"v3/reference/tickers/{symbol}")
    # try:
    #     response = await retry_request(
    #         client,
    #         "GET",
    #         url,
    #         params={
    #             "date": day1,
    #             "apiKey": settings.POLYGON_API_KEY},
    #     )
    #     response.raise_for_status()
    # except (MaxRetryError, httpx.HTTPStatusError) as exc:
    #     logger.error(f"Failed to get ticker details for {day1, symbol}")
    #     return None
    # data = response.json()["results"]
    data = {
        "ticker": symbol,
        "data_source": "polygon"
    }
    # data["symbol"] = symbol
    # data["scan_time"] = scan_time
    # if not all([field in data for field in REQUIRED_DETAILS]):
    #     return None
    return Ticker(**data)


async def _get_tickers_details(
    client: httpx.AsyncClient, symbols: List[str]
) -> List[Ticker]:
    new_tickers = await asyncio.gather(
        *[
            asyncio.create_task(_get_ticker_details(client, symbol))
            for symbol in symbols
        ]
    )
    return [ticker for ticker in new_tickers if ticker is not None]


def read_datetickers_csv(path=settings.DATE_TICKERS_CSV_PATH):
    """
    This reads the date tickers csv, coverts the date into proper string format "%Y-%m-%d",

    ** Returns the List: [[date, symbol], [date, symbol]],
    which will be used to fetch the tickers_date functions as well as the aggregate functions ...
    """
    datetickers_df = pd.read_csv(path)
    try:
        datetickers_df['date'] = pd.to_datetime(datetickers_df['date']).dt.strftime("%Y-%m-%d")
    except TypeError as exc:
        logger.error(f"Failed to convert 'date' column in {settings.DATE_TICKERS_CSV_PATH}")
        raise exc

    return datetickers_df.values.tolist()

# This is the global varaible that houses all the [DateTicker Models]

_ticker_cache = None

async def _fetch_next(
    client: httpx.AsyncClient, url: Optional[str] = None
) -> Mapping[str, Any]:
    """
    If no url is specified, this will fetch PART 1 (of 5) default snapshot of all available tickers on this day,
    else it will accept the successive url for the defualt snapshot of all avail tickers at this moment
    """
    # https://stackoverflow.com/questions/52487663/python-type-hints-typing-mapping-vs-typing-dict
    if url is None:
        url = urljoin(settings.POLYGON_BASE_URL, "v3/reference/tickers")  # Queries all tickers
        filter_params = {
            "market": "stocks",
            "limit": 1_000,
            "currency_name": "usd",
        }
    else:
        filter_params = {}
    response = await retry_request(
        client,
        "GET",
        url,
        params={
            "apiKey": settings.POLYGON_API_KEY,
            **filter_params,
        },
    )
    response.raise_for_status()
    return response.json()


async def _get_tickers(client, startdate, enddate) -> List[Ticker]:
    """
    This function reads the datetickers.csv and grabs each Ticker Details V3
    https://polygon.io/docs/stocks/get_v3_reference_tickers__tickerQ

     **Returns List [DateTicker Models , DateTicker Model, ...]

     FYI: This is where we would swap out any historical fundamental data, most likely
    """
    global _ticker_cache
    # if _ticker_cache is not None:
    #     return _ticker_cache
    start_time = pytime.time()
    logger.info(f"Retrieving tickers...")

    tickers = []  # Ticker Objects
    ticker_symbols: List[str] = []
    response = await _fetch_next(client)

    # get all tickers ...
    ticker_symbols.extend([data["ticker"] for data in response["results"]])
    while (next_url := response.get("next_url")) is not None:
        response = await _fetch_next(client, url=next_url)
        ticker_symbols.extend([data["ticker"] for data in response["results"]])
        if settings.DEBUG:
            break

    # TODO: add manual tickers csv to delisted tickers
    chunk_size = 200

    # GET TICKER DETAILS -> FUNDAMENTALS
    ticker_chunks = [
        ticker_symbols[i: i + chunk_size]
        for i in range(0, len(ticker_symbols), chunk_size)
    ]
    for chunk in ticker_chunks:
        tickers.extend(await _get_tickers_details(client, chunk))
    _ticker_cache = tickers

    # GET TICKER AGG BARS
    ticker_chunks = [
        _ticker_cache[i: i + chunk_size]
        for i in range(0, len(_ticker_cache), chunk_size)
    ]
    for frame in TimeFrames.get_active_tfs_params():
        tickers = []  # reset date tickers model ...
        for chunk in ticker_chunks:
            tickers.extend(await _get_tickers_aggbars(client, chunk, frame, startdate, enddate))
        _ticker_cache = tickers
        logger.info(
            f"Collected Ticker dfs_dict: {frame} in {int(pytime.time() - start_time)} seconds",
        )

    logger.info(
        f"Found {len(ticker_symbols)} tickers in {int(pytime.time() - start_time)} seconds"
    )
    logger.info(
        f"Failed to collect {len(_failed_tickers_aggbars_cache)} Tickers ...",
    )
    print(_failed_tickers_aggbars_cache)
    return _ticker_cache


class TickerSelector:
    """
    This class contains the function get_datetickers returning the [DateTicker] Model,
    and houses parameters used for macro filtering
    """
    def __init__(self, filters: List[DateTickerFilter], startdate, enddate):
        self._filters = filters
        self.startdate = startdate
        self.enddate = enddate

    async def get_tickers(self) -> List[Ticker]:
        """ This function collects the List[DateTicker] Models"""
        # AsyncClient() Resources
        # https://www.youtube.com/watch?v=2FNcJKCfrzI (7m video)
        # https://stackoverflow.com/questions/32986228/difference-between-using-requests-get-and-requests-session-get
        async with httpx.AsyncClient() as client:
            try:
                tickers = await _get_tickers(client, self.startdate, self.enddate)  # This is the resulting [DateTicker] Models List
                # TODO: add some redundancy to fill in missing info ???

                # logger.info(f"Filtering tickers...")
                # for f in self._filters:
                #     datetickers = await f.filter(datetickers)
            except (MaxRetryError, httpx.HTTPStatusError) as exc:
                logger.error(f"Failed to get tickers: {traceback.format_exc()}")
                raise exc
        return tickers
