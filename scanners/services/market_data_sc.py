import asyncio
import json
from typing import Any, Dict, List, Optional
import pandas as pd
import websockets

import scanners.settings_sc as settings


def clean_mkt_agg_data(results, justdates=False):
    """
    Takes the results of polygon agg bars and formats it into usable dataframe

    ** Returns: Pandas Dataframe
    """
    df = pd.DataFrame.from_dict(results)
    if justdates:
        df = df.rename(columns={'t': 'Timestamp'})
    else:
        df = df.rename(columns={'v': 'Volume',
                                              'vw': 'VWAP',
                                              'o': 'Open',
                                              'h': 'High',
                                              'c': 'Close',
                                              'l': 'Low',
                                              't': 'Timestamp',
                                              'n': 'Num of Transactions'})
        df['Volume'] = df['Volume'].astype(int)  # convert float to vol
    df['Date'] = (df['Timestamp'] / 1000).astype(int)  # change epoch to int
    df['Date'] = pd.to_datetime(df['Date'], unit='s')  # convert epoch to int
    df = df.set_index('Date')  # date needs to be the index for mpl finance # TEMP
    df = df.tz_localize('utc')
    df = df.tz_convert('US/Eastern')
    return df


class MarketData:
    index_name = "start"
    name_maps = {
        "v": "Volume",
        "vw": "VWAP",
        "o": "Open",
        "c": "Close",
        "h": "High",
        "l": "Low",
        "t": "Timestamp",
        'n': 'Num of Transactions'
    }

    def __init__(self):
        self._symbol_to_df: Dict[str, pd.DataFrame] = {}

    def get_symbol_data(self, symbol: str) -> pd.DataFrame:
        return self._symbol_to_df.get(symbol)

    def add_data(self, data: List[Dict]):
        parsed_data = {}
        for obj in data:
            if "sym" not in obj:
                continue
            symbol = obj["sym"]
            obj = {v: obj[k] for k, v in self.name_maps.items() if k in obj}
            parsed_data.setdefault(symbol, [])
            parsed_data[symbol].append(obj)
        for symbol, data in parsed_data.items():
            if len(data) == 0:
                continue
            new_df = pd.DataFrame(data).set_index(self.index_name)
            if symbol in self._symbol_to_df:
                self._symbol_to_df[symbol] = pd.concat([self._symbol_to_df[symbol], new_df])
            else:
                self._symbol_to_df[symbol] = new_df


# Singleton
class MarketDataProvider:
    _instance = None

    def __init__(self):
        self._socket = None
        self._market_data = MarketData()  # Creates the above Market Instance
        self._task = asyncio.create_task(self._start())
        self._subscribed = set()

    @property
    def market_data(self):
        return self._market_data

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    # async def get_last_trade(self, ticker: Ticker) -> Optional[Dict[str, Any]]:
    #     async with httpx.AsyncClient() as client:
    #         try:
    #             response = await retry_request(
    #                 client,
    #                 "GET",
    #                 urljoin(
    #                     settings.POLYGON_BASE_URL, f"/v2/last/trade/{ticker.symbol}"
    #                 ),
    #                 params={"apiKey": settings.POLYGON_API_KEY},
    #             )
    #             response.raise_for_status()
    #             data = response.json()
    #             if data["status"] != "OK":
    #                 return None
    #             return data["results"]
    #         except (MaxRetryError, httpx.HTTPStatusError) as ex:
    #             return None

    # async def _subscribe_internal(self, ticker: Ticker):
    #     await self._socket.send(
    #         json.dumps({"action": "subscribe", "params": f"AM.{ticker.symbol}"})
    #     )

    # async def subscribe(self, ticker: Ticker):
    #     if ticker.symbol in self._subscribed:
    #         return
    #     await self._subscribe_internal(ticker)
    #     self._subscribed.add(ticker.symbol)

    async def _start(self):
        async for sock in websockets.connect(settings.POLYGON_WSS):
            try:
                await sock.send(
                    json.dumps({"action": "auth", "params": settings.POLYGON_API_KEY})
                )
                await asyncio.gather(
                    *[
                        asyncio.create_task(self._subscribe_internal(ticker))
                        for ticker in self._subscribed
                    ]
                )
                self._socket = sock
                async for msg in sock:
                    data = json.loads(msg)
                    self._market_data.add_data(data)
            except websockets.ConnectionClosed:
                continue