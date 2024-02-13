import asyncio
import logging
from abc import ABC, abstractmethod
import requests
import pandas as pd
import time as pytime
import os
import scanners.settings_sc as settings
import math


from scanners.models.ticker_sc import Ticker
from scanners.services.timeframes_sc import TimeFrames

from scanners.services.ticker_sc import (
    TickerSelector,
)

from scanners.services.utils_sc import get_plotpath, datechunks
logger = logging.getLogger(__name__)  # TODO: Add colorama on the logging comments


def geturl(symbol, start, end):
    return f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start}/{end}?adjusted=false&sort=asc&limit=50000&apiKey={settings.POLYGON_API_KEY}"


class Scanner(ABC):

    startdate: str
    enddate: str

    timeframe: TimeFrames
    category: str
    _filters = []
    strategy_name: str
    iteration: str
    plot_path: str
    _gapperslist = []
    _gapperspd = None
    _columns = None  # ['Date', 'Ticker', 'Scan Time']
    category: str
    plot_path: str


    def __init__(self, startdate, enddate) -> None:
        self.startdate = startdate
        self.enddate = enddate

    @abstractmethod
    async def run_per_ticker(self, session, ticker: Ticker):
        pass

    async def run(self):

        # TODO: Check if file is open ...
        # Get the working time list ...

        start_time = pytime.time()
        start_time2 = pytime.time()

        chunks = datechunks(self.startdate, self.enddate)  # ~ 7.5 min / month ... for 1 ticker ...

        for i, chunk in enumerate(chunks):
            start, end = chunk[0], chunk[1]
            tickers = await TickerSelector(self._filters, start, end).get_tickers()  # TODO: Find out what this is ...
            if len(tickers) == 0:
                logger.info(f"No tickers to run {self.__class__.__name__} on.")
                return

            with requests.Session() as session:
                await asyncio.gather(
                    *[asyncio.create_task(self.run_per_ticker(session, ticker)) for ticker in tickers]
                )
            logger.info(
                f"Date Chunk {i+1} of {len(chunks)} Completed in {int(pytime.time() - start_time2)} Seconds"
            )
            start_time2 = pytime.time()

        # Exporting the file ...
        if self._gapperslist:
            try:
                self._gapperspd = pd.DataFrame(self._gapperslist, columns=self._columns)
                if not os.path.isdir(f"{self.plot_path}"):  # is path directory even created?
                    os.makedirs(f"{self.plot_path}", mode=777)
                self._gapperspd.to_csv(f"{self.plot_path}{self.strategy_name}_{self.startdate}_to_{self.enddate}.csv", index=False)
                logger.info(
                    f"Exported {len(self._gapperspd)} Results to CSV in {int(pytime.time() - start_time)} Seconds"
                )
            except AssertionError:
                print()
        else:
            logger.info(
                f"No Results in {int(pytime.time() - start_time)} Seconds"
            )
        return

        # end script ...
