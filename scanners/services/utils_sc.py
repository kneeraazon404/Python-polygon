import asyncio
import logging
import os
import traceback
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import datetime
from typing import Any, Callable, Dict, List

import pandas_market_calendars as mcal
from datetime import datetime as pydatetime, timedelta

import httpx
import pytz
import timeit
from httpx import RequestError

from .. import settings_sc

logger = logging.getLogger(__name__)


class MaxRetryError(Exception):
    pass

def get_plotpath(category, scanner):

    path = f"scanners/output/{category}/{scanner}/{datetime.date.today().strftime('%Y-%m-%d')}/"
    iteration = "001"

    if not os.path.isdir(path):  # is path directory even created?
        pass
    # else:
    #     if len(os.listdir(path)) == 0:  # if directory exists, but does not have anything in it
    #         pass
    #     else:
    #         iteration = str(int(os.listdir(path)[-1]) + 1).zfill(3)

    # plot_path = f"{path}{iteration}/"
    return path


def datechunks(startdate, enddate):
    s = pydatetime.strptime(startdate, "%Y-%m-%d") - timedelta(days=5)
    newstartdate = s.strftime("%Y-%m-%d")
    nyse = mcal.get_calendar('NYSE')
    valid_days = nyse.valid_days(start_date=newstartdate, end_date=enddate)
    dates_df = pd.DataFrame(index=valid_days)
    dates_df.insert(loc=0, column="Start Date", value=valid_days.date.astype(str))
    newstartdate = dates_df["Start Date"].loc[:startdate].iloc[-2]
    dates_df = dates_df.loc[newstartdate:]

    dates_df.insert(loc=1, column="End Date", value=dates_df.loc[:, "Start Date"].shift(-3))
    if dates_df["End Date"].isna().all():  # if all the enddate values are emtpy
        dates_df["End Date"].iloc[0] = enddate

    dates_df = dates_df.dropna()
    chunks = dates_df.iloc[0::3, :]
    last_element = chunks["Start Date"].iloc[-1]

    chunks = chunks.reset_index(drop=True)

    left_overs = dates_df.loc[last_element:]
    if len(left_overs) > 1:
        s0 = left_overs["End Date"].iloc[0]
        e0 = left_overs["End Date"].iloc[-1]
        chunks.loc[len(chunks.index)] = [s0, e0]

    return chunks.values.tolist()

RETRIABLE_STATUS = [429, 500, 502, 503, 504]


async def retry_request(client: httpx.AsyncClient, *args, **kwargs):
    retry_count = 0
    while retry_count <= settings_sc.MAX_RETRIES:
        try:
            response = await client.request(*args, **kwargs)
            if (
                200 <= response.status_code < 300
            ) or response.status_code not in RETRIABLE_STATUS:
                return response
            logger.warning(
                f"Status code: {response.status_code}\nContent:\n{response.content.decode('utf-8')}"
            )
        except RequestError:
            logger.warning(f"RequestError: {traceback.format_exc()}")
        logger.info(f"Retry number {retry_count + 1}...")
        await asyncio.sleep(0.1 * 2**retry_count)
        retry_count += 1
    raise MaxRetryError("Max retries reached")


async def execute_tasks(
    task: Callable, args_list: List[Any], kwargs_list: Dict[str, Any]
):
    n_workers = 2 * os.cpu_count() + 1
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        loop = asyncio.get_running_loop()
        tasks = [
            asyncio.create_task(loop.run_in_executor(executor, task(*args, **kwargs)))
            for args, kwargs in zip(args_list, kwargs_list)
        ]
        return await asyncio.gather(*tasks)


def timer(number, repeat):
    def wrapper(func):
        runs = timeit.repeat(func, number=number, repeat=repeat)
        print(sum(runs) / len(runs))
    return wrapper
