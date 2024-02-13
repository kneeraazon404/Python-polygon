import asyncio
import logging
from typing import List
import traceback

from scanners.services.scanner import Scanner
from scanners.scanners.largecapbacksidescanner import LargeCapBacksideScanner
from scanners.scanners.parabscanner import ParabScanner
from scanners.scanners.pushpercscanner import PushPercScanner
from scanners.scanners.scalppmgapscanner import ScalpPMGapScanner
from scanners.scanners.parabscanner_scalp import ParabScannerScalp
from scanners.scanners.scalpPara import ScalpPara
from scanners.scanners.PMGapScannerRetraceLong import PMGapScannerRetraceLong

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


import pandas_market_calendars as mcal

# nyse = mcal.get_calendar('NYSE')
# valid_days = nyse.valid_days(start_date='2021-12-31', end_date='2022-06-01')

async def _run(startdate, enddate):
    scanners: List[Scanner] = [
        # PMGapScanner(startdate=startdate, enddate=enddate),  # 1 month 1/6 -> 7 min
        # ScalpPMGapScanner(startdate=startdate, enddate=enddate),  # 1 month 1/6 -> 7 min
        # ParabScannerScalp(startdate=startdate, enddate=enddate),  # 1 month 1/6 -> 7 min
        # PMGapScannerRetraceLong(startdate=startdate, enddate=enddate),  # 1 month 1/6 -> 7 min
        # ScalpPara(startdate=startdate, enddate=enddate),  # 1 month 1/6 -> 7 min
        # LargeCapBacksideScanner(startdate=startdate, enddate=enddate)
        ParabScanner(startdate=startdate, enddate=enddate),  # 1 month 1/6 -> 7 min
        # PushPercScanner(startdate=startdate, enddate=enddate),  # 1 month 1/6 -> 7 min
        # AfternoonPushPercScanner(startdate=startdate, enddate=enddate),  # 1 month 1/6 -> 7 min
    ]
    # await asyncio.gather(
    #     *[asyncio.create_task(scanner.run()) for scanner in scanners]
    # )

    for scanner in scanners:
        await asyncio.gather(
            *[asyncio.create_task(scanner.run())]
        )


def run():
    for i in [
        ["2023-10-03", "2023-10-04"],
        # ["2022-02-01", "2022-02-28"],
        # ["2022-03-01", "2022-03-31"],
        # ["2022-04-01", "2022-04-30"],
        # ["2022-05-01", "2022-05-31"],
        # ["2022-06-01", "2022-06-30"],
        # ["2022-07-01", "2022-07-31"],
        # ["2022-08-01", "2022-08-31"],
        # ["2022-09-01", "2022-09-30"],
        # ["2022-10-01", "2022-10-31"],
        # ["2022-10-31", "2022-11-22"],

    ]:
        try:
            asyncio.run(_run(startdate=i[0], enddate=i[1]))
        except Exception as exc:
            logger.error(f"Failed to get scan: {traceback.format_exc()}")
            raise exc
    print("Done Main")