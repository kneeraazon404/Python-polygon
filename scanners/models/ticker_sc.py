from datetime import date
from typing import Optional, TypeVar
PandasDataFrame = TypeVar('pandas.core.frame.DataFrame')
DateTime = TypeVar('datetime.datetime')

from pydantic import BaseModel, Field


class Ticker(BaseModel):
    # Mandatory ...
    symbol: str = Field(alias="ticker")
    date_source: Optional[str]

    active: Optional[bool]
    currency_name: Optional[str]
    locale: Optional[str]
    market: Optional[str]
    market_cap: Optional[int] = Field(None)
    name: Optional[str]
    description: Optional[str] = Field("")
    cik: Optional[str] = Field(None)
    composite_figi: Optional[str] = Field(None)
    delisted_utc: Optional[str] = Field(None)
    homepage_url: Optional[str] = Field(None)
    list_date: Optional[date] = Field(None)
    primary_exchange: Optional[str] = Field(None)
    share_class_figi: Optional[str] = Field(None)
    share_class_shares_outstanding: Optional[int] = Field(None)
    sic_code: Optional[str] = Field(None)
    sic_description: Optional[str] = Field(None)
    total_employees: Optional[int] = Field(None)
    type: Optional[str] = Field(None)
    weighted_shares_outstanding: Optional[int] = Field(None)

    # scan_time: Optional[str] = Field(None)
    # day1: Optional[str] = Field(alias="day1")
    # prev_day: Optional[str] = Field(None)
    #
    # prev_close: Optional[float] = Field(None)
    #
    # day1_open: Optional[float] = Field(None)
    # day1_high: Optional[float] = Field(None)
    # day1_low: Optional[float] = Field(None)
    # day1_close: Optional[float] = Field(None)
    #
    # daily_df:  Optional[PandasDataFrame] = Field(None)  # (day1 - 365days TO day1 + 10days): useful for quick forward looking analysis ...

    # active: Optional[bool]
    # currency_name: Optional[str]
    # locale: Optional[str]
    # market: Optional[str]
    # market_cap: Optional[int] = Field(None)
    # name: Optional[str]
    # description: Optional[str] = Field("")
    # cik: Optional[str] = Field(None)
    # composite_figi: Optional[str] = Field(None)
    # delisted_utc: Optional[str] = Field(None)
    # homepage_url: Optional[str] = Field(None)
    # list_date: Optional[date] = Field(None)
    # primary_exchange: Optional[str] = Field(None)
    # share_class_figi: Optional[str] = Field(None)
    # share_class_shares_outstanding: Optional[int] = Field(None)
    # sic_code: Optional[str] = Field(None)
    # sic_description: Optional[str] = Field(None)
    # total_employees: Optional[int] = Field(None)
    # type: Optional[str] = Field(None)
    # weighted_shares_outstanding: Optional[int] = Field(None)

    # scan_time: str = Field(None)
    # day1: str = Field(alias="day1")
    # prev_day: Optional[str] = Field(None)
    #
    # prev_close: Optional[float] = Field(None)
    #
    # day1_open: Optional[float] = Field(None)
    # day1_high: Optional[float] = Field(None)
    # day1_low: Optional[float] = Field(None)
    # day1_close: Optional[float] = Field(None)

    # daily_df:  Optional[PandasDataFrame] = Field(None)  # (day1 - 365days TO day1 + 10days): useful for quick forward looking analysis ...
    dfs_dict: Optional[dict] = Field({})

    def get_all_dfs_dict(self):
        return self.dfs_dict

    def get_frame_dfs_dict(self, frame: str):
        return self.dfs_dict[frame]

