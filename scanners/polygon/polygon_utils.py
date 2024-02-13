from datetime import datetime


def validate_datetime(d, fmt='poly'):
    """ If passed a datetime object, this function will return a string suitable for polygon.
    If passed as string, it will verify the format is YYYY-mm-dd or raise an error"""

    if isinstance(d, datetime) == True:
        return d.strftime("%Y-%m-%d") if fmt == 'poly' else d

    else:
        try:
            datetime.strptime(d, "%Y-%m-%d")
            return d if fmt == 'poly' else datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            try:
                return datetime.strptime(d, '%m/%d/%Y').strftime("%Y-%m-%d") if fmt == 'poly' else datetime.strptime(d, '%m/%d/%Y')
            except ValueError:
                raise ValueError("Incorrect data format, should be YYYY-MM-DD")



def get_agg_bars_url(symbol, multiplier, timespan, from_date, to_date, adjusted='true', sort='asc', limit='50000', apiKey=settings.POLYGON_API_KEY) -> str:

    """ This function will create a valid url for the "Aggregate (Bars) polygon function found at:
    https://polygon.io/docs/stocks/get_v2_aggs_ticker__stocksticker__range__multiplier___timespan___from___to

    you most likely will call it like this ...

    url = get_agg_bars('TSLA', 1 , 'minute', '2021-12-05', '2022-04-24') """

    multiplier = str(multiplier)
    from_date = validate_datetime(from_date)
    to_date = validate_datetime(to_date)
    return 'https://api.polygon.io/v2/aggs/ticker/{}/range/{}/{}/{}/{}?adjusted={}&sort={}&limit={}&apiKey={}'.format(symbol, multiplier, timespan, from_date, to_date, adjusted, sort, limit, apiKey)

