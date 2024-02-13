import os

DEBUG = False  # setting to false pulls all 12K tickers

# Polygon settings
POLYGON_BASE_URL = "https://api.polygon.io/"
POLYGON_API_KEY = os.environ["POLYGON_API_KEY"] = 'REkMrcASf39sKXEEP3WxiNVRvNSKnMJH'
# POLYGON_WSS = "wss://socket.polygon.io/stocks"
# POLYGON_SECONDS_BETWEEN_REQUESTS = 0.1

# folder locations
DATE_TICKERS_CSV_PATH = "files/datetickers.csv"  # .. means one folder above
CHARTS_FOLDER_PATH = '/polypow/polyscripts/output/eod/'
BACKTEST_RESULTS_FOLDER_PATH = "/polypow/polyscripts/output/"
# TODO: Create function to create folder path if not exists already ... and remove it from git path  ...

# Requests settings
MAX_RETRIES = 3

# Scanner settings
MAX_MARKET_CAP = 1_000_000_000
MIN_PRICE = 1.5
MIN_GAP = 0.15
MIN_VOLUME = 100_000
