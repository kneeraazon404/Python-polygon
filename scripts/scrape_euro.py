import requests
from bs4 import BeautifulSoup


def scrape(symbol, URL):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
    print()
    print(f"Scraping {symbol}... ")
    page = requests.get(URL,
                        headers=headers,
                        timeout=5)
    soup = BeautifulSoup(page.content, "html.parser")

    # print(f"regularMarketChangePercent ... ")
    try:
        regularMarketChangePercent = soup.find("fin-streamer", {"data-symbol": symbol, "data-field": "regularMarketChangePercent"}).attrs["value"]
        regularMarketChangePercent = float(regularMarketChangePercent)
        # print(f"Success ... {regularMarketChangePercent}")
    except ValueError:
        print(f"Not Successful: regularMarketChangePercent")
        regularMarketChangePercent = None
    # print(f"quote-market-notice ... ")
    try:
        quoteMarketNotice = soup.find("div", {"id": "quote-market-notice"}).next_element.next
        # print(f"Success ... {quoteMarketNotice}")
    except ValueError:
        print(f"Not Successful: quoteMarketNotice")
        quoteMarketNotice = None

    return regularMarketChangePercent, quoteMarketNotice

urls = {
    "^N100": "https://finance.yahoo.com/quote/%5EN100",
    "^VIX": "https://finance.yahoo.com/quote/%5EVIX?p=^VIX&.tsrc=fin-srch",
    "ES=F": "https://finance.yahoo.com/quote/ES=F?p=ES=F&.tsrc=fin-srch",
    "^GDAXI": "https://finance.yahoo.com/quote/%5EGDAXI?p=%5EGDAXI&.tsrc=fin-srch",
    "^FCHI": "https://finance.yahoo.com/quote/%5EFCHI?p=%5EFCHI&.tsrc=fin-srch",  # CAC 40
    "^SSMI": "https://finance.yahoo.com/quote/%5ESSMI?p=%5ESSMI&.tsrc=fin-srch",
    "^FTSE": "https://finance.yahoo.com/quote/%5EFTSE?p=%5EFTSE&.tsrc=fin-srch",
    "^IBEX": "https://finance.yahoo.com/quote/%5EIBEX?p=%5EIBEX&.tsrc=fin-srch",
}

market_sentiment = {}
for symbol in urls:
    URL = urls[symbol]
    regularMarketChangePercent, quoteMarketNotice = scrape(symbol, URL)
    if regularMarketChangePercent:
        market_sentiment[symbol] = {
            "regularMarketChangePercent": regularMarketChangePercent,
            "quoteMarketNotice": quoteMarketNotice
        }
    # print()
    print(f"{symbol}, regularMarketChangePercent: {regularMarketChangePercent}, quoteMarketNotice: {quoteMarketNotice}")
    # all the values will be stored in a dictionary


