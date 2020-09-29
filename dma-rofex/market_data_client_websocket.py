"""
    Sample Module.
    Example of market data using websocket API.
    The code show how to initialize the connection,
    subscribe to market data for a list of valid and invalid instruments,
    and finally close the connection.
    Go to the official Documentation to check the API Responses.
    Steps:
    1-Initialize the environment
    2-Defines the handlers that will process the messages and exceptions.
    3-Initialize Websocket Connection with the handlers
    4-Subscribes to receive market data messages for a list of valid instruments
    5-Subscribes to an invalid instrument
    6-Wait 5 sec then close the connection
"""
import sys
import pprint
import pyRofex
import pandas as pd
import traceback
import simplejson
import datetime as dt


# 1-Initialize the environment
pyRofex.initialize(user="fedejbrun5018",
                   password="ugklxY0*",
                   account="REM5018",
                   environment=pyRofex.Environment.REMARKET)


# 2-Defines the handlers that will process the messages and exceptions.
def market_data_handler(message):
    try:
        ticker = message.get('instrumentId').get('symbol')
        ticker_entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS]

        if ticker:
            full_book = pyRofex.get_market_data(ticker, ticker_entries, depth=10)
            trade_history = pyRofex.get_trade_history(ticker, dt.date(2020, 1, 1), dt.date.today())
            trade_history_df = pd.DataFrame.from_dict(trade_history)
            print(f"Trade History {ticker}")
            print(pprint.pprint(trade_history_df))

            print(f"Full book for {ticker}")
            print(pprint.pprint(full_book))
        """
        print(message.get('instrumentId'))
        instrument_df = pd.DataFrame(message)
        bid_series = message.get('marketData').get('BI')
        bid_series = pd.DataFrame.from_dict(bid_series)
        ask_series = message.get('marketData').get('OF')
        ask_series = pd.DataFrame.from_dict(ask_series)
        print(bid_series)
        print(ask_series)
        print(instrument_df)
        """
        # Codigo para procesar el mensaje
    except Exception:
        traceback.print_exc()


def error_handler(message):
    print("Error Message Received: {0}".format(message))


def exception_handler(e):
    print("Exception Occurred: {0}".format(e.message))


# 3-Initialize Websocket Connection with the handlers
pyRofex.init_websocket_connection(market_data_handler=market_data_handler,
                                  error_handler=error_handler,
                                  exception_handler=exception_handler)

# 4-Subscribes to receive market data messages
instruments = ["GGALOct20", "GGALDic20"]  # Instruments list to subscribe
entries = [pyRofex.MarketDataEntry.BIDS,
           pyRofex.MarketDataEntry.OFFERS,
           pyRofex.MarketDataEntry.OPEN_INTEREST]

pyRofex.market_data_subscription(tickers=instruments,
                                 entries=entries)

while True:
    try:
        for _ in range(1):
            pass
    except KeyboardInterrupt:
        print("Terminating Connection")
        pyRofex.close_websocket_connection()
        sys.exit(0)
