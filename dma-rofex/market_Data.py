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
import time
import pprint
import pyRofex

# 1-Initialize the environment
pyRofex.initialize(user="fedejbrun5018",
                   password="ugklxY0*",
                   account="REM5018",
                   environment=pyRofex.Environment.REMARKET)


# 2-Defines the handlers that will process the messages and exceptions.
def market_data_handler(message):
    pprint.pprint(message)


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
           pyRofex.MarketDataEntry.LAST]

pyRofex.market_data_subscription(tickers=instruments,
                                 entries=entries)


# Wait 5 sec then close the connection
time.sleep(5)
pyRofex.close_websocket_connection()
