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
import time

import pyRofex
import pandas as pd
import traceback
import simplejson
import datetime as dt
import json


def send_buy_order(ticker, price, qty):
    order = pyRofex.send_order(ticker=ticker,
                               side=pyRofex.Side.BUY,
                               size=qty,
                               price=price,
                               order_type=pyRofex.OrderType.LIMIT)

    print("Send Order Response:")
    pprint.pprint(order)
    order_status = pyRofex.get_order_status(order["order"]["clientId"])
    print("Order Status Response:")
    pprint.pprint(order_status)


# 1-Initialize the environment
pyRofex.initialize(user="fedejbrun5018",
                   password="ugklxY0*",
                   account="REM5018",
                   environment=pyRofex.Environment.REMARKET)


# 2-Defines the handlers that will process the messages and exceptions.
def market_data_handler(message):
    global quoting_price
    global quoting_qty
    global order_sent

    try:
        ticker = message.get('instrumentId').get('symbol')
        ticker_entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS]

        if ticker:
            full_book = pyRofex.get_market_data(ticker, ticker_entries, depth=10)
            # trade_history = pyRofex.get_trade_history(ticker, dt.date(2020, 1, 1), dt.date.today())
            # trade_history_df = pd.DataFrame.from_dict(trade_history.get('trades')).drop('servertime', axis=1)
            # trade_history_df = trade_history_df[['symbol', 'price', 'size', 'datetime']]
            # print(f"Trade History {ticker}")
            # print(pprint.pprint(trade_history_df))

            print(f"Full book for {ticker}")
            print(pprint.pprint(full_book.get('marketData')))
            market_price = float(full_book.get('marketData').get('BI')[0].get('price'))
            quoting_price = market_price + 0.05
            market_qty = int(full_book.get('marketData').get('OF')[0].get('size'))
            quoting_qty = market_qty

        if order_sent < 1:
            send_buy_order("GGALOct20", quoting_price, quoting_qty)
            order_sent += 1

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
instruments = ["GGALOct20"]  # Instruments list to subscribe
entries = [pyRofex.MarketDataEntry.BIDS,
           pyRofex.MarketDataEntry.OFFERS,
           pyRofex.MarketDataEntry.OPEN_INTEREST]

quoting_price = 0.0
quoting_qty = 0
order_sent = 0

pyRofex.market_data_subscription(tickers=instruments,
                                 entries=entries)
pyRofex.order_report_subscription()

time.sleep(5)
instruments.append("GGALDic20")

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
