import socket
import ssl
import logging
from os import system, name
import time
import pyRofex


class ConnectionROFEX:
    def __init__(self, verbose=0):
        # Set the the parameter for the REMARKET environment
        pyRofex.initialize(user="fedejbrun5018",
                           password="ugklxY0*",
                           account="REM5018",
                           environment=pyRofex.Environment.REMARKET)

    # First we define the handlers that will process the messages and exceptions.
    def market_data_handler(message):
        print("Market Data Message Received: {0}".format(message))

    def order_report_handler(message):
        print("Order Report Message Received: {0}".format(message))

    def error_handler(message):
        print("Error Message Received: {0}".format(message))

    def exception_handler(e):
        print("Exception Occurred: {0}".format(e.message))

    def connect(self):
        # Initiate Websocket Connection

        try:
            pyRofex.init_websocket_connection(market_data_handler=self.market_data_handler,
                                              order_report_handler=self.order_report_handler,
                                              error_handler=self.error_handler,
                                              exception_handler=self.exception_handler)
        except Exception as ex:
            pass

    def subscribe_instruments(self, firs_instrument, **args_instruments):

        instruments = [str(firs_instrument)]

        instruments_list = list(args_instruments.keys())

        for instrument in instruments_list:
            instruments.append(str(instrument))

        entries = [pyRofex.MarketDataEntry.BIDS,
                   pyRofex.MarketDataEntry.OFFERS,
                   pyRofex.MarketDataEntry.LAST]

        # TODO cambiar
        pyRofex.market_data_subscription(tickers=instruments,
                                         entries=entries)


def clear_screen():
    # for windows
    if name == 'nt':
        _ = system('cls')

        # for mac and linux(here, os.name is 'posix')
    else:
        _ = system('clear')


if __name__ == '__main__':
    rofex_client = ConnectionROFEX()
    rofex_client.connect()
    rofex_client.subscribe_instruments("GGALOct20")
