import logging
import sys
import traceback
from os import system, name
import os
import time
from pprint import pprint
import pandas as pd

import pyRofex

width = os.get_terminal_size().columns
dash_line = "-" * width


class ROFEXClient:
    """Implements Client to connect to ROFEX DMA server.
    (demo = reMarkets; live = ROFEX)
    :param user: User.
    :type user: Str.
    :param password: User Password.
    :type password: Str.
    :param account: Account provided by Market Authority.
    :type account: Str.
    :param environment: Market environment.
    :type environment: Str.
    """
    def __init__(self, user, password, account, environment, verbose=3):

        self.subscribed_instruments = []

        # Define environment
        env_param = environment.lower()
        environments = {
            'demo': pyRofex.Environment.REMARKET,
            'live': pyRofex.Environment.LIVE,
        }
        env = environments.get(env_param)

        # Create logging file
        # TODO Implementar validacion para Windows/Linux
        filename = 'c:/logs/ROFEX' + env_param \
                   + time.strftime('%Y-%m-%d_%H%M%S', time.localtime(time.time())) + '.log'
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s-%(threadName)s-%(message)s', filename=filename)

        # TODO No entiendo muy bien cual es la funcion de los AddLevelName

        verbosity = verbose
        if verbosity == '1':
            logging.addLevelName('DEBUG', 10)
        elif verbosity == '2':
            logging.addLevelName('DEBUG', 20)
        elif verbosity == '3':
            logging.addLevelName('DEBUG', 30)
        else:
            logging.basicConfig(level=None, format='%(asctime)s-%(threadName)s-%(message)s', filename=filename)
            logging.addLevelName('DEBUG', 30)

        try:
            self.connect(user, password, account, env)
        except KeyboardInterrupt:
            self.disconnect()

    # ==================================================================================================================
    #   Start HANDLERS definition
    # ==================================================================================================================
    def market_data_handler(self, message):
        """
        Handles MarketData Messages received from server.
        :param message: Message received. Comes as a JSON.
        :type message: Dict.
        """
        try:
            self.process_market_data_message(message)
        except Exception:
            traceback.print_exec()

    def order_report_handler(self, message):
        """
        Handles OrderReport Messages received from server.
        :param message: Message received. Comes as a JSON.
        :type message: Dict.
        """
        # TODO implement
        print("Order Report Message Received: {0}".format(message))

    def error_handler(self, message):
        """
        Handles Error Messages received from server.
        :param message: Message received. Comes as a JSON.
        :type message: Dict.
        """
        # TODO implement
        print("Error Message Received: {0}".format(message))

    def exception_handler(self, e):
        """
        Handles Exception Messages received from server.
        :param message: Message received. Comes as a JSON.
        :type message: Dict.
        """
        # TODO implement
        print("Exception Occurred: {0}".format(e.message))

    # ==================================================================================================================
    #   End HANDLERS definition
    # ==================================================================================================================

    # ==================================================================================================================
    #   Start UTILITY FUNCTIONS definition
    # ==================================================================================================================

    def connect(self, user, password, account, environment, ):
        """
        Implements Connection to ROFEX DMA Server.
        (demo = reMarkets; live = ROFEX)
        Initializes environment and WebSocket Connection
            :param user: User.
            :type user: Str.
            :param password: User Password.
            :type password: Str.
            :param account: Account provided by Market Authority.
            :type account: Str.
            :param environment: Market environment.
            :type environment: Str.
        """

        clear_screen()
        print(dash_line)
        message = f'Connecting to {environment} DMA server'
        print(message.center(width))
        print(dash_line)

        if logging.getLevelName('DEBUG') > 1:
            logging.debug(f'ROFEXClient: Starting connection to {environment} DMA Server ')
        try:
            # Initialize Environment
            pyRofex.initialize(user=user,
                               password=password,
                               account=account,
                               environment=environment)

            # Initialize WebSocket Cnnection with Handlers

            pyRofex.init_websocket_connection(market_data_handler=self.market_data_handler,
                                              error_handler=self.error_handler,
                                              exception_handler=self.exception_handler)
        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient ERROR: En exception occurred {e}')

            error_msg = "\033[0;30;47mERROR: INCORRECT CONNECTION PARAMETERS. Check log file " \
                        "for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)

    def disconnect(self):
        """
        Terminates Connection to ROFEX DMA Server.
        """

        if logging.getLevelName('DEBUG') > 1:
            logging.debug(f'ROFEXClient: Disconnecting.')

        print(dash_line)
        message = f'Terminating connection'
        print(message.center(width))
        print(dash_line)
        pyRofex.close_websocket_connection()
        sys.exit(0)

    def subscribe_instruments(self, args):
        """
        Subscribes to MarketData for specified products.
        (Each given Ticker HAS to be a str inside a one element list).
        :param args: List of tickers to subscribe to MarketData.
        :type args: list of list
        """

        entries = [pyRofex.MarketDataEntry.BIDS,
                   pyRofex.MarketDataEntry.OFFERS,
                   pyRofex.MarketDataEntry.LAST]
        try:
            for instrument in args:
                self.subscribed_instruments.append(instrument)

                if logging.getLevelName('DEBUG') > 1:
                    logging.debug(f'ROFEXClient: Subscribing to {instrument} MarketData')

                message = f'Subscribing to {instrument[0]} MarketData'
                print(message)

                pyRofex.market_data_subscription(tickers=instrument,
                                                 entries=entries)
        except Exception as e:

            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient ERROR: En exception occurred {e}')
            e = str(e).upper()
            error_msg = f"\033[0;30;47mERROR: {e} Check log file " \
                        "for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)

    def process_market_data_message(self, message):
        """
        Prints book and MarketData for each updated Ticker whenever the market changes.
        :param message: Message received. Comes as a JSON.
        :type message: Dict.
        """

        try:
            ticker = message.get('instrumentId').get('symbol')
            ticker_entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS]
            full_book = pyRofex.get_market_data(ticker, ticker_entries, depth=10)
            bid_book = full_book.get('marketData').get('BI')
            ask_book = full_book.get('marketData').get('OF')
            last_trade = [message.get('marketData').get('LA')]

            print(dash_line)
            print(f"----------- {ticker} - MarketData ".ljust(width, '-'))
            print("\nBID Book")
            print(pd.DataFrame().from_dict(bid_book))
            print("\nASK Book")
            print(pd.DataFrame().from_dict(ask_book))
            print("\nLAST Price")
            print(pd.DataFrame().from_dict(last_trade))

        except Exception:
            traceback.print_exc()
            pass

    # ==================================================================================================================
    #   End UTILITY FUNCTIONS definition
    # ==================================================================================================================


def clear_screen():
    """
    Used to clear screen buffer.
    Works in Windows and Unix systems
    """
    # for windows
    if name == 'nt':
        _ = system('cls')

    # for mac and linux(here, os.name is 'posix')
    else:
        _ = system('clear')


if __name__ == '__main__':
    rofex_client = ROFEXClient("fedejbrun5018", "ugklxY0*", "REM5018", "demo")
    rofex_client.subscribe_instruments([["GGALOct20"], ["GGALDic20"], ["DODic20"], ["DOOCt20"]])

    while True:
        try:
            for _ in range(1):
                pass
        except KeyboardInterrupt:
            rofex_client.disconnect()
