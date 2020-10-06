import logging
import sys
import traceback
from os import system, name
import os
import time
from pprint import pprint
import pandas as pd
from threading import Lock

import pyRofex

width = os.get_terminal_size().columns
dash_line = "-" * width
active_orders = []
lk = Lock()


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

            error_msg = "\033[0;30;47mERROR: CONNECTION ERROR. Check log file " \
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

    def subscribe_order_report(self, args):
        """
        Subscribes to MarketData for specified products.
        (Each given Ticker HAS to be a str inside a one element list).
        :param args: List of tickers to subscribe to MarketData.
        :type args: list of list
        """
        # TODO implement
        pass

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
        """

    def process_market_data_message(self, message):
        """
        TODO Later this should evaluate every possible action triggerd by specific MarketData messages
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

            full_book_df = pd.concat([pd.DataFrame().from_dict(bid_book), pd.DataFrame().from_dict(ask_book)], axis=1)

            lk.acquire()
            print(dash_line)
            print(f"----------- {ticker} - MarketData ".ljust(width, '-'))
            print(full_book_df)
            lk.release()

        except Exception:
            traceback.print_exc()
            pass

    def placer_order(self, ticker, order_side, order_price, order_qty):
        # TODO implement
        # TODO Atento al parametro cancel_previous

        if order_side.lower() == 'buy':
            order_side = pyRofex.Side.BUY
        elif order_side.lower() == 'sell':
            order_side = pyRofex.side.BUY
        else:
            raise IncorrectOrderSide

        if logging.getLevelName('DEBUG') > 1:
            logging.debug(f'ROFEXClient: Sending {str(order_side).split(".")[-1]} order.')

        try:
            order = pyRofex.send_order(ticker=ticker,
                                       side=order_side,
                                       size=order_qty,
                                       price=order_price,
                                       order_type=pyRofex.OrderType.LIMIT)
            lk.acquire()
            print(dash_line)
            print("Send Order Response:")
            pprint(order)
            active_orders.append(order)
            lk.release()

            order_status = pyRofex.get_order_status(order["order"]["clientId"])
            lk.acquire()
            print(dash_line)
            print("Order Status Response:")
            pprint(order_status)
            lk.release()

        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient ERROR: En exception occurred {e}')

            error_msg = "\033[0;30;47mERROR: Check log file for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)


    def cancel_order(self, ClOrdId):
        # TODO implement

        if logging.getLevelName('DEBUG') > 1:
            logging.debug(f'ROFEXClient: Sending cancel message for order {ClOrdId}')

        try:
            cancel_order = pyRofex.cancel_order(ClOrdId)

            lk.acquire()
            print(dash_line)
            print("Cancel Order Response:")
            pprint(cancel_order)
            lk.release()

            order_status = pyRofex.get_order_status(ClOrdId)
            lk.acquire()
            print(dash_line)
            print("Cancel Order Status Response:")
            pprint(order_status)
            lk.release()

        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient ERROR: En exception occurred {e}')

            error_msg = "\033[0;30;47mERROR: Check log file for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)

    def get_order_status(self):
        # TODO implement
        pass

    def get_all_order_status(self):
        # TODO implement
        pass

    def get_market_data(self):
        # TODO implement
        pass

    def get_segments(self):
        # TODO implement
        pass

    def get_all_instruments(self):
        # TODO implement
        pass

    def get_detailed_instruments(self):
        # TODO implement
        pass

    def get_instrument_details(self):
        # TODO implement
        pass

    def get_trade_history(self):
        # TODO implement
        pass

    def get_market_price(self, ticker):
        # TODO implement
        ticker_entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS]
        full_book = pyRofex.get_market_data(ticker, ticker_entries)
        market_price = float(full_book.get('marketData').get('BI')[0].get('price'))
        return market_price

    def get_quoting_price(self, price):
        quoting_price = price + 0.05
        return quoting_price

    def get_market_qty(self, ticker):
        # TODO implement
        ticker_entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS]
        full_book = pyRofex.get_market_data(ticker, ticker_entries)
        market_qty = float(full_book.get('marketData').get('OF')[0].get('size'))
        return market_qty

    def get_quoting_qty(self, qty):
        # TODO implement
        pass

    # ==================================================================================================================
    #   End UTILITY FUNCTIONS definition
    # ==================================================================================================================


class IncorrectOrderSide(Exception):
    pass


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
    rofex_client.subscribe_instruments([["GGALOct20"], ["GGALDic20"]])

    price = rofex_client.get_market_price("GGALOct20")
    quoting_price = rofex_client.get_quoting_price(price)
    quantity = rofex_client.get_market_qty("GGALOct20")
    rofex_client.placer_order("GGALOct20", "buy", price, 1)

    time.sleep(4)
    lk.acquire()
    print(dash_line)
    print("CANCEL ORDER")
    lk.release()

    cancel_orderId = active_orders.pop().get('order').get('clientId')
    rofex_client.cancel_order(cancel_orderId)

    while True:
        try:
            for _ in range(1):
                pass
        except KeyboardInterrupt:
            rofex_client.disconnect()
