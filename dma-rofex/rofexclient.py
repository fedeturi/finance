import logging
import sys
import traceback
from os import system, name
import os
import time
from pprint import pprint
import pandas as pd
from threading import Lock
import datetime as dt

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
        self.user = user
        self.password = password
        self.account = account

        # Define environment
        env_param = environment.lower()
        environments = {
            'demo': pyRofex.Environment.REMARKET,
            'live': pyRofex.Environment.LIVE,
        }
        self.env = environments.get(env_param)

        # Create logging file
        # TODO Implementar validacion para Windows/Linux
        self.session_time = dt.datetime.now()
        filename = f'c:/logs/ROFEX{env_param}{self.session_time.year}-{self.session_time.month}-' \
                   f'{self.session_time.hour}_{self.session_time.second}.log'
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
            self.connect(self.user, self.password, self.account, self.env)
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

        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient EXCEPTION RECEIVED: {e.message}')

    def order_report_handler(self, message):
        """
        Handles OrderReport Messages received from server.
        :param message: Message received. Comes as a JSON.
        :type message: Dict.
        """
        # TODO implement
        self.process_order_report_message(message)

    def error_handler(self, message):
        """
        Handles Error Messages received from server.
        :param message: Message received. Comes as a JSON.
        :type message: Dict.
        """
        # TODO implement
        if logging.getLevelName('DEBUG') > 1:
            logging.debug(f'ROFEXClient ERROR: Error message received {message}')

        print(dash_line)
        error_msg = f"\033[0;30;47mERROR: \"{message.get('description')}\". \nCheck log file " \
                    "for detailed error message.\033[1;37;40m"
        print(error_msg)

    def exception_handler(self, e):
        """
        Handles Exception Messages received from server.
        :param message: Message received. Comes as a JSON.
        :type message: Dict.
        """
        print(dash_line)
        error_msg = f"\033[0;30;47mERROR: EXCEPTION OCCURED\nCheck log file " \
                    "for detailed error message.\033[1;37;40m"
        print(error_msg)

        if logging.getLevelName('DEBUG') > 1:
            logging.debug(f'ROFEXClient ECEPTION RECEIVED: {e.message}')

    # ==================================================================================================================
    #   End HANDLERS definition
    # ==================================================================================================================

    # ==================================================================================================================
    #   Start UTILITY FUNCTIONS definition
    # ==================================================================================================================

    def connect(self, user, password, account, environment ):
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
                                              exception_handler=self.exception_handler,
                                              order_report_handler=self.order_report_handler)

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

    def subscribe_order_report(self):
        """
        Subscribes to MarketData for specified products.
        (Each given Ticker HAS to be a str inside a one element list).
        :param args: List of tickers to subscribe to MarketData.
        :type args: list of list
        """
        # TODO implement
        try:
            print("Subscribe to OrderReport")
            pyRofex.order_report_subscription()
        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient ERROR: En exception occurred {e}')
            e = str(e).upper()
            error_msg = f"\033[0;30;47mERROR: {e} Check log file " \
                        "for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)
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
            pprint(message)
            ticker = message.get('instrumentId').get('symbol')
            ticker_entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS]
            full_book = pyRofex.get_market_data(ticker, ticker_entries, depth=10)

            if len(full_book.get('marketData').get('BI')) !=0\
                    and len(full_book.get('marketData').get('OF')) != 0:
                bid_book = full_book.get('marketData').get('BI')
                bid_book_df = pd.DataFrame().from_dict(bid_book)
                bid_book_df.columns = ['BP', 'BS']

                ask_book = full_book.get('marketData').get('OF')
                ask_book_df = pd.DataFrame().from_dict(ask_book)
                ask_book_df.columns = ['OP', 'OS']

                last_trade = [message.get('marketData').get('LA')]

                full_book_df = pd.concat([bid_book_df, ask_book_df], axis=1)
                full_book_df = full_book_df[['BS', 'BP', 'OP', 'OS']]
                full_book_df.fillna(0, inplace=True)
                lk.acquire()
                print(dash_line)
                msg_centered = f"   {ticker} - MarketData".ljust(width, ' ')
                md_header = f"\033[0;30;47m{msg_centered}\033[1;37;40m"
                print(md_header)
                print("   BID         ASK")
                print(full_book_df)
                print("\nTop:", full_book_df['BS'].iloc[0], full_book_df['BP'].iloc[0],
                      full_book_df['OP'].iloc[0], full_book_df['OS'].iloc[0])
                lk.release()

            else:
                empty_MD = pd.DataFrame.from_dict({'BS': [0], 'BP': [0],  'OP': [0], 'OS': [0]})
                lk.acquire()
                print(dash_line)
                print(f"----------- {ticker} - MarketData ".ljust(width, '-'))
                print("   BID              ASK")
                print(empty_MD)
                lk.release()

        except Exception:
            traceback.print_exc()
            pass

    def process_order_report_message(self, message):

        orderId = ""
        side = ""
        ticker = ""
        orderQty = ""
        price = ""
        status = ""

        try:
            orderId = message.get('orderReport').get('orderId')
        except Exception:
            pass

        try:
            side = message.get('orderReport').get('side')
        except Exception:
            pass

        try:
            ticker = message.get('orderReport').get('instrumentId').get('symbol')
        except Exception:
            pass

        try:
            orderQty = message.get('orderReport').get('orderQty')
        except Exception:
            pass

        try:
            price = message.get('orderReport').get('price')
        except Exception:
            pass

        try:
            status = message.get('orderReport').get('status')
        except Exception:
            pass

        lk.acquire()
        print(dash_line)
        msg_centered = f"   {ticker} - {status} Order".ljust(width, ' ')
        md_header = f"\033[0;30;47m{msg_centered}\033[1;37;40m"
        print(md_header)
        print(orderId, side, ticker, orderQty, price, status)
        lk.release()

    def placer_order(self, ticker, order_side, order_price, order_qty):
        # TODO implement
        # TODO Atento al parametro cancel_previous

        if order_side.lower() == 'buy':
            order_side = pyRofex.Side.BUY
        elif order_side.lower() == 'sell':
            order_side = pyRofex.Side.BUY
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
            active_orders.append(order)
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
            print(f"----------- Cancel Order Response: ".ljust(width, '-'))
            pprint(cancel_order)
            lk.release()

            order_status = pyRofex.get_order_status(ClOrdId)
            lk.acquire()
            print(dash_line)
            print(f"----------- Cancel Order Status Response: ".ljust(width, '-'))
            pprint(order_status)
            lk.release()

        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient ERROR: En exception occurred {e}')

            error_msg = "\033[0;30;47mERROR: Check log file for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)

    def cancel_all_orders(self, ClOrdId):
        # TODO implement
        pass

    def get_order_status(self):
        # TODO implement
        """
        order_status = pyRofex.get_order_status(order["order"]["clientId"])
        lk.acquire()
        print(dash_line)
        print(f"----------- Order Status Response: ".ljust(width, '-'))
        pprint(order_status)
        lk.release()
        """
        pass

    def get_all_order_status(self):
        # TODO implement
        try:
            all_orders_status = pyRofex.get_all_orders_status(self.account, self.env)
            session_orders = []

            for order in all_orders_status.get('orders'):
                # TODO filtrar por sesion
                parse_time = order.get('transactTime').split('.')[0]
                ord_time = dt.datetime(
                    int(parse_time[:4]),
                    int(parse_time[4:6]),
                    int(parse_time[6:8]),
                    int(parse_time[9:11]),
                    int(parse_time[12:14]),
                    int(parse_time[15:17])
                )

                if ord_time > self.session_time:
                    session_orders.append(order)

            lk.acquire()
            print(dash_line)
            print(f"----------- All Orders Status: ".ljust(width, '-'))
            pprint(session_orders)
            lk.release()

        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient ERROR: En exception occurred {e}')

            error_msg = "\033[0;30;47mERROR: Check log file for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)

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

    def get_market_price(self, ticker, side):
        # TODO implement

        if side == "buy":
            ticker_entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS]
            full_book = pyRofex.get_market_data(ticker, ticker_entries)
            market_price = float(full_book.get('marketData').get('OF')[0].get('price'))

        else:
            ticker_entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS]
            full_book = pyRofex.get_market_data(ticker, ticker_entries)
            market_price = float(full_book.get('marketData').get('BI')[0].get('price'))

        return market_price

    def get_quoting_price(self, price):
        # TODO implement
        quoting_price = price + 0.05
        return quoting_price

    def get_market_qty(self, ticker, side):
        # TODO implement

        if side == "buy":
            ticker_entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS]
            full_book = pyRofex.get_market_data(ticker, ticker_entries)
            market_qty = float(full_book.get('marketData').get('OF')[0].get('size'))

        else:
            ticker_entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS]
            full_book = pyRofex.get_market_data(ticker, ticker_entries)
            market_qty = float(full_book.get('marketData').get('BI')[0].get('size'))

        return market_qty

    def get_quoting_qty(self, qty):
        # TODO implement
        pass

    def build_market_order(self, ticker, side):
        price = rofex_client.get_market_price(ticker, side)
        quantity = rofex_client.get_market_qty(ticker, side)

        if side == "sell":
            rofex_client.placer_order(ticker, "sell", price + 0.5, quantity)
        else:
            rofex_client.placer_order(ticker, "buy", price - 0.5, quantity)

    def send_market_order(self, ticker, side, size, order_type):
        pyRofex.send_order(ticker=ticker, side=side, size=size, order_type=order_type)

    # ==================================================================================================================
    #   End UTILITY FUNCTIONS definition
    # ==================================================================================================================


class IncorrectOrderSide(Exception):
    pass


class Order:
    def __init__(self, ticker, side, price, qty):
        self.ticker = ticker
        self.side = side
        self.price = price
        self.qty = qty

    def get_order(self):
        return [self.ticker, self.side, self.price, self.qty]


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


def build_header():
    # TODO
    pass


if __name__ == '__main__':
    rofex_client = ROFEXClient("fedejbrun5018", "ugklxY0*", "REM5018", "demo")
    rofex_client.subscribe_instruments([["GGALOct20"], ["GGALDic20"], ["instrumento"]])
    rofex_client.subscribe_order_report()

    rofex_client.build_market_order("GGALOct20", "buy")

    """
    price = rofex_client.get_market_price("GGALOct20")
    quoting_price = rofex_client.get_quoting_price(price)
    quantity = rofex_client.get_market_qty("GGALOct20")
    order = build_market_order("GGALOct20", "sell")
    rofex_client.placer_order("GGALOct20", "sell", price + 0.5, quantity)

    price = rofex_client.get_market_price("GGALDic20")
    quantity = rofex_client.get_market_qty("GGALDic20")
    rofex_client.placer_order("GGALOct20", "buy", price - 0.5, quantity)

    price = rofex_client.get_market_price("GGALOct20")
    rofex_client.placer_order("GGALOct20", "buy", price - 0.5, quantity)

    rofex_client.get_all_order_status()

    time.sleep(6)
    lk.acquire()
    print(dash_line)
    print("CANCEL ORDERS")
    lk.release()
    for order in active_orders:
        time.sleep(3)
        cancel_orderId = order.get('order').get('clientId')
        rofex_client.cancel_order(cancel_orderId)"""

    while True:
        try:
            for _ in range(1):
                pass
        except KeyboardInterrupt:
            rofex_client.disconnect()
