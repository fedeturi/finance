import datetime as dt
import logging
import os
import sys
import traceback
from os import system, name
from pprint import pprint
from threading import Lock

import pandas as pd
import pyRofex

width = os.get_terminal_size().columns
dash_line = "-" * width


class ROFEXClient:
    """Implements Client to connect to ROFEX DMA server.
    (demo = reMarkets; live = ROFEX)
    """
    def __init__(self, user, password, account, environment, verbose=3):
        """
        ROFEXClient Class Constructor

        :param user: User
        :type user: str
        :param password: User Password
        :type password: str
        :param account: Account provided by Market Authority
        :type account: str
        :param environment: Market environment
        :type environment: str
        """

        self.subscribed_products = []
        self.user = user
        self.password = password
        self.account = account
        self.active_orders = []
        self.lk = Lock()

        # Define environment
        env_param = environment.upper()
        environments = {
            'DEMO': pyRofex.Environment.REMARKET,
            'LIVE': pyRofex.Environment.LIVE,
        }
        self.env = environments.get(env_param)
        self.session_time = dt.datetime.now()

        create_log_file(self.session_time, env_param, verbose)

        try:
            self.connect(self.user, self.password, self.account, self.env)
        except KeyboardInterrupt:
            self.disconnect()

    # ==================================================================================================================
    #   Start HANDLERS definition
    # ==================================================================================================================
    def market_data_handler(self, md_message):
        """
        Handles MarketData Messages received from server.

        :param md_message: Message received. Comes as a JSON.
        :type md_message: dict
        """
        try:
            self.process_market_data_message(md_message)

        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient MDHandler Exception: {e.message}')

    def order_report_handler(self, or_message):
        """
        Handles OrderReport Messages received from server.

        :param or_message: Message received. Comes as a JSON.
        :type or_message: dict
        """
        # TODO implement
        try:
            self.process_order_report_message(or_message)

        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient ORHandler Exception: {e.message}')

    def error_handler(self, err_message):
        """
        Handles Error Messages received from server.

        :param err_message: Message received. Comes as a JSON.
        :type err_message: dict
        """
        # TODO implement
        if logging.getLevelName('DEBUG') > 1:
            logging.debug(f'ROFEXClient ERROR Handler: Received {err_message}')

        print(dash_line)
        error_msg = f"\033[0;30;47mERROR: \"{err_message.get('description')}\". \nCheck log file " \
                    "for detailed error exception_message.\033[1;37;40m"
        print(error_msg)

    def exception_handler(self, exc_message):
        """
        Handles Exception Messages received from server.

        :param exc_message: Message received. Comes as a JSON.
        :type exc_message: dict
        """
        # TODO evaluar mejor como manejar las posibles excepciones recibidas
        print(dash_line)
        error_msg = f"\033[0;30;47mERROR: EXCEPTION OCCURED\nCheck log file " \
                    "for detailed error message.\033[1;37;40m"
        print(error_msg)

        if logging.getLevelName('DEBUG') > 1:
            logging.debug(f'ROFEXClient EXCEPTION Handler: Received {exc_message.message}')

    # ==================================================================================================================
    #   End HANDLERS definition
    # ==================================================================================================================

    # ==================================================================================================================
    #   Start UTILITY FUNCTIONS definition
    # ==================================================================================================================

    def connect(self, user, password, account, environment):
        """
        Implements Connection to ROFEX DMA Server.
        (demo = reMarkets; live = ROFEX)
        Initializes environment and WebSocket Connection.

            :param user: User.
            :type user: str
            :param password: User Password.
            :type password: str
            :param account: Account provided by Market Authority.
            :type account: str
            :param environment: Market environment.
            :type environment: str
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
                logging.debug(f'ROFEXClient Connection ERROR: En exception occurred {e}')

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

    def subscribe_products(self, args):
        """
        Subscribes to MarketData for specified product/s.
        (Each given Ticker HAS to be a str inside a one element list).

        :param args: List of tickers to subscribe to MarketData.
        :type args: list of list
        """

        entries = [pyRofex.MarketDataEntry.BIDS,
                   pyRofex.MarketDataEntry.OFFERS,
                   pyRofex.MarketDataEntry.LAST]
        try:
            for instrument in args:
                self.subscribed_products.append(instrument)

                if logging.getLevelName('DEBUG') > 1:
                    logging.debug(f'ROFEXClient: Subscribing to {instrument} MarketData')

                message = f'Subscribing to {instrument[0]} MarketData'
                print(message)

                pyRofex.market_data_subscription(tickers=instrument,
                                                 entries=entries)
        except Exception as e:

            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient Product Subscription ERROR: En exception occurred {e}')
            e = str(e).upper()
            error_msg = f"\033[0;30;47mERROR: {e} Check log file " \
                        "for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)

    def subscribe_order_report(self):
        """
        Subscribes to OrderReport service (ExecutionReport in FIX) for
        this account.
        """
        # TODO evaluar si es necesario implementar Snapshot
        # TODO evaluar si es necesario para mas de una cuenta/environment
        # TODO evaluar si es necesario un valor de retorno
        try:
            print("Subscribe to OrderReport")
            pyRofex.order_report_subscription()

        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient Order Report Subscription ERROR: En exception occurred {e}')
            e = str(e).upper()
            error_msg = f"\033[0;30;47mERROR: {e} Check log file " \
                        "for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)

    def process_market_data_message(self, message):
        """
        Prints book and MarketData for each updated Ticker whenever the market changes.
        TODO Habria que sacar la impresion del OrderBook por la limitacion de Request que va a implementar ROFEX

        :param message: Message received. Comes as a JSON.
        :type message: dict
        """
        # TODO Later this should evaluate every possible action triggerd by specific MarketData messages

        try:
            pprint(message)

        except Exception:
            traceback.print_exc()
            pass

    def process_order_report_message(self, message):
        """
        Prints Order Report message.
        TODO Habria que disparar otras funciones de acuerdo a lo que pase

        :param message: Message received. Comes as a JSON.
        :type message: dict
        """
        # TODO Later this should evaluate every possible action triggerd by specific MarketData messages
        orderId = message.get('orderReport').get('orderId')
        side = message.get('orderReport').get('side')
        ticker = message.get('orderReport').get('instrumentId').get('symbol')
        orderQty = message.get('orderReport').get('orderQty')
        price = message.get('orderReport').get('price')
        status = message.get('orderReport').get('status')

        if logging.getLevelName('DEBUG') > 1:
            logging.debug(f'ROFEXClient: Received {str(status)} order.')

        self.lk.acquire()
        print(dash_line)
        msg_centered = f"   {ticker} - {status} Order".ljust(width, ' ')
        md_header = f"\033[0;30;47m{msg_centered}\033[1;37;40m"
        print(md_header)
        print(orderId, side, ticker, orderQty, price, status)
        pprint(message)
        self.lk.release()

    def place_order(self, ticker, side, price, qty):
        """
        Sends LIMIT order request to server and stores response in active_orders list.

        :param ticker: Product ticker.
        :type ticker: str
        :param side: Side for order.
        :type side: str
        :param price: Price for order.
        :type price: float
        :param qty: Quantity for order.
        :type qty: int
        """
        # TODO Atento al parametro cancel_previous

        if side.lower() == 'buy':
            side = pyRofex.Side.BUY
        elif side.lower() == 'sell':
            side = pyRofex.Side.SELL
        else:
            raise IncorrectOrderSide

        if logging.getLevelName('DEBUG') > 1:
            logging.debug(f'ROFEXClient: Sending {str(side).split(".")[-1]} order.')

        try:
            order = pyRofex.send_order(ticker=ticker,
                                       side=side,
                                       size=qty,
                                       price=price,
                                       order_type=pyRofex.OrderType.LIMIT)
            self.lk.acquire()
            # TODO append solamente si status es OK
            self.active_orders.append(order)
            self.lk.release()

        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient ERROR: En exception occurred {e}')

            error_msg = "\033[0;30;47mERROR: Check log file for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)

    def cancel_order(self, ClOrdId):
        """
        Sends CANCEL order request to server and stores response (?) in active_orders list.

        :param ClOrdId: ClOrdId for Order to be cancelled.
        :type ClOrdId: str
        """
        # TODO manejar mejor las responses de place_order y cancel_order
        # TODO seria interesante evaluar si vale la pena que place_order y cancel_order devuelvan algun valor

        if logging.getLevelName('DEBUG') > 1:
            logging.debug(f'ROFEXClient: Sending cancel message for order {ClOrdId}')

        try:
            cancel_order = pyRofex.cancel_order(ClOrdId)

            """
            self.lk.acquire()
            print(dash_line)
            print(f"----------- Cancel Order Response: ".ljust(width, '-'))
            pprint(cancel_order)
            self.lk.release()

            order_status = pyRofex.get_order_status(ClOrdId)
            self.lk.acquire()
            print(dash_line)
            print(f"----------- Cancel Order Status Response: ".ljust(width, '-'))
            pprint(order_status)
            self.lk.release()"""

        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient ERROR: En exception occurred {e}')

            error_msg = "\033[0;30;47mERROR: Check log file for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)

    def cancel_all_orders(self):
        """
        Cancels all active orders sending individual CANCEL order request to server and stores response (?)
        """
        # TODO manejar mejor la response
        # TODO seria interesante evaluar si vale la pena que devuelva algun valor

        while len(self.active_orders) > 0:
            try:
                cancel_order_id = self.active_orders.pop().get('order').get('clientId')
                self.cancel_order(cancel_order_id)

            except Exception as e:
                if logging.getLevelName('DEBUG') > 1:
                    logging.debug(f'ROFEXClient ERROR: En exception occurred {e}')

                error_msg = "\033[0;30;47mERROR: Check log file for detailed error message.\033[1;37;40m"
                print(error_msg)
                print(dash_line)

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

            self.lk.acquire()
            print(dash_line)
            print(f"----------- All Orders Status: ".ljust(width, '-'))
            pprint(session_orders)
            self.lk.release()

        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient ERROR: En exception occurred {e}')

            error_msg = "\033[0;30;47mERROR: Check log file for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)

    def get_market_data(self, ticker):
        """
        Returns MarketData as a :class:`pandas.DataFrame` with full Book for specified ticker.

        :param ticker: Ticker
        :type ticker: str
        :return: full_book
        :rtype: :class:`pandas.DataFrame`
        """

        ticker_entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS]

        try:
            full_book = pyRofex.get_market_data(ticker, ticker_entries, depth=10)

            bid_book = full_book.get('marketData').get('BI')
            bid_book_df = pd.DataFrame().from_dict(bid_book)
            bid_book_df.columns = ['BP', 'BS']

            ask_book = full_book.get('marketData').get('OF')
            ask_book_df = pd.DataFrame().from_dict(ask_book)
            ask_book_df.columns = ['OP', 'OS']

            full_book_df = pd.concat([bid_book_df, ask_book_df], axis=1)
            full_book_df = full_book_df[['BS', 'BP', 'OP', 'OS']]
            full_book_df.fillna(0, inplace=True)

            self.lk.acquire()
            print(dash_line)
            msg_centered = f"   {ticker} - MarketData".ljust(width, ' ')
            md_header = f"\033[0;30;47m{msg_centered}\033[1;37;40m"
            print(md_header)
            print("   BID         ASK")
            print(full_book_df)
            print("\nTop:", full_book_df['BS'].iloc[0], full_book_df['BP'].iloc[0],
                  full_book_df['OP'].iloc[0], full_book_df['OS'].iloc[0])
            self.lk.release()

        except Exception as e:
            if logging.getLevelName('DEBUG') > 1:
                logging.debug(f'ROFEXClient Get Market Data ERROR: En exception occurred {e}')

            error_msg = "\033[0;30;47mERROR: Check log file " \
                        "for detailed error message.\033[1;37;40m"
            print(error_msg)
            print(dash_line)

    def get_segments(self):
        # TODO implement
        pass

    def get_all_instruments(self):
        # TODO implement and add pydoc and try except
        instruments_list = pyRofex.get_all_instruments()
        return instruments_list

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
        """
        Returns MarketPrice for specified Ticker and Side.

        :param ticker: Ticker.
        :type ticker: str
        :param side: Side.
        :type side: str
        :returns: MarketPrice.
        :rtype: float
        """

        if side.lower() == "buy":
            ticker_entries = [pyRofex.MarketDataEntry.OFFERS]
            offer_book = pyRofex.get_market_data(ticker, ticker_entries)
            market_price = float(offer_book.get('marketData').get('OF')[0].get('price'))

        else:
            ticker_entries = [pyRofex.MarketDataEntry.BIDS]
            bid_book = pyRofex.get_market_data(ticker, ticker_entries)
            market_price = float(bid_book.get('marketData').get('BI')[0].get('price'))

        return market_price

    def get_market_qty(self, ticker, side):
        """
        Returns MarketQty for specified Ticker and Side.

        :param ticker: Ticker.
        :type ticker: str
        :param side: Side.
        :type side: str
        :returns: MarketQty.
        :rtype: int
        """

        if side.lower() == "buy":
            ticker_entries = [pyRofex.MarketDataEntry.OFFERS]
            offer_book = pyRofex.get_market_data(ticker, ticker_entries)
            market_qty = int(offer_book.get('marketData').get('OF')[0].get('size'))

        else:
            ticker_entries = [pyRofex.MarketDataEntry.BIDS]
            bid_book = pyRofex.get_market_data(ticker, ticker_entries)
            market_qty = int(bid_book.get('marketData').get('BI')[0].get('size'))

        return market_qty

    def build_market_order(self, ticker, side, qty):
        """
        Builds and sends LIMIT order checking market price and qty.

        :param ticker: Ticker.
        :type ticker: str
        :param side: Side.
        :type side: str
        :param qty: Qty.
        :type qty: int
        :returns: LeavesQty > 0 if not enough Market Liquidity.
        :rtype: int
        """

        if side.lower() == "sell":
            price = self.get_market_price(ticker, side)
            mkt_qty = self.get_market_qty(ticker, side)

            if mkt_qty >= qty:
                self.place_order(ticker, "sell", price, qty)
                leaves_qty = 0
            else:
                self.place_order(ticker, "sell", price, mkt_qty)
                leaves_qty = qty - mkt_qty
        else:
            price = self.get_market_price(ticker, side)
            mkt_qty = self.get_market_qty(ticker, side)

            if mkt_qty >= qty:
                self.place_order(ticker, "buy", price, qty)
                leaves_qty = 0
            else:
                self.place_order(ticker, "buy", price, mkt_qty)
                leaves_qty = qty - mkt_qty

        return leaves_qty

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


def create_log_file(session_time, env_param, verbose):
    """
    Used to create log file.
    Works in Windows and Unix systems
    """
    # for windows
    if name == 'nt':

        filename = f'c:/logs/ROFEX{env_param}-{session_time.year}{session_time.month}' \
                   f'{session_time.day}{session_time.hour}{session_time.minute}{session_time.second}.log'
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

    # for mac and linux(here, os.name is 'posix')
    else:

        filename = f'./logs/ROFEX{env_param}-{session_time.year}{session_time.month}' \
                   f'{session_time.day}{session_time.hour}{session_time.minute}{session_time.second}.log'
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