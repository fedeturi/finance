from globals_ROFEX import account_rofex


class OrderRofex(object):
    """
    Order Object for ROFEX
    """
    def __init__(self, ticker, size, side, price, market='ROFX', account=account_rofex, cancel_previous=False,
                 ClOrdID=None, ord_type='LIMIT', timen_in_force='DAY'):

        self.symbol = ticker
        self.price = price
        self.orderQty = size
        self.side = side
        self.market = market
        self.account = account
        self.cancelPrevious = cancel_previous

        self.quoting = False
        self.status_order = None
        self.ClOrdID = ClOrdID
        self.proprietary = None
        self.transactTime = None
        self.avgPx = 0
        self.cumQty = 0
        self.text = None
        self.execId = None
        self.algo_belonging = None
        self.partial = False
        self.ordType = ord_type
        self.timeInForce = timen_in_force

    def __repr__(self):
        return f' {self.market} Order {self.ClOrdID}: {self.symbol} {self.side} {self.price} {self.orderQty}'

    def place(self, ClOrdID):
        """
        Set OrderID and ClOrdID
        """
        self.ClOrdID = ClOrdID
        self.quoting = True

    def change_price(self, price, ClOrdID):
        """
        Change Price
        """
        self.ClOrdID = ClOrdID
        self.price = price
        self.avgPx = 0
        self.cumQty = 0
        self.partial = False

    def change_qty(self, qty, ClOrdID):
        """
        Change Qty
        """
        self.ClOrdID = ClOrdID
        self.orderQty = qty
        self.avgPx = 0
        self.cumQty = 0
        self.partial = False

    def change(self, price, orderQty, ClOrdID):
        """
        Change Price and Qty
        """
        self.ClOrdID = ClOrdID
        self.orderQty = orderQty
        self.price = price
        self.avgPx = 0
        self.cumQty = 0
        self.partial = False

    def cancel(self):
        """
        Order Canceled
        """
        self.quoting = False
        self.partial = False

    def filled(self, avgPx, cumQty, status, transactTime, text, execId=None):
        """
        Order Filled
        """
        self.quoting = False
        self.avgPx = avgPx
        self.cumQty = cumQty
        self.execId = execId
        self.transactTime = transactTime
        self.status_order = status
        self.text = text
        self.partial = True

    def add_fill(self, price_fill, qty_fill):
        """
        Order Partially Filled
        """
        self.cumQty = self.cumQty + qty_fill
        self.avgPx = (self.avgPx * (self.cumQty - qty_fill) + qty_fill * price_fill) / self.cumQty
        self.orderQty = max(self.orderQty - qty_fill, 0)
        self.partial = True

    def is_initialized(self):
        """
        True if Order is Placed, False otherwise
        """
        return self.ClOrdID is not None

    def copy(self):
        """
        Copy Order Object. (Used in ConnectionHub for non quoting orders)
        """
        new_order = OrderRofex(self.symbol, self.orderQty, self.side, self.price, self.market, self.account,
                               self.cancelPrevious)
        new_order.partial = self.partial
        new_order.partial = False
        new_order.status_order = self.status_order
        new_order.ClOrdID = self.ClOrdID
        new_order.proprietary = self.proprietary
        new_order.transactTime = self.transactTime
        new_order.avgPx = self.avgPx
        new_order.cumQty = self.cumQty
        new_order.text = self.text
        new_order.execId = self.execId
        new_order.algo_belonging = self.algo_belonging
        return new_order
