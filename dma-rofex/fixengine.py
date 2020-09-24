"""
Custom implementation of a FIX engine.

CLASSES:
FIXEngine --------------------------------- Implementation of a FIX Engine.

    FIELDS:
    protocol_type_head -------------------- FIX message header. Always '8=FIXT.1.1|'.

    METHODS:
    bid_ask_data_request_msg()------------- Request snapshot and updates of market depth.
    cancel_order_msg() -------------------- Cancel specific Order.
    change_order_msg() -------------------- Change Order request.
    extract_avgPx() ----------------------- Return calculated average price of all fills on this Order.
    extract_ClOrdID() --------------------- Return unique OrderID for a day anf client.
    extract_CumQty() ---------------------- Return total quantity (e.g. number of shares) filled.
    extract_ext_ClOrdID() ----------------- Return unique ClOrdId of the previous Order (NOT the initial Order)
    --------------------------------------- of the day), used to identify previous Order in cancel/replace requests.
    extract_leavesQty() ------------------- Return quantity open for further execution.
    extract_market_changes() -------------- Return market changes for bid and ask books.
    extract_market_segments() ------------- Return market segment.
    extract_msg_tye() --------------------- Return FIX message type.
    extract_OrderID() --------------------- Return unique OrderID assigned by sell-side.
    extract_price_fill() ------------------ Return price of this last fill.
    extract_qty_fill() -------------------- Return quantity bought/sold on this last fill.
    extract_sec_number() ------------------ Return Integer message sequence number.
    extract_symbol() ---------------------- Return Ticker Symbol.
    extract_symbol_tick() ----------------- Return Ticker Symbol.
    header_msg() -------------------------- Build FIX message header.
    list_request_msg() -------------------- Request snapshots and updates of all securities subscribed.
    log_out() ----------------------------- Log out from server.
    logon() ------------------------------- Logon to server.
    msg_Heartbeat() ----------------------- Send heartbeat.
    msg_TestRequest() --------------------- Add identifier included in Test Request message to be returned in
    --------------------------------------- resulting Heartbeat. ('TestReqIDtemp')
    order_mass_cancel_request_msg() ------- Return OrderMassCancelRequest message
    order_status_msg() -------------------- Status of specific Order.
    place_order_msg() --------------------- Place new Order.
    ResendRequest() ----------------------- ResendRequest of an incomplete message
    symbol_right() ------------------------ Return True if symbol ticker belongs to market list, False otherwise.
    trade_report_msg() -------------------- Request trading report of fills and orders.
    unsubscribe_bid_ask_data_request_msg()- Unsubscribe snapshot and updates request.
"""

import re
from datetime import datetime

import simplefix

from global_queue import *


class FIXEngine(object):
    """
    Class implementing FIX engine.

    Used to build and parse FIX messages.
    """

    def __init__(self):
        self.protocol_type_head = b'8=' + str.encode(protocol_type) + b'\x01'

    def header_msg(self, tag):
        """
        Return standard FIX message header with MsgType = tag.
        """

        # Build msg seq_nu from all messages sent to server.
        global seq_num

        msg = simplefix.FixMessage()
        seq_num[0] += 1
        sendingTime = datetime.now().strftime('%Y%m%d-%H:%M:%S.%f')[:-3]

        msg.append_pair(8, protocol_type)      # Protocol Type
        msg.append_pair(35, tag)               # MsgType
        msg.append_pair(34, seq_num[0])        # MsgSeqNum
        msg.append_pair(49, "fedejbrun5018")        # SenderCompID
        msg.append_pair(52, sendingTime)       # SendingTime
        msg.append_pair(56, "ROFX")   # TargetCompID
        msg.append_pair(115, User[0])          # OnBehalfOfCompID
        msg.append_pair(128, xchange_name[0])  # DeliverToCompID
        return msg

    def logon(self):
        """
        Return LogOn message.
        """
        msg = self.header_msg("A")
        msg.append_pair(98, "0")                # EncryptMethod: None / Other
        msg.append_pair(108, "60")              # HeartBtInt: 60 seconds
        msg.append_pair(553, "fedejbrun5018")           # Username
        msg.append_pair(554, "ugklxY0*")     # Password
        msg.append_pair(1137, "9")              # DefaultApplVerID
        return msg

    def log_out(self):
        """
        Return LogOut message.
        """
        msg = self.header_msg("5")
        return msg

    def msg_Heartbeat(self):
        """
        Return HeartBeat message.
        """
        return self.header_msg("0")

    def msg_TestReq(self):
        """
        Return TestReqID message.
        """
        global TestReqID

        TestReqID[0] += 1
        TestReqIDtemp = TestReqID[0]

        msg = self.header_msg("1")  # MsgType                               (35) = (1) TestRequest
        msg.append_pair(112, TestReqIDtemp)  # TestRequestID               (112) = TestRequestIDtemp
        return msg

    def list_request_msg(self):
        """
        Return SecurityListRequest message.
        """
        msg = self.header_msg("x")  # MsgType                               (35) = (x) SecurityListRequest
        msg.append_pair(263, "1")  # SubscriptionRequestType               (263) = (1) Snapshot + Updates (Subscribe)
        msg.append_pair(320, "01")  # SecurityReqID                        (320) = (01)
        msg.append_pair(559, "4")  # SecurityListRequestType               (559) = (4) All Securities
        return msg

    def bid_ask_data_request_msg(self, prod, max_depth=2):
        """
        Return MarketDataRequest Subscribe message and MDReqIDtemp.
        """
        global MDReqID

        MDReqID[0] += 1
        MDReqIDtemp = MDReqID[0]

        msg = self.header_msg("V")
        msg.append_pair(262, MDReqIDtemp)  # MDReqID                       (262) = MDReqIDtemp
        msg.append_pair(263, "1")  # SubscriptionRequestType               (263) = (1) Snapshot + Updates (Subscribe)
        msg.append_pair(264, str(max_depth))  # MarketDepth                (264) = (2) Book depth (default)
        msg.append_pair(265, "0")  # MDUpdateType                          (265) = (0) Full refresh TODO revisar
        msg.append_pair(266, "Y")  # AggregatedBook                        (266) = (Y) Book entries to be aggregated
        msg.append_pair(7118, "D")  # TODO no se de donde salió esto
        msg.append_pair(146, "1")  # NoRelatedSym                          (146) = (1) N° of repeating symbols specified
        msg.append_pair(55, prod)  # Symbol                                 (55) = (prod) Ticker symbol
        msg.append_pair(267, "2")  # NoMDEntryTypes                        (267) = (2) N° of MDEntryType (269) requested
        msg.append_pair(269, "0")  # MDEntryType                           (269) = (0) Bid
        msg.append_pair(269, "1")  # MDEntryType                           (269) = (1) Offer

        return msg, MDReqIDtemp

    def order_mass_cancel_request_msg(self):
        """
        Return OrderMassCancelRequest message.
        """
        global ClOrdID

        lk.acquire()
        ClOrdID[0] += 1
        ClOrdID_temp = ClOrdID[0]
        lk.release()

        msg = self.header_msg("q")
        msg.append_pair(11, ClOrdID_temp)  # ClOrdID                        (11) = ClOrdID_temp
        msg.append_pair(530, "7")  # MassCancelRequestType                 (530) = (7) Cancel all orders
        msg.append_pair(1300, "DDF")  # MarketSegmentID                   (1300) = (DDF) Market segment
        msg.append_pair(60, msg.get(52))  # TransactTime                    (60) = Transaction Time
        return msg, ClOrdID_temp

    def order_status_msg(self, OrderID, prod, side):
        """
        Return OrderStatusRequest message.
        """
        msg = self.header_msg("H")
        msg.append_pair(37, OrderID)  # OrderID                             (37) = OrderID
        msg.append_pair(54, side)  # Side                                   (54) = Side
        msg.append_pair(55, prod)  # Symbol                                 (55) = Ticker symbol
        msg.append_pair(453, 1)  # NoPartyIDs                              (453) = Number of PartyID
        msg.append_pair(448, PartyID[0])  # PartyID                        (448) = PartyID
        msg.append_pair(447, "D")  # PartyIDSource                         (447) = (D) Proprietary / Custom code
        msg.append_pair(452, 11)  # PartyRole                              (452) = (11) Order Origination Trader
        return msg

    def place_order_msg(self, prod, price, qty, side, ClOrdID_temp,account):
        """
        Return NewOrderSingle message.
        """
        msg = self.header_msg("D")
        msg.append_pair(1, account)  # Account                               (1) = account
        msg.append_pair(11, ClOrdID_temp)  # ClOrdID                        (11) = ClOrdID_temp
        msg.append_pair(38, qty)  # OrderQty                                (38) = Quantity ordered
        msg.append_pair(40, 2)  # OrdType                                   (40) = (2) Limit
        msg.append_pair(44, price)  # Price                                 (44) = Price per unit of quantity
        msg.append_pair(54, side)  # Side                                   (54) = Side
        msg.append_pair(55, prod)  # Symbol                                 (55) = Ticker symbol
        msg.append_pair(60, msg.get(52))  # TransactTime                    (60) = Transaction Time
        return msg

    def cancel_order_msg(self, prod, qty, side, OrigClOrdID, ClOrdID_temp, account):
        """
        Return OrderCancelRequest message.
        """
        msg = self.header_msg("F")
        msg.append_pair(1, account)  # Account                               (1) = account
        msg.append_pair(11, ClOrdID_temp)  # ClOrdID                        (11) = ClOrdID_temp_temp)
        msg.append_pair(38, qty)  # OrderQty                                (38) = Quantity ordered
        msg.append_pair(41, OrigClOrdID)  # OrigClOrdID                     (41) = ClOrdID (11) of the previous order
        msg.append_pair(54, side)  # Side                                   (54) = Side
        msg.append_pair(55, prod)  # Symbol                                 (55) = Ticker symbol
        msg.append_pair(60, msg.get(52))  # TransactTime                    (60) = Transaction Time
        msg.append_pair(453, 1)  # NoPartyIDs                              (453) = Number of PartyID
        msg.append_pair(448, PartyID[0])  # PartyID                        (448) = PartyID
        msg.append_pair(447, "D")  # PartyIDSource                         (447) = (D) Proprietary / Custom code
        msg.append_pair(452, 11)  # PartyRole                              (452) = (11) Order Origination Trader
        return msg

    def change_order_msg(self, prod, price, qty, side, OrigClOrdID, ClOrdID_temp, account):
        """
        Return OrderCancelReplaceRequest message.
        """
        msg = self.header_msg("G")
        msg.append_pair(1, account)  # Account                               (1) = account
        msg.append_pair(11, ClOrdID_temp)  # ClOrdID                        (11) = ClOrdID_temp_temp)_temp)
        msg.append_pair(38, qty)  # OrderQty                                (38) = Quantity ordered
        msg.append_pair(40, 2)  # OrdType                                   (40) = (2) Limit
        msg.append_pair(41, OrigClOrdID)  # OrigClOrdID                     (41) = ClOrdID (11) of the previous order
        msg.append_pair(44, price)  # Price                                 (44) = Price per unit of quantity
        msg.append_pair(54, side)  # Side                                   (54) = Side
        msg.append_pair(55, prod)  # Symbol                                 (55) = Ticker symbol
        msg.append_pair(60, msg.get(52))  # TransactTime                    (60) = Transaction Time
        msg.append_pair(453, 1)  # NoPartyIDs                              (453) = Number of PartyID
        msg.append_pair(448, PartyID[0])  # PartyID                        (448) = PartyIDrtyID[0])
        msg.append_pair(447, "D")  # PartyIDSource                         (447) = (D) Proprietary / Custom code
        msg.append_pair(452, 11)  # PartyRole                              (452) = (11) Order Origination Trader
        return msg

    def ResendRequest(self, BeginSeqNo, EndSeqNo):
        """
        Return ResendRequest message.
        """
        msg = self.header_msg("2")
        msg.append_pair(7, BeginSeqNo)  # BeginSeqNo                         (7) = BeginSeqNo
        msg.append_pair(16, EndSeqNo)  # PartyRole                         (452) = EndSeqNo
        return msg

    def unsubscribe_bid_ask_data_request_msg(self, prod, MDReqID, max_depth=5):
        """
        Return MarketDataRequest Unsubscribe message .
        """
        msg = self.header_msg("V")
        msg.append_pair(262, MDReqID)  # MDReqID                           (262) = MDReqIDtemp
        msg.append_pair(263, "2")  # SubscriptionRequestType               (263) = (2) Disable previous Snapshot + Update Request (Unsubscribe)
        msg.append_pair(264, str(max_depth))  # MarketDepth                (264) = (5) Book depth (default)
        msg.append_pair(265, "0")  # MDUpdateType                          (265) = (0) Full refresh # TODO revisar
        msg.append_pair(266, "Y")  # AggregatedBook                        (266) = (Y) Book entries to be aggregated
        msg.append_pair(7118, "D")  # TODO no se de donde salió esto
        msg.append_pair(146, "1")  # NoRelatedSym                          (146) = (1) N° of repeating symbols specified
        msg.append_pair(55, prod)  # Symbol                                 (55) = (prod) Ticker symbol
        msg.append_pair(267, "2")  # NoMDEntryTypes                        (267) = (2) N° of MDEntryType (269) requested
        msg.append_pair(269, "0")  # MDEntryType                           (269) = (0) Bid
        msg.append_pair(269, "1")  # MDEntryType                           (269) = (1) Offer
        return msg

    def trade_report_msg(self):
        """
        Return TradeCaptureReportRequest message.
        """
        global TradeRequestID
        TradeRequestID[0] += 1

        msg = self.header_msg("AD")
        msg.append_pair(568, TradeRequestID[0])  # TradeRequestID          (568) = TradeRequestID
        msg.append_pair(569, 1)  # TradeRequestType                        (569) = (1) Matched trades matching criteria provided on request
        msg.append_pair(828, 0)  # TrdType                                 (828) = (0) Regular Trade
        msg.append_pair(830, 'AccountDetail')  # TransferReason            (830) = Reason trade is being transferred
        msg.append_pair(453, 1)  # NoPartyIDs                              (453) = Number of PartyID
        msg.append_pair(448, PartyID[0])  # PartyID                        (448) = PartyIDrtyID[0])
        msg.append_pair(447, "D")  # PartyIDSource                         (447) = (D) Proprietary / Custom code
        msg.append_pair(452, 24)  # PartyRole                              (452) = (24) Customer Account
        return msg

    def extract_symbol(self, msg):
        """
        Return Ticker Symbol, extracted from FIX message.
        """
        return msg.get(55).decode("cp1252")

    def extract_symbol_tick(self, symbol, list_market):
        """
        Return Tick for a Ticker Symbol from market.
        """
        symbol_tick = float(re.search(re.compile('.*\x01107=' + symbol + '\x01969=(.+?)\x01.*'), list_market).group(1))
        return symbol_tick

    def symbol_right(self, symbol, list_market):
        """
        Return True if Ticker Symbol exists in market, False otherwise.
        """
        symbol_right = symbol in re.findall(r'\x01107=(.*?)\x01', list_market)
        return symbol_right

    def extract_OrderID(self, msg):
        """
        Return OrderID, extracted from FIX message.
        """
        return msg.get(37).decode("cp1252")

    def extract_market_segment(self, msg):
        """
        Return Market Segment, extracted from FIX message.
        """
        return msg.get(1300).decode("cp1252")

    def extract_CumQty(self, msg):
        """
        Return CumQty (Total quantity filled), extracted from FIX message.
        """
        return int(float(msg.get(14)))

    def extract_avgPx(self, msg):
        """
        Return AvgPx (Calculated average price of all fills on this order), extracted from FIX message.
        """
        return float(msg.get(6))

    def extract_ClOrdID(self, msg):
        """
        Return ClOrdID, extracted from FIX message.
        """
        try:
            return int(msg.get(11).decode("cp1252"))
        except:
            return msg.get(11).decode("cp1252")

    def extract_ext_ClOrdID(self, msg):
        """
        Return OrigClOrdID (used to identify the previous order in cancel
        and cancel/replace requests), extracted from FIX message.
        """
        try:
            return int(msg.get(41).decode("cp1252"))
        except:
            return msg.get(41).decode("cp1252")

    def extract_leavesQty(self, msg):
        """
        Return LeavesQty (Quantity open for further execution), extracted from FIX message.
        """
        return int(float(msg.get(151)))

    def extract_qty_fill(self, msg):
        """
        Return LastQty (Quantity bought/sold on last fill), extracted from FIX message.
        """
        return int(float(msg.get(32)))

    def extract_price_fill(self, msg):
        """
        Return LastPx, extracted from FIX message.
        """
        return float(msg.get(31))

    def extract_sec_numb(self, msg):
        """
        Return MsgSeqNum, extracted from FIX message.
        """
        return int(msg.get(34))

    def extract_msg_type(self, msg):
        """
        Return MsgType, extracted from FIX message.
        AD = TradeCaptureReportRequest
        y = SecurtityList
        9 = ERROR_order
        3 = ERROR_msj
        0 = Heartbeat
        W = MarketDataSnapshotFullRefresh
        or ExecutionReport
        """
        msg_type = ''

        if msg.get(35) == b'AD':
            msg_type = 'TradeCaptureReportRequest'

        elif msg.get(35) == b'y':
            msg_type = 'SecurtityList'

        elif msg.get(35) == b'9':
            msg_type = 'ERROR_order'

        elif msg.get(35) == b'3':
            msg_type = 'ERROR_msj'

        elif msg.get(35) == b'0':
            msg_type = 'Heartbeat'

        elif msg.get(35) == b'W':
            msg_type = 'MarketDataSnapshotFullRefresh'

        else:
            try:
                if b'Operada' in msg.get(58):
                    msg_type = 'ExecutionReport'
            except:
                pass

        return msg_type

    def extract_market_changes(self, msg):
        """
        Return bid_book and ask_book (max_depth=5) provided by server in FIX message.
        """
        mqt_data_list = msg.encode().decode("cp1252").split('\x01269=')
        bid_book = []
        ask_book = []

        if len(mqt_data_list) > 1:

            for msg_parse in mqt_data_list[1:]:

                if msg_parse[0] == '0':
                    data = re.match(r'.*\x01270=(.+?)\x01271=(.+?)\x01.*', msg_parse)

                    if data:
                        bid_book.append(data.group(1, 2))

                if msg_parse[0] == '1':
                    data = re.match(r'.*\x01270=(.+?)\x01271=(.+?)\x01.*', msg_parse)

                    if data:
                        ask_book.append(data.group(1, 2))

        bid_book.sort(key=lambda tup: float(tup[0]), reverse=True)
        ask_book.sort(key=lambda tup: float(tup[0]))

        if len(bid_book) > 5:
            bid_book = bid_book[:5]

        if len(ask_book) > 5:
            ask_book = ask_book[:5]

        return bid_book, ask_book
