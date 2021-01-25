import json
import re
from datetime import datetime

import pytz
import simplefix

from fixengine import FIXEngine
from global_queue import *


class FIXEngineBYMA(FIXEngine):
    """
    Class implementing FIX engine for BYMA.

    Used to build and parse FIX messages.
    """

    def __init__(self):
        FIXEngine.__init__(self)
        self.OwnedAccounts = []

        with open(owned_accounts) as f:
            self.OwnedAccounts = json.load(f)['Own']

    def header_msg(self, tag, contract_type=None):
        """
        Return standard FIX message header with MsgType = tag.
        """

        # Build msg seq_nu from all messages sent to server.
        global seq_num

        msg = simplefix.FixMessage()
        seq_num[0] += 1
        sending_time = datetime.now(pytz.utc).strftime('%Y%m%d-%H:%M:%S.%f')[:-3]

        msg.append_pair(8, protocol_type)                   # Protocol Type
        msg.append_pair(35, tag)                            # MsgType
        msg.append_pair(34, seq_num[0])                     # MsgSeqNum
        msg.append_pair(49, API_KEY[0])                     # SenderCompID
        msg.append_pair(52, sending_time)                   # SendingTime
        msg.append_pair(56, TargetCompID[0])                # TargetCompID

        if contract_type:
            msg.append_pair(128, xchange_name_Futures[0])   # DeliverToCompID
        else:
            msg.append_pair(128, xchange_name[0])           # DeliverToCompID

        return msg

    def logon(self):
        """
        Return LogOn message.
        """

        msg = self.header_msg("A")
        msg.append_pair(98, "0")                            # EncryptMethod: None / Other
        msg.append_pair(108, "60")                          # HeartBtInt: 60 seconds
        msg.append_pair(553, User[0])                       # Username
        msg.append_pair(554, PASSPHRASE[0])                 # Password
        msg.append_pair(1137, "7")                          # DefaultApplVerID
        return msg

    def list_request_msg(self):
        """
        Return SecurityListRequest message.
        """
        global MDReqID

        MDReqID[0] += 1
        MDReqIDtemp = MDReqID[0]

        msg = self.header_msg("x")  # MsgType                               (35) = (x) SecurityListRequest
        msg.append_pair(263, "0")  # SubscriptionRequestType               (263) = (0) Snapshot
        msg.append_pair(320, MDReqIDtemp)  # SecurityReqID                 (320) = (01)
        msg.append_pair(559, "4")  # SecurityListRequestType               (559) = (4) All Securities
        return msg


    def bid_ask_data_request_msg(self, prod, max_deph=2):
        """
        Return MarketDataRequest Subscribe message and MDReqIDtemp.
        """
        global MDReqID

        MDReqID[0] += 1
        MDReqIDtemp = MDReqID[0]
        SecurityID_group = prod.split('-')

        msg = self.header_msg("V")
        msg.append_pair(262, MDReqIDtemp)  # MDReqID                       (262) = MDReqIDtemp
        msg.append_pair(263, "0")  # SubscriptionRequestType               (263) = (0) Snapshot
        msg.append_pair(264, str(max_deph))  # MarketDepth                 (264) = (2) Book depth (default)
        msg.append_pair(265, "1")  # MDUpdateType                          (265) = (1) Incremental Refresh TODO revisar
        msg.append_pair(266, "Y")  # AggregatedBook                        (266) = (Y) Book entries to be aggregated
        msg.append_pair(267, "2")  # NoMDEntryTypes                        (267) = (2) N° of MDEntryType (269) requested
        msg.append_pair(269, "0")  # MDEntryType                           (269) = (0) Bid
        msg.append_pair(269, "1")  # MDEntryType                           (269) = (1) Offer
        msg.append_pair(146, "1")  # NoRelatedSym                          (146) = (1) N° of repeating symbols specified
        msg.append_pair(55, SecurityID_group[0])  # Symbol                  (55) = Ticker symbol
        msg.append_pair(48, prod)  # SecurityID                             (48) = (prod) Security identifier value
        msg.append_pair(167, "CS")  # SecurityType                         (167) = (CS) Common Stock
        msg.append_pair(63, int(SecurityID_group[1]))  # SettlType          (63) = 0001/0002/0003
        msg.append_pair(15, SecurityID_group[-1])  # Currency               (15) = ARS/USD

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
        msg.append_pair(1461, "1")  # NoTargetPartyIDs                    (1461) = (1) N° of target parties identified in a mass action
        msg.append_pair(1462, PartyID[0])  # TargetPartyID                (1462) = PartyID[0]
        msg.append_pair(1463, "D")  # TargetPartyIDSource                 (1463) = (D) PartyIDSource value
        msg.append_pair(1464, "53")  # TargetPartyRole                    (1464) = (53) PartyRole value
        msg.append_pair(60, msg.get(52))  # TransactTime                    (60) = Transaction Time
        return msg, ClOrdID_temp

    def place_order_msg(self, prod, price, qty, side, ClOrdID_temp, account):
        """
        Return OrderMassCancelRequest message.
        """
        SecurityID_group = prod.split('-')

        msg = self.header_msg("D")
        msg.append_pair(1, account)  # Account                               (1) = account
        msg.append_pair(11, ClOrdID_temp)  # ClOrdID                        (11) = ClOrdID_temp
        msg.append_pair(15, SecurityID_group[-1])  # Currency               (15) = ARS/USD
        msg.append_pair(38, qty)  # OrderQty                                (38) = Quantity ordered
        msg.append_pair(40, 2)  # OrdType                                   (40) = (2) Limit
        msg.append_pair(44, price)  # Price                                 (44) = Price per unit of quantity
        msg.append_pair(54, side)  # Side                                   (54) = Side
        msg.append_pair(55, SecurityID_group[0])  # Symbol                  (55) = Ticker symbol
        msg.append_pair(59, 0)  # TimeInForce                               (59) = (0) Day
        msg.append_pair(60, msg.get(52))  # TransactTime                    (60) = Transaction Time
        msg.append_pair(63, int(SecurityID_group[1]))  # SettlType          (63) = 0001/0002/0003
        msg.append_pair(167, "CS")  # SecurityType                         (167) = Common Stock

        # Set OrderCapacity if account Owned or Related to GDP
        if str(account) in self.OwnedAccounts:
            msg.append_pair(528, "P")  # OrderCapacity                     (582) = (P) Principal/Proprietary
        # Set OrderCapacity if account not Owned nor Related to GDP
        else:
            msg.append_pair(528, "A")  # OrderCapacity                     (528) = (A) Agency

        msg.append_pair(1138, qty)  # DisplayQty                          (1138) = qty
        msg.append_pair(29501, 1)  # TradeFlag                           (29501) = (1) None
        msg.append_pair(453, 1)  # NoPartyIDs                              (453) = Number of PartyID
        msg.append_pair(448, PartyID[0])  # PartyID                        (448) = PartyID
        msg.append_pair(447, "D")  # PartyIDSource                         (447) = (D) Proprietary / Custom code
        msg.append_pair(452, 53)  # PartyRole                              (452) = (53) Trader mnemonic
        return msg

    def cancel_order_msg(self, prod, qty, side, OrigClOrdID, ClOrdID_temp, account='1'):
        """
        Return OrderCancelRequest message.
        """
        SecurityID_group = prod.split('-')

        msg = self.header_msg("F")
        msg.append_pair(11, ClOrdID_temp)  # ClOrdID                        (11) = ClOrdID_temp_temp)
        msg.append_pair(41, OrigClOrdID)  # OrigClOrdID                     (41) = ClOrdID (11) of the previous order
        msg.append_pair(15, SecurityID_group[-1])  # Currency               (15) = ARS/USD
        msg.append_pair(55, SecurityID_group[0])  # Symbol                  (55) = Ticker symbol
        msg.append_pair(167, "CS")  # SecurityType                         (167) = (CS) Common Stock
        msg.append_pair(54, side)  # Side                                   (54) = Side
        msg.append_pair(60, msg.get(52))  # TransactTime                    (60) = Transaction Time
        msg.append_pair(63, int(SecurityID_group[1]))  # SettlType          (63) = 0001/0002/0003
        msg.append_pair(453, 1)  # NoPartyIDs                              (453) = Number of PartyID
        msg.append_pair(448, PartyID[0])  # PartyID                        (448) = PartyID
        msg.append_pair(447, "D")  # PartyIDSource                         (447) = (D) Proprietary / Custom code
        msg.append_pair(452, 53)  # PartyRole                              (452) = (53) Trader mnemonic
        return msg

    def change_order_msg(self, prod, price, qty, side, OrigClOrdID, ClOrdID_temp, account='1'):
        msg = self.header_msg("G")
        SecurityID_group = prod.split('-')
        msg.append_pair(11, ClOrdID_temp)
        msg.append_pair(15, SecurityID_group[-1])  # Currency               (15) = ARS/USD
        msg.append_pair(38, qty)
        msg.append_pair(40, 2)
        msg.append_pair(44, price)
        msg.append_pair(54, side)
        msg.append_pair(55, SecurityID_group[0])
        msg.append_pair(59, 0)
        msg.append_pair(41, OrigClOrdID)
        msg.append_pair(60, msg.get(52))
        msg.append_pair(63, int(SecurityID_group[1]))  # SettlType          (63) = 0001/0002/0003
        msg.append_pair(167, "CS")  # SecurityType                         (167) = (CS) Common Stock
        msg.append_pair(528, "P")
        msg.append_pair(1138, qty)
        msg.append_pair(29501, 1)
        msg.append_pair(453, 1)  # NoPartyIDs                              (453) = Number of PartyID
        msg.append_pair(448, PartyID[0])  # PartyID                        (448) = PartyID
        msg.append_pair(447, "D")  # PartyIDSource                         (447) = (D) Proprietary / Custom code
        msg.append_pair(452, 53)  # PartyRole                              (452) = (53) Trader mnemonic
        return msg

    def unsubscribe_bid_ask_data_request_msg(self, prod, MDReqID, max_depth=2):
        msg = self.header_msg("V")

        msg.append_pair(262, MDReqID)
        msg.append_pair(263, "2")
        msg.append_pair(264, str(max_depth))
        msg.append_pair(265, "0")
        msg.append_pair(266, "Y")
        msg.append_pair(267, "2")
        msg.append_pair(269, "0")
        msg.append_pair(269, "1")
        msg.append_pair(146, "1")
        msg.append_pair(55, prod)
        return msg

    def trade_report_msg(self):
        global TradeRequestID
        TradeRequestID[0] += 1

        msg = self.header_msg("AD")
        msg.append_pair(568, TradeRequestID[0])
        msg.append_pair(569, 0)
        msg.append_pair(30001, 1)
        msg.append_pair(453, 1)  # NoPartyIDs                              (453) = Number of PartyID
        msg.append_pair(448, PartyID[0])  # PartyID                        (448) = PartyID
        msg.append_pair(447, 'D')  # PartyIDSource                         (447) = (D) Proprietary / Custom code
        msg.append_pair(452, 53)  # PartyRole                              (452) = (53) Trader mnemonic
        return msg

    def extract_symbol(self, msg):
        return msg.get(48).decode("cp1252")

    def extract_msg_type(self, msg):
        msg_type = ''
        if msg.get(35)==b'AD':
            msg_type ='TradeCaptureReportRequest'
        elif msg.get(150)==b'F' and msg.get(35)==b'8':
            msg_type ='OrderTradingReport'
        elif msg.get(35)==b'0':
            msg_type ='Heartbeat'
        elif msg.get(35)==b'9':
            msg_type ='ERROR_order'
        elif msg.get(35)==b'3':
            msg_type ='ERROR_msj'
        elif msg.get(35)==b'W':
            msg_type ='MarketDataSnapshotFullRefresh'
        elif msg.get(35)==b'X' and msg.get(1021)==b'2':
            msg_type ='MarketDataIncrementalRefresh_price_depth'
        elif msg.get(35)==b'X' and msg.get(1021)==b'3':
            msg_type ='MarketDataIncrementalRefresh_order_depth'
        return msg_type

    def extract_market_from_price_depth(self, msg, bid_book, ask_book, symbol):
        mqt_data_list = msg.encode().decode("cp1252").split('\x01279=')

        if len(mqt_data_list)>1:
            for msg_parse in mqt_data_list[1:]:
                if symbol in msg_parse:

                    if msg_parse[0]=='0':#added
                        data = re.match(r'.*\x01270=(.+?)\x01271=(.+?)\x01.*', msg_parse)
                        pross = msg_parse.split('\x01269=')
                        if data and pross[1][0]=='1'or pross[1][0]=='0':

                            if pross[1][0]=='1':
                                ask_book.append(data.group(1,2))
                            if pross[1][0]=='0':
                                bid_book.append(data.group(1,2))

                            if bid_book:
                                bid_book.sort(key=lambda tup: float(tup[0]), reverse=True)
                            if ask_book:
                                ask_book.sort(key=lambda tup: float(tup[0]))

                    if msg_parse[0]=='1':#changed
                        data = re.match(r'.*\x01270=(.+?)\x01271=(.+?)\x01.*', msg_parse)
                        pross = msg_parse.split('\x01269=')
                        if data and pross[1][0]=='1'or pross[1][0]=='0':

                            if pross[1][0]=='1':
                                for idx, val in enumerate(ask_book):
                                    if val[0]==data.group(1,2)[0]:
                                        ask_book[idx] = data.group(1,2)
                                        break
                            if pross[1][0]=='0':
                                for idx, val in enumerate(bid_book):
                                    if val[0]==data.group(1,2)[0]:
                                        bid_book[idx] = data.group(1,2)
                                        break

                    if msg_parse[0]=='2':#deleted
                        data = re.match(r'.*\x01270=(.+?)\x01271=(.+?)\x01.*', msg_parse)
                        pross = msg_parse.split('\x01269=')
                        if data and pross[1][0]=='1'or pross[1][0]=='0':

                            if pross[1][0]=='1':
                                for idx, val in enumerate(ask_book):
                                    if val[0]==data.group(1,2)[0]:
                                        del ask_book[idx]
                                        break
                            if pross[1][0]=='0':
                                for idx, val in enumerate(bid_book):
                                    if val[0]==data.group(1,2)[0]:
                                        del bid_book[idx]
                                        break

            if len(bid_book)>5:
                bid_book = bid_book[:5]

            if len(ask_book)>5:
                ask_book = ask_book[:5]

        return bid_book, ask_book


