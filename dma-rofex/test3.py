from pprint import pprint

from rofexclient import ROFEXClient, dash_line
import time
from threading import Lock

if __name__ == '__main__':

    rofex_client = ROFEXClient("fedejbrun5018", "ugklxY0*", "REM5018", "DEMO")
    rofex_client.subscribe_products([["GGALOct20"], ["GGALDic20"], ["DODic20"], ["DONov20"], ["DOOct20"]])
    rofex_client.subscribe_order_report()

    time.sleep(2)

    ggal_1_ticker = "GGALOct20"
    ggal_1_px = rofex_client.get_market_price(ggal_1_ticker, "sell")
    ggal_1_qty = rofex_client.get_market_qty(ggal_1_ticker, "sell")
    rofex_client.place_order(ggal_1_ticker, "sell", ggal_1_px + 10, ggal_1_qty)

    ggal_2_ticker = "GGALDic20"
    ggal_2_px = rofex_client.get_market_price(ggal_2_ticker, "sell")
    ggal_2_qty = rofex_client.get_market_qty(ggal_2_ticker, "sell")
    rofex_client.place_order(ggal_2_ticker, "sell", ggal_2_px + 10, ggal_2_qty)

    do_1_ticker = "DOOct20"
    do_1_px = rofex_client.get_market_price(do_1_ticker, "buy")
    do_1_qty = rofex_client.get_market_qty(do_1_ticker, "buy")
    rofex_client.place_order(do_1_ticker, "buy", do_1_px - 1, do_1_qty)

    do_2_ticker = "DONov20"
    do_2_px = rofex_client.get_market_price(do_2_ticker, "buy")
    do_2_qty = rofex_client.get_market_qty(do_2_ticker, "buy")
    rofex_client.place_order(do_2_ticker, "buy", do_2_px - 1, do_2_qty)

    time.sleep(2)
    lk = Lock()
    lk.acquire()
    print(dash_line)
    print("LIST OF ACTIVE ORDERS:")
    pprint(rofex_client.active_orders)
    lk.release()

    time.sleep(2)
    rofex_client.cancel_all_orders()

    while True:
        try:
            for _ in range(1):
                pass
        except KeyboardInterrupt:
            rofex_client.disconnect()
