from rofexclient import ROFEXClient
import time
from pprint import pprint
from threading import Lock

if __name__ == '__main__':
    lk = Lock()

    rofex_client = ROFEXClient("fedejbrun5018", "ugklxY0*", "REM5018", "DEMO")
    rofex_client.subscribe_products([["GGALOct20"], ["GGALDic20"], ["DODic20"], ["DONov20"], ["DOOct20"]])
    rofex_client.subscribe_order_report()

    time.sleep(4)
    rofex_client.place_order("GGALOct20", "buy", 60, 10)
    rofex_client.place_order("GGALDic20", "buy", 60, 10)

    lk.acquire()
    pprint(rofex_client.get_active_orders())
    pprint(rofex_client.get_subscribed_products())
    lk.release()

    time.sleep(1)
    rofex_client.cancel_all_orders()

    while True:
        try:
            for _ in range(1):
                pass
        except KeyboardInterrupt:
            rofex_client.disconnect()
