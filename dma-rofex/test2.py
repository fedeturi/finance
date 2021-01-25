from rofexclient import ROFEXClient, dash_line
import time

if __name__ == '__main__':

    rofex_client = ROFEXClient("fedejbrun5018", "ugklxY0*", "REM5018", "DEMO")
    rofex_client.daemon = True
    rofex_client.start()
    rofex_client.subscribe_products([["GGALOct20"], ["GGALDic20"], ["DODic20"], ["DONov20"], ["DOOct20"]], depth=5)
    rofex_client.subscribe_order_report()

    time.sleep(2)
    leaves_qty = rofex_client.build_market_order("GGALDic20", "sell", 10)
    print(dash_line)
    print(dash_line)
    print("QUEDAN PENDIENTES", leaves_qty, "unidades por falta de liquidez ")
    print(dash_line)

    while True:
        try:
            for _ in range(1):
                pass
        except KeyboardInterrupt:
            rofex_client.shutdown = True
            rofex_client.join()
            break
