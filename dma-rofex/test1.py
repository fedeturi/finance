from rofexclient import ROFEXClient

if __name__ == '__main__':

    rofex_client = ROFEXClient("fedejbrun5018", "ugklxY0*", "REM5018", "DEMO")
    rofex_client.subscribe_products([["GGALOct20"], ["GGALDic20"], ["DODic20"], ["DONov20"], ["DOOct20"]])
    rofex_client.subscribe_order_report()

    while True:
        try:
            for _ in range(1):
                pass
        except KeyboardInterrupt:
            rofex_client.disconnect()
