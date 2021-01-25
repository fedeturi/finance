from rofexclient import ROFEXClient
from pprint import  pprint
import pandas as pd
import pyRofex
import datetime as dt

if __name__ == '__main__':

    rofex_client = ROFEXClient("fedejbrun5018", "ugklxY0*", "REM5018", "DEMO")
    rofex_client.daemon = True
    rofex_client.start()
    pprint(rofex_client.get_instrument_details("MAI.ROSDic20"))

    while True:
        try:
            for _ in range(1):
                pass
        except KeyboardInterrupt:
            rofex_client.shutdown = True
            rofex_client.join()
            break
