"""
CASO 3:
Ingreso de orden tipo LIMIT

DESCRIPCION
Sepedirá al cliente el ingreso de una orden tipo LIMIT

RESULTADO ESPERADO
Ingresa correctamente al libro de órdenes
del Mercado una orden del tipo limit
"""

from rofexclient import ROFEXClient
from pprint import pprint
import sys
import json

if __name__ == '__main__':

    event = {
        'symbol': 'DOMar21',
        'side': 'sell',
        'price': 100.80,
        'qty': 50
    }

    rofex_client = ROFEXClient("fedejbrun5018", "ugklxY0*", "REM5018", "DEMO")

    clordid, order = rofex_client.place_order(event)

    print('Order with ClOrdID: ', clordid)
    pprint(order)

    while True:
        try:
            for _ in range(1):
                pass
        except KeyboardInterrupt:
            rofex_client.disconnect()
