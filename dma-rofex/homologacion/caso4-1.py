"""
CASO 4:
Ingreso de orden tipo LIMIT

DESCRIPCION
Se pedirá al cliente la cancelación de una orden
previamente ingresada que no tuvo llenados
parciales. Se busca validar el manejo de los tags
LeavesQty y CumQty

RESULTADO ESPERADO
Envía un pedido de cancelación sobre una
orden previamente ingresada
"""

from rofexclient import ROFEXClient
from pprint import pprint
import sys
import json
import  time

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

    time.sleep(3)

    cancelled_clordid = rofex_client.cancel_order(clordid+80)

    while True:
        try:
            for _ in range(1):
                pass
        except KeyboardInterrupt:
            rofex_client.disconnect()
