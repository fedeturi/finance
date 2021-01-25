"""
CASO 2-1:
Reference Data lista completa

DESCRIPCION
Se pedirá al cliente que solicite la lista de securities
disponible en el Mercado para operar

RESULTADO ESPERADO
Pide, recibe y procesa correctamente la
lista de securities disponibles en el
mercado
"""

from rofexclient import ROFEXClient
from pprint import pprint
import sys

if __name__ == '__main__':

    rofex_client = ROFEXClient("fedejbrun5018", "ugklxY0*", "REM5018", "DEMO")
    instruments = rofex_client.get_all_instruments()

    with open('./outputs/caso3-securities.txt', 'w') as writer:
        for instrumen in instruments:
            writer.writelines(f'{instrumen}\n')

    pprint(instruments)

    rofex_client.disconnect()
