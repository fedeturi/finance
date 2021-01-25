"""
CASO 4:
Reference Data security en particular

DESCRIPCION
Se pedirá al cliente que solicite info especifica de una
security (se indicara durante la prueba cual será la
security)

RESULTADO ESPERADO
Pide, recibe y procesa correctamente la
información puntual de una security
"""

from rofexclient import ROFEXClient
from pprint import pprint
import sys
import json

if __name__ == '__main__':

    rofex_client = ROFEXClient("fedejbrun5018", "ugklxY0*", "REM5018", "DEMO")
    instrument = rofex_client.get_instrument_details('DOMar21')

    with open('./outputs/caso4-securities.json', 'w') as writer:
        writer.write(json.dumps(instrument))

    pprint(instrument)

    rofex_client.disconnect()
