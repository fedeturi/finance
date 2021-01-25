"""
Caso 1-1: LOGIN INTENTO EXITOSO

DESCRIPCION
Se pedirá al cliente conectarse a la API utilizando un
user y password correcto

RESULTADO ESPERADO
Ingresa correctamente a la plataforma
"""

from rofexclient import ROFEXClient

if __name__ == '__main__':

    rofex_client = ROFEXClient("fedejbrun5018", "ugklxY0*", "REM5018", "DEMO")

    while True:
        try:
            for _ in range(1):
                pass
        except KeyboardInterrupt:
            rofex_client.disconnect()
