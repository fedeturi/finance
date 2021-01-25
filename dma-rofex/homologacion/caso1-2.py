"""
Caso 1-2: LOGIN INTENTO FALLIDO

DESCRIPCION
Se pedirá al cliente conectarse a la API utilizando
credenciales incorrectas

RESULTADO ESPERADO
Procesa correctamente el mensaje de
error
"""

from rofexclient import ROFEXClient

if __name__ == '__main__':

    rofex_client = ROFEXClient("fedeun5018", "ugklxY0*", "REM5018", "DEMO")

    while True:
        try:
            for _ in range(1):
                pass
        except KeyboardInterrupt:
            rofex_client.disconnect()
