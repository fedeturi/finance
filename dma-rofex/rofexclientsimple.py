import socket
import ssl
import time
import quickfix


from fixengine import FIXEngine

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

sock = ssl.wrap_socket(s)
sock.connect((socket.gethostbyname("fix.remarkets.primary.com.ar"), 9876))
sock.setblocking(False)

fix_engine = FIXEngine()

print(fix_engine.logon().encode())

sock.sendall(fix_engine.logon().encode())

while True:
    try:
        msg = sock.recv(1024)
        print(msg.decode("utf-8"))
    except ssl.SSLWantReadError:
        pass
    except KeyboardInterrupt:
        print("Closing socket")
        sock.close()
        break
