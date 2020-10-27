"""
CLASSES:
Connection -------------------------------- Establish connection with server using Socket.

    FIELDS:
    connected ----------------------------- True when connection is established, False otherwise.
    FIXEngine ---------------------------- Instance of FIXEngine to process FIX messages.
    host ---------------------------------- Destination host.
    lst_50_msg_time ----------------------- Time that the last exception_message has been sent.
    msg_send_to_sound --------------------- Quantity of messages sent.
    port ---------------------------------- Destination port.
    reconnected_needed -------------------- True if connection was lost and needs ro reconnect, False otherwise.
    sock ---------------------------------- Instance of SSL wrapped INET STREAM socket to handle connection.
    verbose ------------------------------- Prints messages about connection.

    METHODS:
    close() ------------------------------- Terminate connection.
    connect() ----------------------------- Initialise connection.
    fileno() ------------------------------ Return file descriptor of the socket.
    receive() ----------------------------- Return received data from server.
    send() -------------------------------- Send data to server.
"""

import logging
import socket
import ssl
from os import system, name

import select
import simplefix

from fixengine import FIXEngine
from global_queue import *

filename = 'c:/logs/' + 'ROFEXclient-log' + \
           time.strftime('%Y-%m-%d_%H%M%S', time.localtime(time.time())) + '.log'
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s-%(threadName)s-%(exception_message)s', filename=filename)


def clear_screen():
    # for windows
    if name == 'nt':
        _ = system('cls')

        # for mac and linux(here, os.name is 'posix')
    else:
        _ = system('clear')


class Connection:
    def __init__(self,
                 host="fix.remarkets.primary.com.ar",
                 port=9876, verbose=0):
        self.msg_send_to_sound = 0
        self.host = host
        self.port = port
        self.verbose = verbose
        self.lst_50_msg_time = time.time()

        if self.verbose:
            print('SocketUtils: Creating Socket')

        self.sock = None
        self.FIX_engine = FIXEngine()
        self.connect()
        self.reconnection_needed = False
        self.connected = False
        self.msg_buffer = simplefix.FixParser()

    def close(self):
        if self.verbose:
            print(dash_line)
            info_msg = "\033[0;30;47mSocketUtils: Closing socket.\033[1;37;40m"
            print(info_msg)

        self.sock.close()

        if self.verbose:
            print(dash_line)
            info_msg = "\033[0;30;47mSocketUtils: Socket closed.\033[1;37;40m"
            print(info_msg)

    def fileno(self):
        return self.sock.fileno()

    def connect(self):
        self.connected = False

        while not self.connected:

            try:

                clear_screen()
                print("=" * width)
                connected_message = 'Connecting to ROFEX Demo DMA servert at ' + \
                                    str(self.host) + ':' + str(self.port)
                print(connected_message.center(width))
                print("=" * width)

                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(10)
                self.sock = ssl.wrap_socket(s)
                self.sock.connect((self.host, self.port))

                data = self.FIX_engine.logon()

                logging.debug(simplefix.pretty_print(data.encode()))

                self.sock.send(data.encode())
                self.connected = True

                # TODO quitar
                print(str(self.receive(1024)))

                print("=" * width)
                print('Connection Status: OK'.center(width))
                print("=" * width)
                print('Ready to Launch Strategies'.center(width))
                print("=" * width)
            except Exception as ex:
                pass
                template = "Socket_1: An exception with read msg, of type {0} occurred. Arguments:\n{1!r}"
                print(template.format(type(ex).__name__, ex.args))
            time.sleep(5)

    def send(self, data):
        self.msg_send_to_sound += 1

        while True:
            try:
                if not self.connected:
                    print("####################### No esta conectado")
                    self.connect()
                    self.reconnection_needed = True

                logging.debug(simplefix.pretty_print(data.encode()))

                self.sock.send(data.encode())
                print('####################### Data sent')

                break

            except (ConnectionRefusedError, ConnectionResetError, OSError):
                self.connected = False
                print('Connection lost.')
                raise ValueError('THE CONEECTION IS LOSTTT!!!!! TE PROGRAM WILL TERMINATE!!!')
                break

            except Exception as ex:
                template = "Socket_2: An exception with send msg, of type {0} occurred. Arguments:\n{1!r}"
                print(template.format(type(ex).__name__, ex.args))
                self.connected = False

            time.sleep(timeout)

    def receive(self, size=1024):

        # Asynchronous I/O file descriptors from socket
        # infds --- Inbound queue waits until ready for reading
        # outfds -- Outbound queue waits until ready for writing
        # errfds -- Error queue wait for an exceptional condition (mostly errors)
        infds, outfds, errfds = select.select([self.sock], [self.sock], [])

        data = self.sock.recv()

        return infds, data


        """
        while True:
            try:
                if not self.connected:
                    self.connect()
                return infds
                break

            except (ConnectionRefusedError, ConnectionResetError, OSError):
                self.connected = False

            except Exception as ex:
                template = "Socket_3: An exception with read msg, of type {0} occurred. Arguments:\n{1!r}"
                print(template.format(type(ex).__name__, ex.args))
                self.connected = False

            time.sleep(timeout)
        """

    def __str__(self):
        return 'SocketUtils.Socket\nSocket created on Host=' + str(self.host) + ',Port=' + str(self.port)
