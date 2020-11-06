"""
CLASSES:
Connection -------------------------------- Establish connection with server using Socket.

    FIELDS:
    connected ----------------------------- True when connection is established, False otherwise.
    FIXEngine ---------------------------- Instance of FIXEngine to process FIX messages.
    host ---------------------------------- Destination host.
    lst_50_msg_time ----------------------- Time that the last message has been sent.
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

import simplefix
import winsound

from fixengine import FIXEngine
from fixenginebyma import FIXEngineBYMA
from global_queue import *


def clear_screen():
    """
    Utility function that clears screen buffer.
    """
    # for windows
    if name == 'nt':
        _ = system('cls')

        # for mac and linux(here, os.name is 'posix')
    else:
        _ = system('clear')


class Connection:
    """
    WebSocket implementation.
    """

    def __init__(self, host=host, port=port, verbose=0):
        """
        Connection Class constructor.

        :param host: DMA Server Host.
        :type host: str
        :param port: DMA Server Port.
        :type port: int
        :param verbose: Verbosity lebel.
        :type verbose: int
        """
        # Qty of sent messages
        self.msg_send_to_sound = 0

        # DMA Server Host & Port
        self.host = host[0]
        self.port = port[0]

        self.verbose = verbose

        # Time of last sent message
        self.lst_50_msg_time = time.time()

        if self.verbose:
            print(f'SocketUtils: Creating WebSocket to {self.host}:{self.port}')

        self.sock = None
        self.connected = False

        # TODO creo que mas adelante vamos a necesitar ambos FIXEngines
        if 'ByMA' in enviroment[0]:
            self.FIX_engine = FIXEngineBYMA()
        else:
            self.FIX_engine = FIXEngine()

        self.connect()
        self.reconnected_needed = False

    def close(self):
        """
        Closes WebSocket connection.
        """
        if self.verbose:
            print(f'SocketUtils: Terminating connection to {self.host}:{self.port}. Closing WebSocket.')
        self.sock.close()

        if self.verbose:
            print(f'SocketUtils: WebSocket closed.')

    def fileno(self):
        """
        Returns file descriptor of the socket.

        :returns: fileno: Socket File Descriptor.
        :rtype: int
        """
        return self.sock.fileno()

    def connect(self):
        """
        Initiates connection to [host]:[port] with SSL wrapped WebSocket.
        """

        self.connected = False

        print(f"before reconnection {self.connected}")

        while not self.connected:
            try:
                clear_screen()
                print("=" * width)
                connected_message = 'Connecting to ' + enviroment[0] + " DMA servert at " +\
                                    str(self.host) + ':' + str(self.port)
                print(connected_message.center(width))
                print("=" * width)

                # Instance of IPv4 TCP/IP STREAMING socket
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(10)

                # Wrap socket with SSL. Encrypts connection.
                self.sock = ssl.wrap_socket(s)

                # Connect
                self.sock.connect((self.host, self.port))

                # Build Logon Message
                print('SENDING LOGON')
                data = self.FIX_engine.logon()
                print(data)
                print(data.encode())

                if logging.getLevelName('DEBUG') > 20:
                    logging.debug(simplefix.pretty_print(data.encode()))

                # Send Logon Message
                connection_status = self.sock.send(data.encode())
                print(f'DATA SENT {connection_status}')
                self.connected = True
                print(self.connected)

                print('Connection Status: OK'.center(width))
                print("=" * width)
                print('Ready to Launch Strategies'.center(width))
                print("=" * width)

            except Exception as ex:
                pass
                template = "Socket_1: An exception with read msg, of type {0} occurred. Arguments:\n{1!r}"
                print(template.format(type(ex).__name__, ex.args))

            time.sleep(5)

            return 1

    def send(self, data):
        """
        Sends message to server.

        :param data: Encoded message to send.
        :type data: str
        """

        self.msg_send_to_sound += 1

        while True:
            try:

                # CHECK 1: Connects if not connected.
                if not self.connected:
                    winsound.PlaySound(soundfile_connection_lost, winsound.SND_ASYNC | winsound.SND_ALIAS)
                    self.connect()
                    self.reconnected_needed = True

                if logging.getLevelName('DEBUG') > 20:
                    logging.debug(simplefix.pretty_print(data.encode()))

                self.sock.send(data.encode())

                # CHECK 2: BYMA messages Throttle limitation
                if 'ByMA' in enviroment[0] and self.msg_send_to_sound > 50:
                    print('Average total msg per minute:',
                          round(60 * int(data.get(34)) / (time.time() - Lunch_time[0]), 2))

                    if 51 * 60 / (time.time() - self.lst_50_msg_time) > 600:
                        os.system("start " + os.getcwd() + "\\alert1.wav")
                        print(dash_line)
                        print('Problem with number of msg:')
                        print(' ')
                        print(simplefix.pretty_print(data.encode()))
                        print(' ')
                        print('Extrapolate aprox msg per minute:',
                              round(51 * 60 / (time.time() - self.lst_50_msg_time), 2))
                        print(' ')
                        print(dash_line)

                    self.lst_50_msg_time = time.time()
                    self.msg_send_to_sound = 0

                if self.verbose:
                    print(f'SocketUtils: Data sent')
                break

            except (ConnectionRefusedError, ConnectionResetError, OSError) as ex:
                self.connected = False
                winsound.PlaySound(soundfile_connection_lost, winsound.SND_ASYNC | winsound.SND_ALIAS)
                template = "Socket_4: Connection Lost. {0} occurred. Arguments:\n{1!r}"
                print(template.format(type(ex).__name__, ex.args))
                raise ValueError('ERROR: Connection Lost. Program will terminate.')
                break

            except Exception as ex:
                template = "Socket_2: An exception with send msg, of type {0} occurred. Arguments:\n{1!r}"
                print(template.format(type(ex).__name__, ex.args))
                winsound.PlaySound(soundfile_connection_lost, winsound.SND_ASYNC | winsound.SND_ALIAS)
                self.connected = False

            time.sleep(timeout)

    def receive(self, size=1024):
        """
        Receives data from server.

        :param size: Size of buffer.
        :type size: int
        :returns: Data buffer
        :rtype: bytearray
        """
        while True:

            try:

                # CHECK 1: Connects if not connected.
                if not self.connected:
                    winsound.PlaySound(soundfile_connection_lost, winsound.SND_ASYNC | winsound.SND_ALIAS)
                    self.connect()

                if self.verbose:
                    print('Receiving data...')

                data = self.sock.recv(size)
                return data
                break

            except (ConnectionRefusedError, ConnectionResetError, OSError):
                self.connected = False
            except Exception as ex:
                template = "Socket_3: An exception with read msg, of type {0} occurred. Arguments:\n{1!r}"
                print(template.format(type(ex).__name__, ex.args))
                self.connected = False
            time.sleep(timeout)

    def __str__(self):
        return 'SocketUtils.Socket\nSocket created on Host=' + str(self.host) + ',Port=' + str(self.port)
