import time
from simplefix import FixParser
import select

from socketROFEX import Connection

if __name__ == '__main__':
    msg_buffer = FixParser()
    sk = Connection()

    infds, outfds, errfds = select.select([sk], [sk], [])

    while True:
        try:
            # Received messages buffer

            print(infds, outfds, errfds)

            msgtemp = sk.receive()
            print(msgtemp)

            if msgtemp:
                msg_buffer.append_buffer(msgtemp)
                msg = msg_buffer.get_message()
                # Repeat until receive buffer is empty
                while msg:
                    print(msg)
                    msg = msg_buffer.get_message()

            if len(infds) != 0:
                msgtemp = sk.receive()
                print(msgtemp)

                if len(msgtemp) != 0:
                    msg_buffer.append_buffer(msgtemp)
                    msg = msg_buffer.get_message()

                    # Repeat until receive buffer is empty
                    while msg:
                        print(msg)
                        msg = msg_buffer.get_message()

            time.sleep(1)

        except KeyboardInterrupt:
            sk.close()
            break
