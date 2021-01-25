import json
from threading import Lock
import time
import os

"""
    Global variables, used by many modules, common to all.
"""

# ----- Process and Threads --------------------------------------------------------------------------------------------
lk = Lock()                                         # Instance of GIL used by threads
timeout = 0.00005                                   # Timeout used in sleep and wait scenarios
launch_time = [time.time()]                         # Session initialization time
soundfile = "thats_good.wav"                        # Sound: FILL
soundfile_connection_lost = "doh.wav"               # Sound: CONNECTION LOST
soundfile_send_msg = "tick.wav"                     # Sound: QTY OF MESSAGES SENT (BYMA)
soundfile_fill_not_registered_msg = "Error.wav"     # Sound: ERROR
threads_list = []                                   # A list of all running threads. WebSocket threads and ALGOs
algos_request_subscribed_byma = []                  # Subscription of ALGOs in BYMA
algos_request_unsubscribe_byma = []                 # Unsubscription of ALGOs in BYMA
algos_request_subscribed_rofex = []                 # Subscription of ALGOs in ROFEX
algos_request_unsubscribe_rofex = []                # Unsubscription of ALGOs in ROFEX

with open("min_tick_ROFEX.json") as f:
    all_markets_description_rofex = json.load(f)    # ROFEX Product conditions

# ----- Cosmetic -------------------------------------------------------------------------------------------------------
width = os.get_terminal_size().columns              # Width of the terminal
dash_line = "-" * width                             # Separator line
