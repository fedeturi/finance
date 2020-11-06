from threading import Lock
import time
import os

lk = Lock()
timeout = 0.00005
Lunch_time = [time.time()]
algos_request_subscribed = []  # subscribe the algo
algos_request_unsubscribe = []  # unsubscribe the algo
EmptyMsg = b''
ClOrdID = [1]
TradeRequestID = [1]
global_process = ['None']
MDReqID = [1]
TestReqID = [1]
seq_num = [0]
seq_num_server = [1]
enviroment = ['DEMO_ROFEX']
soundfile = "thats_good.wav"
soundfile_connection_lost = "doh.wav"
soundfile_send_msg = "tick.wav"
soundfile_fill_not_registered_msg = "Error.wav"
owned_accounts = "OwnAccounts.json"
protocol_type = 'FIXT.1.1'
xchange_name = ["ROFX"]
port = [9876]
TargetCompID = ['ROFX']
Futures_Users = ['']
xchange_name_Futures = ['']
host = ['fix.remarkets.primary.com.ar']
API_KEY = ["fedejbrun5018"]
PASSPHRASE = ["ugklxY0*"]
User = ["fedejbrun5018"]
account = ["REM5018"]  # TODO refactor to account_byma
account_rofex = "REM5018"  # TODO change when in Production
PartyID = ['']
file_for_conditions = ['']
threads_list = []  # A list of all running threads. This are autospreaders and socket.
width = os.get_terminal_size().columns
dash_line = "-" * width  # Separator line