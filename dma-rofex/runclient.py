import threading

from rofexclient import ROFEXClient


request_total = 0


def func(thread_name):
    global request_total
    requests = 0
    while True:
        try:
            requests += 1
            rofex_client.get_market_data("GGALDic20")
            print(f"Thread {thread_name} requests {requests}")
            request_total += 1
            print("TOTAL", request_total)
        except Exception as e:
            print(e)
            request_total += requests
        except KeyboardInterrupt:
            request_total += requests
            print(f"Thread {thread_name} interrupted at {requests}")
            break


if __name__ == '__main__':
    rofex_client = ROFEXClient("fedejbrun5018", "ugklxY0*", "REM5018", "demo")

    try:
        thread1 = threading.Thread(target=func, args=(1,), name="1")
        thread2 = threading.Thread(target=func, args=(2,), name="2")
        thread3 = threading.Thread(target=func, args=(3,), name="3")
        thread4 = threading.Thread(target=func, args=(4,), name="4")
        thread5 = threading.Thread(target=func, args=(5,), name="5")
        thread6 = threading.Thread(target=func, args=(6,), name="6")
        thread7 = threading.Thread(target=func, args=(7,), name="7")
        thread8 = threading.Thread(target=func, args=(8,), name="8")
        thread9 = threading.Thread(target=func, args=(9,), name="9")
        thread10 = threading.Thread(target=func, args=(10,), name="10")
        thread11 = threading.Thread(target=func, args=(11,), name="11")
        thread12 = threading.Thread(target=func, args=(12,), name="12")
        thread13 = threading.Thread(target=func, args=(13,), name="13")
        thread14 = threading.Thread(target=func, args=(14,), name="14")
        thread15 = threading.Thread(target=func, args=(15,), name="15")
        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()
        thread5.start()
        thread6.start()
        thread7.start()
        thread8.start()
        thread9.start()
        thread10.start()
        thread11.start()
        thread12.start()
        thread13.start()
        thread14.start()
        thread15.start()
        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()
        thread5.join()
        thread6.join()
        thread7.join()
        thread8.join()
        thread9.join()
        thread10.join()
        thread11.join()
        thread12.join()
        thread13.join()
        thread14.join()
        thread15.join()

        rofex_client.disconnect()

    except Exception as e:
        print(e)
