import datetime
import psutil


def logger(msj, type=None):   
    mem_usage = psutil.Process().memory_info().rss / 1024 ** 2
    avail_mem = psutil.virtual_memory().available / 1024 ** 2
    now = datetime.datetime.now()
    if type is not None:
        print(f"\n") 
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}][{mem_usage:.2f}mb/{avail_mem:.2f}mb]:  " + msj)
