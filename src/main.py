import time
import signal
import sys
import traceback
from threading import Event
from processes import inbound, outbound
from config import settings
from utils import force, common
from connectors import salesforce
stop_event = Event()

def handle_sigterm(*args):
    stop_event.set()
    #force.log(common.heartbeatWrapper('Go to sleep', 'SIGTERM'))

signal.signal(signal.SIGTERM, handle_sigterm)

def main():
    sf = salesforce.get_instance()
    new_era_start = time.time()
    # force.log(common.heartbeatWrapper('Hello, world!'))
    try:
        while  not stop_event.is_set():
            starting_point =time.time()
            inbound.process()
            outbound.process()
            rest = max(0, float(settings.WINDOW) - (time.time() - starting_point))
            if rest > 0: time.sleep(rest)
            # stop_event.set()
    except Exception as e:
        #if settings.DEBUG:
        print(f'possible step:{str(e)}')
        force.log(sf, common.crushWrapper(traceback.format_exc()[-32000:], str(e)))
        #traceback.print_exc()
        sys.exit(1)
if __name__ == "__main__":
    main()