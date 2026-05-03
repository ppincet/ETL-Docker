import time
import signal
from threading import Event
from collections import deque
from processes import inbound, outbound
from config import settings
from utils import force, common, loggin
from connectors import sftp
from connectors.salesforce import SalesforceClient 
import logging
from datetime import datetime

sf_client = SalesforceClient()
stop_event = Event()
loggin.setup_logging()
logger = logging.getLogger(__name__)
debug = settings.DEBUG

def handle_sigterm(signum, frame):
    if debug:
        logger.info("Received SIGTERM, stopping gracefully...")
    # here to fire sf log
    stop_event.set()

signal.signal(signal.SIGTERM, handle_sigterm)

def main():
    if debug:
        logger.info(f'---- started ----')
    heart_tick_counter = 5
    conns = {'sftp_in': None, 'sftp_out': None, 'sf_conn': None}
 
    personal = True
    try:
        while personal and not stop_event.is_set():
            personal = False
            print(f'next start tick at {datetime.now().strftime('%H:%M:%S')}')
            starting_point = time.time()
            conns['sf_conn'] = sf_client.get_instance()
            conns['sftp_in'] = sftp.ensure_connection(conns['sftp_in'])
            conns['sftp_out'] = sftp.ensure_connection(conns['sftp_out'])
            pipeline = deque([
                (inbound.process, ['sf_conn', 'sftp_in'], {'strict': True}),
                (outbound.process, ['sf_conn', 'sftp_out'], {'strict': True}),
            ])
            heart_tick_counter += 1
            if heart_tick_counter >= 5:
                if settings.DEBUG:
                    print(f"heart tick: {time.strftime('%H:%M:%S')}")
                heart_tick_counter = 0
            for func, res_keys, _ in pipeline:
                if stop_event.is_set(): break
                try:
                    kwargs = {key: conns[key] for key in res_keys}
                    func(**kwargs, strict=True)
                except Exception as e:
                    print(f"Execution error in {func.__name__}: {e}")
                    logger.error(f"Execution error in {func.__name__}: {e}")
            sleep_time = max(0, float(settings.WINDOW) - (time.time() - starting_point))
            stop_event.wait(sleep_time)
    except Exception as e:
        print(f'from main: {e}')
 
if __name__ == "__main__": 
    main()

