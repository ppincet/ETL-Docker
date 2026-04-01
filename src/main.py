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

def handle_sigterm(signum, frame):
    print("Received SIGTERM, stopping gracefully...")
    logger.info("Received SIGTERM, stopping gracefully...")
    stop_event.set()

signal.signal(signal.SIGTERM, handle_sigterm)

def main():
    logger.info(f'started')
    heart_tick_counter = 5
    resources = {
        'sf_conn': sf_client.get_instance,
        'sftp_conn': sftp.get_instance,
    }
    personal = True
    try:
        while personal and not stop_event.is_set():
            personal = False
            print('next start tick')
            starting_point = time.time()
            
            pipeline = deque([
                (inbound.process, ['sf_conn', 'sftp_conn'], {'strict': True}),
                (outbound.process, ['sf_conn', 'sftp_conn'], {'strict': True}),
            ])
            heart_tick_counter += 1
            if heart_tick_counter >= 5:
                if settings.DEBUG:
                    print(f"heart tick: {time.strftime('%H:%M:%S')}")
                heart_tick_counter = 0
            while pipeline:
                if stop_event.is_set(): break
                
                func, resource_keys, static_kwargs = pipeline[0]
                task_name = f"{func.__module__}.{func.__name__}"
                try:
                    injected_args = {}
                    for key in resource_keys:
                        injected_args[key] = resources[key]()
                    
                    final_kwargs = {**injected_args, **static_kwargs}
                    func(**final_kwargs)

                except Exception as e:
                    if settings.DEBUG:
                        print(f'❌ Task {task_name} failed: {e}')
                    try:
                        print(f"⚠️ Attempting to refresh Salesforce connection...")
                        sf_client.refresh_connection()
                    except Exception as auth_e:
                        print(f"❌ Refresh failed: {auth_e}")

                finally:
                    pipeline.popleft()                    
            rest = max(0, float(settings.WINDOW) - (time.time() - starting_point))
            if rest > 0: 
                time.sleep(rest)

    finally:
        try:
            if 'sftp_conn' in resources:
                sftp_client = resources['sftp_conn']()
                if hasattr(sftp_client, 'close'): 
                    sftp_client.close()
        except Exception as e:
            pass 

if __name__ == "__main__": 
    main()

'''
import time
import signal
from multiprocessing import Process, Event

# Global stop signal for the parent to communicate with children
stop_event = Event()

def task_worker(name, task_func, resource_keys, window_secs, static_kwargs):
    """
    Runs in its own process. 
    Handles its own lifecycle and connection management.
    """
    # Child processes need their own signal handling
    signal.signal(signal.SIGTERM, lambda signum, frame: stop_event.set())

    print(f"🚀 Starting worker: {name} (Window: {window_secs}s)")

    while not stop_event.is_set():
        start_time = time.time()
        
        # 1. Setup / Connect (Fresh connection per tick)
        # Assuming your clients handle their own internal singleton or refresh
        try:
            injected_args = {}
            if 'sf_conn' in resource_keys:
                injected_args['sf_conn'] = sf_client.get_instance()
            if 'sftp_conn' in resource_keys:
                injected_args['sftp_conn'] = sftp.get_instance()

            # 2. Execute
            final_kwargs = {**injected_args, **static_kwargs}
            task_func(**final_kwargs)
            
        except Exception as e:
            print(f"❌ {name} failed: {e}")
            # Optional: sf_client.refresh_connection()
        
        finally:
            # 3. Cleanup: Explicitly close SFTP to avoid hitting server limits
            if 'sftp_conn' in injected_args:
                try:
                    injected_args['sftp_conn'].close()
                except:
                    pass

        # 4. Independent Window Calculation
        elapsed = time.time() - start_time
        sleep_time = max(0, window_secs - elapsed)
        
        if sleep_time > 0:
            # Check stop_event frequently even during sleep
            stop_event.wait(sleep_time)

def main():
    # Define your tasks and their specific windows
    tasks = [
        {
            "name": "Inbound",
            "func": inbound.process,
            "resources": ['sf_conn', 'sftp_conn'],
            "window": 30, # 30 second window
            "kwargs": {'strict': True}
        },
        {
            "name": "Outbound",
            "func": outbound.process,
            "resources": ['sf_conn', 'sftp_conn'],
            "window": 60, # 60 second window
            "kwargs": {'strict': True}
        }
    ]

    processes = []

    for t in tasks:
        p = Process(
            target=task_worker, 
            args=(t['name'], t['func'], t['resources'], t['window'], t['kwargs'])
        )
        p.start()
        processes.append(p)

    # Keep main alive until SIGTERM
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Stopping all processes...")
        stop_event.set()
        for p in processes:
            p.terminate()

if __name__ == "__main__":
    main()
'''