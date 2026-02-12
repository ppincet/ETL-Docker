import time
import signal
from threading import Event
from collections import deque
from processes import inbound, outbound
from config import settings
from utils import force, common
from connectors import sftp
from connectors.salesforce import SalesforceClient 

stop_event = Event()

def handle_sigterm(signum, frame):
    print("Received SIGTERM, stopping gracefully...")
    stop_event.set()

signal.signal(signal.SIGTERM, handle_sigterm)

def main():
    sf_client = SalesforceClient()
    heart_tick_counter = 5
    resources = {
        'sf_conn': sf_client.get_instance,
        'sftp_conn': sftp.get_instance,
    }

    try:
        while not stop_event.is_set():
            starting_point = time.time()
            
            pipeline = deque([
                (inbound.process, ['sf_conn', 'sftp_conn'], {'strict': True}),
                (outbound.process, ['sf_conn', 'sftp_conn'], {'strict': True}),
            ])
            heart_tick_counter += 1
            if heart_tick_counter >= 5:
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
                    print(f'from main exc:{e}')
                    if settings.DEBUG:
                        print(f'❌ Task {task_name} failed: {e}')
                    try:
                        print(f"⚠️ Attempting to refresh Salesforce connection...")
                        sf_client.refresh_connection()
                    except Exception as auth_e:
                        print(f"❌ Refresh failed: {auth_e}")

                finally:
                    pipeline.popleft()                    
                    if settings.DEBUG:
                        print(f'Finished {task_name}. Moving to next step...')
            rest = max(0, float(settings.WINDOW) - (time.time() - starting_point))
            if rest > 0: 
                time.sleep(rest)

    finally:
        try:
            if 'sftp_conn' in resources:
                sftp_client = resources['sftp_conn']()
                if hasattr(sftp_client, 'close'): 
                    sftp_client.close()
            if settings.DEBUG:
                print("Connections closed")
        except Exception as e:
            pass 

if __name__ == "__main__":
    main()