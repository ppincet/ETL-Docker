import logging
from logging.handlers import RotatingFileHandler
from config import settings
import os
debug = settings.DEBUG
def setup_logging(log_file="etl.log", level=logging.INFO):
    print('hello from logging')
    try:

        log_dir = 'logs'
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, log_file)
        file_handler = RotatingFileHandler(
            log_path, 
            maxBytes = settings.SYS_MAX_LOG_SIZE, 
            backupCount = settings.SYS_MAX_LOG_ROTATION
        )
        file_handler.setLevel(level)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.WARNING) 

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        logging.basicConfig(
            level=level,
            handlers=[file_handler, console_handler],
            force=True
        )
        # min level is warning & up to critical
        logging.getLogger("paramiko").setLevel(logging.WARNING)
        print("logging initialized succesfully")
        if debug:
            logging.info("Logging initialized successfully.")
    except Exception as e:
        print(f'exc from logging: {e}')
        raise