import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logging(log_file="app.log", level=logging.INFO):
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_path = os.path.join(log_dir, log_file)
    file_handler = RotatingFileHandler(
        log_path, 
        maxBytes=10 * 1024 * 1024, 
        backupCount=5
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
        handlers=[file_handler, console_handler]
    )

    logging.info("Logging initialized successfully.")