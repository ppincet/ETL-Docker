import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)
CONSUMER_KEY = os.getenv('SF_CONSUMER_KEY')
USERNAME = os.getenv('SF_USERNAME')
PRIVATE_KEY = os.getenv('SF_PRIVATE_KEY').replace('\\n', '\n') 
LOGIN_URL = os.getenv('SF_LOGIN_URL')
WINDOW = os.getenv('WINDOW')
DEBUG = os.getenv('DEBUG')
SSH_USERNAME = os.getenv('SSH_USERNAME')
SSH_PASSWORD = os.getenv('SSH_PASSWORD')
SSH_HOST = os.getenv('SSH_HOST')
SSH_REMOTE_IFOLDER = os.getenv('SSH_REMOTE_IFOLDER')
SSH_REMOTE_UFOLDER = os.getenv('SSH_REMOTE_UFOLDER')
SSH_FILE_IPREFIX = os.getenv('SSH_FILE_IPREFIX')