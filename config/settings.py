from dotenv import load_dotenv
import os

load_dotenv()

DELAY_MIN = float(os.getenv("DELAY_MIN", 1.5))
DELAY_MAX = float(os.getenv("DELAY_MAX", 4.0))
PROXY_URL = os.getenv("PROXY_URL", None)

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")
DB_PATH = os.getenv("DB_PATH", "./data/plates.db")
CHECKPOINT_FILE = os.path.join(os.getenv("OUTPUT_DIR", "./data"), "checkpoint.json")

# Путь к Chrome профилю — клиент меняет под себя в .env
CHROME_PROFILE = os.getenv(
    "CHROME_PROFILE",
    os.path.expanduser("~/Library/Application Support/Google/Chrome/Profile 3")
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

BASE_URL = "https://platesmania.com"

RETRY_ATTEMPTS = 5
RETRY_WAIT_MIN = 2
RETRY_WAIT_MAX = 30
