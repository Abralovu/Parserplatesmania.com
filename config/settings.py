import os
import platform
import threading

from dotenv import load_dotenv

load_dotenv()

# ─── Задержки между запросами ─────────────────────────────────────────────────
DELAY_MIN = float(os.getenv("DELAY_MIN", "1.5"))
DELAY_MAX = float(os.getenv("DELAY_MAX", "4.0"))

if DELAY_MIN > DELAY_MAX:
    raise ValueError(
        f"DELAY_MIN ({DELAY_MIN}) не может быть больше DELAY_MAX ({DELAY_MAX})"
    )

# ─── Браузер и сессии ─────────────────────────────────────────────────────────
SESSION_SIZE = int(os.getenv("SESSION_SIZE", "500"))
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", "100"))

# Chrome профиль — используется в scraper/worker_pool (обратная совместимость)
CHROME_PROFILE = os.getenv(
    "CHROME_PROFILE",
    os.path.expanduser("~/Library/Application Support/Google/Chrome/Profile 3"),
)

_raw_profiles = os.getenv("CHROME_PROFILES", "")
CHROME_PROFILES: list[str] = (
    [p.strip() for p in _raw_profiles.split(",") if p.strip()]
    if _raw_profiles
    else [CHROME_PROFILE]
)

# ─── Прокси ───────────────────────────────────────────────────────────────────
PROXY_URL = os.getenv("PROXY_URL", None)

_raw_proxy_list = os.getenv("PROXY_LIST", "")
PROXY_LIST: list[str] = (
    [p.strip() for p in _raw_proxy_list.split(",") if p.strip()]
    if _raw_proxy_list
    else ([PROXY_URL] if PROXY_URL else [])
)

# ─── Пути ─────────────────────────────────────────────────────────────────────
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")
DB_PATH = os.getenv("DB_PATH", "./data/plates.db")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "checkpoint.json")

# ─── Прочее ───────────────────────────────────────────────────────────────────
BASE_URL = "https://platesmania.com"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "5"))
RETRY_WAIT_MIN = int(os.getenv("RETRY_WAIT_MIN", "2"))
RETRY_WAIT_MAX = int(os.getenv("RETRY_WAIT_MAX", "30"))

# ─── Telegram (v2.0) ──────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", None)
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", None)

# ─── Режим запуска ────────────────────────────────────────────────────────────
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"

# ─── Camoufox ─────────────────────────────────────────────────────────────────
_default_cfox_os = "macos" if platform.system() == "Darwin" else "linux"
CAMOUFOX_OS = os.getenv("CAMOUFOX_OS", _default_cfox_os)
CAMOUFOX_HUMANIZE = os.getenv("CAMOUFOX_HUMANIZE", "true").lower() == "true"

# ─── Глобальный флаг остановки ────────────────────────────────────────────────
stop_event = threading.Event()