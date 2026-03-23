import os
import threading
from dotenv import load_dotenv

load_dotenv()

# ─── Задержки между запросами ─────────────────────────────────────────────────
DELAY_MIN = float(os.getenv("DELAY_MIN", "1.5"))
DELAY_MAX = float(os.getenv("DELAY_MAX", "4.0"))

# Валидация — если DELAY_MIN > DELAY_MAX, random.uniform сломается
if DELAY_MIN > DELAY_MAX:
    raise ValueError(
        f"DELAY_MIN ({DELAY_MIN}) не может быть больше DELAY_MAX ({DELAY_MAX})"
    )

# ─── Браузер и сессии ─────────────────────────────────────────────────────────

# Сколько страниц парсим в одной браузерной сессии
# После SESSION_SIZE браузер перезапускается — защита от утечек памяти
SESSION_SIZE = int(os.getenv("SESSION_SIZE", "500"))

# Как часто сохраняем checkpoint (каждые N записей)
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", "100"))

# Chrome профиль — клиент меняет под себя в .env
CHROME_PROFILE = os.getenv(
    "CHROME_PROFILE",
    os.path.expanduser("~/Library/Application Support/Google/Chrome/Profile 3"),
)

# Список профилей для многопоточности (v2.0)
# Пример в .env: CHROME_PROFILES=Profile 1,Profile 2,Profile 3
_raw_profiles = os.getenv("CHROME_PROFILES", "")
CHROME_PROFILES: list[str] = (
    [p.strip() for p in _raw_profiles.split(",") if p.strip()]
    if _raw_profiles
    else [CHROME_PROFILE]
)

# Прокси — опционально
PROXY_URL = os.getenv("PROXY_URL", None)

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

# True на VPS (нет GUI), False локально для отладки
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"

# Глобальный флаг остановки — проверяется в воркерах
stop_event = threading.Event()