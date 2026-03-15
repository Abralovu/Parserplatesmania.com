import random
import time
from playwright.sync_api import sync_playwright, Page
from utils.logger import get_logger
from config.settings import DELAY_MIN, DELAY_MAX, PROXY_URL, CHROME_PROFILE

logger = get_logger(__name__)

SESSION_SIZE = 500


class BrowserSession:
    """
    Менеджер браузерной сессии.
    Один браузер открывается на SESSION_SIZE запросов.
    KillBot проходится один раз при старте сессии.
    """

    def __init__(self, country: str):
        self.country = country
        self._pw = None
        self._context = None
        self._page = None

    def __enter__(self):
        self._pw = sync_playwright().start()
        self._context = self._pw.chromium.launch_persistent_context(
            user_data_dir=CHROME_PROFILE,
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
            channel="chrome",
            proxy={"server": PROXY_URL} if PROXY_URL else None,
        )
        self._page = self._context.new_page()
        self._warmup()
        return self

    def __exit__(self, *args):
        try:
            self._context.close()
            self._pw.stop()
        except Exception:
            pass

    def _warmup(self):
        """Прогрев — проходим KillBot через галерею страны."""
        url = f"https://platesmania.com/{self.country}/gallery"
        logger.info(f"Browser warmup: {url}")
        self._page.goto(url, wait_until="domcontentloaded", timeout=20000)
        try:
            self._page.wait_for_function(
                "() => !document.title.includes('верификац') && "
                "!document.title.includes('verification')",
                timeout=15000
            )
            logger.info("KillBot passed — session ready")
        except Exception:
            logger.warning("KillBot warmup timeout — proceeding anyway")

    def fetch(self, url: str) -> str | None:
        """Загружает страницу. Задержка встроена."""
        _human_delay()
        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=15000)
        except Exception as e:
            logger.error(f"Timeout/error fetching {url}: {e}")
            return None

        self._page.wait_for_timeout(1500)
        html = self._page.content()

        if _is_blocked(html):
            logger.warning(f"Blocked on {url} — re-warming")
            self._warmup()
            return None

        return html


def _human_delay() -> None:
    delay = random.uniform(DELAY_MIN, DELAY_MAX)
    logger.debug(f"Sleeping {delay:.2f}s")
    time.sleep(delay)


def _is_blocked(html: str) -> bool:
    markers = [
        "killbot user verification",
        "проверка пользователя",
        "cf-browser-verification",
        "just a moment",
        "access denied",
    ]
    html_lower = html.lower()
    for marker in markers:
        if marker in html_lower:
            logger.warning(f"Block marker: '{marker}'")
            return True
    return False
