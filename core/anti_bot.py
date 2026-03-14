import random
import time
from playwright.sync_api import sync_playwright, BrowserContext, Page, Playwright
from utils.logger import get_logger
from config.settings import DELAY_MIN, DELAY_MAX, PROXY_URL

logger = get_logger(__name__)

CHROME_PROFILE = "/Users/akhrorabrolov/Library/Application Support/Google/Chrome/Profile 3"

# Сколько страниц парсить в одной сессии браузера
# После этого перезапускаем браузер — профилактика утечек памяти
SESSION_SIZE = 500


class BrowserSession:
    """
    Менеджер браузерной сессии.
    
    Использование:
        with BrowserSession("ru") as session:
            html = session.fetch("https://platesmania.com/ru/nomer123")
    
    Браузер открывается ОДИН РАЗ на весь блок with.
    warmup делается ОДИН РАЗ при входе.
    Все fetch внутри — быстрые переходы без повторного KillBot.
    """

    def __init__(self, country: str):
        self.country = country
        self._pw = None
        self._context = None
        self._page = None
        self._requests = 0

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
        """Один раз проходим KillBot через галерею."""
        url = f"https://platesmania.com/{self.country}/gallery"
        logger.info(f"Browser warmup: {url}")
        self._page.goto(url, wait_until="domcontentloaded", timeout=20000)
        try:
            self._page.wait_for_function(
                "() => !document.title.includes('верификац') && !document.title.includes('verification')",
                timeout=15000
            )
            logger.info("KillBot passed — session ready")
        except Exception:
            logger.warning("KillBot warmup timeout — proceeding anyway")

    def fetch(self, url: str) -> str | None:
        """
        Загружает страницу в текущей сессии.
        Быстро — браузер уже открыт, KillBot уже пройден.
        """
        human_delay()
        self._page.goto(url, wait_until="domcontentloaded", timeout=15000)
        self._page.wait_for_timeout(1500)
        html = self._page.content()
        self._requests += 1

        if is_blocked(html):
            logger.warning(f"Blocked: {url} — re-warming")
            self._warmup()  # Переварм если вдруг заблокировало
            return None

        return html


def fetch_page_playwright(url: str) -> str | None:
    """
    Одиночный fetch — для тестов и retry логики в scraper.py
    Для массового парсинга используй BrowserSession напрямую.
    """
    parts = url.split("/")
    country = parts[3] if len(parts) > 3 else "ru"

    with BrowserSession(country) as session:
        return session.fetch(url)


def human_delay() -> None:
    delay = random.uniform(DELAY_MIN, DELAY_MAX)
    logger.debug(f"Sleeping {delay:.2f}s")
    time.sleep(delay)


def is_blocked(html: str) -> bool:
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
