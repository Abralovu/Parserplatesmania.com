import random
import time
from typing import Optional

from playwright.sync_api import sync_playwright, BrowserContext

from utils.logger import get_logger
from config.settings import (
    DELAY_MIN, DELAY_MAX,
    PROXY_URL, CHROME_PROFILE,
    BASE_URL, HEADLESS,
)

logger = get_logger(__name__)

# Маркеры блокировки — проверяем в html после каждого запроса
_BLOCK_MARKERS = [
    "killbot user verification",
    "проверка пользователя",
    "cf-browser-verification",
    "just a moment",
    "access denied",
]


class BrowserSession:
    """
    Менеджер браузерной сессии.

    Принимает chrome_profile — это позволяет запускать
    несколько экземпляров с разными профилями (v2.0 многопоточность).

    Использование:
        with BrowserSession(country='ru', chrome_profile='/path/to/profile') as session:
            html = session.fetch('https://...')
    """

    def __init__(self, country: str, chrome_profile: Optional[str] = None):
        self.country = country
        # Если профиль не передан — берём дефолтный из settings
        self._profile = chrome_profile or CHROME_PROFILE
        self._pw = None
        self._context: Optional[BrowserContext] = None
        self._page = None

    def __enter__(self) -> "BrowserSession":
        import asyncio
        asyncio.set_event_loop(None)
        self._pw = sync_playwright().start()
        self._context = self._pw.chromium.launch_persistent_context(
            user_data_dir=self._profile,
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled"],
            channel="chrome",
            proxy={"server": PROXY_URL} if PROXY_URL else None,
        )
        self._page = self._context.new_page()
        self._warmup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Закрываем браузер. Логируем если была ошибка."""
        if exc_type is not None:
            logger.error(f"Session exiting with error: {exc_type.__name__}: {exc_val}")
        try:
            if self._context:
                self._context.close()
        except Exception as e:
            logger.warning(f"Error closing context: {e}")
        try:
            if self._pw:
                self._pw.stop()
        except Exception as e:
            logger.warning(f"Error stopping playwright: {e}")

    def fetch(self, url: str) -> Optional[str]:
        from config.settings import stop_event
        if stop_event.is_set():
            return None
        
        _human_delay()
        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=15000)
        except Exception as e:
            logger.error(f"Timeout/error fetching {url}: {e}")
            return None

        from config.settings import stop_event
        if stop_event.is_set():
            return None

        self._page.wait_for_timeout(1500)
        html = self._page.content()

        if _is_blocked(html):
            logger.warning(f"Blocked on {url} — re-warming")
            warmup_ok = self._warmup()
            if not warmup_ok:
                logger.error("Re-warmup failed")
            return None

        return html

    def _warmup(self) -> bool:
        from config.settings import stop_event
        if stop_event.is_set():
            return False
        
        url = f"{BASE_URL}/{self.country}/gallery"
        logger.info(f"Browser warmup: {url}")
        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=20000)
        except Exception as e:
            logger.error(f"Warmup navigation failed: {e}")
            return False

        try:
            self._page.wait_for_function(
                "() => !document.title.includes('верификац') && "
                "!document.title.includes('verification')",
                timeout=15000,
            )
            logger.info("KillBot passed — session ready")
            return True
        except Exception:
            logger.warning("KillBot warmup timeout — proceeding anyway")
            return False

    def _warmup(self) -> bool:
        """
        Прогрев сессии — проходим KillBot через галерею страны.
        Возвращает True если KillBot пройден, False если таймаут.
        """
        url = f"{BASE_URL}/{self.country}/gallery"
        logger.info(f"Browser warmup: {url}")

        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=20000)
        except Exception as e:
            logger.error(f"Warmup navigation failed: {e}")
            return False

        try:
            self._page.wait_for_function(
                "() => !document.title.includes('верификац') && "
                "!document.title.includes('verification')",
                timeout=15000,
            )
            logger.info("KillBot passed — session ready")
            return True
        except Exception:
            logger.warning("KillBot warmup timeout — proceeding anyway")
            return False


# ─── Приватные утилиты ────────────────────────────────────────────────────────

def _human_delay() -> None:
    """Случайная задержка между запросами — имитация человека."""
    delay = random.uniform(DELAY_MIN, DELAY_MAX)
    logger.debug(f"Sleeping {delay:.2f}s")
    time.sleep(delay)


def _is_blocked(html: str) -> bool:
    """Проверяет наличие маркеров блокировки в HTML."""
    html_lower = html.lower()
    for marker in _BLOCK_MARKERS:
        if marker in html_lower:
            logger.warning(f"Block marker found: '{marker}'")
            return True
    return False