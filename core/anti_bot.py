#!/usr/bin/env python3
"""
BrowserSession — управляет одной сессией Camoufox.
Обходит KillBot через Firefox anti-detect browser с ротацией прокси.
Автор: viramax
"""

import random
import time
from typing import Optional

from config.settings import (
    BASE_URL,
    CAMOUFOX_HUMANIZE,
    CAMOUFOX_OS,
    DELAY_MAX,
    DELAY_MIN,
    HEADLESS,
    PROXY_LIST,
    stop_event,
)
from core.proxy_pool import ProxyPool, build_proxy_pool
from utils.logger import get_logger

logger = get_logger(__name__)

_BLOCK_MARKERS = [
    "killbot user verification",
    "user verification",
    "проверка пользователя",
    "cf-browser-verification",
    "just a moment",
    "access denied",
]

# Глобальный пул прокси — создаётся один раз при импорте модуля
_PROXY_POOL: Optional[ProxyPool] = build_proxy_pool(PROXY_LIST)

# Сайты для прогрева — накапливаем cookies и историю
_WARMUP_SITES = [
    "https://www.google.com",
    "https://www.wikipedia.org",
    "https://www.weather.com",
]

# Максимальное количество попыток пройти KillBot при warmup
_MAX_KILLBOT_RETRIES = 3

# Время ожидания JS challenge KillBot (мс)
_KILLBOT_WAIT_MS = 30_000


class BrowserSession:
    """
    Менеджер браузерной сессии на базе Camoufox.
    Принимает profile_path для persistent context.
    Каждая сессия получает свой прокси из пула.
    """

    def __init__(self, country: str, profile_path: Optional[str] = None):
        self.country = country
        self._profile_path = profile_path
        self._browser = None
        self._page = None
        self._proxy_dict: Optional[dict] = None
        self._camoufox_instance = None

    def __enter__(self) -> "BrowserSession":
        from camoufox.sync_api import Camoufox

        # Получаем прокси из пула
        if _PROXY_POOL:
            self._proxy_dict = _PROXY_POOL.next()
            logger.info(f"Using proxy: {self._proxy_dict['server']}")

        # На Linux "virtual" использует встроенный Xvfb Camoufox
        headless_mode = "virtual" if HEADLESS else False

        # Параметры запуска Camoufox
        launch_kwargs = {
            "headless": headless_mode,
            "os": CAMOUFOX_OS,
            "humanize": CAMOUFOX_HUMANIZE,
            "geoip": True,
            "block_webrtc": True,
            "enable_cache": True,
        }

        if self._proxy_dict:
            launch_kwargs["proxy"] = self._proxy_dict

        if self._profile_path:
            launch_kwargs["persistent_context"] = True
            launch_kwargs["user_data_dir"] = self._profile_path

        self._camoufox_instance = Camoufox(**launch_kwargs)
        self._browser = self._camoufox_instance.start()

        # При persistent_context первая страница уже открыта
        pages = self._browser.pages if hasattr(self._browser, "pages") else []
        if pages:
            self._page = pages[0]
        else:
            self._page = self._browser.new_page()

        self._warmup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            logger.error(
                f"Session exiting with error: {exc_type.__name__}: {exc_val}"
            )
        try:
            if self._page and not self._page.is_closed():
                self._page.close()
        except Exception as e:
            logger.warning(f"Error closing page: {e}")
        # Camoufox.stop() закрывает browser + playwright за нас
        try:
            if self._camoufox_instance:
                self._camoufox_instance.stop()
        except Exception as e:
            logger.warning(f"Error stopping Camoufox: {e}")

    def fetch(self, url: str) -> Optional[str]:
        """Загружает страницу. Возвращает HTML или None если заблокированы."""
        if stop_event.is_set():
            return None

        _human_delay()

        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=20_000)
        except Exception as e:
            logger.error(f"Timeout/error fetching {url}: {e}")
            return None

        if stop_event.is_set():
            return None

        self._simulate_reading()

        html = self._page.content()

        if _is_blocked(html):
            logger.warning(f"Blocked on {url} — re-warming")
            warmup_ok = self._warmup()
            if not warmup_ok:
                logger.error("Re-warmup failed — KillBot still blocking")
            return None

        return html

    def _warmup(self) -> bool:
        """
        Прогрев сессии:
        1. Посещаем нейтральные сайты (накапливаем cookies/историю)
        2. Заходим на галерею platesmania
        3. Ждём прохождения KillBot JS challenge
        """
        if stop_event.is_set():
            return False

        # Шаг 1: Посещаем 1-2 нейтральных сайта для "истории"
        neutral_sites = random.sample(
            _WARMUP_SITES, k=min(2, len(_WARMUP_SITES))
        )
        for site_url in neutral_sites:
            if stop_event.is_set():
                return False
            try:
                logger.debug(f"Warmup: visiting {site_url}")
                self._page.goto(
                    site_url, wait_until="domcontentloaded", timeout=15_000
                )
                self._simulate_reading()
                time.sleep(random.uniform(1.0, 3.0))
            except Exception as e:
                logger.debug(f"Warmup neutral site failed ({site_url}): {e}")

        # Шаг 2: Заходим на целевой сайт
        gallery_url = f"{BASE_URL}/{self.country}/gallery"
        logger.info(f"Warmup: navigating to {gallery_url}")

        for attempt in range(1, _MAX_KILLBOT_RETRIES + 1):
            if stop_event.is_set():
                return False

            try:
                self._page.goto(
                    gallery_url, wait_until="domcontentloaded", timeout=25_000
                )
            except Exception as e:
                logger.warning(
                    f"Warmup navigation failed (attempt {attempt}): {e}"
                )
                time.sleep(random.uniform(3.0, 6.0))
                continue

            self._simulate_reading()

            # Ждём прохождения KillBot JS challenge
            try:
                self._page.wait_for_function(
                    """() => {
                        const title = document.title.toLowerCase();
                        return !title.includes('верификац')
                            && !title.includes('verification')
                            && !title.includes('killbot');
                    }""",
                    timeout=_KILLBOT_WAIT_MS,
                )
                logger.info(
                    f"KillBot passed on attempt {attempt} — session ready"
                )
                self._browse_gallery_pages(gallery_url, pages_count=3)
                return True

            except Exception:
                logger.warning(
                    f"KillBot challenge timeout "
                    f"(attempt {attempt}/{_MAX_KILLBOT_RETRIES})"
                )
                if attempt < _MAX_KILLBOT_RETRIES:
                    wait = random.uniform(5.0, 10.0)
                    logger.info(f"Waiting {wait:.1f}s before retry...")
                    time.sleep(wait)

        logger.error("KillBot warmup failed after all attempts")
        return False

    def _browse_gallery_pages(self, gallery_url: str, pages_count: int) -> None:
        """Листаем страницы галереи для прогрева. Накапливаем cookies."""
        for page_num in range(2, pages_count + 2):
            if stop_event.is_set():
                return
            try:
                url = f"{gallery_url}?page={page_num}"
                self._page.goto(
                    url, wait_until="domcontentloaded", timeout=15_000
                )
                self._simulate_reading()
                time.sleep(random.uniform(1.5, 3.5))
            except Exception as e:
                logger.debug(f"Gallery page {page_num} warmup failed: {e}")

    def _simulate_reading(self) -> None:
        """
        Имитирует чтение страницы: скролл вниз, пауза.
        Mouse movements обработаны Camoufox (humanize=True).
        """
        try:
            time.sleep(random.uniform(0.5, 1.5))

            scroll_y = random.randint(200, 600)
            self._page.evaluate(f"window.scrollBy(0, {scroll_y})")
            time.sleep(random.uniform(0.3, 0.8))

            if random.random() < 0.4:
                scroll_y2 = random.randint(100, 400)
                self._page.evaluate(f"window.scrollBy(0, {scroll_y2})")
                time.sleep(random.uniform(0.3, 0.6))
        except Exception:
            pass


def _human_delay() -> None:
    """Рандомная задержка между запросами."""
    delay = random.uniform(DELAY_MIN, DELAY_MAX)
    logger.debug(f"Sleeping {delay:.2f}s")
    time.sleep(delay)


def _is_blocked(html: str) -> bool:
    """Проверяет HTML на маркеры блокировки."""
    html_lower = html.lower()
    for marker in _BLOCK_MARKERS:
        if marker in html_lower:
            logger.warning(f"Block marker found: '{marker}'")
            return True
    return False