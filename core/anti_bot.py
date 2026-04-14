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
    stop_event,
)
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

_WARMUP_SITES = [
    "https://www.google.com",
    "https://www.wikipedia.org",
    "https://www.weather.com",
]

_MAX_KILLBOT_RETRIES = 3
_KILLBOT_UI_WAIT_S = 20
_DRAG_DISTANCE_PX = 250

_FIND_DRAGGABLE_JS = """() => {
    const all = document.querySelectorAll('*');
    const result = [];
    for (const el of all) {
        const rect = el.getBoundingClientRect();
        const w = rect.width;
        const h = rect.height;
        if (w >= 40 && w <= 70 && h >= 40 && h <= 70 && rect.top > 100) {
            const style = getComputedStyle(el);
            const bg = style.backgroundImage || '';
            if (bg.includes('linear-gradient') && bg.includes('0, 115, 230')) {
                result.push({
                    x: Math.round(rect.x + w / 2),
                    y: Math.round(rect.y + h / 2),
                    w: Math.round(w),
                    h: Math.round(h),
                    cursor: style.cursor,
                });
            }
        }
    }
    return result;
}"""


class BrowserSession:
    """
    Менеджер браузерной сессии на базе Camoufox.
    Обходит KillBot через drag-bypass слайдера.

    is_first_session=True  → полный warmup (нейтральные сайты + drag bypass)
    is_first_session=False → только drag bypass если нужен (профиль уже прогрет)
    """

    def __init__(
        self,
        country: str,
        profile_path: Optional[str] = None,
        is_first_session: bool = True,
    ):
        self.country = country
        self._profile_path = profile_path
        self._is_first_session = is_first_session
        self._browser = None
        self._page = None
        self._camoufox_instance = None

    def __enter__(self) -> "BrowserSession":
        import asyncio
        asyncio.set_event_loop(None)

        from camoufox.sync_api import Camoufox

        headless_mode = "virtual" if HEADLESS else False

        launch_kwargs = {
            "headless": headless_mode,
            "os": CAMOUFOX_OS,
            "humanize": CAMOUFOX_HUMANIZE,
            "block_webrtc": True,
            "enable_cache": True,
            "window": (1280, 720),
        }

        if self._profile_path:
            launch_kwargs["persistent_context"] = True
            launch_kwargs["user_data_dir"] = self._profile_path

        self._camoufox_instance = Camoufox(**launch_kwargs)
        self._browser = self._camoufox_instance.start()

        pages = self._browser.pages if hasattr(self._browser, "pages") else []
        self._page = pages[0] if pages else self._browser.new_page()

        self._warmup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            logger.error(f"Session exiting with error: {exc_type.__name__}: {exc_val}")

        try:
            if self._page and not self._page.is_closed():
                self._page.close()
        except Exception as e:
            logger.warning(f"Error closing page: {e}")

        try:
            if self._browser:
                self._browser.close()
        except Exception as e:
            logger.warning(f"Error closing browser: {e}")

        self._force_kill_firefox()

        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if not loop.is_closed():
                loop.close()
        except Exception:
            pass
        asyncio.set_event_loop(None)

    def _force_kill_firefox(self) -> None:
        if not self._profile_path:
            return
        import subprocess
        try:
            result = subprocess.run(
                ["pgrep", "-f", self._profile_path],
                capture_output=True,
                text=True,
                timeout=5,
            )
            pids = result.stdout.strip().split("\n")
            for pid in pids:
                if pid.strip():
                    subprocess.run(
                        ["kill", "-9", pid.strip()],
                        capture_output=True,
                        timeout=5,
                    )
                    logger.debug(f"Force killed Firefox pid={pid.strip()}")
        except Exception:
            pass

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

        html = self._page.content()

        if _is_blocked(html):
            logger.warning(f"Blocked on {url} — re-warming")
            warmup_ok = self._warmup()
            if not warmup_ok:
                logger.error("Re-warmup failed — KillBot still blocking")
            return None

        if "img-responsive" in html:
            self._simulate_reading()

        return html
    
    def _warmup(self) -> bool:
        if stop_event.is_set():
            return False

        if self._is_first_session:
            neutral_sites = random.sample(_WARMUP_SITES, k=min(2, len(_WARMUP_SITES)))
            for site_url in neutral_sites:
                if stop_event.is_set():
                    return False
                try:
                    logger.debug(f"Warmup: visiting {site_url}")
                    self._page.goto(
                        site_url, wait_until="domcontentloaded", timeout=15_000
                    )
                    self._simulate_reading()
                    time.sleep(random.uniform(1.0, 2.0))
                except Exception as e:
                    logger.debug(f"Warmup neutral site failed ({site_url}): {e}")

        # KillBot bypass — всегда проверяем
        gallery_url = f"{BASE_URL}/{self.country}/gallery"
        logger.info(f"Warmup: navigating to {gallery_url}")

        for attempt in range(1, _MAX_KILLBOT_RETRIES + 1):
            if stop_event.is_set():
                return False

            try:
                self._page.goto(
                    gallery_url, wait_until="domcontentloaded", timeout=30_000
                )
            except Exception as e:
                logger.warning(f"Warmup navigation failed (attempt {attempt}): {e}")
                time.sleep(random.uniform(3.0, 6.0))
                continue

            self._page.wait_for_timeout(3000)
            title = self._page.title().lower()

            if "verification" not in title and "верификац" not in title:
                logger.info(f"KillBot not detected on attempt {attempt} — direct access")
                return True

            logger.info(f"KillBot detected (attempt {attempt}) — executing drag bypass")
            if self._drag_bypass():
                logger.info("KillBot drag bypass successful — session ready")
                return True

            logger.warning(f"Drag bypass failed (attempt {attempt}/{_MAX_KILLBOT_RETRIES})")
            if attempt < _MAX_KILLBOT_RETRIES:
                wait = random.uniform(5.0, 10.0)
                logger.info(f"Waiting {wait:.1f}s before retry...")
                time.sleep(wait)

        logger.error("KillBot warmup failed after all attempts")
        return False

    def _drag_bypass(self) -> bool:
        logger.debug(f"Waiting {_KILLBOT_UI_WAIT_S}s for KillBot UI to load")
        self._page.wait_for_timeout(_KILLBOT_UI_WAIT_S * 1000)

        self._page.evaluate("document.querySelector('#preloader-w')?.remove()")
        self._page.wait_for_timeout(1000)

        elements = self._page.evaluate(_FIND_DRAGGABLE_JS)

        if not elements:
            logger.warning("No draggable elements found")
            return False

        logger.debug(f"Found {len(elements)} draggable candidates")
        elements.sort(key=lambda e: 0 if e["cursor"] == "pointer" else 1)

        for el in elements:
            if stop_event.is_set():
                return False

            start_x, start_y = el["x"], el["y"]
            logger.debug(f"Dragging element at ({start_x},{start_y})")

            self._page.mouse.move(start_x, start_y)
            self._page.mouse.down()
            steps = _DRAG_DISTANCE_PX // 10
            for i in range(steps):
                self._page.mouse.move(
                    start_x + ((i + 1) * 10),
                    start_y + random.randint(-2, 2),
                )
                time.sleep(random.uniform(0.03, 0.06))
            self._page.mouse.up()

            self._page.wait_for_timeout(3000)
            title = self._page.title().lower()

            if "verification" not in title and "верификац" not in title:
                logger.info(f"Drag bypass succeeded — title: {self._page.title()}")
                return True

            logger.debug("Drag did not bypass — trying next element")

        return False

    def _simulate_reading(self) -> None:
        try:
            time.sleep(random.uniform(0.3, 0.8))
            scroll_y = random.randint(200, 600)
            self._page.evaluate(f"window.scrollBy(0, {scroll_y})")
            time.sleep(random.uniform(0.2, 0.5))
        except Exception:
            pass


def _human_delay() -> None:
    delay = random.uniform(DELAY_MIN, DELAY_MAX)
    logger.debug(f"Sleeping {delay:.2f}s")
    time.sleep(delay)


def _is_blocked(html: str) -> bool:
    html_lower = html.lower()
    for marker in _BLOCK_MARKERS:
        if marker in html_lower:
            logger.warning(f"Block marker found: '{marker}'")
            return True
    return False