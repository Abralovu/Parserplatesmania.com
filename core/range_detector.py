import re
from typing import Optional

from bs4 import BeautifulSoup

from core.anti_bot import BrowserSession
from utils.logger import get_logger
from config.settings import BASE_URL

logger = get_logger(__name__)

_COUNTRY_START_IDS: dict[str, int] = {
    "ru": 1,
    "de": 1,
    "us": 1,
    "ua": 1,
    "by": 1,
    "kz": 1,
}

_PLATE_URL_PATTERN = re.compile(r"/nomer(\d+)")


def detect_range(country: str, profile_path: Optional[str] = None) -> tuple[int, int]:
    """
    Определяет диапазон ID для страны автоматически.
    При ошибке возвращает безопасный дефолт (1, 100_000).
    """
    start_id = _COUNTRY_START_IDS.get(country, 1)
    end_id = _fetch_max_id(country, profile_path)

    if end_id is None:
        logger.warning(f"Could not detect max ID for {country} — using default 100_000")
        return start_id, 100_000

    logger.info(f"Detected range for {country}: {start_id}..{end_id}")
    return start_id, end_id


def _fetch_max_id(country: str, profile_path: Optional[str]) -> Optional[int]:
    """
    Открывает галерею страны и ищет максимальный ID.
    """
    try:
        with BrowserSession(country=country, profile_path=profile_path) as session:
            gallery_url = f"{BASE_URL}/{country}/gallery"
            html = session.fetch(gallery_url)
            if not html:
                logger.error(f"Cannot fetch gallery for {country}")
                return None

            last_page = _parse_last_page(html)
            if last_page and last_page > 1:
                last_url = f"{gallery_url}?page={last_page}"
                html = session.fetch(last_url)
                if not html:
                    logger.warning("Cannot fetch last page — using first page IDs")

            return _parse_max_id(html, country)

    except Exception as e:
        logger.error(f"range_detector failed for {country}: {e}")
        return None


def _parse_last_page(html: str) -> Optional[int]:
    """Ищет номер последней страницы в пагинации."""
    soup = BeautifulSoup(html, "lxml")
    page_numbers = []

    for a in soup.find_all("a", href=True):
        match = re.search(r"[?&]page=(\d+)", a["href"])
        if match:
            page_numbers.append(int(match.group(1)))

    if not page_numbers:
        return None

    last_page = max(page_numbers)
    logger.debug(f"Last gallery page: {last_page}")
    return last_page


def _parse_max_id(html: str, country: str) -> Optional[int]:
    """Ищет максимальный ID номера на странице галереи."""
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    ids = []

    for a in soup.find_all("a", href=True):
        match = _PLATE_URL_PATTERN.search(a["href"])
        if match:
            ids.append(int(match.group(1)))

    if not ids:
        logger.warning(f"No plate IDs found on gallery page for {country}")
        return None

    max_id = max(ids)
    logger.debug(f"Max ID found on page: {max_id}")
    return max_id