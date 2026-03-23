import os
import threading
from typing import Optional

import requests

from utils.logger import get_logger
from config.settings import OUTPUT_DIR

logger = get_logger(__name__)

PHOTOS_DIR = os.path.join(OUTPUT_DIR, "photos")


_thread_local = threading.local()

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://platesmania.com/",
}

_VALID_EXTENSIONS = {"jpg", "jpeg", "png", "webp", "gif"}


def download_photo(photo_url: str, plate_id: int, country: str) -> Optional[str]:
    """
    Скачивает фото и сохраняет локально.
    Возвращает локальный путь или None если не удалось.

    Структура папок:
        data/photos/ru/31393852.jpg
        data/photos/de/12345.jpg

    Thread-safe: использует per-thread requests.Session.
    Пропускает уже скачанные файлы.
    """
    if not photo_url:
        return None

    filepath = _build_filepath(photo_url, plate_id, country)

    # Файл уже существует — не скачиваем снова
    if os.path.exists(filepath):
        return filepath

    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    return _fetch_and_save(photo_url, filepath)


def _build_filepath(photo_url: str, plate_id: int, country: str) -> str:
    """Строит путь к файлу. Валидирует расширение."""
    try:
        raw_ext = photo_url.split(".")[-1].split("?")[0].lower()
        ext = raw_ext if raw_ext in _VALID_EXTENSIONS else "jpg"
    except Exception:
        ext = "jpg"

    return os.path.join(PHOTOS_DIR, country, f"{plate_id}.{ext}")


def _fetch_and_save(photo_url: str, filepath: str) -> Optional[str]:
    """Скачивает файл и сохраняет на диск."""
    session = _get_session()

    try:
        response = session.get(photo_url, timeout=15)
        if response.status_code == 200:
            with open(filepath, "wb") as f:
                f.write(response.content)
            logger.debug(f"Downloaded: {filepath}")
            return filepath

        logger.warning(f"Photo download failed {photo_url}: HTTP {response.status_code}")
        return None

    except Exception as e:
        logger.warning(f"Photo download error {photo_url}: {e}")
        return None


def _get_session() -> requests.Session:
    """
    Возвращает requests.Session для текущего потока.
    Создаёт новую если ещё нет — lazy initialization.
    """
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        session.headers.update(_HEADERS)
        _thread_local.session = session
    return _thread_local.session