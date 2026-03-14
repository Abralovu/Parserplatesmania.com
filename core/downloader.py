import os
import requests
from utils.logger import get_logger
from config.settings import OUTPUT_DIR

logger = get_logger(__name__)

PHOTOS_DIR = os.path.join(OUTPUT_DIR, "photos")


def download_photo(photo_url: str, plate_id: int, country: str) -> str | None:
    """
    Скачивает фото и сохраняет локально.
    Возвращает локальный путь или None если не удалось.
    
    Структура папок:
    data/photos/ru/31393852.jpg
    data/photos/de/12345.jpg
    """
    if not photo_url:
        return None

    country_dir = os.path.join(PHOTOS_DIR, country)
    os.makedirs(country_dir, exist_ok=True)

    ext = photo_url.split(".")[-1].split("?")[0] or "jpg"
    filename = f"{plate_id}.{ext}"
    filepath = os.path.join(country_dir, filename)

    # Если уже скачано — не скачиваем снова
    if os.path.exists(filepath):
        return filepath

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Referer": "https://platesmania.com/",
        }
        response = requests.get(photo_url, headers=headers, timeout=15)
        if response.status_code == 200:
            with open(filepath, "wb") as f:
                f.write(response.content)
            logger.debug(f"Downloaded: {filepath}")
            return filepath
        else:
            logger.warning(f"Photo download failed {photo_url}: HTTP {response.status_code}")
            return None
    except Exception as e:
        logger.warning(f"Photo download error {photo_url}: {e}")
        return None
