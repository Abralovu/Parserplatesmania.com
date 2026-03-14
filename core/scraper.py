import re
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from bs4 import BeautifulSoup

from core.anti_bot import fetch_page_playwright, BrowserSession, human_delay, is_blocked
from core.downloader import download_photo
from storage.models import PlateRecord
from storage.database import save_batch, get_count
from utils.logger import get_logger
from utils.retry import scraper_retry
from utils.checkpoint import save_checkpoint, load_checkpoint
from config.settings import BASE_URL, THREADS

logger = get_logger(__name__)

# Каждые N записей сохраняем checkpoint
CHECKPOINT_EVERY = 100


@scraper_retry
def fetch_page(url: str) -> Optional[str]:
    """
    Загружает HTML страницы.
    Декоратор @scraper_retry автоматически повторит
    запрос до 5 раз если упадёт ConnectionError/TimeoutError.
    """
    
    html = fetch_page_playwright(url)
    return html

def parse_plate_page(html: str, plate_id: int, country: str) -> Optional[PlateRecord]:
    """
    Парсит HTML страницы одного номера.
    Структура сайта (проверено на реальных страницах):
    - Номер:  <h1>
    - Фото:   <img class="img-responsive center-block"> где src содержит /m/
    - Инфо:   <title> содержит "номер, Марка Модель (Регион)"
    """
    soup = BeautifulSoup(html, "lxml")

    # --- Проверка что это страница номера а не редирект ---
    title_tag = soup.find("title")
    title_text = title_tag.get_text(strip=True) if title_tag else ""
    if "верификац" in title_text.lower() or "verification" in title_text.lower():
        raise ConnectionError(f"KillBot page returned for id={plate_id}")

    # Проверяем H1 — если это каталог а не номер, пропускаем
    h1_check = soup.find("h1")
    h1_text = h1_check.get_text(strip=True) if h1_check else ""
    CATALOG_MARKERS = ["номера росси", "номера укра", "номера казах", "all license", "photos"]
    if any(m in h1_text.lower() for m in CATALOG_MARKERS):
        logger.debug(f"Catalog page returned for id={plate_id}, skipping")
        return None

    # --- Номер --- из <h1>
    plate_number = None
    h1 = soup.find("h1")
    if h1:
        plate_number = h1.get_text(strip=True)
    if not plate_number:
        logger.warning(f"No plate number found for id={plate_id}")
        return None

    # --- Фото --- img с классом img-responsive и /m/ в src (medium size)
    photo_url = None
    for img in soup.find_all("img", class_="img-responsive"):
        src = img.get("src", "")
        if "/m/" in src and "platesmania.com" in src:
            photo_url = src
            break
    if not photo_url:
        logger.warning(f"No photo found for id={plate_id}")
        return None

    # --- Марка/Модель/Регион --- парсим из title
    # Формат: "а639нм14, Lexus RX (Республика Саха (Якутия)) Номер России"
    car_brand, car_model, region = None, None, None
    if "," in title_text:
        rest = title_text.split(",", 1)[1].strip()  # "Lexus RX (Регион) Номер России"
        region_match = re.search(r"\(([^)]+)\)", rest)
        if region_match:
            region = region_match.group(1).strip()
        car_info = re.sub(r"\([^)]+\)", "", rest)  # убираем скобки
        car_info = re.sub(r"Номер.*$", "", car_info).strip()  # убираем "Номер России"
        parts = car_info.split(None, 1)  # ["Lexus", "RX"]
        if parts:
            car_brand = parts[0]
            car_model = parts[1] if len(parts) > 1 else None

    return PlateRecord(
        plate_id=plate_id,
        plate_number=plate_number,
        photo_url=photo_url,
        country=country,
        region=region,
        city=None,
        car_brand=car_brand,
        car_model=car_model,
        description=None,
    )


def _extract_meta(soup: BeautifulSoup, keyword: str) -> Optional[str]:
    """Ищет значение в мета-тегах по ключевому слову."""
    tag = soup.find("meta", attrs={"name": re.compile(keyword, re.I)})
    if tag:
        return tag.get("content", "").strip() or None
    return None


def _extract_text(soup: BeautifulSoup, keyword: str) -> Optional[str]:
    """Ищет текст в любом теге у которого class/id содержит ключевое слово."""
    tag = soup.find(class_=re.compile(keyword, re.I))
    if tag:
        return tag.get_text(strip=True) or None
    return None


def scrape_one(plate_id: int, country: str) -> Optional[PlateRecord]:
    """
    Полный цикл для одного номера:
    загрузить страницу → распарсить → вернуть запись.
    human_delay() вызывается ДО запроса — не после,
    чтобы задержка была даже если запрос упал.
    """
    human_delay()
    url = f"{BASE_URL}/{country}/nomer{plate_id}"
    html = fetch_page(url)
    if html is None:
        return None
    return parse_plate_page(html, plate_id, country)


def scrape_range(
    country: str,
    start_id: int,
    end_id: int,
    resume: bool = True,
) -> None:
    """
    Главная функция парсинга диапазона ID.
    
    Использует BrowserSession — один браузер на SESSION_SIZE записей.
    Это в 5-8 раз быстрее чем открывать браузер на каждый запрос.
    
    Поток работы:
    1. Открыть браузер → warmup (KillBot) → парсить SESSION_SIZE номеров
    2. Сохранить checkpoint → закрыть браузер
    3. Повторить с новой сессией
    """
    from core.anti_bot import SESSION_SIZE
    actual_start = load_checkpoint(country) if resume else start_id
    if actual_start > start_id:
        logger.info(f"Resuming from checkpoint: id={actual_start}")

    ids = list(range(actual_start, end_id + 1))
    total = len(ids)
    processed = 0
    batch: list[PlateRecord] = []

    logger.info(f"Starting: country={country}, ids={actual_start}..{end_id}, total={total}")

    # Разбиваем на сессии по SESSION_SIZE
    for session_start in range(0, total, SESSION_SIZE):
        session_ids = ids[session_start: session_start + SESSION_SIZE]
        logger.info(f"New browser session: {session_ids[0]}..{session_ids[-1]}")

        try:
            with BrowserSession(country) as session:
                for pid in session_ids:
                    try:
                        url = f"https://platesmania.com/{country}/nomer{pid}"
                        html = session.fetch(url)
                        if html:
                            record = parse_plate_page(html, pid, country)
                            if record:
                                # Скачиваем фото локально
                                local = download_photo(record.photo_url, pid, country)
                                record.local_path = local
                                batch.append(record)
                        processed += 1

                        if processed % CHECKPOINT_EVERY == 0:
                            saved = asyncio.run(save_batch(batch))
                            save_checkpoint(pid, country)
                            logger.info(
                                f"Progress: {processed}/{total} | "
                                f"saved={saved} | last_id={pid}"
                            )
                            batch.clear()

                    except Exception as e:
                        logger.error(f"Failed id={pid}: {e}")
                        continue  # Не останавливаемся

        except Exception as e:
            logger.error(f"Session error: {e} — restarting session")
            time.sleep(5)
            continue

    if batch:
        asyncio.run(save_batch(batch))

    logger.info(f"Done: country={country}, processed={processed}")
