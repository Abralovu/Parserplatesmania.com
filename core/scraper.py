import re
import time
import asyncio
from typing import Optional
from bs4 import BeautifulSoup

from core.anti_bot import BrowserSession, SESSION_SIZE
from core.downloader import download_photo
from storage.models import PlateRecord
from storage.database import save_batch
from utils.logger import get_logger
from utils.checkpoint import save_checkpoint, load_checkpoint
from config.settings import BASE_URL

logger = get_logger(__name__)

CHECKPOINT_EVERY = 100


def parse_plate_page(html: str, plate_id: int, country: str) -> Optional[PlateRecord]:
    """
    Парсит HTML страницы одного номера.
    Структура сайта:
      Номер  → <h1>
      Фото   → <img class="img-responsive"> с /m/ в src
      Инфо   → <title>: "номер, Марка Модель (Регион) Номер Страны"
    """
    soup = BeautifulSoup(html, "lxml")

    # Защита от KillBot страниц
    title_tag = soup.find("title")
    title_text = title_tag.get_text(strip=True) if title_tag else ""
    if "верификац" in title_text.lower() or "verification" in title_text.lower():
        raise ConnectionError(f"KillBot page for id={plate_id}")

    # Защита от страниц каталога
    h1_tag = soup.find("h1")
    h1_text = h1_tag.get_text(strip=True) if h1_tag else ""
    SKIP_MARKERS = [
        "номера росси", "номера укра", "номера казах",
        "all license", "photos", "номера нет на сайте", "not found"
    ]
    if any(m in h1_text.lower() for m in SKIP_MARKERS):
        logger.debug(f"Skip id={plate_id}: not a plate page")
        return None

    # Номер
    plate_number = h1_text or None
    if not plate_number:
        logger.warning(f"No plate number for id={plate_id}")
        return None

    # Фото — img с /m/ в src (medium размер)
    photo_url = None
    for img in soup.find_all("img", class_="img-responsive"):
        src = img.get("src", "")
        if "/m/" in src and "platesmania.com" in src:
            photo_url = src
            break
    if not photo_url:
        logger.warning(f"No photo for id={plate_id}")
        return None

    # Марка / Модель / Регион из title
    # Формат: "а639нм14, Lexus RX (Республика Саха (Якутия)) Номер России"
    car_brand, car_model, region = None, None, None
    if "," in title_text:
        rest = title_text.split(",", 1)[1].strip()
        # Регион — последние скобки перед "Номер"
        region_match = re.search(r"\(([^()]+)\)\s*Номер", rest)
        if not region_match:
            region_match = re.search(r"\(([^()]+)\)", rest)
        if region_match:
            region = region_match.group(1).strip()
        # Марка и модель — убираем скобки и хвост
        car_info = re.sub(r"\([^()]*\)", "", rest)
        car_info = re.sub(r"\s*Номер.*$", "", car_info, flags=re.IGNORECASE).strip(" ,)(")
        parts = car_info.split(None, 1)
        if parts:
            car_brand = parts[0].strip(" ,)(")
            car_model = parts[1].strip(" ,)(") if len(parts) > 1 else None
            if car_model in (")", "(", "))", ")(", ""):
                car_model = None

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


def scrape_range(country: str, start_id: int, end_id: int, resume: bool = True) -> None:
    """
    Парсит диапазон ID для указанной страны.
    
    resume=True  — продолжает с последнего checkpoint
    resume=False — начинает с start_id (--fresh флаг)
    
    Сессии по SESSION_SIZE записей:
    браузер открывается один раз → парсит SESSION_SIZE страниц → закрывается
    это предотвращает утечки памяти при многочасовом парсинге
    """
    actual_start = load_checkpoint(country, start_id) if resume else start_id
    if actual_start > start_id:
        logger.info(f"Resuming from checkpoint: id={actual_start}")

    ids = list(range(actual_start, end_id + 1))
    total = len(ids)
    processed = 0
    batch: list[PlateRecord] = []

    logger.info(f"Starting: country={country}, ids={actual_start}..{end_id}, total={total}")

    for session_start in range(0, total, SESSION_SIZE):
        session_ids = ids[session_start: session_start + SESSION_SIZE]
        logger.info(f"Session: {session_ids[0]}..{session_ids[-1]}")

        try:
            with BrowserSession(country) as session:
                for pid in session_ids:
                    try:
                        url = f"{BASE_URL}/{country}/nomer{pid}"
                        html = session.fetch(url)
                        if not html:
                            processed += 1
                            continue

                        record = parse_plate_page(html, pid, country)
                        if record:
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
                        processed += 1
                        continue

        except Exception as e:
            logger.error(f"Session crashed: {e} — restarting in 5s")
            time.sleep(5)
            continue

    if batch:
        asyncio.run(save_batch(batch))

    total_saved = asyncio.run(__import__("storage.database", fromlist=["get_count"]).get_count(country))
    logger.info(f"Done: country={country}, processed={processed}, total in DB={total_saved}")
