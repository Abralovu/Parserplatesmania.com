#!/usr/bin/env python3
"""
Core scraper — парсит диапазон ID для указанной страны.
Использует синхронный sqlite3 — без конфликта event loop.
Автор: viramax
"""

import re
import time
from typing import Optional

from bs4 import BeautifulSoup

from config.settings import BASE_URL, SESSION_SIZE, CHECKPOINT_EVERY, stop_event
from core.anti_bot import BrowserSession
from core.downloader import download_photo
from storage.database import sync_save_batch, sync_id_exists, sync_get_count
from storage.models import PlateRecord
from utils.checkpoint import load_checkpoint, save_checkpoint
from utils.logger import get_logger

logger = get_logger(__name__)

_SKIP_MARKERS = [
    "номера росси", "номера укра", "номера казах",
    "all license", "photos", "номера нет на сайте", "not found",
]


def parse_plate_page(html: str, plate_id: int, country: str) -> Optional[PlateRecord]:
    """
    Парсит HTML страницы одного номера.
    Возвращает PlateRecord или None если страница не является номером.
    Raises ConnectionError если обнаружена страница KillBot.
    """
    soup = BeautifulSoup(html, "lxml")

    title_text = _get_title_text(soup)
    _check_killbot(title_text, plate_id)

    h1_text = _get_h1_text(soup)
    if _is_skip_page(h1_text):
        logger.debug(f"Skip id={plate_id}: not a plate page")
        return None

    plate_number = h1_text or None
    if not plate_number:
        logger.warning(f"No plate number for id={plate_id}")
        return None

    photo_url = _extract_photo_url(soup, plate_id)
    if not photo_url:
        return None

    brand, model, region = _extract_car_info(title_text)

    return PlateRecord(
        plate_id=plate_id,
        plate_number=plate_number,
        photo_url=photo_url,
        country=country,
        region=region,
        city=None,
        car_brand=brand,
        car_model=model,
        description=None,
    )


def scrape_range(country: str, start_id: int, end_id: int, resume: bool = True) -> None:
    """
    Парсит диапазон ID для указанной страны.
    range() без list() — не загружает миллион элементов в память.
    """
    actual_start = load_checkpoint(country, start_id) if resume else start_id

    # range() — ленивый генератор, не list. При 1M ID не выделяем 1M элементов.
    ids   = range(actual_start, end_id + 1)
    total = end_id - actual_start + 1

    if total <= 0:
        logger.warning(f"Empty range: start={actual_start} > end={end_id}")
        return

    logger.info(f"Starting: country={country}, ids={actual_start}..{end_id}, total={total}")

    processed = 0
    batch: list[PlateRecord] = []

    for session_start in range(0, total, SESSION_SIZE):
        if stop_event.is_set():
            break

        session_ids = list(ids[session_start: session_start + SESSION_SIZE])
        logger.info(f"Session: {session_ids[0]}..{session_ids[-1]}")

        batch, processed = _run_session(
            session_ids=session_ids,
            country=country,
            batch=batch,
            processed=processed,
            total=total,
        )

    # Финальный flush
    if batch:
        saved = sync_save_batch(batch)
        logger.info(f"Final batch saved: {saved} records")

    total_saved = sync_get_count(country)
    logger.info(f"Done: country={country}, processed={processed}, total in DB={total_saved}")


def _run_session(
    session_ids: list,
    country: str,
    batch: list,
    processed: int,
    total: int,
) -> tuple[list, int]:
    """
    Одна браузерная сессия для части диапазона.
    stop_event импортируется на уровне модуля — нет import внутри цикла.
    """
    try:
        with BrowserSession(country) as session:
            for pid in session_ids:
                if stop_event.is_set():
                    logger.info("Stop signal — halting scraper")
                    break

                try:
                    if sync_id_exists(pid):
                        logger.debug(f"Skip existing id={pid}")
                        processed += 1
                        continue

                    url  = f"{BASE_URL}/{country}/nomer{pid}"
                    html = session.fetch(url)
                    if not html:
                        processed += 1
                        continue

                    record = parse_plate_page(html, pid, country)
                    if record:
                        local             = download_photo(record.photo_url, pid, country)
                        record.local_path = local
                        batch.append(record)

                    processed += 1

                    if processed % CHECKPOINT_EVERY == 0:
                        saved = sync_save_batch(batch)
                        save_checkpoint(pid, country)
                        batch.clear()
                        logger.info(
                            f"Progress: {processed}/{total} | "
                            f"saved={saved} | last_id={pid}"
                        )

                except ConnectionError as e:
                    logger.warning(f"KillBot on id={pid}: {e}")
                    processed += 1
                    continue
                except Exception as e:
                    logger.error(f"Failed id={pid}: {e}")
                    processed += 1
                    continue

    except Exception as e:
        logger.error(f"Session crashed: {e} — saving batch and restarting in 5s")
        if batch:
            sync_save_batch(batch)
            batch.clear()
        time.sleep(5)

    return batch, processed


# ─── Приватные парсеры HTML ───────────────────────────────────────────────────

def _get_title_text(soup: BeautifulSoup) -> str:
    tag = soup.find("title")
    return tag.get_text(strip=True) if tag else ""


def _get_h1_text(soup: BeautifulSoup) -> str:
    tag = soup.find("h1")
    return tag.get_text(strip=True) if tag else ""


def _check_killbot(title_text: str, plate_id: int) -> None:
    lower = title_text.lower()
    if "верификац" in lower or "verification" in lower:
        raise ConnectionError(f"KillBot page for id={plate_id}")


def _is_skip_page(h1_text: str) -> bool:
    lower = h1_text.lower()
    return any(m in lower for m in _SKIP_MARKERS)


def _extract_photo_url(soup: BeautifulSoup, plate_id: int) -> Optional[str]:
    for img in soup.find_all("img", class_="img-responsive"):
        src = img.get("src", "")
        if "/m/" in src and "platesmania.com" in src:
            return src
    logger.warning(f"No photo for id={plate_id}")
    return None


def _extract_car_info(
    title_text: str,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Парсит марку, модель, регион из title тега."""
    if "," not in title_text:
        return None, None, None

    rest = title_text.split(",", 1)[1].strip()

    region_match = re.search(r"\(([^()]+)\)\s*Номер", rest)
    if not region_match:
        region_match = re.search(r"\(([^()]+)\)", rest)
    region = region_match.group(1).strip() if region_match else None

    car_info = re.sub(r"\([^()]*\)", "", rest)
    car_info = re.sub(r"\s*Номер.*$", "", car_info, flags=re.IGNORECASE).strip(" ,)(")

    parts = car_info.split(None, 1)
    if not parts:
        return None, None, region

    brand = parts[0].strip(" ,)(") or None
    model = parts[1].strip(" ,)(") if len(parts) > 1 else None
    if model in (")", "(", "))", ")(", ""):
        model = None

    return brand, model, region