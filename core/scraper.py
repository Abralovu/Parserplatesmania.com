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
    "license plates of",  
]

_COUNTRY_NAME_TO_CODE: dict[str, str] = {
    "russia": "ru",
    "ukraine": "ua",
    "germany": "de",
    "poland": "pl",
    "kazakhstan": "kz",
    "belarus": "by",
    "usa": "us",
    "united states": "us",
    "united kingdom": "gb",
    "france": "fr",
    "italy": "it",
    "spain": "es",
    "netherlands": "nl",
    "belgium": "be",
    "austria": "at",
    "switzerland": "ch",
    "sweden": "se",
    "norway": "no",
    "finland": "fi",
    "denmark": "dk",
    "lithuania": "lt",
    "latvia": "lv",
    "estonia": "ee",
    "czech republic": "cz",
    "czechia": "cz",
    "slovakia": "sk",
    "hungary": "hu",
    "romania": "ro",
    "bulgaria": "bg",
    "serbia": "rs",
    "croatia": "hr",
    "greece": "gr",
    "portugal": "pt",
    "turkey": "tr",
    "armenia": "am",
    "georgia": "ge",
    "azerbaijan": "az",
    "uzbekistan": "uz",
    "kyrgyzstan": "kg",
    "moldova": "md",
    "israel": "il",
    "uae": "ae",
    "united arab emirates": "ae",
    "china": "cn",
    "japan": "jp",
    "south korea": "kr",
    "korea": "kr",
}


def extract_country_from_title(title_text: str) -> Optional[str]:
    lower = title_text.lower()

    if "license plates of" in lower:
        return None

    match = re.search(r"license plate\s+(.+)$", lower)
    if not match:
        return None

    country_name = match.group(1).strip()
    return _COUNTRY_NAME_TO_CODE.get(country_name)


def parse_plate_page(html: str, plate_id: int, country: str) -> Optional[PlateRecord]:
    soup = BeautifulSoup(html, "lxml")

    title_text = _get_title_text(soup)
    _check_killbot(title_text, plate_id)


    real_country = extract_country_from_title(title_text)

    if not real_country:
        logger.debug(f"Skip id={plate_id}: not a plate page (title={title_text[:60]})")
        return None

    if country != "global" and real_country != country:
        logger.debug(f"Skip id={plate_id}: country={real_country}, expected={country}")
        return None

    h1_text = _get_h1_text(soup)
    if _is_skip_page(h1_text):
        logger.debug(f"Skip id={plate_id}: skip marker in h1")
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
        country=real_country,  
        region=region,
        city=None,
        car_brand=brand,
        car_model=model,
        description=None,
    )


def scrape_range(country: str, start_id: int, end_id: int, resume: bool = True) -> None:

    actual_start = load_checkpoint(country, start_id) if resume else start_id

    ids = range(actual_start, end_id + 1)
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

    if batch:
        saved = sync_save_batch(batch)
        logger.info(f"Final batch saved: {saved} records")

    total_saved = sync_get_count(country) if country != "global" else -1
    logger.info(f"Done: country={country}, processed={processed}, total in DB={total_saved}")


def _run_session(
    session_ids: list,
    country: str,
    batch: list,
    processed: int,
    total: int,
) -> tuple[list, int]:
    try:
        warmup_country = country if country != "global" else "ua"
        with BrowserSession(warmup_country) as session:
            for pid in session_ids:
                if stop_event.is_set():
                    logger.info("Stop signal — halting scraper")
                    break

                try:
                    if sync_id_exists(pid):
                        logger.debug(f"Skip existing id={pid}")
                        processed += 1
                        continue
                    
                    if country == "global":
                        url = f"{BASE_URL}/ru/nomer{pid}"
                    else:
                        url = f"{BASE_URL}/{country}/nomer{pid}"

                    html = session.fetch(url)
                    if not html:
                        processed += 1
                        continue

                    record = parse_plate_page(html, pid, country)
                    if record:
                        local = download_photo(record.photo_url, pid, record.country)
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
    if "," not in title_text:
        return None, None, None

    rest = title_text.split(",", 1)[1].strip()

    region_match = re.search(r"\(([^()]+)\)\s*(?:License plate|Номер)", rest, re.IGNORECASE)
    if not region_match:
        region_match = re.search(r"\(([^()]+)\)", rest)
    region = region_match.group(1).strip() if region_match else None

    car_info = re.sub(r"\([^()]*\)", "", rest)
    car_info = re.sub(r"\s*(?:License plate|Номер).*$", "", car_info, flags=re.IGNORECASE)
    car_info = car_info.strip(" ,)(")

    parts = car_info.split(None, 1)
    if not parts:
        return None, None, region

    brand = parts[0].strip(" ,)(") or None
    model = parts[1].strip(" ,)(") if len(parts) > 1 else None
    if model in (")", "(", "))", ")(", ""):
        model = None

    return brand, model, region