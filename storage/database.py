import aiosqlite
import asyncio
import os
from typing import List, Optional
from storage.models import PlateRecord
from utils.logger import get_logger
from config.settings import DB_PATH

logger = get_logger(__name__)


async def init_db() -> None:
    """
    Создаёт таблицу если её нет.
    Вызывается один раз при старте парсера.
    """
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS plates (
                plate_id     INTEGER PRIMARY KEY,
                plate_number TEXT NOT NULL,
                photo_url    TEXT NOT NULL,
                country      TEXT NOT NULL,
                region       TEXT,
                city         TEXT,
                car_brand    TEXT,
                car_model    TEXT,
                description  TEXT,
                photo_date   TEXT,
                local_path   TEXT,
                scraped_at   TEXT NOT NULL
            )
        """)
        # Индекс для быстрого поиска по стране
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_country 
            ON plates(country)
        """)
        await db.commit()
    logger.info(f"Database ready: {DB_PATH}")


async def save_record(record: PlateRecord) -> bool:
    """
    Сохраняет одну запись.
    INSERT OR IGNORE — если ID уже есть, просто пропускаем.
    Возвращает True если запись новая, False если дубликат.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT OR IGNORE INTO plates 
            (plate_id, plate_number, photo_url, country, region, city,
             car_brand, car_model, description, photo_date, local_path, scraped_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            record.plate_id, record.plate_number, record.photo_url,
            record.country, record.region, record.city,
            record.car_brand, record.car_model, record.description,
            record.photo_date, record.local_path, record.scraped_at,
        ))
        await db.commit()
        return cursor.rowcount > 0  # rowcount=1 новая, rowcount=0 дубликат


async def save_batch(records: List[PlateRecord]) -> int:
    """
    Сохраняет список записей одной транзакцией.
    Намного быстрее чем save_record по одной.
    Возвращает количество новых записей.
    """
    if not records:
        return 0
    async with aiosqlite.connect(DB_PATH) as db:
        saved = 0
        for record in records:
            cursor = await db.execute("""
                INSERT OR IGNORE INTO plates 
                (plate_id, plate_number, photo_url, country, region, city,
                 car_brand, car_model, description, photo_date, local_path, scraped_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                record.plate_id, record.plate_number, record.photo_url,
                record.country, record.region, record.city,
                record.car_brand, record.car_model, record.description,
                record.photo_date, record.local_path, record.scraped_at,
            ))
            saved += cursor.rowcount
        await db.commit()
    logger.debug(f"Batch saved: {saved}/{len(records)} new records")
    return saved


async def get_count(country: Optional[str] = None) -> int:
    """Сколько записей уже собрано."""
    async with aiosqlite.connect(DB_PATH) as db:
        if country:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM plates WHERE country=?", (country,)
            )
        else:
            cursor = await db.execute("SELECT COUNT(*) FROM plates")
        row = await cursor.fetchone()
        return row[0] if row else 0
