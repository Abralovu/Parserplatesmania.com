#!/usr/bin/env python3
"""
Database layer — два уровня:
  async (aiosqlite) — для FastAPI endpoints
  sync  (sqlite3)   — для парсера, без конфликта event loop
Автор: viramax
"""

import os
import sqlite3
import threading
from typing import List, Optional

import aiosqlite

from storage.models import PlateRecord
from utils.logger import get_logger
from config.settings import DB_PATH

logger = get_logger(__name__)

# ─── SQL константы ────────────────────────────────────────────────────────────

# DDL один раз — используется в async и sync init, нет дублирования
_DDL_CREATE_TABLE = """
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
"""

_DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_country        ON plates(country)",
    "CREATE INDEX IF NOT EXISTS idx_country_region ON plates(country, region)",
    "CREATE INDEX IF NOT EXISTS idx_country_brand  ON plates(country, car_brand)",
]

_INSERT_SQL = """
    INSERT OR IGNORE INTO plates
    (plate_id, plate_number, photo_url, country, region, city,
     car_brand, car_model, description, photo_date, local_path, scraped_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
"""

# Один sync connection на поток — thread-safe
_thread_local = threading.local()


# ─── Инициализация ────────────────────────────────────────────────────────────

async def init_db() -> None:
    """
    Создаёт таблицу и индексы если их нет.
    Вызывается один раз при старте FastAPI и CLI.
    """
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(_DDL_CREATE_TABLE)
        for idx_sql in _DDL_INDEXES:
            await db.execute(idx_sql)
        await db.commit()

    logger.info(f"Database ready: {DB_PATH}")


def sync_init_db() -> None:
    """
    Синхронная инициализация БД — для парсера.
    Использует те же DDL константы что и async версия — нет дублирования.
    """
    conn = _get_sync_conn()
    conn.execute(_DDL_CREATE_TABLE)
    for idx_sql in _DDL_INDEXES:
        conn.execute(idx_sql)
    conn.commit()
    logger.info(f"Database ready (sync): {DB_PATH}")


# ─── Async (FastAPI) ──────────────────────────────────────────────────────────

async def save_batch(records: List[PlateRecord]) -> int:
    """
    Сохраняет батч записей.
    Использует cursor.rowcount вместо SELECT COUNT — на один запрос меньше.
    """
    if not records:
        return 0

    tuples = [_record_to_tuple(r) for r in records]

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.executemany(_INSERT_SQL, tuples)
        await db.commit()
        # rowcount после INSERT OR IGNORE — количество реально вставленных строк
        saved = cursor.rowcount

    logger.debug(f"Batch saved: {saved}/{len(records)} records")
    return saved


async def id_exists(plate_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM plates WHERE plate_id=? LIMIT 1", (plate_id,)
        )
        return await cursor.fetchone() is not None


async def get_count(country: Optional[str] = None) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        if country:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM plates WHERE country=?", (country,)
            )
        else:
            cursor = await db.execute("SELECT COUNT(*) FROM plates")
        row = await cursor.fetchone()
        return row[0] if row else 0


async def get_counts_by_country() -> dict[str, int]:
    """
    Один GROUP BY запрос вместо N+1 отдельных SELECT COUNT.
    Используется в /api/status вместо цикла по всем странам.
    """
    if not os.path.exists(DB_PATH):
        return {}
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT country, COUNT(*) FROM plates GROUP BY country"
        )
        rows = await cursor.fetchall()
    return {row[0]: row[1] for row in rows}


async def get_records(
    country: Optional[str] = None,
    region: Optional[str] = None,
    car_brand: Optional[str] = None,
    limit: Optional[int] = None,
    offset: int = 0,
) -> List[PlateRecord]:
    conditions: list[str] = []
    params: list = []

    if country:
        conditions.append("country=?")
        params.append(country)
    if region:
        conditions.append("region=?")
        params.append(region)
    if car_brand:
        conditions.append("car_brand=?")
        params.append(car_brand)

    where        = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit_clause = "LIMIT ? OFFSET ?" if limit else ""
    if limit:
        params.extend([limit, offset])

    sql = f"SELECT * FROM plates {where} ORDER BY plate_id {limit_clause}"

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(sql, params)
        rows   = await cursor.fetchall()

    return [_row_to_record(row) for row in rows]


# ─── Sync (парсер) ────────────────────────────────────────────────────────────

def _get_sync_conn() -> sqlite3.Connection:
    if not hasattr(_thread_local, "conn"):
        db_dir = os.path.dirname(os.path.abspath(DB_PATH))
        os.makedirs(db_dir, exist_ok=True)
        conn = sqlite3.connect(os.path.abspath(DB_PATH), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _thread_local.conn = conn
    return _thread_local.conn


def sync_save_batch(records: list) -> int:
    """
    Синхронная запись батча — для парсера.
    rowcount после executemany — количество реально вставленных строк.
    """
    if not records:
        return 0

    conn   = _get_sync_conn()
    tuples = [_record_to_tuple(r) for r in records]

    try:
        cursor = conn.execute("BEGIN")  # явная транзакция
        cursor = conn.executemany(_INSERT_SQL, tuples)
        conn.commit()
        return cursor.rowcount
    except sqlite3.Error as e:
        logger.error(f"sync_save_batch error: {e}")
        conn.rollback()
        return 0


def sync_id_exists(plate_id: int) -> bool:
    conn   = _get_sync_conn()
    cursor = conn.execute(
        "SELECT 1 FROM plates WHERE plate_id=? LIMIT 1", (plate_id,)
    )
    return cursor.fetchone() is not None


def sync_get_count(country: Optional[str] = None) -> int:
    conn = _get_sync_conn()
    if country:
        cursor = conn.execute(
            "SELECT COUNT(*) FROM plates WHERE country=?", (country,)
        )
    else:
        cursor = conn.execute("SELECT COUNT(*) FROM plates")
    row = cursor.fetchone()
    return row[0] if row else 0


# ─── Утилиты ──────────────────────────────────────────────────────────────────

def _record_to_tuple(record: PlateRecord) -> tuple:
    return (
        record.plate_id,    record.plate_number, record.photo_url,
        record.country,     record.region,       record.city,
        record.car_brand,   record.car_model,    record.description,
        record.photo_date,  record.local_path,   record.scraped_at,
    )


def _row_to_record(row: aiosqlite.Row) -> PlateRecord:
    return PlateRecord(
        plate_id=row["plate_id"],
        plate_number=row["plate_number"],
        photo_url=row["photo_url"],
        country=row["country"],
        region=row["region"],
        city=row["city"],
        car_brand=row["car_brand"],
        car_model=row["car_model"],
        description=row["description"],
        photo_date=row["photo_date"],
        local_path=row["local_path"],
        scraped_at=row["scraped_at"],
    )