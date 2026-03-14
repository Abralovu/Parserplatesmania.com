import asyncio
import csv
import os
import aiosqlite
from config.settings import DB_PATH, OUTPUT_DIR
from utils.logger import get_logger

logger = get_logger(__name__)


async def export_csv(country: str = None, output_file: str = None) -> str:
    """
    Экспортирует данные из БД в CSV.
    Если country указана — только по этой стране.
    Возвращает путь к файлу.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not output_file:
        suffix = f"_{country}" if country else "_all"
        output_file = os.path.join(OUTPUT_DIR, f"plates{suffix}.csv")

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if country:
            cursor = await db.execute(
                "SELECT * FROM plates WHERE country=? ORDER BY plate_id",
                (country,)
            )
        else:
            cursor = await db.execute("SELECT * FROM plates ORDER BY plate_id")

        rows = await cursor.fetchall()

    if not rows:
        logger.warning("No data to export")
        return None

    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        # utf-8-sig — Excel на Windows открывает без кракозябр
        writer = csv.DictWriter(f, fieldnames=[
            "plate_id", "plate_number", "photo_url", "country",
            "region", "city", "car_brand", "car_model",
            "description", "photo_date", "local_path", "scraped_at"
        ])
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))

    logger.info(f"Exported {len(rows)} records to {output_file}")
    return output_file
