import csv
import os
from typing import Optional

import aiosqlite

from config.settings import DB_PATH, OUTPUT_DIR
from utils.logger import get_logger

logger = get_logger(__name__)


async def export_csv(
    country: Optional[str] = None,
    region: Optional[str] = None,
    car_brand: Optional[str] = None,
    output_file: Optional[str] = None,
) -> Optional[str]:
    """
    Экспортирует данные в CSV.
    Фильтры: country, region, car_brand.
    Сортировка: страна → регион → марка → ID.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    conditions = []
    params = []

    if country:
        conditions.append("country=?")
        params.append(country)
    if region:
        conditions.append("region LIKE ?")
        params.append(f"%{region}%")
    if car_brand:
        conditions.append("car_brand LIKE ?")
        params.append(f"%{car_brand}%")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM plates {where} ORDER BY country, region, car_brand, plate_id"

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(sql, params)
        rows = await cursor.fetchall()

    if not rows:
        logger.warning("No data to export")
        return None

    if not output_file:
        parts = []
        if country:
            parts.append(country)
        if region:
            parts.append(region.replace(" ", "_")[:20])
        if car_brand:
            parts.append(car_brand.replace(" ", "_")[:15])
        suffix = f"_{'_'.join(parts)}" if parts else "_all"
        output_file = os.path.join(OUTPUT_DIR, f"plates{suffix}.csv")

    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "plate_id", "plate_number", "photo_url", "country",
            "region", "city", "car_brand", "car_model",
            "description", "photo_date", "local_path", "scraped_at",
        ])
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))

    logger.info(f"CSV exported: {output_file} ({len(rows)} records)")
    return output_file