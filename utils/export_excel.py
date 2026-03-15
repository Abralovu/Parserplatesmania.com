import asyncio
import os
import aiosqlite
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from config.settings import DB_PATH, OUTPUT_DIR
from utils.logger import get_logger

logger = get_logger(__name__)


async def export_excel_with_photos(country: str = None) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    suffix = f"_{country}" if country else "_all"
    output_file = os.path.join(OUTPUT_DIR, f"plates{suffix}.xlsx")
    # Абсолютный путь — openpyxl требует
    output_file = os.path.abspath(output_file)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if country:
            cursor = await db.execute(
                "SELECT * FROM plates WHERE country=? ORDER BY plate_id", (country,)
            )
        else:
            cursor = await db.execute("SELECT * FROM plates ORDER BY plate_id")
        rows = await cursor.fetchall()

    if not rows:
        logger.warning("No data to export")
        return None

    wb = Workbook()
    ws = wb.active
    ws.title = "Plates"

    # --- Заголовки ---
    headers = ["Фото", "Номер", "Страна", "Регион", "Марка", "Модель", "Ссылка на фото", "Дата"]
    header_fill = PatternFill("solid", fgColor="1F3864")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    thin = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )

    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = thin
    ws.row_dimensions[1].height = 25

    # Фиксированная высота строки в Excel единицах
    # 1 pt ≈ 0.75 px, нам нужно ~80px = ~60pt
    ROW_HEIGHT_PT = 70
    # Ширина колонки A в символах (примерно 130px / 7px на символ ≈ 18)
    COL_A_WIDTH = 20

    for row_idx, record in enumerate(rows, 2):
        ws.row_dimensions[row_idx].height = ROW_HEIGHT_PT

        row_fill = PatternFill("solid", fgColor="EBF0FA" if row_idx % 2 == 0 else "FFFFFF")

        # --- Фото ---
        local_path = record["local_path"]
        if local_path:
            abs_path = os.path.abspath(local_path)
            if os.path.exists(abs_path):
                try:
                    img = XLImage(abs_path)
                    img.height = 85   # px
                    img.width = 125   # px
                    # anchor — точная привязка к ячейке A{row_idx}
                    img.anchor = f"A{row_idx}"
                    ws.add_image(img)
                except Exception as e:
                    ws.cell(row=row_idx, column=1, value="ошибка фото")
                    logger.warning(f"Image error id={record['plate_id']}: {e}")
            else:
                ws.cell(row=row_idx, column=1, value="нет файла")
        else:
            ws.cell(row=row_idx, column=1, value="нет фото")

        # --- Данные ---
        data = [
            record["plate_number"],
            record["country"].upper() if record["country"] else "—",
            record["region"] or "—",
            record["car_brand"] or "—",
            record["car_model"] or "—",
            record["photo_url"] or "—",
            record["scraped_at"][:10] if record["scraped_at"] else "—",
        ]
        for col_idx, value in enumerate(data, 2):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.fill = row_fill
            cell.alignment = Alignment(vertical="center", horizontal="left")
            cell.border = thin

    # --- Ширина колонок ---
    widths = [COL_A_WIDTH, 14, 8, 25, 16, 14, 55, 12]
    for col, width in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(col)].width = width

    ws.freeze_panes = "B2"

    wb.save(output_file)
    logger.info(f"Excel exported: {output_file} ({len(rows)} records)")
    return output_file
