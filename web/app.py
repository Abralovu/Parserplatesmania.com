#!/usr/bin/env python3
"""
FastAPI веб-панель — управление парсером через браузер.
WebSocket — прогресс в реальном времени.
Автор: viramax
"""

import asyncio
import json
import os
import shutil
from contextlib import asynccontextmanager
from datetime import datetime
from queue import Empty, Queue
from typing import Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from storage.database import get_count, get_records, init_db
from utils.checkpoint import get_all_checkpoints
from utils.logger import get_logger
from config.settings import OUTPUT_DIR, DB_PATH

logger = get_logger(__name__)

_ALL_COUNTRIES = [
    "ru", "de", "pl", "ua", "by", "kz", "us", "gb", "fr", "it",
    "es", "nl", "be", "at", "ch", "se", "no", "fi", "dk", "lt",
    "lv", "ee", "cz", "sk", "hu", "ro", "bg", "rs", "hr", "gr",
    "pt", "tr", "am", "ge", "az", "uz", "kg", "md", "il", "ae",
    "cn", "jp", "kr",
]

# Файл истории сессий
_HISTORY_FILE = os.path.join(OUTPUT_DIR, "history.json")

_scrape_state = {
    "is_running": False,
    "country": None,
    "start_id": 0,
    "end_id": 0,
    "processed": 0,
    "saved": 0,
    "current_id": 0,
    "workers": 1,
    "error": None,
    "started_at": None,
}

_progress_queue: Queue = Queue()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("Web panel ready")
    yield


app = FastAPI(title="PlatesMania Scraper", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="web/static"), name="static")


# ─── HTML ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    html_path = os.path.join("web", "static", "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


# ─── API ──────────────────────────────────────────────────────────────────────

@app.get("/api/status")
async def get_status():
    stats = {}
    for country in _ALL_COUNTRIES:
        count = await get_count(country)
        if count > 0:
            stats[country] = count
    return {
        "scraper": _scrape_state,
        "database": stats,
        "checkpoints": get_all_checkpoints(),
    }


@app.post("/api/scrape/start")
async def start_scrape(body: dict):
    global _scrape_state

    if _scrape_state["is_running"]:
        return {"ok": False, "error": "Парсер уже запущен"}

    country = body.get("country", "ru")
    workers = int(body.get("workers", 1))
    fresh = bool(body.get("fresh", False))
    auto = bool(body.get("auto", False))
    start_id = int(body.get("start_id", 1))
    end_id = int(body.get("end_id", 100_000))

    _scrape_state.update({
        "is_running": True,
        "country": country,
        "workers": workers,
        "processed": 0,
        "saved": 0,
        "error": None,
        "started_at": datetime.utcnow().isoformat(),
    })

    asyncio.get_event_loop().run_in_executor(
        None,
        _run_scraper_sync,
        country, workers, fresh, auto, start_id, end_id,
    )

    label = "ВСЕ СТРАНЫ" if country == "all" else country.upper()
    return {"ok": True, "message": f"Запущен: {label}"}


@app.post("/api/scrape/stop")
async def stop_scrape():
    global _scrape_state
    _scrape_state["is_running"] = False
    from config.settings import stop_event
    stop_event.set()
    return {"ok": True, "message": "Остановка"}


@app.get("/api/records")
async def get_filtered_records(
    country: Optional[str] = None,
    region: Optional[str] = None,
    car_brand: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    records = await get_records(
        country=country, region=region,
        car_brand=car_brand, limit=limit, offset=offset,
    )
    return {"records": [r.to_dict() for r in records], "count": len(records)}


@app.get("/api/download/excel")
async def download_excel(
    country: Optional[str] = None,
    region: Optional[str] = None,
    car_brand: Optional[str] = None,
):
    from utils.export_excel import export_excel_with_photos
    path = await export_excel_with_photos(
        country=country if country and country != "all" else None,
        region=region or None,
        car_brand=car_brand or None,
    )
    if path and os.path.exists(path):
        return FileResponse(
            path=path, filename=os.path.basename(path),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    return {"error": "Нет данных"}


@app.get("/api/download/csv")
async def download_csv(
    country: Optional[str] = None,
    region: Optional[str] = None,
    car_brand: Optional[str] = None,
):
    from utils.export import export_csv
    path = await export_csv(
        country=country if country and country != "all" else None,
        region=region or None,
        car_brand=car_brand or None,
    )
    if path and os.path.exists(path):
        return FileResponse(
            path=path, filename=os.path.basename(path),
            media_type="text/csv",
        )
    return {"error": "Нет данных"}


# ─── История сессий ───────────────────────────────────────────────────────────

@app.get("/api/history")
async def get_history():
    """Возвращает историю всех сессий парсинга."""
    if not os.path.exists(_HISTORY_FILE):
        return {"sessions": []}
    try:
        with open(_HISTORY_FILE, "r", encoding="utf-8") as f:
            return {"sessions": json.load(f)}
    except Exception:
        return {"sessions": []}


# ─── Сброс данных ─────────────────────────────────────────────────────────────

@app.post("/api/reset")
async def reset_all(body: dict):
    """
    Полный сброс: база данных, фото, checkpoint, профили.
    Требует подтверждения: { "confirm": true }
    """
    if not body.get("confirm"):
        return {"ok": False, "error": "Требуется подтверждение"}

    if _scrape_state["is_running"]:
        return {"ok": False, "error": "Сначала остановите парсер"}

    errors = []

    # Удаляем базу данных
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except Exception as e:
            errors.append(f"DB: {e}")

    # Удаляем фото
    photos_dir = os.path.join(OUTPUT_DIR, "photos")
    if os.path.exists(photos_dir):
        try:
            shutil.rmtree(photos_dir)
        except Exception as e:
            errors.append(f"Photos: {e}")

    # Удаляем checkpoint
    checkpoint_file = os.path.join(OUTPUT_DIR, "checkpoint.json")
    if os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
        except Exception as e:
            errors.append(f"Checkpoint: {e}")

    # Удаляем профили
    profiles_dir = os.path.join(OUTPUT_DIR, "profiles")
    if os.path.exists(profiles_dir):
        try:
            shutil.rmtree(profiles_dir)
        except Exception as e:
            errors.append(f"Profiles: {e}")

    # Пересоздаём базу
    await init_db()

    if errors:
        return {"ok": False, "error": " | ".join(errors)}

    logger.info("Full reset completed")
    return {"ok": True, "message": "Все данные удалены"}


# ─── WebSocket ────────────────────────────────────────────────────────────────

@app.websocket("/ws/progress")
async def websocket_progress(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket client connected")
    try:
        while True:
            messages = []
            while True:
                try:
                    messages.append(_progress_queue.get_nowait())
                except Empty:
                    break

            if messages:
                last = messages[-1]
                _scrape_state.update({
                    "processed": last.processed,
                    "saved": last.saved,
                    "current_id": last.current_id,
                })
                await websocket.send_json({
                    "type": "progress",
                    "processed": last.processed,
                    "saved": last.saved,
                    "current_id": last.current_id,
                    "is_done": last.is_done,
                    "worker_id": last.worker_id,
                })
                if last.is_done:
                    _scrape_state["is_running"] = False

            await websocket.send_json({"type": "heartbeat", "state": _scrape_state})
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")


# ─── Запуск парсера ───────────────────────────────────────────────────────────

def _save_session_to_history(
    country: str,
    workers: int,
    saved: int,
    started_at: str,
    finished_at: str,
    stopped_manually: bool,
) -> None:
    """Сохраняет сессию в историю."""
    sessions = []
    if os.path.exists(_HISTORY_FILE):
        try:
            with open(_HISTORY_FILE, "r", encoding="utf-8") as f:
                sessions = json.load(f)
        except Exception:
            sessions = []

    label = "Все страны" if country == "all" else country.upper()
    sessions.insert(0, {
        "country": label,
        "workers": workers,
        "saved": saved,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": "остановлен" if stopped_manually else "завершён",
    })

    # Храним последние 50 сессий
    sessions = sessions[:50]

    try:
        with open(_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(sessions, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Cannot save history: {e}")


def _run_scraper_sync(
    country: str,
    workers: int,
    fresh: bool,
    auto: bool,
    start_id: int,
    end_id: int,
) -> None:
    global _scrape_state
    from config.settings import stop_event
    stop_event.clear()

    started_at = _scrape_state.get("started_at") or datetime.utcnow().isoformat()

    try:
        countries = _ALL_COUNTRIES if country == "all" else [country]
        for c in countries:
            if not _scrape_state["is_running"] or stop_event.is_set():
                logger.info(f"Stop signal — halting at country={c}")
                break
            _scrape_state["country"] = c
            _scrape_one(c, workers, fresh, auto, start_id, end_id)
    except Exception as e:
        logger.error(f"Scraper error: {e}")
        _scrape_state["error"] = str(e)
    finally:
        stopped_manually = stop_event.is_set()
        _scrape_state["is_running"] = False

        # Сохраняем сессию в историю
        _save_session_to_history(
            country=country,
            workers=workers,
            saved=_scrape_state.get("saved", 0),
            started_at=started_at,
            finished_at=datetime.utcnow().isoformat(),
            stopped_manually=stopped_manually,
        )


def _scrape_one(
    country: str,
    workers: int,
    fresh: bool,
    auto: bool,
    start_id: int,
    end_id: int,
) -> None:
    if country == "all":
        logger.error("_scrape_one called with country='all' — bug!")
        return

    if auto:
        try:
            from core.range_detector import detect_range
            start_id, end_id = detect_range(country)
            logger.info(f"Auto range {country}: {start_id}..{end_id}")
        except Exception as e:
            logger.warning(f"Auto-detect failed for {country}: {e} — using defaults")
            start_id = 1
            end_id = 100_000

    try:
        if workers > 1:
            from core.worker_pool import WorkerPool
            from core.profile_manager import ProfileManager
            manager = ProfileManager(count=workers)
            manager.ensure_ready(country=country)
            pool = WorkerPool(manager, _progress_queue)
            pool.run(
                country=country,
                start_id=start_id,
                end_id=end_id,
                workers=workers,
                resume=not fresh,
            )
        else:
            from core.scraper import scrape_range
            scrape_range(
                country=country,
                start_id=start_id,
                end_id=end_id,
                resume=not fresh,
            )
    except Exception as e:
        logger.error(f"Error scraping {country}: {e}")


def start_web(host: str = "0.0.0.0", port: int = 8000) -> None:
    uvicorn.run(app, host=host, port=port, log_level="warning")