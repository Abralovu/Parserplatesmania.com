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
import threading
from contextlib import asynccontextmanager
from datetime import datetime
from queue import Empty, Queue
from typing import Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from config.settings import DB_PATH, OUTPUT_DIR, stop_event
from storage.database import get_count, get_records, init_db
from utils.checkpoint import get_all_checkpoints
from utils.logger import get_logger

logger = get_logger(__name__)

# ─── Константы ────────────────────────────────────────────────────────────────

_ALL_COUNTRIES: list[str] = [
    "ru", "de", "pl", "ua", "by", "kz", "us", "gb", "fr", "it",
    "es", "nl", "be", "at", "ch", "se", "no", "fi", "dk", "lt",
    "lv", "ee", "cz", "sk", "hu", "ro", "bg", "rs", "hr", "gr",
    "pt", "tr", "am", "ge", "az", "uz", "kg", "md", "il", "ae",
    "cn", "jp", "kr",
]

_HISTORY_FILE: str = os.path.join(OUTPUT_DIR, "history.json")
_HISTORY_MAX_SESSIONS: int = 50

# ─── Состояние парсера ────────────────────────────────────────────────────────

# Lock защищает _scrape_state от race condition:
# executor thread пишет, FastAPI event loop читает одновременно.
_state_lock = threading.Lock()

_scrape_state: dict = {
    "is_running": False,
    "country": None,
    "workers": 1,
    "processed": 0,
    "saved": 0,
    "current_id": 0,
    "error": None,
    "started_at": None,
}

# Lock защищает history.json от одновременной записи
# при парсинге "Все страны" (несколько потоков финишируют подряд).
_history_lock = threading.Lock()

_progress_queue: Queue = Queue()


# ─── Pydantic схемы ───────────────────────────────────────────────────────────

class ScrapeStartRequest(BaseModel):
    country: str = Field(default="ru", min_length=2, max_length=3)
    workers: int = Field(default=1, ge=1, le=30)
    auto: bool = False
    fresh: bool = False
    start_id: int = Field(default=1, ge=1)
    end_id: int = Field(default=100_000, ge=1)

    @field_validator("country")
    @classmethod
    def validate_country(cls, v: str) -> str:
        allowed = set(_ALL_COUNTRIES) | {"all"}
        if v not in allowed:
            raise ValueError(f"Unknown country: {v}")
        return v


class ResetRequest(BaseModel):
    confirm: bool = False


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("Web panel ready")
    yield


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="PlatesMania Scraper", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="web/static"), name="static")


# ─── HTML ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root() -> str:
    html_path = os.path.join("web", "static", "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


# ─── API: статус ──────────────────────────────────────────────────────────────

@app.get("/api/status")
async def get_status() -> dict:
    """
    Статистика по БД — один GROUP BY запрос вместо N+1.
    """
    from storage.database import get_counts_by_country
    stats = await get_counts_by_country()
    with _state_lock:
        state_snapshot = dict(_scrape_state)
    return {
        "scraper": state_snapshot,
        "database": stats,
        "checkpoints": get_all_checkpoints(),
    }


# ─── API: запуск парсера ──────────────────────────────────────────────────────

@app.post("/api/scrape/start")
async def start_scrape(body: ScrapeStartRequest) -> dict:
    with _state_lock:
        if _scrape_state["is_running"]:
            return {"ok": False, "error": "Парсер уже запущен"}
        _scrape_state.update({
            "is_running": True,
            "country": body.country,
            "workers": body.workers,
            "processed": 0,
            "saved": 0,
            "current_id": 0,
            "error": None,
            "started_at": datetime.utcnow().isoformat(),
        })

    # stop_event сбрасываем ДО запуска потока — нет окна гонки
    stop_event.clear()

    loop = asyncio.get_running_loop()
    loop.run_in_executor(
        None,
        _run_scraper_sync,
        body.country,
        body.workers,
        body.fresh,
        body.auto,
        body.start_id,
        body.end_id,
    )

    label = "ВСЕ СТРАНЫ" if body.country == "all" else body.country.upper()
    return {"ok": True, "message": f"Запущен: {label}"}


# ─── API: остановка ───────────────────────────────────────────────────────────

@app.post("/api/scrape/stop")
async def stop_scrape() -> dict:
    stop_event.set()
    with _state_lock:
        _scrape_state["is_running"] = False
    return {"ok": True, "message": "Остановка"}


# ─── API: записи ─────────────────────────────────────────────────────────────

@app.get("/api/records")
async def get_filtered_records(
    country: Optional[str] = None,
    region: Optional[str] = None,
    car_brand: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    records = await get_records(
        country=country,
        region=region,
        car_brand=car_brand,
        limit=limit,
        offset=offset,
    )
    return {"records": [r.to_dict() for r in records], "count": len(records)}


# ─── API: экспорт ─────────────────────────────────────────────────────────────

@app.get("/api/download/excel", response_model=None)
async def download_excel(
    country: Optional[str] = None,
    region: Optional[str] = None,
    car_brand: Optional[str] = None,
) -> FileResponse | dict:
    from utils.export_excel import export_excel_with_photos
    path = await export_excel_with_photos(
        country=country if country and country != "all" else None,
        region=region or None,
        car_brand=car_brand or None,
    )
    if path and os.path.exists(path):
        return FileResponse(
            path=path,
            filename=os.path.basename(path),
            media_type=(
                "application/vnd.openxmlformats-officedocument"
                ".spreadsheetml.sheet"
            ),
        )
    return {"error": "Нет данных"}


@app.get("/api/download/csv", response_model=None)
async def download_csv(
    country: Optional[str] = None,
    region: Optional[str] = None,
    car_brand: Optional[str] = None,
) -> FileResponse | dict:
    from utils.export import export_csv
    path = await export_csv(
        country=country if country and country != "all" else None,
        region=region or None,
        car_brand=car_brand or None,
    )
    if path and os.path.exists(path):
        return FileResponse(
            path=path,
            filename=os.path.basename(path),
            media_type="text/csv",
        )
    return {"error": "Нет данных"}


# ─── API: история ─────────────────────────────────────────────────────────────

@app.get("/api/history")
async def get_history() -> dict:
    if not os.path.exists(_HISTORY_FILE):
        return {"sessions": []}
    try:
        with open(_HISTORY_FILE, "r", encoding="utf-8") as f:
            return {"sessions": json.load(f)}
    except Exception as e:
        logger.warning(f"Cannot read history: {e}")
        return {"sessions": []}


# ─── API: сброс ───────────────────────────────────────────────────────────────

@app.post("/api/reset")
async def reset_all(body: ResetRequest) -> dict:
    """
    Полный сброс: база данных, фото, checkpoint, профили.
    Требует явного подтверждения: { "confirm": true }
    """
    if not body.confirm:
        return {"ok": False, "error": "Требуется подтверждение"}

    with _state_lock:
        if _scrape_state["is_running"]:
            return {"ok": False, "error": "Сначала остановите парсер"}

    errors: list[str] = []

    targets = [
        ("DB",          lambda: os.remove(DB_PATH)),
        ("Photos",      lambda: shutil.rmtree(os.path.join(OUTPUT_DIR, "photos"))),
        ("Checkpoint",  lambda: os.remove(os.path.join(OUTPUT_DIR, "checkpoint.json"))),
        ("Profiles",    lambda: shutil.rmtree(os.path.join(OUTPUT_DIR, "profiles"))),
    ]

    for label, action in targets:
        try:
            action()
        except FileNotFoundError:
            pass  # уже не существует — ок
        except Exception as e:
            errors.append(f"{label}: {e}")

    await init_db()

    if errors:
        return {"ok": False, "error": " | ".join(errors)}

    logger.info("Full reset completed")
    return {"ok": True, "message": "Все данные удалены"}


# ─── WebSocket ────────────────────────────────────────────────────────────────

@app.websocket("/ws/progress")
async def websocket_progress(websocket: WebSocket) -> None:
    await websocket.accept()
    logger.info("WebSocket client connected")
    try:
        while True:
            # Дренируем очередь — ищем is_done по ВСЕМ сообщениям,
            # не только последнему. is_done может прийти не последним
            # если поток успел отправить ещё одно сообщение после него.
            messages = []
            while True:
                try:
                    messages.append(_progress_queue.get_nowait())
                except Empty:
                    break

            if messages:
                last = messages[-1]
                is_done = any(m.is_done for m in messages)

                with _state_lock:
                    _scrape_state.update({
                        "processed": last.processed,
                        "saved": last.saved,
                        "current_id": last.current_id,
                    })
                    if is_done:
                        _scrape_state["is_running"] = False

                await websocket.send_json({
                    "type": "progress",
                    "processed": last.processed,
                    "saved": last.saved,
                    "current_id": last.current_id,
                    "is_done": is_done,
                    "worker_id": last.worker_id,
                })

            with _state_lock:
                state_snapshot = dict(_scrape_state)

            await websocket.send_json({
                "type": "heartbeat",
                "state": state_snapshot,
            })
            await asyncio.sleep(0.5)

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")


# ─── Фоновый запуск парсера ───────────────────────────────────────────────────

def _save_session_to_history(
    country: str,
    workers: int,
    saved: int,
    started_at: str,
    finished_at: str,
    stopped_manually: bool,
) -> None:
    """
    Сохраняет сессию в history.json.
    Lock защищает от race condition при парсинге "Все страны":
    несколько потоков могут финишировать почти одновременно.
    """
    os.makedirs(os.path.dirname(_HISTORY_FILE), exist_ok=True)

    with _history_lock:
        sessions: list[dict] = []
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
        sessions = sessions[:_HISTORY_MAX_SESSIONS]

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
    """
    Точка входа для executor thread.
    Итерирует по странам (или одна страна), вызывает _scrape_one.
    """
    with _state_lock:
        started_at = _scrape_state.get("started_at") or datetime.utcnow().isoformat()

    try:
        countries = _ALL_COUNTRIES if country == "all" else [country]
        for c in countries:
            if stop_event.is_set():
                logger.info(f"Stop signal — halting at country={c}")
                break
            with _state_lock:
                _scrape_state["country"] = c
            _scrape_one(c, workers, fresh, auto, start_id, end_id)
    except Exception as e:
        logger.error(f"Scraper fatal error: {e}")
        with _state_lock:
            _scrape_state["error"] = str(e)
    finally:
        stopped_manually = stop_event.is_set()
        with _state_lock:
            _scrape_state["is_running"] = False
            saved = _scrape_state.get("saved", 0)

        _save_session_to_history(
            country=country,
            workers=workers,
            saved=saved,
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
    """Парсит одну страну — выбирает single/multi-thread режим."""
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
            from core.profile_manager import ProfileManager
            from core.worker_pool import WorkerPool
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


# ─── Точка входа ──────────────────────────────────────────────────────────────

def start_web(host: str = "0.0.0.0", port: int = 8000) -> None:
    uvicorn.run(app, host=host, port=port, log_level="warning")