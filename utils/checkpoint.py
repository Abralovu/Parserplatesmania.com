import json
import os
import threading
from typing import Optional
from utils.logger import get_logger
from config.settings import CHECKPOINT_FILE

logger = get_logger(__name__)

# Один Lock на весь процесс — защищает checkpoint.json от одновременной записи
_lock = threading.Lock()


def save_checkpoint(last_id: int, country: str) -> None:
    """
    Сохраняет последний обработанный ID для страны.
    Thread-safe: блокирует файл на время записи.
    """
    with _lock:
        data = _load_raw()
        data[country] = last_id
        _write_safe(data)
    logger.debug(f"Checkpoint saved: country={country}, last_id={last_id}")


def load_checkpoint(country: str, start_id: int) -> int:
    """
    Возвращает ID с которого продолжать.
    Если checkpoint есть и он больше start_id — берём его.
    Иначе возвращает start_id без изменений.
    """
    with _lock:
        data = _load_raw()

    saved = data.get(country)
    if saved and isinstance(saved, int) and saved > start_id:
        logger.info(f"Resuming from checkpoint: country={country}, id={saved}")
        return saved
    return start_id


def reset_checkpoint(country: str) -> None:
    """
    Сбрасывает checkpoint для страны.
    country='all' — удаляет весь файл.
    """
    with _lock:
        if country == "all":
            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)
                logger.info("All checkpoints reset")
            return

        data = _load_raw()
        if country in data:
            data.pop(country)
            _write_safe(data)
            logger.info(f"Checkpoint reset: country={country}")
        else:
            logger.warning(f"No checkpoint found for country={country}")


def get_all_checkpoints() -> dict:
    """Возвращает все checkpoints — используется в веб-панели для статуса."""
    with _lock:
        return _load_raw()


# ─── Приватные утилиты ────────────────────────────────────────────────────────

def _load_raw() -> dict:
    """
    Читает checkpoint.json.
    Защита от: отсутствия файла, повреждённого JSON (краш при записи).
    Вызывать только внутри _lock.
    """
    if not os.path.exists(CHECKPOINT_FILE):
        return {}

    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except json.JSONDecodeError as e:
        logger.error(f"Checkpoint file corrupted: {e} — starting fresh")
        _backup_corrupted()
        return {}
    except OSError as e:
        logger.error(f"Cannot read checkpoint file: {e}")
        return {}


def _write_safe(data: dict) -> None:
    """
    Атомарная запись через временный файл.
    Защита от: обрыва записи (VPS crash) — файл не corrupted.
    Вызывать только внутри _lock.
    """
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)

    # Пишем во временный файл, потом атомарно переименовываем
    tmp_path = CHECKPOINT_FILE + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, CHECKPOINT_FILE)  # атомарная операция на Linux/Mac
    except OSError as e:
        logger.error(f"Cannot write checkpoint: {e}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def _backup_corrupted() -> None:
    """Переименовывает повреждённый файл для ручного анализа."""
    backup = CHECKPOINT_FILE + ".corrupted"
    try:
        os.replace(CHECKPOINT_FILE, backup)
        logger.warning(f"Corrupted checkpoint backed up to: {backup}")
    except OSError:
        pass