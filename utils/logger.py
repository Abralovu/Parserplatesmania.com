#!/usr/bin/env python3
"""
Логгер — Rich вывод с указанием файла и строки.
Thread-safe: инициализация только один раз через _initialized flag.
Автор: viramax
"""

import logging
import threading
from rich.logging import RichHandler
from rich.console import Console
from config.settings import LOG_LEVEL

console = Console()

# Гарантируем инициализацию ровно один раз даже при многопоточности
_init_lock = threading.Lock()
_initialized = False


def get_logger(name: str) -> logging.Logger:
    """
    Возвращает логгер для модуля.
    Инициализирует root logger только при первом вызове.
    Все последующие вызовы просто возвращают getLogger(name).
    """
    global _initialized

    if not _initialized:
        with _init_lock:
            # Double-checked locking — второй поток уже найдёт _initialized=True
            if not _initialized:
                logging.basicConfig(
                    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                    format="%(message)s",
                    datefmt="[%X]",
                    handlers=[
                        RichHandler(
                            console=console,
                            rich_tracebacks=True,
                            show_path=True,
                        )
                    ],
                )
                _initialized = True

    return logging.getLogger(name)