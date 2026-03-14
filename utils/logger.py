import logging
import sys
from rich.logging import RichHandler
from rich.console import Console
from config.settings import LOG_LEVEL

console = Console()

def get_logger(name: str) -> logging.Logger:
    """
    Создаёт логгер с красивым выводом через Rich.
    Каждый модуль вызывает get_logger(__name__) — 
    и мы видим ИЗ КАКОГО файла пришло сообщение.
    """
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(
                console=console,
                rich_tracebacks=True,   # красивые traceback при ошибках
                show_path=True,         # показывает файл и строку
            )
        ],
        force=True,  # перезаписывает дефолтный logging если уже инициализирован
    )
    return logging.getLogger(name)
