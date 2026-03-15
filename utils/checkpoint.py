import json
import os
from utils.logger import get_logger
from config.settings import CHECKPOINT_FILE

logger = get_logger(__name__)


def save_checkpoint(last_id: int, country: str = "all") -> None:
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    data = _load_all()
    data[country] = last_id
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f, indent=2)
    logger.debug(f"Checkpoint saved: country={country}, last_id={last_id}")


def load_checkpoint(country: str, start_id: int) -> int:
    """
    Возвращает ID с которого продолжать.
    Если checkpoint есть и он больше start_id — берём его.
    Иначе — start_id. Никогда не возвращает 1 по умолчанию.
    """
    data = _load_all()
    saved = data.get(country)
    if saved and saved > start_id:
        return saved
    return start_id


def reset_checkpoint(country: str = "all") -> None:
    if not os.path.exists(CHECKPOINT_FILE):
        return
    if country == "all":
        os.remove(CHECKPOINT_FILE)
        logger.info("All checkpoints reset")
    else:
        data = _load_all()
        data.pop(country, None)
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Checkpoint reset for country={country}")


def _load_all() -> dict:
    if not os.path.exists(CHECKPOINT_FILE):
        return {}
    with open(CHECKPOINT_FILE, "r") as f:
        return json.load(f)
