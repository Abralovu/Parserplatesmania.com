import json
import os
from utils.logger import get_logger

logger = get_logger(__name__)

CHECKPOINT_FILE = "./data/checkpoint.json"


def save_checkpoint(last_id: int, country: str = "all") -> None:
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    data = load_checkpoint()
    data[country] = last_id
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f, indent=2)
    logger.debug(f"Checkpoint saved: country={country}, last_id={last_id}")


def load_checkpoint(country: str = "all") -> int | dict:
    if not os.path.exists(CHECKPOINT_FILE):
        return {} if country == "all" else 1
    with open(CHECKPOINT_FILE, "r") as f:
        data = json.load(f)
    if country == "all":
        return data
    return data.get(country, 1)


def reset_checkpoint(country: str = "all") -> None:
    if not os.path.exists(CHECKPOINT_FILE):
        return
    if country == "all":
        os.remove(CHECKPOINT_FILE)
        logger.info("All checkpoints reset")
    else:
        data = load_checkpoint()
        data.pop(country, None)
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Checkpoint reset for country={country}")
