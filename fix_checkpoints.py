
import json
import os

DB_PATH = "./data/plates.db"
CHECKPOINT_FILE = "./data/checkpoint.json"

COUNTRIES_TO_RESET = ["de", "by", "uz", "am", "az"]

if not os.path.exists(CHECKPOINT_FILE):
    print("checkpoint.json не найден")
    exit(1)

with open(CHECKPOINT_FILE, "r") as f:
    checkpoints = json.load(f)

print("До сброса:")
for c in COUNTRIES_TO_RESET:
    print(f"  {c}: {checkpoints.get(c, 'нет')}")

for c in COUNTRIES_TO_RESET:
    if c in checkpoints:
        del checkpoints[c]

with open(CHECKPOINT_FILE, "w") as f:
    json.dump(checkpoints, f, indent=2)

print("\nПосле сброса:")
with open(CHECKPOINT_FILE, "r") as f:
    updated = json.load(f)
    print(json.dumps(updated, indent=2))

print("\nГотово. Удали этот файл: rm fix_checkpoints.py")