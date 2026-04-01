import json
import os
import shutil
from dataclasses import dataclass, asdict
from typing import Optional

from config.settings import OUTPUT_DIR
from utils.logger import get_logger

logger = get_logger(__name__)

PROFILES_DIR = os.path.join(OUTPUT_DIR, "profiles")
PROFILES_META = os.path.join(PROFILES_DIR, "profiles.json")


@dataclass
class ProfileInfo:
    """Метаданные одного профиля браузера."""
    profile_id: str
    path: str
    is_warmed: bool = False
    is_blocked: bool = False
    warmed_at: Optional[str] = None
    blocked_count: int = 0


class ProfileManager:
    """
    Управляет пулом браузерных профилей.

    Создаёт N профилей (папки на диске).
    При блокировке — помечает профиль и даёт следующий.

    Прогрев НЕ выполняется здесь — воркеры сами прогреваются
    через BrowserSession._warmup() с drag-bypass KillBot.

    Причина: BrowserSession запускается внутри ThreadPoolExecutor
    (чистый поток без asyncio loop), а ensure_ready() вызывается
    из daemon thread где uvicorn event loop мешает Playwright sync API.
    """

    def __init__(self, count: int = 3):
        self._count = count
        self._profiles: list[ProfileInfo] = []
        self._current_index = 0
        os.makedirs(PROFILES_DIR, exist_ok=True)
        self._load_or_create()

    def ensure_ready(self) -> None:
        """
        Гарантирует что профили созданы и метаданные сохранены.
        Прогрев не выполняется — воркеры прогреваются сами
        при первом запуске BrowserSession.__enter__().
        """
        self._save_meta()
        available = sum(1 for p in self._profiles if not p.is_blocked)
        logger.info(
            f"Profiles ready: {available}/{len(self._profiles)} available"
        )

    def get_next_profile(self) -> Optional[str]:
        """Возвращает путь к следующему незаблокированному профилю."""
        available = [p for p in self._profiles if not p.is_blocked]
        if not available:
            logger.warning("All profiles blocked — resetting")
            self._reset_all_blocked()
            available = [p for p in self._profiles if not p.is_blocked]
        if not available:
            logger.error("No available profiles — all blocked")
            return None
        profile = available[self._current_index % len(available)]
        self._current_index += 1
        return profile.path

    def mark_blocked(self, profile_path: str) -> None:
        """
        Увеличивает счётчик блокировок.
        После 3х подряд — профиль помечается как заблокированный.
        """
        profile = self._find_by_path(profile_path)
        if not profile:
            logger.warning(f"Profile not found: {profile_path}")
            return

        profile.blocked_count += 1
        logger.debug(
            f"Block count {profile.profile_id}: "
            f"{profile.blocked_count}/3"
        )

        if profile.blocked_count >= 3:
            profile.is_blocked = True
            logger.warning(
                f"Profile {profile.profile_id} permanently blocked"
            )

        self._save_meta()

    def reset_block_count(self, profile_path: str) -> None:
        """Сбрасывает счётчик блокировок после успешного запуска."""
        profile = self._find_by_path(profile_path)
        if profile:
            profile.blocked_count = 0
            self._save_meta()

    def get_stats(self) -> dict:
        """Статистика профилей — для веб-панели."""
        return {
            "total": len(self._profiles),
            "warmed": sum(
                1 for p in self._profiles if p.is_warmed
            ),
            "blocked": sum(
                1 for p in self._profiles if p.is_blocked
            ),
            "available": sum(
                1 for p in self._profiles if not p.is_blocked
            ),
        }

    # ─── Приватные методы ─────────────────────────────────────────────────

    def _load_or_create(self) -> None:
        """Загружает существующие профили или создаёт новые."""
        if os.path.exists(PROFILES_META):
            self._load_meta()
            if len(self._profiles) < self._count:
                logger.info(
                    f"Adding "
                    f"{self._count - len(self._profiles)} profiles"
                )
                self._create_profiles(
                    start_index=len(self._profiles)
                )
        else:
            logger.info(f"Creating {self._count} new profiles")
            self._create_profiles(start_index=0)

    def _create_profiles(self, start_index: int) -> None:
        """Создаёт папки для новых профилей."""
        for i in range(start_index, self._count):
            profile_id = f"profile_{i}"
            path = os.path.abspath(
                os.path.join(PROFILES_DIR, profile_id)
            )
            os.makedirs(path, exist_ok=True)
            self._profiles.append(
                ProfileInfo(profile_id=profile_id, path=path)
            )
            logger.info(f"Created profile: {profile_id}")
        self._save_meta()

    def _reset_all_blocked(self) -> None:
        """Сбрасывает все заблокированные профили."""
        for profile in self._profiles:
            if profile.is_blocked:
                try:
                    if os.path.exists(profile.path):
                        shutil.rmtree(profile.path)
                    os.makedirs(profile.path, exist_ok=True)
                except Exception as e:
                    logger.error(
                        f"Cannot reset profile "
                        f"{profile.profile_id}: {e}"
                    )
                profile.is_blocked = False
                profile.is_warmed = False
                profile.blocked_count = 0
                profile.warmed_at = None
                logger.info(f"Reset profile: {profile.profile_id}")
        self._save_meta()

    def _find_by_path(self, path: str) -> Optional[ProfileInfo]:
        """Находит ProfileInfo по пути к папке профиля."""
        return next(
            (p for p in self._profiles if p.path == path), None
        )

    def _save_meta(self) -> None:
        """Атомарная запись метаданных через .tmp файл."""
        tmp = PROFILES_META + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    [asdict(p) for p in self._profiles],
                    f,
                    indent=2,
                )
            os.replace(tmp, PROFILES_META)
        except OSError as e:
            logger.error(f"Cannot save profiles meta: {e}")

    def _load_meta(self) -> None:
        """Загружает метаданные из JSON."""
        try:
            with open(PROFILES_META, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._profiles = [ProfileInfo(**d) for d in data]
            logger.info(
                f"Loaded {len(self._profiles)} existing profiles"
            )
        except (json.JSONDecodeError, OSError, TypeError) as e:
            logger.error(
                f"Cannot load profiles meta: {e} — starting fresh"
            )
            self._profiles = []