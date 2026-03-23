#!/usr/bin/env python3
"""
ProfileManager — автоматическое создание и прогрев Chrome профилей.
Профили хранятся в data/profiles/ — не трогает системный Chrome.
Клиент не делает ничего вручную — всё создаётся при первом запуске.
Автор: viramax
"""

import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

from playwright.sync_api import sync_playwright

from config.settings import BASE_URL, OUTPUT_DIR
from utils.logger import get_logger

logger = get_logger(__name__)

PROFILES_DIR = os.path.join(OUTPUT_DIR, "profiles")
PROFILES_META = os.path.join(PROFILES_DIR, "profiles.json")
WARMUP_PAGES = 5
PROFILE_TTL_HOURS = 12


@dataclass
class ProfileInfo:
    """Метаданные одного Chrome профиля."""
    profile_id: str
    path: str
    is_warmed: bool = False
    is_blocked: bool = False
    warmed_at: Optional[str] = None
    blocked_count: int = 0


class ProfileManager:
    """
    Управляет пулом Chrome профилей.

    При первом запуске создаёт N профилей и прогревает их.
    При повторном — проверяет TTL и перегревает если нужно.
    При блокировке — помечает профиль и даёт следующий.

    Использование:
        manager = ProfileManager(count=3)
        manager.ensure_ready(country='ru')
        path = manager.get_next_profile()
    """

    def __init__(self, count: int = 3):
        self._count = count
        self._profiles: list[ProfileInfo] = []
        self._current_index = 0
        os.makedirs(PROFILES_DIR, exist_ok=True)
        self._load_or_create()

    def ensure_ready(self, country: str) -> None:
        """
        Гарантирует что все профили готовы к работе.
        Создаёт если нет, прогревает если холодные или TTL истёк.
        """
        for profile in self._profiles:
            if profile.is_blocked:
                logger.warning(f"Profile {profile.profile_id} blocked — skipping")
                continue
            if self._needs_warmup(profile):
                self._warmup_profile(profile, country)
        self._save_meta()

    def get_next_profile(self) -> Optional[str]:
        available = [p for p in self._profiles if not p.is_blocked]
        if not available:
            logger.warning("All profiles blocked — resetting and re-warming")
            self._reset_all_blocked()
            available = [p for p in self._profiles if not p.is_blocked]
        if not available:
            logger.error("No available profiles — all blocked")
            return None
        profile = available[self._current_index % len(available)]
        self._current_index += 1
        return profile.path

    def _reset_all_blocked(self) -> None:
        """Сбрасывает все заблокированные профили и создаёт новые папки."""
        import shutil
        for profile in self._profiles:
            if profile.is_blocked:
                # Удаляем старую папку и создаём новую чистую
                try:
                    if os.path.exists(profile.path):
                        shutil.rmtree(profile.path)
                    os.makedirs(profile.path, exist_ok=True)
                except Exception as e:
                    logger.error(f"Cannot reset profile {profile.profile_id}: {e}")
                profile.is_blocked = False
                profile.is_warmed = False
                profile.blocked_count = 0
                profile.warmed_at = None
                logger.info(f"Reset profile: {profile.profile_id}")
        self._save_meta()

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
        logger.debug(f"Block count {profile.profile_id}: {profile.blocked_count}/3")

        if profile.blocked_count >= 3:
            profile.is_blocked = True
            logger.warning(f"Profile {profile.profile_id} permanently blocked")

        self._save_meta()

    def reset_block_count(self, profile_path: str) -> None:
        """Сбрасывает счётчик блокировок после успешного re-warmup."""
        profile = self._find_by_path(profile_path)
        if profile:
            profile.blocked_count = 0
            self._save_meta()

    def get_stats(self) -> dict:
        """Статистика профилей — для веб-панели."""
        return {
            "total": len(self._profiles),
            "warmed": sum(1 for p in self._profiles if p.is_warmed),
            "blocked": sum(1 for p in self._profiles if p.is_blocked),
            "available": sum(
                1 for p in self._profiles
                if not p.is_blocked
            ),
        }

    # ─── Приватные методы ─────────────────────────────────────────────────────

    def _load_or_create(self) -> None:
        """Загружает существующие профили или создаёт новые."""
        if os.path.exists(PROFILES_META):
            self._load_meta()
            if len(self._profiles) < self._count:
                logger.info(f"Adding {self._count - len(self._profiles)} profiles")
                self._create_profiles(start_index=len(self._profiles))
        else:
            logger.info(f"Creating {self._count} new Chrome profiles")
            self._create_profiles(start_index=0)

    def _create_profiles(self, start_index: int) -> None:
        """Создаёт папки для новых профилей."""
        for i in range(start_index, self._count):
            profile_id = f"profile_{i}"
            path = os.path.abspath(os.path.join(PROFILES_DIR, profile_id))
            os.makedirs(path, exist_ok=True)
            self._profiles.append(ProfileInfo(profile_id=profile_id, path=path))
            logger.info(f"Created profile: {profile_id}")
        self._save_meta()

    def _warmup_profile(self, profile: ProfileInfo, country: str) -> None:
        """
        Прогревает профиль — посещает WARMUP_PAGES страниц галереи.
        KillBot видит реальную историю браузера.
        """
        logger.info(f"Warming up {profile.profile_id}...")
        pw = None
        context = None

        try:
            pw = sync_playwright().start()
            context = pw.chromium.launch_persistent_context(
                user_data_dir=profile.path,
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
                channel="chrome",
            )
            page = context.new_page()

            gallery_url = f"{BASE_URL}/{country}/gallery"
            page.goto(gallery_url, wait_until="domcontentloaded", timeout=20000)

            try:
                page.wait_for_function(
                    "() => !document.title.includes('верификац') && "
                    "!document.title.includes('verification')",
                    timeout=15000,
                )
            except Exception:
                logger.warning(f"{profile.profile_id}: KillBot timeout during warmup")

            for page_num in range(2, WARMUP_PAGES + 1):
                try:
                    url = f"{gallery_url}?page={page_num}"
                    page.goto(url, wait_until="domcontentloaded", timeout=15000)
                    page.wait_for_timeout(1500 + (page_num * 200))
                except Exception as e:
                    logger.warning(f"Warmup page {page_num} failed: {e}")

            profile.is_warmed = True
            profile.warmed_at = datetime.utcnow().isoformat()
            profile.is_blocked = False
            logger.info(f"{profile.profile_id}: warmup complete")

        except Exception as e:
            logger.error(f"{profile.profile_id}: warmup failed: {e}")
        finally:
            if context:
                try:
                    context.close()
                except Exception:
                    pass
            if pw:
                try:
                    pw.stop()
                except Exception:
                    pass

    def _needs_warmup(self, profile: ProfileInfo) -> bool:
        """Профиль нужно греть если никогда не грели или TTL истёк."""
        if not profile.is_warmed or not profile.warmed_at:
            return True
        try:
            warmed_at = datetime.fromisoformat(profile.warmed_at)
            hours_ago = (datetime.utcnow() - warmed_at).total_seconds() / 3600
            return hours_ago > PROFILE_TTL_HOURS
        except Exception:
            return True

    def _find_by_path(self, path: str) -> Optional[ProfileInfo]:
        """Находит ProfileInfo по пути к папке профиля."""
        return next((p for p in self._profiles if p.path == path), None)

    def _save_meta(self) -> None:
        """Атомарная запись метаданных через .tmp файл."""
        tmp = PROFILES_META + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump([asdict(p) for p in self._profiles], f, indent=2)
            os.replace(tmp, PROFILES_META)
        except OSError as e:
            logger.error(f"Cannot save profiles meta: {e}")

    def _load_meta(self) -> None:
        """Загружает метаданные из JSON. Защита от повреждённого файла."""
        try:
            with open(PROFILES_META, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._profiles = [ProfileInfo(**d) for d in data]
            logger.info(f"Loaded {len(self._profiles)} existing profiles")
        except (json.JSONDecodeError, OSError, TypeError) as e:
            logger.error(f"Cannot load profiles meta: {e} — starting fresh")
            self._profiles = []