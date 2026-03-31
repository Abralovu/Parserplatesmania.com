import json
import os
import random
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

from config.settings import BASE_URL, OUTPUT_DIR, HEADLESS
from utils.logger import get_logger

logger = get_logger(__name__)

PROFILES_DIR = os.path.join(OUTPUT_DIR, "profiles")
PROFILES_META = os.path.join(PROFILES_DIR, "profiles.json")
WARMUP_PAGES = 3
PROFILE_TTL_HOURS = 12

# Время ожидания загрузки KillBot UI (секунды)
_KILLBOT_UI_WAIT_S = 20

# Расстояние drag слайдера (пиксели)
_DRAG_DISTANCE_PX = 250

# JS для поиска draggable элемента KillBot
_FIND_DRAGGABLE_JS = """() => {
    const all = document.querySelectorAll('*');
    const result = [];
    for (const el of all) {
        const rect = el.getBoundingClientRect();
        const w = rect.width;
        const h = rect.height;
        if (w >= 40 && w <= 70 && h >= 40 && h <= 70 && rect.top > 100) {
            const style = getComputedStyle(el);
            const bg = style.backgroundImage || '';
            if (bg.includes('linear-gradient') && bg.includes('0, 115, 230')) {
                result.push({
                    x: Math.round(rect.x + w / 2),
                    y: Math.round(rect.y + h / 2),
                    w: Math.round(w),
                    h: Math.round(h),
                    cursor: style.cursor,
                });
            }
        }
    }
    return result;
}"""


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
            logger.info(f"Creating {self._count} new profiles")
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
        Прогревает профиль через Camoufox с drag-bypass KillBot.
        Без прокси — через datacenter IP.
        """
        import asyncio
        from camoufox.sync_api import Camoufox
        from config.settings import (
            CAMOUFOX_HUMANIZE,
            CAMOUFOX_OS,
            HEADLESS,
            stop_event,
        )

        if stop_event.is_set():
            return

        logger.info(f"Warming up {profile.profile_id} with Camoufox...")

        headless_mode = "virtual" if HEADLESS else False

        launch_kwargs = {
            "headless": headless_mode,
            "os": CAMOUFOX_OS,
            "humanize": CAMOUFOX_HUMANIZE,
            "block_webrtc": True,
            "persistent_context": True,
            "user_data_dir": profile.path,
            "window": (1280, 720),
        }

        cfox = None
        browser = None
        try:
            asyncio.set_event_loop(None)

            cfox = Camoufox(**launch_kwargs)
            browser = cfox.start()

            pages = browser.pages if hasattr(browser, "pages") else []
            page = pages[0] if pages else browser.new_page()

            # Посещаем нейтральный сайт
            try:
                page.goto(
                    "https://www.google.com",
                    wait_until="domcontentloaded",
                    timeout=15_000,
                )
                page.wait_for_timeout(2000)
                page.evaluate("window.scrollBy(0, 300)")
                page.wait_for_timeout(1000)
            except Exception as e:
                logger.debug(f"Neutral warmup failed: {e}")

            # Посещаем галерею
            gallery_url = f"{BASE_URL}/{country}/gallery"
            page.goto(
                gallery_url, wait_until="domcontentloaded", timeout=30_000
            )
            page.wait_for_timeout(3000)

            title = page.title().lower()
            if "verification" in title or "верификац" in title:
                logger.info(f"{profile.profile_id}: KillBot detected — drag bypass")
                passed = self._drag_bypass_page(page, stop_event)
                if passed:
                    logger.info(f"{profile.profile_id}: KillBot drag bypass OK")
                else:
                    logger.warning(f"{profile.profile_id}: KillBot drag bypass failed")
            else:
                logger.info(f"{profile.profile_id}: no KillBot — direct access")

            # Листаем галерею
            for page_num in range(2, WARMUP_PAGES + 2):
                if stop_event.is_set():
                    break
                try:
                    url = f"{gallery_url}?page={page_num}"
                    page.goto(
                        url, wait_until="domcontentloaded", timeout=15_000
                    )
                    page.evaluate(
                        f"window.scrollBy(0, {random.randint(200, 500)})"
                    )
                    page.wait_for_timeout(1500 + (page_num * 300))
                except Exception as e:
                    logger.warning(f"Warmup page {page_num} failed: {e}")

            profile.is_warmed = True
            profile.warmed_at = datetime.utcnow().isoformat()
            profile.is_blocked = False
            logger.info(f"{profile.profile_id}: warmup complete")

        except Exception as e:
            logger.error(f"{profile.profile_id}: warmup failed: {e}")
        finally:
            try:
                if browser:
                    browser.close()
            except Exception:
                pass
            try:
                if cfox:
                    cfox.stop()
            except Exception:
                pass

    @staticmethod
    def _drag_bypass_page(page, stop_event) -> bool:
        """
        Drag-bypass KillBot слайдера на переданной странице.
        Переиспользуется в warmup профилей.
        """
        logger.info(f"Waiting {_KILLBOT_UI_WAIT_S}s for KillBot UI to load")
        page.wait_for_timeout(_KILLBOT_UI_WAIT_S * 1000)

        page.evaluate("document.querySelector('#preloader-w')?.remove()")
        page.wait_for_timeout(1000)

        elements = page.evaluate(_FIND_DRAGGABLE_JS)

        if not elements:
            logger.warning("No draggable elements found in warmup")
            return False

        logger.debug(f"Found {len(elements)} draggable candidates")
        elements.sort(key=lambda e: 0 if e["cursor"] == "pointer" else 1)

        for el in elements:
            if stop_event.is_set():
                return False

            start_x = el["x"]
            start_y = el["y"]
            logger.debug(
                f"Dragging element at ({start_x},{start_y}) "
                f"{el['w']}x{el['h']} cursor={el['cursor']}"
            )

            page.mouse.move(start_x, start_y)
            page.mouse.down()
            steps = _DRAG_DISTANCE_PX // 10
            for i in range(steps):
                page.mouse.move(
                    start_x + ((i + 1) * 10),
                    start_y + random.randint(-2, 2),
                )
                time.sleep(random.uniform(0.03, 0.06))
            page.mouse.up()

            page.wait_for_timeout(3000)
            title = page.title().lower()

            if "verification" not in title and "верификац" not in title:
                logger.info(f"Drag bypass succeeded — title: {page.title()}")
                return True

            logger.debug("Drag did not bypass — trying next element")

        return False

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