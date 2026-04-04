import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from queue import Queue
from typing import Optional

from config.settings import BASE_URL, SESSION_SIZE, CHECKPOINT_EVERY, stop_event
from core.anti_bot import BrowserSession
from core.downloader import download_photo
from core.profile_manager import ProfileManager
from core.scraper import parse_plate_page
from storage.database import sync_save_batch, sync_id_exists, sync_get_count
from utils.checkpoint import save_checkpoint, load_checkpoint
from utils.logger import get_logger

logger = get_logger(__name__)

# ─── Константы ────────────────────────────────────────────────────────────────

# Пауза между запусками воркеров (секунды).
# 8 одновременных Camoufox.start() = malloc corruption → segfault.
# Stagger даёт каждому Firefox время запуститься и выделить память.
_STAGGER_DELAY_S = 5

# Максимум retry при crash одного блока SESSION_SIZE.
# После исчерпания — воркер пропускает блок и идёт к следующему.
_MAX_SESSION_RETRIES = 3

# Если N блоков подряд crash — поток сломан, нет смысла продолжать.
# (asyncio loop заражён, или системная проблема)
_MAX_CONSECUTIVE_FAILURES = 5

# Ошибки при которых текущая сессия должна немедленно завершиться.
# Browser мёртв — продолжать fetch бессмысленно, только спамит ошибки
# и блокирует профили.
_FATAL_SESSION_ERRORS = (
    "browser has been closed",
    "target page, context or browser",
    "page crashed",
)


@dataclass
class WorkerProgress:
    """Прогресс одного воркера — передаётся в веб-панель через Queue."""
    worker_id: int
    processed: int
    saved: int
    current_id: int
    is_blocked: bool = False
    is_done: bool = False
    error: Optional[str] = None


@dataclass
class ScrapeTask:
    """Задача для одного воркера — диапазон ID и профиль."""
    worker_id: int
    country: str
    start_id: int
    end_id: int
    profile_path: str


class WorkerPool:
    """
    Управляет пулом потоков-парсеров.
    Каждый поток = отдельный Camoufox профиль = отдельный диапазон ID.

    Использование:
        pool = WorkerPool(profile_manager, progress_queue)
        pool.run(country='ru', start_id=31000000, end_id=32000000, workers=3)
    """

    def __init__(
        self,
        profile_manager: ProfileManager,
        progress_queue: Optional[Queue] = None,
    ):
        self._manager = profile_manager
        self._progress_queue = progress_queue or Queue()

    def run(
        self,
        country: str,
        start_id: int,
        end_id: int,
        workers: int,
        resume: bool = True,
    ) -> dict:
        """
        Запускает N потоков для парсинга диапазона ID.
        Возвращает итоговую статистику.
        """
        actual_start = load_checkpoint(country, start_id) if resume else start_id
        tasks = self._split_range(country, actual_start, end_id, workers)

        if not tasks:
            logger.error("No tasks created — check profile availability")
            return {"processed": 0, "saved": 0, "workers": 0}

        logger.info(
            f"Starting pool: country={country}, "
            f"range={actual_start}..{end_id}, "
            f"workers={len(tasks)}"
        )

        stats = {"processed": 0, "saved": 0, "workers": len(tasks)}

        with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            futures = {
                executor.submit(self._run_worker, task): task
                for task in tasks
            }
            for future in as_completed(futures):
                task = futures[future]
                try:
                    result = future.result()
                    stats["processed"] += result["processed"]
                    stats["saved"] += result["saved"]
                except Exception as e:
                    logger.error(f"Worker {task.worker_id} failed: {e}")

        total_in_db = sync_get_count(country)
        logger.info(
            f"Pool done: processed={stats['processed']}, "
            f"saved={stats['saved']}, total in DB={total_in_db}"
        )
        return stats

    # ─── Приватные методы ─────────────────────────────────────────────────

    def _run_worker(self, task: ScrapeTask) -> dict:
        """
        Один воркер — парсит свой диапазон ID.

        Staggered start: ждёт worker_id * _STAGGER_DELAY_S секунд
        перед запуском — предотвращает segfault от одновременного
        malloc в 8 Firefox процессах.

        При crash сессии — retry до _MAX_SESSION_RETRIES на блок.
        При _MAX_CONSECUTIVE_FAILURES подряд — воркер останавливается
        (поток сломан, нет смысла продолжать).
        """
        # ── Stagger delay ──
        # Воркер 0 стартует сразу, воркер 1 через 5с, воркер 7 через 35с.
        # Пауза разбита на 1-секундные шаги чтобы stop_event реагировал.
        if task.worker_id > 0:
            stagger = task.worker_id * _STAGGER_DELAY_S
            logger.info(
                f"Worker {task.worker_id}: waiting {stagger}s (stagger)"
            )
            for _ in range(stagger):
                if stop_event.is_set():
                    return {"processed": 0, "saved": 0}
                time.sleep(1)

        processed = 0
        saved = 0
        batch: list = []
        profile_path = task.profile_path
        consecutive_blocks = 0
        consecutive_failures = 0  # Счётчик подряд упавших блоков

        logger.info(
            f"Worker {task.worker_id}: "
            f"range={task.start_id}..{task.end_id}, "
            f"profile={_profile_name(profile_path)}"
        )

        for session_start in range(
            0, task.end_id - task.start_id + 1, SESSION_SIZE
        ):
            if stop_event.is_set():
                break

            # Если поток сломан — выходим
            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                logger.error(
                    f"Worker {task.worker_id}: {consecutive_failures} "
                    f"consecutive failures — stopping worker"
                )
                break

            session_ids = list(
                range(
                    task.start_id + session_start,
                    min(
                        task.start_id + session_start + SESSION_SIZE,
                        task.end_id + 1,
                    ),
                )
            )

            # ── Retry loop для одного блока ──
            session_ok = False
            for retry in range(_MAX_SESSION_RETRIES):
                if stop_event.is_set():
                    break

                result = self._run_session(
                    task=task,
                    session_ids=session_ids,
                    batch=batch,
                    processed=processed,
                    saved=saved,
                    profile_path=profile_path,
                    consecutive_blocks=consecutive_blocks,
                )

                processed = result["processed"]
                saved = result["saved"]
                batch = result["batch"]
                consecutive_blocks = result["consecutive_blocks"]
                profile_path = result["profile_path"]

                if not result["crashed"]:
                    session_ok = True
                    break

                # Retry с логом
                if retry < _MAX_SESSION_RETRIES - 1:
                    logger.warning(
                        f"Worker {task.worker_id}: session retry "
                        f"{retry + 1}/{_MAX_SESSION_RETRIES} "
                        f"for block {session_ids[0]}..{session_ids[-1]}"
                    )

            if session_ok:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                if not stop_event.is_set():
                    logger.error(
                        f"Worker {task.worker_id}: block "
                        f"{session_ids[0]}..{session_ids[-1]} failed "
                        f"after {_MAX_SESSION_RETRIES} retries — skipping"
                    )

        # Финальный flush батча
        if batch:
            count = sync_save_batch(batch)
            saved += count
            logger.info(
                f"Worker {task.worker_id}: final batch saved {count}"
            )

        self._report_progress(WorkerProgress(
            worker_id=task.worker_id,
            processed=processed,
            saved=saved,
            current_id=task.end_id,
            is_done=True,
        ))

        return {"processed": processed, "saved": saved}

    def _run_session(
            self,
            task: ScrapeTask,
            session_ids: list,
            batch: list,
            processed: int,
            saved: int,
            profile_path: str,
            consecutive_blocks: int,
        ) -> dict:
            """
            Одна браузерная сессия для части диапазона.
            При 5 блокировках подряд — меняет профиль.
            При fatal ошибке (browser мёртв) — немедленный break.
            Возвращает crashed=True если сессия упала.
            """
            crashed = False
            browser_dead = False

            try:
                with BrowserSession(task.country, profile_path) as session:
                    for pid in session_ids:
                        if stop_event.is_set():
                            logger.info(
                                f"Worker {task.worker_id}: "
                                f"stop signal received"
                            )
                            break

                        try:
                            if sync_id_exists(pid):
                                processed += 1
                                continue

                            html = session.fetch(
                                f"{BASE_URL}/{task.country}/nomer{pid}"
                            )

                            if not html:
                                consecutive_blocks += 1
                                processed += 1
                                if consecutive_blocks >= 5:
                                    profile_path = self._handle_profile_block(
                                        task.worker_id, profile_path
                                    )
                                    consecutive_blocks = 0
                                    # Профиль сменён, но browser старый —
                                    # нужно завершить сессию чтобы следующий
                                    # retry создал browser с новым профилем
                                    break
                                continue

                            consecutive_blocks = 0
                            record = parse_plate_page(html, pid, task.country)

                            if record:
                                local = download_photo(
                                    record.photo_url, pid, task.country
                                )
                                record.local_path = local
                                batch.append(record)

                            processed += 1

                            if processed % CHECKPOINT_EVERY == 0:
                                count = sync_save_batch(batch)
                                saved += count
                                batch.clear()
                                save_checkpoint(pid, task.country)
                                self._report_progress(WorkerProgress(
                                    worker_id=task.worker_id,
                                    processed=processed,
                                    saved=saved,
                                    current_id=pid,
                                ))
                                logger.info(
                                    f"Worker {task.worker_id}: "
                                    f"processed={processed} saved={saved} "
                                    f"id={pid}"
                                )

                        except ConnectionError as e:
                            logger.warning(
                                f"Worker {task.worker_id}: "
                                f"KillBot id={pid}: {e}"
                            )
                            processed += 1
                            continue
                        except Exception as e:
                            err_msg = str(e).lower()
                            # Browser мёртв — нет смысла продолжать fetch
                            if any(f in err_msg for f in _FATAL_SESSION_ERRORS):
                                logger.error(
                                    f"Worker {task.worker_id}: "
                                    f"browser dead at id={pid}: {e} "
                                    f"— ending session"
                                )
                                browser_dead = True
                                break
                            logger.error(
                                f"Worker {task.worker_id}: "
                                f"failed id={pid}: {e}"
                            )
                            processed += 1
                            continue

            except Exception as e:
                logger.error(
                    f"Worker {task.worker_id}: session crashed: {e}"
                )
                crashed = True

            # browser_dead = break изнутри with (browser умер).
            # Нужен retry чтобы создать свежий browser.
            if browser_dead:
                crashed = True

            # При любом crash — сохраняем batch и даём паузу
            if crashed:
                if batch:
                    sync_save_batch(batch)
                    batch.clear()
                time.sleep(5)

            return {
                "processed": processed,
                "saved": saved,
                "batch": batch,
                "consecutive_blocks": consecutive_blocks,
                "profile_path": profile_path,
                "crashed": crashed,
            }

    def _handle_profile_block(self, worker_id: int, current_path: str) -> str:
        """
        Обрабатывает блокировку профиля.
        Пауза 5 минут разбита на 1-секундные интервалы —
        stop_event проверяется на каждой итерации.
        """
        logger.warning(
            f"Worker {worker_id}: 5 consecutive blocks — "
            f"switching profile"
        )
        self._manager.mark_blocked(current_path)
        new_path = self._manager.get_next_profile()

        if new_path:
            logger.info(
                f"Worker {worker_id}: switched to "
                f"{_profile_name(new_path)}"
            )
            return new_path

        logger.error(
            f"Worker {worker_id}: no profiles available — "
            f"pausing up to 5min"
        )

        for _ in range(300):
            if stop_event.is_set():
                logger.info(
                    f"Worker {worker_id}: stop signal during pause"
                )
                break
            time.sleep(1)

        return current_path

    def _report_progress(self, progress: WorkerProgress) -> None:
        """Отправляет прогресс в Queue — веб-панель читает оттуда."""
        try:
            self._progress_queue.put_nowait(progress)
        except Exception:
            pass

    def _split_range(
            self,
            country: str,
            start_id: int,
            end_id: int,
            workers: int,
        ) -> list[ScrapeTask]:
            """
            Делит диапазон ID между воркерами.
            Количество воркеров ограничено числом доступных профилей —
            два воркера с одним профилем = lock файл Firefox = crash.
            """
            # Не создавать больше воркеров чем профилей
            available_profiles = self._manager.get_stats()["available"]
            if workers > available_profiles:
                logger.warning(
                    f"Requested {workers} workers but only "
                    f"{available_profiles} profiles available — "
                    f"reducing to {available_profiles}"
                )
                workers = available_profiles

            if workers <= 0:
                logger.error("No available profiles — cannot start")
                return []

            total = end_id - start_id + 1
            chunk = max(1, total // workers)
            tasks = []

            # Собираем уникальные профили для каждого воркера
            used_profiles: set[str] = set()

            for i in range(workers):
                chunk_start = start_id + i * chunk
                chunk_end = (
                    chunk_start + chunk - 1
                    if i < workers - 1
                    else end_id
                )
                profile_path = self._manager.get_next_profile()

                if not profile_path:
                    logger.error(
                        f"No profile for worker {i} — stopping split"
                    )
                    break

                # Защита от дубликатов: если профиль уже выдан
                # другому воркеру — пропускаем
                if profile_path in used_profiles:
                    logger.warning(
                        f"Profile {_profile_name(profile_path)} "
                        f"already assigned — skipping worker {i}"
                    )
                    continue

                used_profiles.add(profile_path)

                tasks.append(ScrapeTask(
                    worker_id=i,
                    country=country,
                    start_id=chunk_start,
                    end_id=chunk_end,
                    profile_path=profile_path,
                ))
                logger.debug(
                    f"Worker {i}: "
                    f"range={chunk_start}..{chunk_end}, "
                    f"profile={_profile_name(profile_path)}"
                )

            return tasks


# ─── Утилиты ──────────────────────────────────────────────────────────────────

def _profile_name(path: str) -> str:
    """Короткое имя профиля для логов."""
    return os.path.basename(path) if path else "unknown"