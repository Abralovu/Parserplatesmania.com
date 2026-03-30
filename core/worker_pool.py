#!/usr/bin/env python3
"""
WorkerPool — многопоточный парсер.
Каждый поток использует отдельный Chrome профиль.
Полностью синхронный — без asyncio, без конфликтов event loop.
Автор: viramax
"""

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
    Каждый поток = отдельный Chrome профиль = отдельный диапазон ID.

    Использование:
        pool = WorkerPool(profile_manager, progress_queue)
        pool.run(country='ru', start_id=31000000, end_id=32000000, workers=3)
    """

    def __init__(
        self,
        profile_manager: ProfileManager,
        progress_queue: Optional[Queue] = None,
    ):
        self._manager        = profile_manager
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
        tasks        = self._split_range(country, actual_start, end_id, workers)

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
            futures = {executor.submit(self._run_worker, task): task for task in tasks}
            for future in as_completed(futures):
                task = futures[future]
                try:
                    result = future.result()
                    stats["processed"] += result["processed"]
                    stats["saved"]     += result["saved"]
                except Exception as e:
                    logger.error(f"Worker {task.worker_id} failed: {e}")

        total_in_db = sync_get_count(country)
        logger.info(
            f"Pool done: processed={stats['processed']}, "
            f"saved={stats['saved']}, total in DB={total_in_db}"
        )
        return stats

    # ─── Приватные методы ─────────────────────────────────────────────────────

    def _run_worker(self, task: ScrapeTask) -> dict:
        """
        Один воркер — парсит свой диапазон ID.
        Полностью синхронный — свой sqlite3 connection через threading.local.
        При блокировке профиля — берёт следующий доступный.
        """
        ids                 = range(task.start_id, task.end_id + 1)
        processed           = 0
        saved               = 0
        batch               = []
        profile_path        = task.profile_path
        consecutive_blocks  = 0

        logger.info(
            f"Worker {task.worker_id}: "
            f"range={task.start_id}..{task.end_id}, "
            f"profile={_profile_name(profile_path)}"
        )

        for session_start in range(0, task.end_id - task.start_id + 1, SESSION_SIZE):
            if stop_event.is_set():
                break

            session_ids = list(
                range(
                    task.start_id + session_start,
                    min(task.start_id + session_start + SESSION_SIZE, task.end_id + 1),
                )
            )

            result = self._run_session(
                task=task,
                session_ids=session_ids,
                batch=batch,
                processed=processed,
                saved=saved,
                profile_path=profile_path,
                consecutive_blocks=consecutive_blocks,
            )

            processed          = result["processed"]
            saved              = result["saved"]
            batch              = result["batch"]
            consecutive_blocks = result["consecutive_blocks"]
            profile_path       = result["profile_path"]

        # Финальный flush батча
        if batch:
            count  = sync_save_batch(batch)
            saved += count
            logger.info(f"Worker {task.worker_id}: final batch saved {count}")

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
        stop_event импортируется на уровне модуля — нет import внутри цикла.
        """
        try:
            with BrowserSession(task.country, profile_path) as session:
                for pid in session_ids:
                    if stop_event.is_set():
                        logger.info(f"Worker {task.worker_id}: stop signal received")
                        break

                    try:
                        if sync_id_exists(pid):
                            processed += 1
                            continue

                        html = session.fetch(f"{BASE_URL}/{task.country}/nomer{pid}")

                        if not html:
                            consecutive_blocks += 1
                            processed          += 1
                            if consecutive_blocks >= 5:
                                profile_path       = self._handle_profile_block(
                                    task.worker_id, profile_path
                                )
                                consecutive_blocks = 0
                            continue

                        consecutive_blocks = 0
                        record = parse_plate_page(html, pid, task.country)

                        if record:
                            local          = download_photo(record.photo_url, pid, task.country)
                            record.local_path = local
                            batch.append(record)

                        processed += 1

                        if processed % CHECKPOINT_EVERY == 0:
                            count  = sync_save_batch(batch)
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
                                f"processed={processed} saved={saved} id={pid}"
                            )

                    except ConnectionError as e:
                        logger.warning(f"Worker {task.worker_id}: KillBot id={pid}: {e}")
                        processed += 1
                        continue
                    except Exception as e:
                        logger.error(f"Worker {task.worker_id}: failed id={pid}: {e}")
                        processed += 1
                        continue

        except Exception as e:
            logger.error(
                f"Worker {task.worker_id}: session crashed: {e} — saving batch"
            )
            if batch:
                sync_save_batch(batch)
                batch.clear()
            time.sleep(5)

        return {
            "processed":          processed,
            "saved":              saved,
            "batch":              batch,
            "consecutive_blocks": consecutive_blocks,
            "profile_path":       profile_path,
        }

    def _handle_profile_block(self, worker_id: int, current_path: str) -> str:
        """
        Обрабатывает блокировку профиля.
        Пауза 5 минут разбита на короткие интервалы — stop_event проверяется
        каждую секунду, остановка не блокируется на 5 минут.
        """
        logger.warning(f"Worker {worker_id}: 5 consecutive blocks — switching profile")
        self._manager.mark_blocked(current_path)
        new_path = self._manager.get_next_profile()

        if new_path:
            logger.info(f"Worker {worker_id}: switched to {_profile_name(new_path)}")
            return new_path

        logger.error(f"Worker {worker_id}: no profiles available — pausing up to 5min")

        # Ждём по 1 секунде вместо одного sleep(300) —
        # stop_event проверяется на каждой итерации
        for _ in range(300):
            if stop_event.is_set():
                logger.info(f"Worker {worker_id}: stop signal during pause")
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
        Делит диапазон ID на равные части между воркерами.
        Каждому воркеру назначается свой профиль.
        """
        total = end_id - start_id + 1
        chunk = max(1, total // workers)
        tasks = []

        for i in range(workers):
            chunk_start  = start_id + i * chunk
            chunk_end    = chunk_start + chunk - 1 if i < workers - 1 else end_id
            profile_path = self._manager.get_next_profile()

            if not profile_path:
                logger.error(f"No profile for worker {i} — stopping split")
                break

            tasks.append(ScrapeTask(
                worker_id=i,
                country=country,
                start_id=chunk_start,
                end_id=chunk_end,
                profile_path=profile_path,
            ))
            logger.debug(
                f"Worker {i}: range={chunk_start}..{chunk_end}, "
                f"profile={_profile_name(profile_path)}"
            )

        return tasks


# ─── Утилиты ──────────────────────────────────────────────────────────────────

def _profile_name(path: str) -> str:
    """Короткое имя профиля для логов."""
    return os.path.basename(path) if path else "unknown"