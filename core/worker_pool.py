import os
import threading
import time
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

_STAGGER_DELAY_S = 5
_MAX_SESSION_RETRIES = 3
_MAX_CONSECUTIVE_FAILURES = 5

_MAX_CONCURRENT_BROWSERS = 6


_EMPTY_SKIP_THRESHOLD = 30
_EMPTY_SKIP_JUMP = 3000

_FATAL_SESSION_ERRORS = (
    "browser has been closed",
    "target page, context or browser",
    "page crashed",
)


@dataclass
class WorkerProgress:
    worker_id: int
    processed: int
    saved: int
    current_id: int
    is_done: bool = False


@dataclass
class ScrapeTask:
    worker_id: int
    country: str
    start_id: int
    end_id: int
    profile_path: str


class WorkerPool:
    def __init__(
        self,
        profile_manager: ProfileManager,
        progress_queue: Optional[Queue] = None,
    ):
        self._manager = profile_manager
        self._progress_queue = progress_queue or Queue()
        self._browser_semaphore = threading.Semaphore(_MAX_CONCURRENT_BROWSERS)

    def run(
        self,
        country: str,
        start_id: int,
        end_id: int,
        workers: int,
        resume: bool = True,
    ) -> dict:
        actual_start = load_checkpoint(country, start_id) if resume else start_id
        tasks = self._split_range(country, actual_start, end_id, workers)

        if not tasks:
            logger.error("No tasks created — check profile availability")
            return {"processed": 0, "saved": 0, "workers": 0}

        logger.info(
            f"Starting pool: country={country}, "
            f"range={actual_start}..{end_id}, "
            f"workers={len(tasks)}, "
            f"max_browsers={_MAX_CONCURRENT_BROWSERS}"
        )

        stats = {"processed": 0, "saved": 0, "workers": len(tasks)}
        results: dict[int, dict] = {}
        results_lock = threading.Lock()

        def run_and_collect(task: ScrapeTask) -> None:
            result = self._run_worker(task)
            with results_lock:
                results[task.worker_id] = result

        threads = [
            threading.Thread(target=run_and_collect, args=(task,), daemon=True)
            for task in tasks
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for result in results.values():
            stats["processed"] += result["processed"]
            stats["saved"] += result["saved"]

        total_in_db = sync_get_count(country)
        logger.info(
            f"Pool done: processed={stats['processed']}, "
            f"saved={stats['saved']}, total in DB={total_in_db}"
        )
        return stats

    def _run_worker(self, task: ScrapeTask) -> dict:
        if task.worker_id > 0:
            stagger = task.worker_id * _STAGGER_DELAY_S
            logger.info(f"Worker {task.worker_id}: stagger {stagger}s")
            for _ in range(stagger):
                if stop_event.is_set():
                    return {"processed": 0, "saved": 0}
                time.sleep(1)

        processed = 0
        saved = 0
        batch: list = []
        profile_path = task.profile_path
        consecutive_failures = 0

        logger.info(
            f"Worker {task.worker_id}: "
            f"range={task.start_id}..{task.end_id}, "
            f"profile={_profile_name(profile_path)}"
        )

        session_starts = range(0, task.end_id - task.start_id + 1, SESSION_SIZE)

        for block_index, session_offset in enumerate(session_starts):
            if stop_event.is_set():
                break

            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                logger.error(
                    f"Worker {task.worker_id}: {consecutive_failures} "
                    f"consecutive failures — stopping"
                )
                break

            session_ids = list(range(
                task.start_id + session_offset,
                min(task.start_id + session_offset + SESSION_SIZE, task.end_id + 1),
            ))

            is_first_session = (block_index == 0)
            session_ok = False

            for retry in range(_MAX_SESSION_RETRIES):
                if stop_event.is_set():
                    break

                result_container: dict = {}
                session_thread = threading.Thread(
                    target=self._run_session_in_thread,
                    args=(
                        task, session_ids, batch, processed, saved,
                        profile_path, result_container, is_first_session,
                    ),
                    daemon=True,
                )
                session_thread.start()
                session_thread.join()

                if not result_container:
                    logger.error(
                        f"Worker {task.worker_id}: session thread returned no result"
                    )
                    continue

                processed = result_container["processed"]
                saved = result_container["saved"]
                batch = result_container["batch"]
                profile_path = result_container["profile_path"]

                if not result_container["crashed"]:
                    session_ok = True
                    break

                if retry < _MAX_SESSION_RETRIES - 1:
                    logger.warning(
                        f"Worker {task.worker_id}: retry "
                        f"{retry + 1}/{_MAX_SESSION_RETRIES} "
                        f"block {session_ids[0]}..{session_ids[-1]}"
                    )
                    is_first_session = True

            if session_ok:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                if not stop_event.is_set():
                    logger.error(
                        f"Worker {task.worker_id}: block "
                        f"{session_ids[0]}..{session_ids[-1]} "
                        f"failed after {_MAX_SESSION_RETRIES} retries — skipping"
                    )

        if batch:
            count = sync_save_batch(batch)
            saved += count
            logger.info(f"Worker {task.worker_id}: final flush {count} records")

        self._report_progress(WorkerProgress(
            worker_id=task.worker_id,
            processed=processed,
            saved=saved,
            current_id=task.end_id,
            is_done=True,
        ))

        return {"processed": processed, "saved": saved}

    def _run_session_in_thread(
        self,
        task: ScrapeTask,
        session_ids: list,
        batch: list,
        processed: int,
        saved: int,
        profile_path: str,
        result_container: dict,
        is_first_session: bool = True,
    ) -> None:
        crashed = False
        browser_dead = False
        consecutive_blocks = 0
        consecutive_empty = 0  # счётчик пустых ID подряд для skip логики

        logger.debug(f"Worker {task.worker_id}: waiting for browser slot...")
        while not self._browser_semaphore.acquire(timeout=5):
            if stop_event.is_set():
                result_container.update({
                    "processed": processed,
                    "saved": saved,
                    "batch": batch,
                    "profile_path": profile_path,
                    "crashed": False,
                })
                return

        logger.debug(f"Worker {task.worker_id}: browser slot acquired")

        try:
            with BrowserSession(
                task.country,
                profile_path,
                is_first_session=is_first_session,
            ) as session:
                # Используем индекс вместо for-in чтобы поддерживать skip
                pid_index = 0
                while pid_index < len(session_ids):
                    if stop_event.is_set():
                        break

                    pid = session_ids[pid_index]
                    pid_index += 1

                    try:
                        if sync_id_exists(pid):
                            processed += 1
                            continue

                        html = session.fetch(
                            f"{BASE_URL}/{task.country}/nomer{pid}"
                        )

                        if not html:
                            consecutive_blocks += 1
                            consecutive_empty += 1
                            processed += 1

                            # Skip пустого диапазона:
                            # 30 пустых ID подряд → прыгаем на 3000 вперёд.
                            # Сдвигаем pid_index до первого ID >= (pid + jump).
                            if consecutive_empty >= _EMPTY_SKIP_THRESHOLD:
                                skip_to = pid + _EMPTY_SKIP_JUMP
                                while (
                                    pid_index < len(session_ids)
                                    and session_ids[pid_index] < skip_to
                                ):
                                    pid_index += 1
                                logger.info(
                                    f"Worker {task.worker_id}: "
                                    f"{consecutive_empty} empty IDs — "
                                    f"jumping to id≈{skip_to}"
                                )
                                consecutive_empty = 0

                            if consecutive_blocks >= 5:
                                profile_path = self._handle_profile_block(
                                    task.worker_id, profile_path
                                )
                                consecutive_blocks = 0
                                break
                            continue

                        consecutive_blocks = 0
                        consecutive_empty = 0
                        record = parse_plate_page(html, pid, task.country)

                        if record:
                            local = download_photo(
                                record.photo_url, pid, record.country
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
                                f"processed={processed} saved={saved} id={pid}"
                            )

                    except ConnectionError as e:
                        logger.warning(
                            f"Worker {task.worker_id}: KillBot id={pid}: {e}"
                        )
                        processed += 1

                    except Exception as e:
                        err_lower = str(e).lower()
                        if any(f in err_lower for f in _FATAL_SESSION_ERRORS):
                            logger.error(
                                f"Worker {task.worker_id}: browser dead id={pid}: {e}"
                            )
                            browser_dead = True
                            break
                        logger.error(
                            f"Worker {task.worker_id}: failed id={pid}: {e}"
                        )
                        processed += 1

        except Exception as e:
            logger.error(f"Worker {task.worker_id}: session crashed: {e}")
            crashed = True

        finally:
            self._browser_semaphore.release()
            logger.debug(f"Worker {task.worker_id}: browser slot released")

        if browser_dead:
            crashed = True

        if crashed and batch:
            sync_save_batch(batch)
            batch.clear()
            time.sleep(5)

        result_container.update({
            "processed": processed,
            "saved": saved,
            "batch": batch,
            "profile_path": profile_path,
            "crashed": crashed,
        })

    def _handle_profile_block(self, worker_id: int, current_path: str) -> str:
        logger.warning(f"Worker {worker_id}: 5 blocks — switching profile")
        self._manager.mark_blocked(current_path)
        new_path = self._manager.get_next_profile()

        if new_path:
            logger.info(f"Worker {worker_id}: switched to {_profile_name(new_path)}")
            return new_path

        logger.error(f"Worker {worker_id}: no profiles — pausing 5min")
        for _ in range(300):
            if stop_event.is_set():
                break
            time.sleep(1)

        return current_path

    def _report_progress(self, progress: WorkerProgress) -> None:
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
        available_profiles = self._manager.get_stats()["available"]
        if workers > available_profiles:
            logger.warning(
                f"Reducing workers {workers} → {available_profiles} (profile limit)"
            )
            workers = available_profiles

        if workers <= 0:
            logger.error("No available profiles")
            return []

        total = end_id - start_id + 1
        chunk = max(1, total // workers)
        tasks = []
        used_profiles: set[str] = set()

        for i in range(workers):
            chunk_start = start_id + i * chunk
            chunk_end = chunk_start + chunk - 1 if i < workers - 1 else end_id

            profile_path = self._manager.get_next_profile()
            if not profile_path:
                logger.error(f"No profile for worker {i} — stopping")
                break

            if profile_path in used_profiles:
                logger.warning(f"Duplicate profile for worker {i} — skipping")
                continue

            used_profiles.add(profile_path)
            tasks.append(ScrapeTask(
                worker_id=i,
                country=country,
                start_id=chunk_start,
                end_id=chunk_end,
                profile_path=profile_path,
            ))

        return tasks


def _profile_name(path: str) -> str:
    return os.path.basename(path) if path else "unknown"