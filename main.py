#!/usr/bin/env python3
"""
CLI точка входа — PlatesMania Scraper v2.0
Команды: scrape, status, excel, export, reset
Автор: viramax
"""

import argparse
import asyncio
import os
import platform
import subprocess

from rich.console import Console
from rich.table import Table

from storage.database import init_db, get_count
from core.scraper import scrape_range
from utils.checkpoint import reset_checkpoint, load_checkpoint
from utils.logger import get_logger
from config.settings import OUTPUT_DIR

logger = get_logger(__name__)
console = Console()


def print_banner() -> None:
    console.print("""
[bold cyan]
██████╗ ██╗      █████╗ ████████╗███████╗███████╗
██╔══██╗██║     ██╔══██╗╚══██╔══╝██╔════╝██╔════╝
██████╔╝██║     ███████║   ██║   █████╗  ███████╗
██╔═══╝ ██║     ██╔══██║   ██║   ██╔══╝  ╚════██║
██║     ███████╗██║  ██║   ██║   ███████╗███████║
╚═╝     ╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝╚══════╝
[/bold cyan]
[dim]PlatesMania Scraper v2.0 — professional edition[/dim]
""")


def cmd_scrape(args) -> None:
    """
    Запуск парсинга.
    --auto      — сам определяет диапазон ID
    --workers N — количество потоков (по числу профилей)
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    asyncio.run(init_db())
    from storage.database import sync_init_db
    sync_init_db()
    
    start_id, end_id = _resolve_range(args)
    if start_id is None or end_id is None:
        return

    logger.info(f"Target: country={args.country}, range={start_id}..{end_id}")
    logger.info(f"Workers: {args.workers} | Resume: {not args.fresh}")

    if args.workers > 1:
        _run_multi_worker(args, start_id, end_id)
    else:
        scrape_range(
            country=args.country,
            start_id=start_id,
            end_id=end_id,
            resume=not args.fresh,
        )


def _resolve_range(args) -> tuple[int | None, int | None]:
    """
    Определяет диапазон ID.
    --auto → читает максимальный ID с сайта.
    Иначе → берёт --start и --end из аргументов.
    """
    if args.auto:
        console.print("[cyan]Auto-detecting range...[/cyan]")
        try:
            from core.range_detector import detect_range
            from core.profile_manager import ProfileManager

            manager = ProfileManager(count=args.workers)
            manager.ensure_ready(country=args.country)
            profile_path = manager.get_next_profile()

            start_id, end_id = detect_range(args.country, profile_path)
            console.print(f"[green]Detected range: {start_id}..{end_id}[/green]")
            return start_id, end_id
        except Exception as e:
            logger.error(f"Auto-detect failed: {e}")
            console.print("[red]Auto-detect failed — use --start and --end manually[/red]")
            return None, None

    return args.start, args.end


def _run_multi_worker(args, start_id: int, end_id: int) -> None:
    """Запускает многопоточный парсинг через WorkerPool."""
    try:
        from core.worker_pool import WorkerPool
        from core.profile_manager import ProfileManager
        from queue import Queue

        manager = ProfileManager(count=args.workers)
        manager.ensure_ready(country=args.country)

        progress_queue: Queue = Queue()
        pool = WorkerPool(manager, progress_queue)

        console.print(f"[cyan]Starting {args.workers} workers...[/cyan]")
        pool.run(
            country=args.country,       
            start_id=start_id,
            end_id=end_id,
            workers=args.workers,
            resume=not args.fresh,     
        )
    except Exception as e:
        logger.error(f"Multi-worker failed: {e}")
        console.print("[red]Multi-worker error — falling back to single thread[/red]")
        scrape_range(
            country=args.country,
            start_id=start_id,
            end_id=end_id,
            resume=not args.fresh,
        )


def cmd_status(args) -> None:
    """Показывает статус — сколько собрано, где checkpoint."""
    asyncio.run(init_db())

    table = Table(title="Scraper Status", show_header=True)
    table.add_column("Country", style="cyan")
    table.add_column("Records in DB", style="green")
    table.add_column("Last checkpoint ID", style="yellow")

    countries = args.countries.split(",") if args.countries else ["ru", "de", "us"]

    async def _collect_stats() -> list[tuple]:
        results = []
        for country in countries:
            count = await get_count(country if country != "all" else None)
            checkpoint = load_checkpoint(country, 1)
            results.append((country, str(count), str(checkpoint)))
        return results

    rows = asyncio.run(_collect_stats())
    for row in rows:
        table.add_row(*row)
    console.print(table)


def cmd_excel(args) -> None:
    """Экспорт в Excel с фото внутри."""
    from utils.export_excel import export_excel_with_photos

    path = asyncio.run(export_excel_with_photos(
        country=args.country if args.country != "all" else None,
    ))
    if path:
        console.print(f"[green]Excel exported: {path}[/green]")
        _open_file(path)
    else:
        console.print("[red]No data found[/red]")


def cmd_export(args) -> None:
    """Экспорт в CSV."""
    from utils.export import export_csv

    path = asyncio.run(export_csv(
        country=args.country if args.country != "all" else None,
        output_file=args.output,
    ))
    if path:
        console.print(f"[green]Exported to: {path}[/green]")
    else:
        console.print("[red]No data found[/red]")


def cmd_reset(args) -> None:
    """Сброс checkpoint для страны."""
    reset_checkpoint(args.country)
    console.print(f"[yellow]Checkpoint reset for: {args.country}[/yellow]")


def _open_file(path: str) -> None:
    """
    Открывает файл в системном приложении.
    На VPS (Linux headless) — логирует путь, не падает.
    """
    system = platform.system()
    try:
        if system == "Darwin":
            subprocess.run(["open", path], check=False)
        elif system == "Windows":
            os.startfile(path)  # type: ignore[attr-defined]
        else:
            logger.info(f"File saved at: {path} (open manually)")
    except Exception as e:
        logger.warning(f"Could not open file automatically: {e}")
        
def cmd_web(args) -> None:
    """Запуск веб-панели."""
    asyncio.run(init_db())
    console.print(f"[green]Web panel: http://localhost:{args.port}[/green]")
    from web.app import start_web
    start_web(host=args.host, port=args.port)


def main() -> None:
    print_banner()

    parser = argparse.ArgumentParser(
        description="PlatesMania Scraper v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- scrape ---
    p_scrape = subparsers.add_parser("scrape", help="Start scraping")
    p_scrape.add_argument("--country",  required=True,        help="Country code: ru, de, us")
    p_scrape.add_argument("--start",    type=int, default=1,  help="Start plate ID")
    p_scrape.add_argument("--end",      type=int, default=100_000, help="End plate ID")
    p_scrape.add_argument("--fresh",    action="store_true",  help="Ignore checkpoint")
    p_scrape.add_argument("--auto",     action="store_true",  help="Auto-detect range from site")
    p_scrape.add_argument("--workers",  type=int, default=1,  help="Number of parallel workers")
    p_scrape.set_defaults(func=cmd_scrape)

    # --- status ---
    p_status = subparsers.add_parser("status", help="Show progress")
    p_status.add_argument("--countries", default="ru,de,us", help="Comma-separated codes")
    p_status.set_defaults(func=cmd_status)

    # --- excel ---
    p_excel = subparsers.add_parser("excel", help="Export to Excel with photos")
    p_excel.add_argument("--country", default="all", help="Country code or 'all'")
    p_excel.set_defaults(func=cmd_excel)

    # --- export ---
    p_export = subparsers.add_parser("export", help="Export to CSV")
    p_export.add_argument("--country", default="all", help="Country code or 'all'")
    p_export.add_argument("--output",  default=None,  help="Output file path")
    p_export.set_defaults(func=cmd_export)

    # --- reset ---
    p_reset = subparsers.add_parser("reset", help="Reset checkpoint")
    p_reset.add_argument("--country", required=True, help="Country code or 'all'")
    p_reset.set_defaults(func=cmd_reset)
    
    # --- web ---
    p_web = subparsers.add_parser("web", help="Start web panel")
    p_web.add_argument("--host", default="0.0.0.0", help="Host")
    p_web.add_argument("--port", type=int, default=8000, help="Port")
    p_web.set_defaults(func=cmd_web)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()