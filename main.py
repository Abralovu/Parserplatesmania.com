import argparse
import asyncio
import sys
from rich.console import Console
from rich.table import Table

from storage.database import init_db, get_count
from core.scraper import scrape_range
from utils.checkpoint import reset_checkpoint, load_checkpoint
from utils.logger import get_logger
from config.settings import OUTPUT_DIR
import os

logger = get_logger(__name__)
console = Console()


def print_banner():
    console.print("""
[bold cyan]
██████╗ ██╗      █████╗ ████████╗███████╗███████╗
██╔══██╗██║     ██╔══██╗╚══██╔══╝██╔════╝██╔════╝
██████╔╝██║     ███████║   ██║   █████╗  ███████╗
██╔═══╝ ██║     ██╔══██║   ██║   ██╔══╝  ╚════██║
██║     ███████╗██║  ██║   ██║   ███████╗███████║
╚═╝     ╚══════╝╚═╝  ╚═╝   ╚═╝   ╚══════╝╚══════╝
[/bold cyan]
[dim]PlatesMania Scraper — professional edition[/dim]
""")


def cmd_scrape(args):
    """Запуск парсинга."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Инициализируем БД (создаст таблицу если нет)
    asyncio.run(init_db())

    logger.info(f"Target: country={args.country}, range={args.start}..{args.end}")
    logger.info(f"Resume: {not args.fresh}")

    scrape_range(
        country=args.country,
        start_id=args.start,
        end_id=args.end,
        resume=not args.fresh,
    )


def cmd_status(args):
    """Показывает статус — сколько собрано, где checkpoint."""
    asyncio.run(init_db())

    table = Table(title="Scraper Status", show_header=True)
    table.add_column("Country", style="cyan")
    table.add_column("Records in DB", style="green")
    table.add_column("Last checkpoint ID", style="yellow")

    countries = args.countries.split(",") if args.countries else ["ru", "de", "us", "all"]

    for country in countries:
        count = asyncio.run(get_count(country if country != "all" else None))
        checkpoint = load_checkpoint(country)
        table.add_row(country, str(count), str(checkpoint))

    console.print(table)


def cmd_reset(args):
    """Сброс checkpoint для страны."""
    reset_checkpoint(args.country)
    console.print(f"[yellow]Checkpoint reset for: {args.country}[/yellow]")


def main():
    print_banner()

    parser = argparse.ArgumentParser(
        description="PlatesMania Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- Команда: scrape ---
    p_scrape = subparsers.add_parser("scrape", help="Start scraping")
    p_scrape.add_argument("--country", required=True, help="Country code: ru, de, us ...")
    p_scrape.add_argument("--start",   type=int, default=1,       help="Start plate ID")
    p_scrape.add_argument("--end",     type=int, default=100000,  help="End plate ID")
    p_scrape.add_argument("--fresh",   action="store_true",       help="Ignore checkpoint, start fresh")
    p_scrape.set_defaults(func=cmd_scrape)

    # --- Команда: status ---
    p_status = subparsers.add_parser("status", help="Show progress")
    p_status.add_argument("--countries", default="ru,de,us", help="Comma-separated country codes")
    p_status.set_defaults(func=cmd_status)

    # --- Команда: reset ---
    p_reset = subparsers.add_parser("reset", help="Reset checkpoint")
    p_reset.add_argument("--country", required=True, help="Country code or 'all'")
    p_reset.set_defaults(func=cmd_reset)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
