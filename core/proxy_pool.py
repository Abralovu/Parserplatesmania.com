import threading
from typing import Optional
from urllib.parse import urlparse      

from utils.logger import get_logger

logger = get_logger(__name__)


class ProxyPool:
    """
    Потокобезопасный пул прокси с round-robin ротацией.

    Использование:
        pool = ProxyPool(["http://user1:pass@host:10019", ...])
        proxy_dict = pool.next()
        # -> {"server": "http://host:10019", "username": "user1", "password": "pass"}
    """

    def __init__(self, proxy_list: list[str]):
        if not proxy_list:
            raise ValueError("proxy_list cannot be empty")

        self._proxies = proxy_list
        self._index = 0
        self._lock = threading.Lock()
        logger.info(f"ProxyPool initialized with {len(self._proxies)} proxies")

    def next(self) -> dict:
        """
        Возвращает следующий прокси в формате Camoufox/Playwright.
        Потокобезопасный round-robin.
        """
        with self._lock:
            raw_url = self._proxies[self._index % len(self._proxies)]
            self._index += 1

        return self._parse(raw_url)

    def get_by_index(self, index: int) -> dict:
        """Возвращает прокси по индексу (для привязки профиля к прокси)."""
        raw_url = self._proxies[index % len(self._proxies)]
        return self._parse(raw_url)

    @property
    def size(self) -> int:
        return len(self._proxies)

    @staticmethod
    def _parse(proxy_url: str) -> dict:
        """
        Парсит proxy URL в формат Camoufox/Playwright.
        Вход: http://user:pass@host:port
        Выход: {"server": "http://host:port", "username": "...", "password": "..."}
        """
        # from urllib.parse import urlparse  ← УДАЛИТЬ эту строку
        p = urlparse(proxy_url)
        result = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
        if p.username:
            result["username"] = p.username
        if p.password:
            result["password"] = p.password
        return result


def build_proxy_pool(proxy_list_raw: list[str]) -> Optional[ProxyPool]:
    """
    Создаёт ProxyPool из списка прокси.
    Если список пуст — возвращает None (работаем без прокси).
    """
    # Фильтруем пустые строки
    cleaned = [p.strip() for p in proxy_list_raw if p.strip()]
    if not cleaned:
        logger.warning("No proxies provided — running without proxy")
        return None
    return ProxyPool(cleaned)