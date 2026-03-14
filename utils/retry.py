from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import logging
from config.settings import RETRY_ATTEMPTS, RETRY_WAIT_MIN, RETRY_WAIT_MAX

logger = logging.getLogger(__name__)

# Исключения при которых имеет смысл повторять запрос
RETRYABLE_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    OSError,
)

def make_retry_decorator():
    """
    Возвращает декоратор @retry с нашими настройками.
    Используй так:
    
        @make_retry_decorator()
        def fetch_page(url): ...
    """
    return retry(
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential(
            multiplier=1,
            min=RETRY_WAIT_MIN,
            max=RETRY_WAIT_MAX,
        ),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,  # если все попытки исчерпаны — пробрасываем исключение дальше
    )

# Готовый декоратор для импорта
scraper_retry = make_retry_decorator()
