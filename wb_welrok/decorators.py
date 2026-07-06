import asyncio
import functools
import logging

logger = logging.getLogger(__name__)


def retry(retries=3, delay=1.0, exceptions=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exc = RuntimeError("Retry attempts exhausted without exception")
            for attempt in range(1, retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    logger.warning("Attempt %s failed: %s", attempt, e)
                    if attempt < retries:
                        backoff = delay * (2 ** (attempt - 1))
                        logger.warning("Waiting %s seconds before next attempt", backoff)
                        await asyncio.sleep(backoff)
            raise last_exc

        return wrapper

    return decorator
