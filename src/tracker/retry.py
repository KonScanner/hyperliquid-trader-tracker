"""Shared exponential-backoff-with-jitter helper for the HTTP client.

Ported from the sibling ``hyperdash-crawl`` project (its ``retry.py``), unchanged in
behaviour: retry only transient statuses, honour a server ``Retry-After`` hint, escalate
hint-less throttles via exponential growth.
"""

import asyncio
import logging
import random

from tracker.config import Settings

logger = logging.getLogger(__name__)

# Single source of truth for which HTTP statuses warrant a retry. 501 (Not Implemented) is
# intentionally excluded — it is a permanent error, not transient.
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


async def backoff_or_raise(attempt: int, settings: Settings, label: str, err: Exception) -> int:
    """Sleep with exponential backoff + jitter and return the next attempt number.

    Raises ``err`` once ``max_retries`` is exhausted, so callers can write a simple
    ``attempt = await backoff_or_raise(...)`` retry loop.
    """
    if attempt >= settings.max_retries:
        logger.error("%s: giving up after %d retries", label, attempt)
        raise err
    # Honour a server-supplied Retry-After (rate-limit hint); otherwise exponential backoff.
    # A hint-less rate limit arrives as retry_after == 0.0, so `if retry_after:` lets it fall
    # through to exponential growth rather than backing off by jitter alone.
    retry_after = getattr(err, "retry_after", None)
    if retry_after:
        delay = float(retry_after) + random.uniform(0, settings.backoff_base_s)
    else:
        delay = min(settings.backoff_cap_s, settings.backoff_base_s * (2**attempt))
        delay += random.uniform(0, settings.backoff_base_s)
    logger.warning("%s: transient failure (%s); retry %d in %.1fs", label, err, attempt + 1, delay)
    await asyncio.sleep(delay)
    return attempt + 1
