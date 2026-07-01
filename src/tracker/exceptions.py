"""The tracker's exception hierarchy.

Everything raised by the transport/enrichment layers descends from :class:`TrackerError`
so callers can catch the whole family with one ``except`` without swallowing unrelated
``ValueError``/``RuntimeError`` from the stdlib.
"""


class TrackerError(Exception):
    """Base class for every error this package raises."""


class ParseError(TrackerError):
    """A response could not be parsed into the shape we expected (bad/absent JSON, wrong type)."""


class RateLimitedError(TrackerError):
    """A transient failure worth retrying: HTTP 429/5xx or a transport error.

    Carries an optional ``retry_after`` (seconds) parsed from a server hint; ``None`` means
    "no hint, use exponential backoff".
    """

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class AuthRequiredError(TrackerError):
    """A permanent 401/403 — never retried. Should not happen on the public read API."""
