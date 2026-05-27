from __future__ import annotations

import logging
import sys
from typing import Any, Literal

import structlog


_redis_client: Any = None
_LOG_KEY = "chain_indexer:logs"
_MAX_LOGS = 500


def set_log_redis_client(client: Any) -> None:
    global _redis_client
    _redis_client = client


def _redis_log_sink(_logger: Any, _method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    if _redis_client is not None:
        import json
        try:
            line = json.dumps(event_dict, default=str)
            _redis_client.lpush(_LOG_KEY, line)
            _redis_client.ltrim(_LOG_KEY, 0, _MAX_LOGS - 1)
        except Exception:
            pass
    return event_dict


def configure_logging(
    *,
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO",
    format: Literal["json", "console"] = "json",
) -> None:
    """Configure stdlib logging + structlog. Idempotent."""
    log_level = getattr(logging, level)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
        force=True,
    )

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        _redis_log_sink,
    ]
    if format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=False))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
