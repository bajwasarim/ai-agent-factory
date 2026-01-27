"""Centralized logging utility with colored console output."""

import logging
import sys
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler


_loggers: dict[str, logging.Logger] = {}


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """
    Get or create a logger with colored console output.

    Args:
        name: Logger name (typically __name__ of calling module).
        level: Optional logging level. Defaults to INFO.

    Returns:
        Configured logger instance.
    """
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(level or logging.INFO)

    if not logger.handlers:
        console = Console(stderr=True)
        handler = RichHandler(
            console=console,
            show_time=True,
            show_path=False,
            rich_tracebacks=True,
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)

    logger.propagate = False
    _loggers[name] = logger
    return logger
