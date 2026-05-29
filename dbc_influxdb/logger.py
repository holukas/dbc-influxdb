"""Package logging.

Provides a single package logger (``dbc_influxdb``) and a helper to attach a
:class:`rich.logging.RichHandler` for nicely formatted console output.

Following library conventions, importing this module does not configure any
handler on its own. :func:`setup_logging` is called by
:class:`dbc_influxdb.main.dbcInflux` so that the default behaviour (messages
printed to the console) is preserved, but applications embedding this library
can configure their own handlers instead.
"""
import logging

from rich.logging import RichHandler

log = logging.getLogger("dbc_influxdb")


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Attach a RichHandler to the package logger (idempotent).

    Args:
        level: logging level for the package logger.

    Returns:
        The configured package logger.
    """
    if not log.handlers:
        handler = RichHandler(rich_tracebacks=True, show_path=False, markup=False)
        handler.setFormatter(logging.Formatter("%(message)s", datefmt="[%X]"))
        log.addHandler(handler)
        log.propagate = False
    log.setLevel(level)
    return log
