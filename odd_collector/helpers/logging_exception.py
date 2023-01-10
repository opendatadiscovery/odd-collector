from contextlib import contextmanager
from logging import Logger


@contextmanager
def logging_exception(exc_msg: str, logger: Logger) -> None:
    """Wrap any code block with this context manager to catch exceptions occurring in it and log them"""
    try:
        yield None
    except Exception as e:
        logger.exception(exc_msg)
