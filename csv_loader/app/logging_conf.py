import logging
import logging.handlers
from pathlib import Path
import os


def setup_logging(log_dir: str) -> None:
    """
    Configure logging so that output goes to:
    1. stdout  (for Docker logs)
    2. file in logs/app.log (for local inspection)
    """

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / "app.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )
    console_handler.setFormatter(console_fmt)

    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8"
    )
    file_handler.setLevel(logging.INFO)
    file_fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )
    file_handler.setFormatter(file_fmt)

    logger.handlers.clear()

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info("Logging initialized. Writing to stdout and %s", log_file)
