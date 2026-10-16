"""Timestamped logger shared by the Milestone 2 services."""
import logging
import os
import sys

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Logger that writes to logs/<name>.log and to the terminal."""
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        logger.propagate = False
        fmt = logging.Formatter(FORMAT)
        for handler in (logging.FileHandler(os.path.join(LOG_DIR, f"{name}.log")),
                        logging.StreamHandler(sys.stdout)):
            handler.setFormatter(fmt)
            logger.addHandler(handler)
    return logger