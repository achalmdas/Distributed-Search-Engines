"""
common/logger.py
CECS 327 - Distributed Search Engine, Milestone 1

Shared logging setup so index_node, gateway, and client all log in the
same format (matches the format already used in index_node.py).
"""

import logging


def setup_logging(name):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
    )
    return logging.getLogger(name)