"""Logging."""

import logging

__HANDLER = logging.StreamHandler()
__HANDLER.setLevel(logging.INFO)

__LOGGER = logging.getLogger(__name__)
__LOGGER.setLevel(logging.DEBUG)
__LOGGER.addHandler(__HANDLER)


def get_logger():
    """Return the logger."""
    return __LOGGER
