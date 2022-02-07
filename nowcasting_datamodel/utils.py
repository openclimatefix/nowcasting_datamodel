""" Utils functions for models """
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def datetime_must_have_timezone(cls, v: datetime):
    """Enforce that this variable must have a timezone"""
    if v.tzinfo is None:
        logger.debug(f"{v} must have a timezone, for cls {cls}." f"So we are going to add UTC ")
        v = v.replace(tzinfo=timezone.utc)
    return v


def convert_to_camelcase(snake_str: str) -> str:
    """Converts a given snake_case string into camelCase"""
    first, *others = snake_str.split("_")
    return "".join([first.lower(), *map(str.title, others)])
