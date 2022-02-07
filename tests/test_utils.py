""" Utils functions for test """
from datetime import datetime, timezone

import pytest

# Used constants
from nowcasting_datamodel.utils import convert_to_camelcase, datetime_must_have_timezone


def test_datetime_must_have_timezone():
    """Test function datetime_must_have_timezone"""

    time_now = datetime.now(timezone.utc)

    # check functions works
    new_time_now = datetime_must_have_timezone(None, time_now)
    assert new_time_now == time_now

    # check functions adds timezone
    time_now = datetime.now()
    time_now = datetime_must_have_timezone(None, time_now)
    assert new_time_now != time_now
    assert new_time_now.tzinfo == timezone.utc


def test_convert_to_camelcase():
    """Test convert to camelcase works"""
    assert convert_to_camelcase("foo_bar") == "fooBar"
    assert convert_to_camelcase("foo_bar_baz") == "fooBarBaz"
