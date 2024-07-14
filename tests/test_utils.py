""" Utils functions for test """

from datetime import datetime, timezone

# Used constants
from nowcasting_datamodel.utils import convert_to_camelcase, datetime_with_timezone


def test_datetime_with_timezone_handles_none():
    assert datetime_with_timezone(None, None) is None


def test_datetime_with_timezone_handles_string():
    dt = datetime_with_timezone(None, "2024-07-14T18:01:26+00:00")
    assert isinstance(dt, datetime)
    assert dt.tzinfo == timezone.utc


def test_datetime_with_timezone():
    """Test function datetime_with_timezone"""

    time_now = datetime.now(timezone.utc)

    # check functions works
    new_time_now = datetime_with_timezone(None, time_now)
    assert new_time_now == time_now

    # check functions adds timezone
    time_now = datetime.now()
    time_now = datetime_with_timezone(None, time_now)
    assert new_time_now != time_now
    assert new_time_now.tzinfo == timezone.utc


def test_convert_to_camelcase():
    """Test convert to camelcase works"""
    assert convert_to_camelcase("foo_bar") == "fooBar"
    assert convert_to_camelcase("foo_bar_baz") == "fooBarBaz"
