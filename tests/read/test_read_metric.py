from datetime import datetime

from nowcasting_datamodel.models import MetricSQL, DatetimeIntervalSQL
from nowcasting_datamodel.read.read_metric import get_metric, get_datetime_interval


def test_get_metric(db_session):

    db_session.add(MetricSQL(name="test"))

    metric = get_metric(session=db_session, name="test")
    assert metric.name == "test"
    assert len(db_session.query(MetricSQL).all()) == 1


def test_get_new_metric(db_session):
    assert len(db_session.query(MetricSQL).all()) == 0

    _ = get_metric(session=db_session, name="test")
    metric = get_metric(session=db_session, name="test")
    assert metric.name == "test"
    assert len(db_session.query(MetricSQL).all()) == 1


def test_get_datetime_interval(db_session):

    start_datetime = datetime(2022, 1, 1)
    end_datetime = datetime(2022, 1, 1)
    db_session.add(
        DatetimeIntervalSQL(start_datetime_utc=start_datetime, end_datetime_utc=end_datetime)
    )

    datetime_interval = get_datetime_interval(
        session=db_session, start_datetime_utc=start_datetime, end_datetime_utc=end_datetime
    )
    assert datetime_interval.start_datetime_utc == start_datetime
    assert datetime_interval.end_datetime_utc == end_datetime
    assert len(db_session.query(DatetimeIntervalSQL).all()) == 1


def test_get_new_datetime_interval(db_session):

    start_datetime = datetime(2022, 1, 1)
    end_datetime = datetime(2022, 1, 1)
    assert len(db_session.query(DatetimeIntervalSQL).all()) == 0

    _ = get_datetime_interval(
        session=db_session, start_datetime_utc=start_datetime, end_datetime_utc=end_datetime
    )
    datetime_interval = get_datetime_interval(
        session=db_session, start_datetime_utc=start_datetime, end_datetime_utc=end_datetime
    )
    assert datetime_interval.start_datetime_utc == start_datetime
    assert datetime_interval.end_datetime_utc == end_datetime
    assert len(db_session.query(DatetimeIntervalSQL).all()) == 1
