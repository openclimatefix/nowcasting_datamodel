from datetime import datetime

from nowcasting_datamodel.models import DatetimeIntervalSQL, LocationSQL, MetricSQL, MetricValueSQL
from nowcasting_datamodel.read.read_metric import (
    get_datetime_interval,
    get_metric,
    get_metric_value,
)


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


def test_get_metric_value(db_session):

    start_datetime = datetime(2022, 1, 1)
    end_datetime = datetime(2022, 1, 1)

    datetime_interval = DatetimeIntervalSQL(
        start_datetime_utc=start_datetime, end_datetime_utc=end_datetime
    )
    metric = MetricSQL(name="test")
    location = LocationSQL(label="gsp_1", gsp_id=1)

    metric_value = MetricValueSQL(value=0.1, number_of_data_points=2)
    metric_value.datetime_interval = datetime_interval
    metric_value.metric = metric
    metric_value.location = location

    db_session.add(datetime_interval)
    db_session.add(metric)
    db_session.add(location)
    db_session.add(metric_value)

    metric_value = MetricValueSQL(value=0.2, number_of_data_points=2)
    metric_value.datetime_interval = datetime_interval
    metric_value.metric = metric
    metric_value.location = location
    db_session.add(metric_value)

    metric_values = get_metric_value(
        session=db_session,
        gsp_id=1,
        start_datetime_utc=start_datetime,
        end_datetime_utc=end_datetime,
        name='test'
    )

    assert len(metric_values) == 1
    assert metric_values[0].value == 0.2


def test_get_metric_value_forecast_horizon_minutes(db_session):

    start_datetime = datetime(2022, 1, 1)
    end_datetime = datetime(2022, 1, 1)

    datetime_interval = DatetimeIntervalSQL(
        start_datetime_utc=start_datetime, end_datetime_utc=end_datetime
    )
    metric = MetricSQL(name="test")
    location = LocationSQL(label="gsp_1", gsp_id=1)

    metric_value = MetricValueSQL(value=0.1, number_of_data_points=2, forecast_horizon_minutes=240)
    metric_value.datetime_interval = datetime_interval
    metric_value.metric = metric
    metric_value.location = location

    db_session.add(datetime_interval)
    db_session.add(metric)
    db_session.add(location)
    db_session.add(metric_value)

    metric_values = get_metric_value(
        session=db_session,
        gsp_id=1,
        forecast_horizon_minutes=240,
        start_datetime_utc=start_datetime,
        end_datetime_utc=end_datetime,
        name='test'
    )

    assert len(metric_values) == 1
    assert metric_values[0].value == 0.1
