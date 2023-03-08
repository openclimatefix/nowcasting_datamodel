""" Read database functions

1. Get the one metric
2. get datetime interval
"""
import logging
from datetime import datetime
from typing import List

from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models.metric import DatetimeIntervalSQL, MetricSQL, MetricValueSQL

logger = logging.getLogger(__name__)


def get_metric(session: Session, name: str) -> MetricSQL:
    """
    Get metric object from database

    :param session: database session
    :param name: name of the metric

    return: One Metric SQl object

    """

    # start main query
    query = session.query(MetricSQL)

    # filter on name
    query = query.filter(MetricSQL.name == name)

    # get all results
    metrics = query.all()

    if len(metrics) == 0:
        logger.debug(f"Metric for name {name} does not exist so going to add it")

        metric = MetricSQL(name=name, description="TODO need to update")

        session.add(metric)
        session.commit()

    else:
        metric = metrics[0]

    return metric


def get_datetime_interval(
    session: Session, start_datetime_utc: datetime, end_datetime_utc: datetime
) -> DatetimeIntervalSQL:
    """
    Get datetime interval object from database

    :param session: database session
    :param start_datetime_utc: start datetime of the interval
    :param end_datetime_utc: end datetime of the interval

    return: One Datetime interval SQl object

    """

    # start main query
    query = session.query(DatetimeIntervalSQL)

    # filter on start_datetime_utc
    query = query.filter(DatetimeIntervalSQL.start_datetime_utc == start_datetime_utc)

    # filter on end_datetime_utc
    query = query.filter(DatetimeIntervalSQL.end_datetime_utc == end_datetime_utc)

    # get all results
    datetime_intervals = query.all()

    if len(datetime_intervals) == 0:
        logger.debug(
            f"Datetime interbal for {start_datetime_utc=} "
            f"and {end_datetime_utc=} does not exist so going to add it"
        )

        datetime_interval = DatetimeIntervalSQL(
            start_datetime_utc=start_datetime_utc, end_datetime_utc=end_datetime_utc
        )

        session.add(datetime_interval)
        session.commit()

    else:
        datetime_interval = datetime_intervals[0]

    return datetime_interval


def read_latest_me_national(
    session: Session, metric_name: str = "Half Hourly ME"
) -> List[MetricValueSQL]:
    """
    Get the latest me for the national forecast

    This is grouped by 'time_of_day' and 'forecast_horizon_minutes'

    :param session: database sessions
    :param metric_name: metric name, defaulted to "Half Hourly ME"
    :return: list of MetricValueSQL for ME for each 'time_of_day' and 'forecast_horizon_minutes'
    """

    # start main query
    query = session.query(MetricValueSQL)
    query = query.join(MetricSQL)

    query = query.distinct(MetricValueSQL.time_of_day, MetricValueSQL.forecast_horizon_minutes)

    # filter on metric name
    query = query.filter(MetricSQL.name == metric_name)

    query = query.order_by(
        MetricValueSQL.time_of_day,
        MetricValueSQL.forecast_horizon_minutes,
        MetricValueSQL.created_utc.desc(),
    )

    # get all results
    metric_values = query.all()

    return metric_values
