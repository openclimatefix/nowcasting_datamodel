from datetime import datetime


import numpy as np

from nowcasting_datamodel.models.metric import (
    Metric,
    MetricSQL,
    DatetimeInterval,
    DatetimeIntervalSQL,
    MetricValue,
    MetricValueSQL,
)


def test_datetime_interval(datetime_interval):
    _ = datetime_interval.to_orm()


def test_metric(metric):
    _ = metric.to_orm()


def test_metric_value(datetime_interval, metric, location):

    metric_value = MetricValue(
        value=7.8,
        metric=metric,
        datetime_interval=datetime_interval,
        location=location,
        number_of_data_points=100,
    )
    metric_value.to_orm()
