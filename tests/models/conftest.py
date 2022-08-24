import os
from typing import List

import pytest
from datetime import datetime
from nowcasting_datamodel.models.metric import (
    Metric,
    MetricSQL,
    DatetimeInterval,
    DatetimeIntervalSQL,
    MetricValue,
    MetricValueSQL,
)

from nowcasting_datamodel.models.gsp import Location


@pytest.fixture
def datetime_interval():
    return DatetimeInterval(
        start_datetime_utc=datetime(2022, 1, 1), end_datetime_utc=datetime(2022, 1, 1)
    )


@pytest.fixture
def metric():

    return Metric(name="test_metric", description="just a test metric really")


@pytest.fixture
def location():

    return Location(label="GSP_1", gsp_id=1)


# @pytest.fixture
# def forecast_sql(db_session):
#
#     # create
#     f = make_fake_forecasts(gsp_ids=[1], session=db_session)
#
#     # add
#     db_session.add_all(f)
#
#     return f
#
