from datetime import datetime

import pytest

from nowcasting_datamodel.models.gsp import Location
from nowcasting_datamodel.models.metric import (
    DatetimeInterval,
    Metric,
)


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
