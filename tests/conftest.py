import os
from typing import List

import pytest

from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.fake import make_fake_forecasts, make_fake_me_latest
from nowcasting_datamodel.models import MetricValueSQL
from nowcasting_datamodel.models.forecast import ForecastSQL


@pytest.fixture
def forecast_sql(db_session):
    # create
    f = make_fake_forecasts(gsp_ids=[1], session=db_session)

    # add
    db_session.add_all(f)

    return f


@pytest.fixture
def forecasts(db_session) -> List[ForecastSQL]:
    # create
    f = make_fake_forecasts(gsp_ids=list(range(0, 10)), session=db_session)

    # add
    db_session.add_all(f)

    return f


@pytest.fixture
def forecasts_all(db_session) -> List[ForecastSQL]:
    # create
    f = make_fake_forecasts(gsp_ids=list(range(1, N_GSP + 1)), session=db_session)

    # add
    db_session.add_all(f)

    return f


"""
This is a bit complicated and sensitive to change
https://gist.github.com/kissgyorgy/e2365f25a213de44b9a2 helped me get going
"""


@pytest.fixture
def db_connection():
    url = os.getenv("DB_URL", "sqlite:///test.db")

    connection = DatabaseConnection(url=url, echo=False)
    connection.create_all()

    yield connection

    connection.drop_all()


@pytest.fixture(scope="function", autouse=True)
def db_session(db_connection):
    """Creates a new database session for a test."""

    connection = db_connection.engine.connect()
    t = connection.begin()

    with db_connection.Session(bind=connection) as s:
        s.begin()
        yield s
        s.rollback()

    t.rollback()
    connection.close()


@pytest.fixture
def latest_me(db_session) -> List[MetricValueSQL]:
    # create
    metric_values = make_fake_me_latest(session=db_session)

    # add
    db_session.add_all(metric_values)
    db_session.commit()

    return metric_values
