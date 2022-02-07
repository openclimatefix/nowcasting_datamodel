import os
from typing import List

import pytest

from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.fake import make_fake_forecasts
from nowcasting_datamodel.models import Base, ForecastSQL


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
    f = make_fake_forecasts(gsp_ids=list(range(0, N_GSP)), session=db_session)

    # add
    db_session.add_all(f)

    return f


@pytest.fixture
def db_connection():

    url = os.getenv("DB_URL")

    connection = DatabaseConnection(url=url)
    Base.metadata.create_all(connection.engine)

    yield connection

    Base.metadata.drop_all(connection.engine)


@pytest.fixture(scope="function", autouse=True)
def db_session(db_connection):
    """Creates a new database session for a test."""

    with db_connection.get_session() as s:
        s.begin()
        yield s
        s.rollback()
