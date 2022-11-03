import os
from typing import List

import pytest

from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.connection import Base_Forecast, DatabaseConnection
from nowcasting_datamodel.fake import make_fake_forecasts
from nowcasting_datamodel.models.forecast import ForecastSQL
from nowcasting_datamodel.models.pv import Base_PV


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
def db_connection_pv():

    url = os.getenv("DB_URL_PV", "sqlite:///test_pv.db")

    connection = DatabaseConnection(url=url, base=Base_PV)
    Base_PV.metadata.create_all(connection.engine)

    yield connection

    Base_PV.metadata.drop_all(connection.engine)


@pytest.fixture(scope="function", autouse=True)
def db_session_pv(db_connection_pv):
    """Creates a new database session for a test."""

    connection = db_connection_pv.engine.connect()
    t = connection.begin()

    with db_connection_pv.get_session() as s:
        s.begin()
        yield s
        s.rollback()

    t.rollback()
    connection.close()
