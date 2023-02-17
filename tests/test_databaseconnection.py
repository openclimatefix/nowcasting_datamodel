from typing import List

from nowcasting_datamodel.fake import N_FAKE_FORECASTS
from nowcasting_datamodel.models import ForecastSQL


def test_get_session(db_connection):
    with db_connection.get_session() as _:
        pass


def test_read_forecast(db_session):
    forecasts = db_session.query(ForecastSQL).all()
    assert len(forecasts) == 0


def test_read_forecast_one(db_session, forecast_sql):
    # query
    forecasts: List[ForecastSQL] = db_session.query(ForecastSQL).all()
    assert len(forecasts) == 1
    assert forecast_sql[0] == forecasts[0]
    assert len(forecasts[0].forecast_values) == N_FAKE_FORECASTS
