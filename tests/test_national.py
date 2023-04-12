""" Test national forecast"""
# Used constants
import pytest

from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.models import LocationSQL
from nowcasting_datamodel.models.forecast import Forecast, ForecastSQL
from nowcasting_datamodel.national import make_national_forecast


def test_make_national_forecast(forecasts_all, db_session):
    locations = db_session.query(LocationSQL).all()
    assert len(locations) == N_GSP

    national_forecast = make_national_forecast(forecasts=forecasts_all, session=db_session)
    db_session.add(national_forecast)
    db_session.commit()

    assert type(national_forecast) == ForecastSQL

    locations = db_session.query(LocationSQL).all()
    assert len(locations) == N_GSP + 1

    # run it again, check there are still 339 locations
    national_forecast = make_national_forecast(forecasts=forecasts_all, session=db_session)
    db_session.add(national_forecast)
    db_session.commit()
    locations = db_session.query(LocationSQL).all()
    assert len(locations) == N_GSP + 1


def test_make_national_forecast_error(forecasts_all, db_session):
    forecasts_all = [Forecast.from_orm(f) for f in forecasts_all]
    forecasts_all[0].location.gsp_id = 2

    with pytest.raises(Exception):
        _ = make_national_forecast(forecasts=forecasts_all, session=db_session)
