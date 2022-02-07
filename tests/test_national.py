""" Test national forecast"""
# Used constants
import pytest

from nowcasting_datamodel.models import Forecast, ForecastSQL
from nowcasting_datamodel.national import make_national_forecast


def test_make_national_forecast(forecasts_all):

    forecasts_all = [Forecast.from_orm(f) for f in forecasts_all]

    national_forecast = make_national_forecast(forecasts=forecasts_all)

    assert type(national_forecast) == ForecastSQL


def test_make_national_forecast_error(forecasts_all):

    forecasts_all = [Forecast.from_orm(f) for f in forecasts_all]
    forecasts_all[0].location.gsp_id = 2

    with pytest.raises(Exception):
        _ = make_national_forecast(forecasts=forecasts_all)
