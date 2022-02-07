""" Test national forecast"""
# Used constants
from nowcasting_datamodel.models import Forecast, ForecastSQL
from nowcasting_datamodel.national import make_national_forecast


def test_make_national_forecast(forecasts_all):

    forecasts_all = [Forecast.from_orm(f) for f in forecasts_all]

    national_forecast = make_national_forecast(forecasts=forecasts_all)

    assert type(national_forecast) == ForecastSQL
