""" Test Forecast Models"""
# Used constants

from nowcasting_datamodel.models import Forecast, ManyForecasts


def test_normalize_forecasts(forecasts_all):

    v = forecasts_all[0].forecast_values[0].expected_power_generation_megawatts
    forecasts_all = [Forecast.from_orm(f) for f in forecasts_all]

    forecasts_all[0].normalize()
    assert forecasts_all[0].forecast_values[0].expected_power_generation_megawatts \
           == v / forecasts_all[0].location.installed_capacity_mw

    m = ManyForecasts(forecasts=forecasts_all)
    m.normalize()



