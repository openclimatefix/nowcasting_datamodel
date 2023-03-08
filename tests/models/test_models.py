""" Test Forecast Models"""
# Used constants
from datetime import datetime

import pytest

from nowcasting_datamodel.models import Status
from nowcasting_datamodel.models.forecast import (
    Forecast,
    ForecastValue,
    ForecastValueLatestSQL,
    ManyForecasts,
)


def test_adjust_forecasts(forecasts):
    v = forecasts[0].forecast_values[0].expected_power_generation_megawatts
    forecasts[0].forecast_values[0].adjust_mw = 1.23
    forecasts = [Forecast.from_orm(f) for f in forecasts]

    forecasts[0].adjust()
    assert forecasts[0].forecast_values[0].expected_power_generation_megawatts == v - 1.23

    assert "expected_power_generation_megawatts" in forecasts[0].forecast_values[0].dict()
    assert "_adjust_mw" not in forecasts[0].forecast_values[0].dict()

    m = ManyForecasts(forecasts=forecasts)
    m.adjust()


def test_normalize_forecasts(forecasts):
    v = forecasts[0].forecast_values[0].expected_power_generation_megawatts
    forecasts_all = [Forecast.from_orm(f) for f in forecasts]

    forecasts_all[0].normalize()
    assert (
        forecasts_all[0].forecast_values[0].expected_power_generation_normalized
        == v / forecasts_all[0].location.installed_capacity_mw
    )

    m = ManyForecasts(forecasts=forecasts_all)
    m.normalize()


def test_normalize_forecasts_no_installed_capacity(forecasts):
    forecast = Forecast.from_orm(forecasts[0])
    forecast.location.installed_capacity_mw = None

    v = forecast.forecast_values[0].expected_power_generation_megawatts

    # set installed capacity to None, and check no normalization takes place
    forecast.location.installed_capacity_mw = None
    forecast.normalize()
    assert forecast.forecast_values[0].expected_power_generation_megawatts == v
    assert forecast.forecast_values[0].expected_power_generation_normalized == 0


def test_status_validation():
    _ = Status(message="Testing", status="ok")

    with pytest.raises(Exception):
        _ = Status(message="test", status="TEst")


def test_status_orm():
    status = Status(message="testing", status="warning")
    ormed_status = status.to_orm()
    status_orm = Status.from_orm(ormed_status)

    assert status_orm.message == status.message
    assert status_orm.status == status.status


def test_forecast_latest_to_pydantic(forecast_sql):
    forecast_sql = forecast_sql[0]

    f1 = ForecastValueLatestSQL(
        target_time=datetime(2023, 1, 1), expected_power_generation_megawatts=1, gsp_id=1
    )

    f2 = ForecastValueLatestSQL(
        gsp_id=1, target_time=datetime(2023, 1, 1, 0, 30), expected_power_generation_megawatts=2
    )

    forecast_sql.forecast_values_latest = [f1, f2]

    forecast = Forecast.from_orm(forecast_sql)
    assert forecast.forecast_values[0] != ForecastValue.from_orm(f1)

    forecast = Forecast.from_orm_latest(forecast_sql=forecast_sql)
    assert forecast.forecast_values[0] == ForecastValue.from_orm(f1)
