""" Test Forecast Models"""

# Used constants
from datetime import datetime, timezone
import numpy as np

import pytest

from nowcasting_datamodel.models import Status, MLModelSQL
from nowcasting_datamodel.models.forecast import (
    Forecast,
    ForecastValue,
    ForecastValueLatestSQL,
    ForecastValueSQL,
    ManyForecasts,
)


def test_adjust_forecasts(forecasts):
    forecasts[0].forecast_values[0].expected_power_generation_megawatts = 10.0
    forecasts[0].forecast_values[0].adjust_mw = 1.23
    forecasts = [Forecast.model_validate(f, from_attributes=True) for f in forecasts]

    assert forecasts[0].forecast_values[0]._adjust_mw == 1.23

    forecasts[0].adjust(limit=1.22)
    assert forecasts[0].forecast_values[0].expected_power_generation_megawatts == 8.78
    assert "expected_power_generation_megawatts" in forecasts[0].forecast_values[0].model_dump()
    assert "_adjust_mw" not in forecasts[0].forecast_values[0].model_dump()


def test_adjust_forecast_neg(forecasts):
    forecasts[0].forecast_values[0].expected_power_generation_megawatts = 10.0
    forecasts[0].forecast_values[0].adjust_mw = -1.23
    forecasts = [Forecast.model_validate(f, from_attributes=True) for f in forecasts]

    forecasts[0].adjust(limit=1.22)
    assert forecasts[0].forecast_values[0].expected_power_generation_megawatts == 11.22
    assert "expected_power_generation_megawatts" in forecasts[0].forecast_values[0].model_dump()
    assert "_adjust_mw" not in forecasts[0].forecast_values[0].model_dump()


def test_adjust_forecast_below_zero(forecasts):
    v = forecasts[0].forecast_values[0].expected_power_generation_megawatts
    forecasts[0].forecast_values[0].adjust_mw = v + 100
    forecasts = [Forecast.model_validate(f, from_attributes=True) for f in forecasts]
    forecasts[0].forecast_values[0]._properties = {"10": v - 100}

    forecasts[0].adjust(limit=v * 3)

    assert forecasts[0].forecast_values[0].expected_power_generation_megawatts == 0.0
    assert forecasts[0].forecast_values[0]._properties["10"] == 0.0
    assert "expected_power_generation_megawatts" in forecasts[0].forecast_values[0].model_dump()
    assert "_adjust_mw" not in forecasts[0].forecast_values[0].model_dump()


def test_adjust_many_forecasts(forecasts):
    forecasts[0].forecast_values[0].adjust_mw = 1.23
    forecasts = [Forecast.model_validate(f, from_attributes=True) for f in forecasts]
    m = ManyForecasts(forecasts=forecasts)
    m.adjust()


def test_normalize_forecasts(forecasts):
    v = forecasts[0].forecast_values[0].expected_power_generation_megawatts
    forecasts_all = [Forecast.model_validate(f, from_attributes=True) for f in forecasts]

    forecasts_all[0].normalize()
    assert (
        forecasts_all[0].forecast_values[0].expected_power_generation_normalized
        == v / forecasts_all[0].location.installed_capacity_mw
    )

    m = ManyForecasts(forecasts=forecasts_all)
    m.normalize()


def test_normalize_forecasts_no_installed_capacity(forecasts):
    forecast = Forecast.model_validate(forecasts[0], from_attributes=True)
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
    status_orm = Status.model_validate(ormed_status, from_attributes=True)

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

    forecast = Forecast.model_validate(forecast_sql, from_attributes=True)
    assert forecast.forecast_values[0] != ForecastValue.model_validate(f1, from_attributes=True)

    forecast = Forecast.model_validate_latest(forecast_sql=forecast_sql, from_attributes=True)
    assert forecast.forecast_values[0] == ForecastValue.model_validate(f1, from_attributes=True)


def test_forecast_value_from_orm(forecast_sql):
    forecast_sql = forecast_sql[0]

    f = ForecastValueSQL(
        target_time=datetime(2023, 1, 1, 0, 30), expected_power_generation_megawatts=1
    )

    actual = ForecastValue.model_validate(f, from_attributes=True)
    expected = ForecastValue(
        target_time=datetime(2023, 1, 1, 0, 30, tzinfo=timezone.utc),
        expected_power_generation_megawatts=1.0,
    )
    assert actual == expected


@pytest.mark.parametrize("null_value", [float("NaN"), np.nan, None])
def test_forecast_value_from_orm_from_adjust_mw_nan(forecast_sql, null_value):
    forecast_sql = forecast_sql[0]

    f = ForecastValueSQL(
        target_time=datetime(2023, 1, 1, 0, 30), expected_power_generation_megawatts=1
    )
    f.adjust_mw = null_value

    actual = ForecastValue.model_validate(f, from_attributes=True)
    expected = ForecastValue(
        target_time=datetime(2023, 1, 1, 0, 30, tzinfo=timezone.utc),
        expected_power_generation_megawatts=1.0,
    )
    assert actual == expected
    assert actual._adjust_mw == 0.0


def test_forecast_latest_distinct_target_time(db_session):
    f1 = ForecastValueLatestSQL(
        target_time=datetime(2023, 1, 1), expected_power_generation_megawatts=1, gsp_id=1
    )
    db_session.add(f1)
    db_session.commit()

    with pytest.raises(Exception):
        f2 = ForecastValueLatestSQL(
            gsp_id=1, target_time=datetime(2023, 1, 1), expected_power_generation_megawatts=2
        )
        db_session.add(f2)
        db_session.commit()


def test_forecast_latest_distinct_gsp(db_session):
    f1 = ForecastValueLatestSQL(
        target_time=datetime(2023, 1, 1), expected_power_generation_megawatts=1, gsp_id=1
    )
    db_session.add(f1)
    db_session.commit()

    with pytest.raises(Exception):
        f2 = ForecastValueLatestSQL(
            gsp_id=1, target_time=datetime(2023, 1, 1, 30), expected_power_generation_megawatts=2
        )
        db_session.add(f2)
        db_session.commit()


def test_forecast_latest_distinct_model_id(db_session):
    m1 = MLModelSQL(name="test_1", version="0.1")
    m2 = MLModelSQL(name="test_2", version="0.2")
    db_session.add_all([m1, m2])
    db_session.commit()

    f1 = ForecastValueLatestSQL(
        target_time=datetime(2023, 1, 1),
        expected_power_generation_megawatts=1,
        gsp_id=1,
        model_id=1,
    )
    db_session.add(f1)
    db_session.commit()

    f2 = ForecastValueLatestSQL(
        target_time=datetime(2023, 1, 1),
        expected_power_generation_megawatts=2,
        model_id=2,
        gsp_id=1,
    )
    db_session.add(f2)
    db_session.commit()

    # with pytest.raises(Exception):
    #     f2 = ForecastValueLatestSQL(
    #         gsp_id=1,
    #         target_time=datetime(2023, 1, 1),
    #         expected_power_generation_megawatts=2,
    #         model_id=1
    #     )
    #     db_session.add(f2)
    #     db_session.commit()
