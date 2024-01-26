from datetime import datetime, timezone

import pandas as pd
from freezegun import freeze_time

from nowcasting_datamodel.fake import make_fake_forecasts
from nowcasting_datamodel.models import (
    MetricValueSQL,
)
from nowcasting_datamodel.save.adjust import (
    reduce_metric_values_to_correct_forecast_horizon,
    add_adjust_to_forecasts,
    add_adjust_to_national_forecast,
    get_forecast_horizon_from_forecast,
)


def test_reduce_metric_values_to_correct_forecast_horizon(latest_me):
    datetime_now = datetime(2022, 1, 9, 16, 30)

    df = reduce_metric_values_to_correct_forecast_horizon(
        latest_me=latest_me, datetime_now=datetime_now, hours_ahead=8
    )

    assert len(df) == 17
    assert df.iloc[0].value == 16 * 60 + 30
    assert df.iloc[0].datetime == pd.Timestamp("2022-01-09 16:30")
    assert df.iloc[1].value == 17 * 60.0 + 30 * 10000
    assert df.iloc[1].datetime == pd.Timestamp("2022-01-09 17:00")
    assert df.iloc[2].value == 17 * 60 + 30 + 60 * 10000


@freeze_time("2023-01-09 16:25")
def test_add_adjust_to_forecasts(latest_me, db_session):
    assert len(db_session.query(MetricValueSQL).all()) > 0

    datetime_now = datetime(2023, 1, 9, 16, 30, tzinfo=timezone.utc)
    forecasts = make_fake_forecasts(
        gsp_ids=list(range(0, 2)), session=db_session, t0_datetime_utc=datetime_now
    )

    add_adjust_to_forecasts(session=db_session, forecasts_sql=forecasts, max_adjust_percentage=None)

    assert forecasts[0].forecast_values[0].adjust_mw == 16 * 60 + 30
    assert forecasts[1].forecast_values[0].adjust_mw == 0.0


@freeze_time("2023-01-09 16:25")
def test_add_adjust_to_national_forecast(latest_me, db_session):
    assert len(db_session.query(MetricValueSQL).all()) > 0

    datetime_now = datetime(2023, 1, 9, 16, 30, tzinfo=timezone.utc)
    forecast = make_fake_forecasts(
        gsp_ids=list(range(0, 1)), session=db_session, t0_datetime_utc=datetime_now
    )[0]

    # don't adjust
    add_adjust_to_national_forecast(session=db_session, forecast=forecast, max_adjust_percentage=None)
    assert forecast.forecast_values[0].adjust_mw == 16 * 60 + 30

    # check if zero, then adjust value is zero
    forecast.forecast_values[0].expected_power_generation_megawatts = 0.0
    add_adjust_to_national_forecast(session=db_session, forecast=forecast, max_adjust_percentage=0.2)
    assert forecast.forecast_values[0].adjust_mw == 0.0

    # check value is at 20%
    forecast.forecast_values[0].expected_power_generation_megawatts = 1.0
    add_adjust_to_national_forecast(session=db_session, forecast=forecast, max_adjust_percentage=0.2)
    assert forecast.forecast_values[0].adjust_mw == 0.2


@freeze_time("2023-01-09 16:25")
def test_add_adjust_to_forecasts_no_me_values(db_session):
    """Test to check, if there are no me values, make sure the adjust mw are 0 not NaN"""

    datetime_now = datetime(2023, 1, 9, 16, 30, tzinfo=timezone.utc)
    forecasts = make_fake_forecasts(
        gsp_ids=list(range(0, 2)), session=db_session, t0_datetime_utc=datetime_now
    )

    add_adjust_to_forecasts(session=db_session, forecasts_sql=forecasts)

    assert forecasts[1].forecast_values[0].adjust_mw == 0.0


def test_get_forecast_horizon_from_forecast(db_session):
    forecasts = make_fake_forecasts(gsp_ids=range(0, 1), session=db_session)

    hours = get_forecast_horizon_from_forecast(forecast=forecasts[0])
    assert hours == 24 + 24 + 8
