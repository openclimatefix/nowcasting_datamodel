from datetime import datetime, timezone

from nowcasting_datamodel.fake import (
    make_fake_forecast,
    make_fake_forecast_value,
    make_fake_input_data_last_updated,
    make_fake_location,
    make_fake_national_forecast,
)
from nowcasting_datamodel.models import (
    GSP,
    GSPSQL,
    Forecast,
    ForecastSQL,
    ForecastValue,
    ForecastValueSQL,
    InputDataLastUpdated,
    InputDataLastUpdatedSQL,
)


def test_make_fake_location():
    location_sql: GSPSQL = make_fake_location(1)
    location = GSP.from_orm(location_sql)
    _ = GSP.to_orm(location)


def test_make_fake_input_data_last_updated():
    input_sql: InputDataLastUpdatedSQL = make_fake_input_data_last_updated()
    input = InputDataLastUpdated.from_orm(input_sql)
    _ = InputDataLastUpdated.to_orm(input)


def test_make_fake_forecast_value():

    target = datetime(2022, 1, 1, tzinfo=timezone.utc)

    forecast_value_sql: ForecastValueSQL = make_fake_forecast_value(target_time=target)
    forecast_value = ForecastValue.from_orm(forecast_value_sql)
    _ = ForecastValue.to_orm(forecast_value)


def test_make_fake_forecast(db_session):
    forecast_sql: ForecastSQL = make_fake_forecast(gsp_id=1, session=db_session)
    forecast = Forecast.from_orm(forecast_sql)
    forecast_sql = Forecast.to_orm(forecast)
    _ = Forecast.from_orm(forecast_sql)


def test_make_national_fake_forecast():
    forecast_sql: ForecastSQL = make_fake_national_forecast()
    forecast = Forecast.from_orm(forecast_sql)
    _ = Forecast.to_orm(forecast)
