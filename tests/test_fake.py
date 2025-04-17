from datetime import datetime, timezone

from nowcasting_datamodel.fake import (
    generate_fake_forecasts,
    make_fake_forecast,
    make_fake_forecast_value,
    make_fake_forecasts,
    make_fake_gsp_yields,
    make_fake_input_data_last_updated,
    make_fake_intensity,
    make_fake_location,
    make_fake_national_forecast,
)
from nowcasting_datamodel.models import (
    Forecast,
    ForecastSQL,
    ForecastValue,
    ForecastValueLatestSQL,
    GSPYieldSQL,
    InputDataLastUpdated,
    InputDataLastUpdatedSQL,
    Location,
    LocationSQL,
)
from nowcasting_datamodel.models.forecast import ForecastValueSQL


def test_make_fake_intensity():
    assert make_fake_intensity(datetime(2022, 1, 1)) == 0
    assert make_fake_intensity(datetime(2022, 1, 1, 6)) == 0
    assert 0 < make_fake_intensity(datetime(2022, 1, 1, 9)) < 1
    assert make_fake_intensity(datetime(2022, 1, 1, 12)) == 1
    assert 0 < make_fake_intensity(datetime(2022, 1, 1, 15)) < 1
    assert make_fake_intensity(datetime(2022, 1, 1, 18)) == 0
    assert make_fake_intensity(datetime(2022, 1, 2)) == 0


def test_make_fake_location():
    location_sql: LocationSQL = make_fake_location(1)
    location = Location.model_validate(location_sql, from_attributes=True)
    _ = Location.to_orm(location)


def test_make_fake_input_data_last_updated():
    input_sql: InputDataLastUpdatedSQL = make_fake_input_data_last_updated()
    input = InputDataLastUpdated.model_validate(input_sql, from_attributes=True)
    _ = InputDataLastUpdated.to_orm(input)


def test_make_fake_forecast_value():
    target = datetime(2023, 1, 1, tzinfo=timezone.utc)

    forecast_value_sql: ForecastValueSQL = make_fake_forecast_value(target_time=target)
    forecast_value = ForecastValue.model_validate(forecast_value_sql, from_attributes=True)
    _ = ForecastValue.to_orm(forecast_value)


def test_make_fake_forecast(db_session):
    forecast_sql: ForecastSQL = make_fake_forecast(gsp_id=1, session=db_session)
    forecast = Forecast.model_validate(forecast_sql, from_attributes=True)
    forecast_sql = Forecast.to_orm(forecast)
    _ = Forecast.model_validate(forecast_sql, from_attributes=True)

    from sqlalchemy import text

    f = ForecastValueSQL(target_time="2023-01-01 12:00:00", expected_power_generation_megawatts=3)
    db_session.add(f)
    db_session.commit()
    _ = db_session.execute(text("SELECT * FROM forecast_value_2023_01")).all()


def test_make_fake_forecast_with_creation_time(db_session):
    t0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    forecast_creation_time = datetime(2023, 1, 1, 1, tzinfo=timezone.utc)
    forecast_sql: ForecastSQL = make_fake_forecast(gsp_id=1, session=db_session, t0_datetime_utc=t0)
    assert forecast_sql.forecast_creation_time == t0

    forecast_sql: ForecastSQL = make_fake_forecast(
        gsp_id=1,
        session=db_session,
        t0_datetime_utc=t0,
        forecast_creation_time=forecast_creation_time,
    )
    assert forecast_sql.forecast_creation_time == forecast_creation_time


def test_generate_fake_forecasts(db_session):
    fake_forecasts = generate_fake_forecasts(
        session=db_session, gsp_ids=[0, 1, 2, 3], add_latest=True
    )

    assert len(db_session.query(ForecastSQL).all()) == 0
    assert len(db_session.query(ForecastValueSQL).all()) == 0
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 0
    assert len(fake_forecasts) == 4


def test_make_fake_forecasts(db_session):
    make_fake_forecasts(session=db_session, gsp_ids=[0, 1, 2, 3], add_latest=True)

    assert len(db_session.query(ForecastSQL).all()) == 4
    assert len(db_session.query(ForecastValueSQL).all()) == 4 * 112
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 4 * 112


def test_make_fake_forecasts_without_latest(db_session):
    make_fake_forecasts(session=db_session, gsp_ids=[0, 1, 2, 3], add_latest=False)

    assert len(db_session.query(ForecastSQL).all()) == 4
    assert len(db_session.query(ForecastValueSQL).all()) == 4 * 112
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 0


def test_make_national_fake_forecast(db_session):
    forecast_sql: ForecastSQL = make_fake_national_forecast(session=db_session)
    forecast = Forecast.model_validate(forecast_sql, from_attributes=True)
    _ = Forecast.to_orm(forecast)


def test_make_fake_gsp_yields(db_session):
    make_fake_gsp_yields(session=db_session, gsp_ids=list(range(0, 10)))

    assert len(db_session.query(LocationSQL).all()) == 10
    # 2 days for each location, both regimes
    assert len(db_session.query(GSPYieldSQL).all()) == 10 * 48 * 2 * 2
