import logging
from datetime import datetime, timezone

from freezegun import freeze_time

from nowcasting_datamodel.fake import make_fake_forecast
from nowcasting_datamodel.read.read import get_forecast_values

logger = logging.getLogger(__name__)


@freeze_time("2023-01-01 12:00:00")
def test_get_latest_forecast_created_utc_gsp(db_session):
    t0_datetime_utc = datetime(2023, 1, 1, 12, tzinfo=timezone.utc)

    f1 = make_fake_forecast(gsp_id=1, session=db_session, t0_datetime_utc=t0_datetime_utc)
    f2 = make_fake_forecast(gsp_id=1, session=db_session, t0_datetime_utc=t0_datetime_utc)

    for i in range(len(f1.forecast_values)):
        f1.forecast_values[i].created_utc = datetime(2023, 1, 1, 10, tzinfo=timezone.utc)

    for i in range(len(f2.forecast_values)):
        f2.forecast_values[i].created_utc = datetime(2023, 1, 1, 12, tzinfo=timezone.utc)

    db_session.add_all([f1, f2])
    db_session.commit()

    forecast_values = get_forecast_values(
        session=db_session, forecast_horizon_minutes=120, gsp_id=1, only_return_latest=True
    )
    assert len(forecast_values) == 16
    assert forecast_values[0].created_utc == datetime(2023, 1, 1, 10, tzinfo=timezone.utc)
    assert forecast_values[0].target_time == datetime(2023, 1, 1, 12, tzinfo=timezone.utc)
    assert forecast_values[3].created_utc == datetime(2023, 1, 1, 10, tzinfo=timezone.utc)
    assert forecast_values[3].target_time == datetime(2023, 1, 1, 13, 30, tzinfo=timezone.utc)
    assert forecast_values[4].created_utc == datetime(2023, 1, 1, 12, tzinfo=timezone.utc)
    assert forecast_values[4].target_time == datetime(2023, 1, 1, 14, tzinfo=timezone.utc)

    assert forecast_values[8].created_utc == datetime(2023, 1, 1, 12, tzinfo=timezone.utc)
    assert forecast_values[8].target_time == datetime(2023, 1, 1, 16, tzinfo=timezone.utc)

    assert forecast_values[-1].target_time == datetime(2023, 1, 1, 19, 30, tzinfo=timezone.utc)


@freeze_time("2023-01-01 12:00:00")
def test_get_latest_forecast__with_end_datetime(db_session):
    t0_datetime_utc = datetime(2023, 1, 1, 12, tzinfo=timezone.utc)

    f1 = make_fake_forecast(gsp_id=1, session=db_session, t0_datetime_utc=t0_datetime_utc)
    f2 = make_fake_forecast(gsp_id=1, session=db_session, t0_datetime_utc=t0_datetime_utc)

    for i in range(len(f1.forecast_values)):
        f1.forecast_values[i].created_utc = datetime(2023, 1, 1, 10, tzinfo=timezone.utc)

    for i in range(len(f2.forecast_values)):
        f2.forecast_values[i].created_utc = datetime(2023, 1, 1, 12, tzinfo=timezone.utc)

    db_session.add_all([f1, f2])
    db_session.commit()

    forecast_values = get_forecast_values(
        session=db_session,
        forecast_horizon_minutes=120,
        gsp_id=1,
        only_return_latest=True,
        end_datetime=datetime(2023, 1, 1, 13, tzinfo=timezone.utc),
    )
    assert len(forecast_values) == 3
    assert forecast_values[0].created_utc == datetime(2023, 1, 1, 10, tzinfo=timezone.utc)
    assert forecast_values[0].target_time == datetime(2023, 1, 1, 12, tzinfo=timezone.utc)


@freeze_time("2023-01-01 12:00:00")
def test_get_latest_forecast_created_utc_multi_gsp(db_session):
    t0_datetime_utc = datetime(2023, 1, 1, 12, tzinfo=timezone.utc)

    f1 = make_fake_forecast(gsp_id=1, session=db_session, t0_datetime_utc=t0_datetime_utc)
    f2 = make_fake_forecast(gsp_id=1, session=db_session, t0_datetime_utc=t0_datetime_utc)
    f3 = make_fake_forecast(gsp_id=2, session=db_session, t0_datetime_utc=t0_datetime_utc)

    for i in range(len(f1.forecast_values)):
        f1.forecast_values[i].created_utc = datetime(2023, 1, 1, 10, tzinfo=timezone.utc)

    for i in range(len(f2.forecast_values)):
        f2.forecast_values[i].created_utc = datetime(2023, 1, 1, 12, tzinfo=timezone.utc)

    for i in range(len(f3.forecast_values)):
        f3.forecast_values[i].created_utc = datetime(2023, 1, 1, 10, tzinfo=timezone.utc)

    db_session.add_all([f1, f2, f3])
    db_session.commit()

    forecast_values = get_forecast_values(
        session=db_session, forecast_horizon_minutes=120, gsp_ids=[1, 2], only_return_latest=True
    )
    assert len(forecast_values) == 32
    assert forecast_values[0].forecast.location.gsp_id == 1
    assert forecast_values[0].created_utc == datetime(2023, 1, 1, 10, tzinfo=timezone.utc)
    assert forecast_values[0].target_time == datetime(2023, 1, 1, 12, tzinfo=timezone.utc)
    assert forecast_values[3].created_utc == datetime(2023, 1, 1, 10, tzinfo=timezone.utc)
    assert forecast_values[3].target_time == datetime(2023, 1, 1, 13, 30, tzinfo=timezone.utc)
    assert forecast_values[4].created_utc == datetime(2023, 1, 1, 12, tzinfo=timezone.utc)
    assert forecast_values[4].target_time == datetime(2023, 1, 1, 14, tzinfo=timezone.utc)

    assert forecast_values[8].created_utc == datetime(2023, 1, 1, 12, tzinfo=timezone.utc)
    assert forecast_values[8].target_time == datetime(2023, 1, 1, 16, tzinfo=timezone.utc)

    assert forecast_values[-1].target_time == datetime(2023, 1, 1, 19, 30, tzinfo=timezone.utc)
    assert forecast_values[-1].forecast.location.gsp_id == 2
