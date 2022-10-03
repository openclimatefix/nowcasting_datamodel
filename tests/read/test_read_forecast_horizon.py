import logging
from datetime import datetime, timezone

from nowcasting_datamodel.fake import N_FAKE_FORECASTS, make_fake_forecast
from nowcasting_datamodel.read.read import get_forecast_values

logger = logging.getLogger(__name__)


def test_get_latest_forecast_created_utc_gsp(db_session):
    t0_datetime_utc = datetime(2022, 1, 1, 12, tzinfo=timezone.utc)

    f1 = make_fake_forecast(gsp_id=1, session=db_session, t0_datetime_utc=t0_datetime_utc)
    f2 = make_fake_forecast(gsp_id=1, session=db_session, t0_datetime_utc=t0_datetime_utc)

    for i in range(len(f1.forecast_values)):
        f1.forecast_values[i].created_utc = datetime(2022, 1, 1, 10, tzinfo=timezone.utc)

    for i in range(len(f2.forecast_values)):
        f2.forecast_values[i].created_utc = datetime(2022, 1, 1, 12, tzinfo=timezone.utc)

    db_session.add_all([f1, f2])
    db_session.commit()

    forecast_values = get_forecast_values(
        session=db_session, forecast_horizon_minutes=120, gsp_id=1, only_return_latest=True
    )
    assert len(forecast_values) == 16
    assert forecast_values[0].created_utc == datetime(2022, 1, 1, 10, tzinfo=timezone.utc)
    assert forecast_values[0].target_time == datetime(2022, 1, 1, 12, tzinfo=timezone.utc)
    assert forecast_values[3].created_utc == datetime(2022, 1, 1, 10, tzinfo=timezone.utc)
    assert forecast_values[3].target_time == datetime(2022, 1, 1, 13, 30, tzinfo=timezone.utc)
    assert forecast_values[4].created_utc == datetime(2022, 1, 1, 12, tzinfo=timezone.utc)
    assert forecast_values[4].target_time == datetime(2022, 1, 1, 14, tzinfo=timezone.utc)

    assert forecast_values[8].created_utc == datetime(2022, 1, 1, 12, tzinfo=timezone.utc)
    assert forecast_values[8].target_time == datetime(2022, 1, 1, 16, tzinfo=timezone.utc)

    assert forecast_values[-1].target_time == datetime(2022, 1, 1, 19, 30, tzinfo=timezone.utc)
