import logging
from datetime import datetime, timezone

from nowcasting_datamodel.fake import make_fake_forecast
from nowcasting_datamodel.read.read import get_forecast_values

logger = logging.getLogger(__name__)


def test_get_latest_forecast_created_utc_gsp(db_session):
    t0_datetime_utc = datetime(2022, 1, 1, 12, tzinfo=timezone.utc)

    f1 = make_fake_forecast(gsp_id=1, session=db_session, t0_datetime_utc=t0_datetime_utc)
    f2 = make_fake_forecast(gsp_id=1, session=db_session, t0_datetime_utc=t0_datetime_utc)

    f1.forecast_values[0].created_utc = datetime(2022, 1, 1, 10, tzinfo=timezone.utc)
    f1.forecast_values[1].created_utc = datetime(2022, 1, 1, 10, tzinfo=timezone.utc)

    f2.forecast_values[0].created_utc = datetime(2022, 1, 1, 12, tzinfo=timezone.utc)
    f2.forecast_values[1].created_utc = datetime(2022, 1, 1, 12, tzinfo=timezone.utc)

    db_session.add_all([f1, f2])
    db_session.commit()

    forecast_values = get_forecast_values(
        session=db_session, forecast_horizon_minutes=120, gsp_id=1
    )
    assert len(forecast_values) == 2
    assert forecast_values[0].created_utc == datetime(2022, 1, 1, 10, tzinfo=timezone.utc)
    assert forecast_values[0].target_time == datetime(2022, 1, 1, 12, tzinfo=timezone.utc)
