import logging
from datetime import datetime, timedelta, timezone

import pytest
from freezegun import freeze_time

from nowcasting_datamodel.fake import (
    make_fake_forecast,
    make_fake_forecasts,
    make_fake_national_forecast,
    make_fake_pv_system,
)
from nowcasting_datamodel.models import (
    Forecast,
    ForecastValue,
    ForecastValueLatestSQL,
    ForecastValueSQL,
    InputDataLastUpdatedSQL,
    LocationSQL,
    MLModel,
    PVSystem,
    Status,
    national_gb_label,
)
from nowcasting_datamodel.read.read import (
    get_all_gsp_ids_latest_forecast,
    get_all_locations,
    get_forecast_values,
    get_latest_forecast,
    get_latest_forecast_created_utc,
    get_latest_forecast_for_gsps,
    get_latest_input_data_last_updated,
    get_latest_national_forecast,
    get_latest_status,
    get_location,
    get_model,
    get_pv_system,
    update_latest_input_data_last_updated,
)
from nowcasting_datamodel.save import save_pv_system

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

    f = get_latest_forecast_for_gsps(session=db_session, forecast_horizon_hours=2, gsp_ids=[1])
    assert len(f) == 1
    assert f[0].forecast_values[0].created_utc == datetime(2022, 1, 1, 10, tzinfo=timezone.utc)
    assert f[0].forecast_values[0].target_time == datetime(2022, 1, 1, 12, tzinfo=timezone.utc)
