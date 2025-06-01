from datetime import datetime, timezone
import os
import numpy as np
import pytest
from freezegun import freeze_time

from nowcasting_datamodel.fake import N_FAKE_FORECASTS, make_fake_forecasts
from nowcasting_datamodel.models.forecast import (
    ForecastSQL,
    ForecastValueLatestSQL,
    ForecastValueSevenDaysSQL,
    ForecastValueSQL,
)

from nowcasting_datamodel.save.save import save, save_all_forecast_values_seven_days


@freeze_time("2024-01-01 00:00:00")
def test_save(db_session, latest_me):
    # Make sure save works where no forecast already exists
    forecasts = make_fake_forecasts(
        gsp_ids=range(0, 10),
        session=db_session,
        t0_datetime_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    save(
        session=db_session,
        forecasts=forecasts,
    )

    forecast_values = db_session.query(ForecastValueSQL).all()
    assert np.max([fv.adjust_mw for fv in forecast_values]) != 0.0

    # 10 forecast, + 10 historic ones
    assert len(db_session.query(ForecastSQL).all()) == 20
    assert len(db_session.query(ForecastValueSQL).all()) == 10 * N_FAKE_FORECASTS
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 10 * N_FAKE_FORECASTS
    assert len(db_session.query(ForecastValueSevenDaysSQL).all()) == 10 * N_FAKE_FORECASTS

    # Make sure save works where forecast exists already
    forecasts = make_fake_forecasts(gsp_ids=range(0, 10), session=db_session)
    save(session=db_session, forecasts=forecasts)

    # 20 forecast, + 10 historic ones
    assert len(db_session.query(ForecastSQL).all()) == 30
    forecast_values = db_session.query(ForecastValueSQL).all()
    assert len(forecast_values) == 20 * N_FAKE_FORECASTS
    forecast_values_latest = db_session.query(ForecastValueLatestSQL).all()
    assert len(forecast_values_latest) == 10 * N_FAKE_FORECASTS
    forecast_values_seven_days = db_session.query(ForecastValueSevenDaysSQL).all()
    assert len(forecast_values_seven_days) == 20 * N_FAKE_FORECASTS

    assert forecast_values[0].properties is not None
    assert forecast_values_latest[0].properties is not None
    assert forecast_values_seven_days[0].properties is not None

    # check that for gsp_id the results look right
    forecast_latest_values = (
        db_session.query(ForecastValueLatestSQL).filter(ForecastValueLatestSQL.gsp_id == 2).all()
    )
    assert forecast_latest_values[0].gsp_id == forecast_latest_values[1].gsp_id
    assert forecast_latest_values[0].properties is not None


@freeze_time("2024-01-01 00:00:00")
def test_save_distinct_last_seven_days(db_session, latest_me):
    # Make sure save works where no forecast already exists
    forecasts = make_fake_forecasts(
        gsp_ids=range(0, 10),
        session=db_session,
        t0_datetime_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    save(session=db_session, forecasts=forecasts, remove_non_distinct_last_seven_days=True)

    forecast_values = db_session.query(ForecastValueSQL).all()
    assert np.max([fv.adjust_mw for fv in forecast_values]) != 0.0

    # 10 forecast, + 10 historic ones
    assert len(db_session.query(ForecastSQL).all()) == 20
    assert len(db_session.query(ForecastValueSQL).all()) == 10 * N_FAKE_FORECASTS
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 10 * N_FAKE_FORECASTS
    assert len(db_session.query(ForecastValueSevenDaysSQL).all()) == 10 * N_FAKE_FORECASTS

    # Make sure save works where forecast exists already
    forecasts = make_fake_forecasts(gsp_ids=range(0, 10), session=db_session)
    save(session=db_session, forecasts=forecasts, remove_non_distinct_last_seven_days=True)

    # 20 forecast, + 10 historic ones
    assert len(db_session.query(ForecastSQL).all()) == 30
    forecast_values = db_session.query(ForecastValueSQL).all()
    assert len(forecast_values) == 20 * N_FAKE_FORECASTS
    forecast_values_latest = db_session.query(ForecastValueLatestSQL).all()
    assert len(forecast_values_latest) == 10 * N_FAKE_FORECASTS
    forecast_values_seven_days = db_session.query(ForecastValueSevenDaysSQL).all()
    assert len(forecast_values_seven_days) == 10 * (N_FAKE_FORECASTS + 49)

    assert forecast_values[0].properties is not None
    assert forecast_values_latest[0].properties is not None
    assert forecast_values_seven_days[0].properties is not None


@freeze_time("2024-01-01 00:00:00")
def test_save_no_adjuster(db_session):
    # Make sure save works where no forecast already exists
    forecasts = make_fake_forecasts(
        gsp_ids=range(0, 10),
        session=db_session,
        t0_datetime_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    save(session=db_session, forecasts=forecasts, apply_adjuster=False)

    # 10 forecast, + 10 historic ones
    assert len(db_session.query(ForecastSQL).all()) == 20
    assert len(db_session.query(ForecastValueSQL).all()) == 10 * N_FAKE_FORECASTS
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 10 * N_FAKE_FORECASTS
    assert len(db_session.query(ForecastValueSevenDaysSQL).all()) == 10 * N_FAKE_FORECASTS

    # Make sure save works where forecast exists already
    forecasts = make_fake_forecasts(gsp_ids=range(0, 10), session=db_session)
    save(session=db_session, forecasts=forecasts, apply_adjuster=False)

    # 20 forecast, + 10 historic ones
    assert len(db_session.query(ForecastSQL).all()) == 30
    assert len(db_session.query(ForecastValueSQL).all()) == 20 * N_FAKE_FORECASTS
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 10 * N_FAKE_FORECASTS
    assert len(db_session.query(ForecastValueSevenDaysSQL).all()) == 20 * N_FAKE_FORECASTS

    # check that for gsp_id the results look right
    forecast_latest_values = (
        db_session.query(ForecastValueLatestSQL).filter(ForecastValueLatestSQL.gsp_id == 2).all()
    )
    assert forecast_latest_values[0].gsp_id == forecast_latest_values[1].gsp_id


def test_save_all_forecast_values_seven_days(db_session):
    now = datetime.now(tz=timezone.utc)
    forecasts = make_fake_forecasts(gsp_ids=range(0, 3), session=db_session, t0_datetime_utc=now)

    save_all_forecast_values_seven_days(
        session=db_session, forecasts=forecasts, remove_non_distinct=False
    )

    assert len(db_session.query(ForecastValueSevenDaysSQL).all()) == 3 * 112


@pytest.fixture()
def dont_use_adjuster():
    os.environ["USE_ADJUSTER"] = "0"
    yield
    os.environ["USE_ADJUSTER"] = "1"


def test_save_dont_use_adjuster(db_session, latest_me, dont_use_adjuster):
    # Make sure save works where no forecast already exists
    forecasts = make_fake_forecasts(gsp_ids=range(0, 10), session=db_session)
    save(
        session=db_session,
        forecasts=forecasts,
        apply_adjuster=True,  # this gets override by the env variable
    )

    forecast_values = db_session.query(ForecastValueSQL).all()
    assert np.max([fv.adjust_mw for fv in forecast_values]) == 0.0
