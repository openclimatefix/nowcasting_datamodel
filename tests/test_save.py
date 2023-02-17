from datetime import datetime, timezone

from nowcasting_datamodel.fake import N_FAKE_FORECASTS, make_fake_forecasts
from nowcasting_datamodel.models.forecast import (
    ForecastSQL,
    ForecastValueLatestSQL,
    ForecastValueSevenDaysSQL,
    ForecastValueSQL,
)
from nowcasting_datamodel.models.pv import PVSystem, PVSystemSQL
from nowcasting_datamodel.save import save, save_all_forecast_values_seven_days, save_pv_system


def test_save(db_session):
    forecasts = make_fake_forecasts(gsp_ids=range(0, 10), session=db_session)

    save(session=db_session, forecasts=forecasts)

    # 10 forecast, + 10 historic ones
    assert len(db_session.query(ForecastSQL).all()) == 20
    assert len(db_session.query(ForecastValueSQL).all()) == 10 * N_FAKE_FORECASTS
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 10 * N_FAKE_FORECASTS
    assert len(db_session.query(ForecastValueSevenDaysSQL).all()) == 10 * N_FAKE_FORECASTS

    forecasts = make_fake_forecasts(gsp_ids=range(0, 10), session=db_session)
    save(session=db_session, forecasts=forecasts)

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


def test_save_pv_system(db_session_pv):
    pv_systems = db_session_pv.query(PVSystemSQL).all()
    assert len(pv_systems) == 0

    pv_system = PVSystem(pv_system_id=2, provider="pvoutput.org", latitude=55, longitude=0)

    save_pv_system(session=db_session_pv, pv_system=pv_system)

    pv_systems = db_session_pv.query(PVSystemSQL).all()
    assert len(pv_systems) == 1

    save_pv_system(session=db_session_pv, pv_system=pv_system)

    pv_systems = db_session_pv.query(PVSystemSQL).all()
    assert len(pv_systems) == 1


def test_save_all_forecast_values_seven_days(db_session):
    now = datetime.now(tz=timezone.utc)
    forecasts = make_fake_forecasts(gsp_ids=range(0, 3), session=db_session, t0_datetime_utc=now)

    save_all_forecast_values_seven_days(session=db_session, forecasts=forecasts)

    assert len(db_session.query(ForecastValueSevenDaysSQL).all()) == 3 * 112
