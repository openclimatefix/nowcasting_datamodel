from datetime import datetime, timedelta, timezone

import pytest
from freezegun import freeze_time

from nowcasting_datamodel.fake import (
    N_FAKE_FORECASTS,
    make_fake_forecast_value,
    make_fake_forecasts,
)
from nowcasting_datamodel.models import ForecastValueSevenDaysSQL
from nowcasting_datamodel.models.forecast import (
    ForecastSQL,
    ForecastValueLatestSQL,
    ForecastValueSQL,
)
from nowcasting_datamodel.update import (
    add_forecast_last_7_days_and_remove_old_data,
    change_forecast_value_to_forecast_last_7_days,
    update_all_forecast_latest,
    update_forecast_latest,
)


def test_model_duplicate_key(db_session):
    f1 = ForecastValueLatestSQL(
        gsp_id=1, target_time=datetime(2023, 1, 1), expected_power_generation_megawatts=1
    )

    f2 = ForecastValueLatestSQL(
        gsp_id=1, target_time=datetime(2023, 1, 1), expected_power_generation_megawatts=2
    )

    db_session.add(f1)
    db_session.commit()
    with pytest.raises(Exception):
        db_session.add(f2)
        db_session.commit()


def test_update_one_gsp(db_session):
    db_session.query(ForecastValueSQL).delete()
    db_session.query(ForecastSQL).delete()

    assert len(db_session.query(ForecastSQL).all()) == 0

    f1 = ForecastValueSQL(target_time=datetime(2023, 1, 1), expected_power_generation_megawatts=1.1)
    f2 = ForecastValueSQL(
        target_time=datetime(2023, 1, 1, 0, 30), expected_power_generation_megawatts=2.1
    )
    f = make_fake_forecasts(gsp_ids=[1], session=db_session, forecast_values=[f1, f2])
    forecast_sql = f[0]
    assert len(db_session.query(ForecastSQL).all()) == 1

    forecast_sql.forecast_values = [f1, f2]

    update_forecast_latest(forecast=forecast_sql, session=db_session)

    forecast_values_latest = db_session.query(ForecastValueLatestSQL).all()
    assert len(db_session.query(ForecastValueSQL).all()) == 2
    assert len(forecast_values_latest) == 2
    assert len(db_session.query(ForecastSQL).all()) == 2
    assert forecast_values_latest[0].expected_power_generation_megawatts == 1.1

    # new forecast is made
    # create and add
    f3 = ForecastValueSQL(target_time=datetime(2023, 1, 1), expected_power_generation_megawatts=3)

    f4 = ForecastValueSQL(
        target_time=datetime(2023, 1, 1, 0, 30), expected_power_generation_megawatts=4
    )
    f = make_fake_forecasts(gsp_ids=[1], session=db_session, forecast_values=[f3, f4])
    forecast_sql = f[0]
    update_forecast_latest(forecast=forecast_sql, session=db_session)

    forecast_values_latest = db_session.query(ForecastValueLatestSQL).all()
    assert len(db_session.query(ForecastValueSQL).all()) == 4
    assert len(forecast_values_latest) == 2
    assert len(db_session.query(ForecastSQL).all()) == 3
    assert forecast_values_latest[0].expected_power_generation_megawatts == 3

    # check that for gsp_id the results look right
    forecast_latest_values = (
        db_session.query(ForecastValueLatestSQL).filter(ForecastValueLatestSQL.gsp_id == 1).all()
    )
    assert forecast_latest_values[0].gsp_id == forecast_latest_values[1].gsp_id
    assert forecast_latest_values[0].forecast_id == forecast_latest_values[1].forecast_id


def test_update_all_forecast_latest(db_session):
    f1 = make_fake_forecasts(gsp_ids=list(range(0, 10)), session=db_session)
    db_session.add_all(f1)

    f2 = make_fake_forecasts(gsp_ids=list(range(0, 10)), session=db_session)
    db_session.add_all(f2)

    update_all_forecast_latest(forecasts=f1, session=db_session)
    update_all_forecast_latest(forecasts=f2, session=db_session)


def test_update_one_gsp_wtih_time_step(db_session):
    with freeze_time("2023-01-01") as f:
        db_session.query(ForecastValueSQL).delete()
        db_session.query(ForecastSQL).delete()

        assert len(db_session.query(ForecastSQL).all()) == 0

        f1 = ForecastValueSQL(
            target_time=datetime(2023, 1, 1), expected_power_generation_megawatts=1
        )
        f2 = ForecastValueSQL(
            target_time=datetime(2023, 1, 1, 0, 30), expected_power_generation_megawatts=2
        )
        f = make_fake_forecasts(gsp_ids=[1], session=db_session, forecast_values=[f1, f2])
        forecast_sql = f[0]
        assert len(db_session.query(ForecastSQL).all()) == 1

        forecast_sql.forecast_values = [f1, f2]
        update_forecast_latest(forecast=forecast_sql, session=db_session)

    assert len(db_session.query(ForecastValueSQL).all()) == 2
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 2
    assert len(db_session.query(ForecastSQL).all()) == 2
    assert len(db_session.query(ForecastSQL).filter(ForecastSQL.historic == True).all()) == 1

    with freeze_time("2023-01-01 00:30") as f:
        # new forecast is made
        # create and add
        f3 = ForecastValueSQL(
            target_time=datetime(2023, 1, 1, 0, 30), expected_power_generation_megawatts=3
        )

        f4 = ForecastValueSQL(
            target_time=datetime(2023, 1, 1, 1), expected_power_generation_megawatts=4
        )
        f = make_fake_forecasts(gsp_ids=[1], session=db_session, forecast_values=[f3, f4])
        forecast_sql = f[0]

        assert len(db_session.query(ForecastSQL).filter(ForecastSQL.historic == True).all()) == 1

        update_forecast_latest(forecast=forecast_sql, session=db_session)

        assert len(db_session.query(ForecastValueSQL).all()) == 4
        assert len(db_session.query(ForecastValueLatestSQL).all()) == 3
        assert len(db_session.query(ForecastSQL).all()) == 3

        # check that for gsp_id the results look right
        forecast_latest_values = (
            db_session.query(ForecastValueLatestSQL)
            .filter(ForecastValueLatestSQL.gsp_id == 1)
            .all()
        )
        assert forecast_latest_values[0].gsp_id == forecast_latest_values[1].gsp_id
        assert forecast_latest_values[0].forecast_id == forecast_latest_values[1].forecast_id

        forecasts_historic = (
            db_session.query(ForecastSQL).filter(ForecastSQL.historic == True).all()
        )

        assert (
            forecasts_historic[0].forecast_creation_time.isoformat()
            == datetime(2023, 1, 1, 0, 30, tzinfo=timezone.utc).isoformat()
        )


def test_update_all_forecast_latest_update_none(db_session):
    f1 = make_fake_forecasts(gsp_ids=list(range(0, 10)), session=db_session)
    db_session.add_all(f1)

    f2 = make_fake_forecasts(gsp_ids=list(range(0, 10)), session=db_session)
    db_session.add_all(f2)

    update_all_forecast_latest(
        forecasts=f1, session=db_session, update_national=False, update_gsp=False
    )
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 0


def test_update_all_forecast_latest_update_national(db_session):
    f1 = make_fake_forecasts(gsp_ids=list(range(0, 10)), session=db_session)
    db_session.add_all(f1)

    f2 = make_fake_forecasts(gsp_ids=list(range(0, 10)), session=db_session)
    db_session.add_all(f2)

    update_all_forecast_latest(
        forecasts=f1, session=db_session, update_national=True, update_gsp=False
    )
    assert len(db_session.query(ForecastValueLatestSQL).all()) == N_FAKE_FORECASTS


def test_update_all_forecast_latest_update_gsps(db_session):
    f1 = make_fake_forecasts(gsp_ids=list(range(0, 10)), session=db_session)
    db_session.add_all(f1)

    f2 = make_fake_forecasts(gsp_ids=list(range(0, 10)), session=db_session)
    db_session.add_all(f2)

    update_all_forecast_latest(
        forecasts=f1, session=db_session, update_national=True, update_gsp=True
    )
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 10 * N_FAKE_FORECASTS


def test_change_forecast_value_to_forecast_last_7_days(db_session):
    assert len(db_session.query(ForecastValueSevenDaysSQL).all()) == 0

    now = datetime.now(tz=timezone.utc)
    forecast_value = make_fake_forecast_value(target_time=now)

    forecast_value_last_seven_days = change_forecast_value_to_forecast_last_7_days(
        forecast=forecast_value
    )
    add_forecast_last_7_days_and_remove_old_data(
        forecast_values=[forecast_value_last_seven_days], session=db_session
    )

    assert len(db_session.query(ForecastValueSevenDaysSQL).all()) == 1


def test_forecast_last_7_days_old_data(db_session):
    assert len(db_session.query(ForecastValueSevenDaysSQL).all()) == 0

    now_minus_8_days = datetime.now(tz=timezone.utc) - timedelta(days=8)
    forecast_value = ForecastValueSevenDaysSQL(
        target_time=now_minus_8_days, expected_power_generation_megawatts=1
    )

    add_forecast_last_7_days_and_remove_old_data(
        forecast_values=[forecast_value], session=db_session
    )

    assert len(db_session.query(ForecastValueSevenDaysSQL).all()) == 0
