from datetime import datetime, timezone

import pytest
from freezegun import freeze_time

from nowcasting_datamodel.fake import make_fake_forecasts
from nowcasting_datamodel.models.models import ForecastSQL, ForecastValueLatestSQL, ForecastValueSQL
from nowcasting_datamodel.update import update_forecast_latest


def test_model_duplicate_key(db_session):

    f1 = ForecastValueLatestSQL(
        gsp_id=1, target_time=datetime(2022, 1, 1), expected_power_generation_megawatts=1
    )

    f2 = ForecastValueLatestSQL(
        gsp_id=1, target_time=datetime(2022, 1, 1), expected_power_generation_megawatts=2
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

    f1 = ForecastValueSQL(target_time=datetime(2022, 1, 1), expected_power_generation_megawatts=1)
    f2 = ForecastValueSQL(
        target_time=datetime(2022, 1, 1, 0, 30), expected_power_generation_megawatts=2
    )
    f = make_fake_forecasts(gsp_ids=[1], session=db_session, forecast_values=[f1, f2])
    forecast_sql = f[0]
    assert len(db_session.query(ForecastSQL).all()) == 1

    forecast_sql.forecast_values = [f1, f2]

    update_forecast_latest(forecast=forecast_sql, session=db_session)

    assert len(db_session.query(ForecastValueSQL).all()) == 2
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 2
    assert len(db_session.query(ForecastSQL).all()) == 2

    # new forecast is made
    # create and add
    f3 = ForecastValueSQL(target_time=datetime(2022, 1, 1), expected_power_generation_megawatts=3)

    f4 = ForecastValueSQL(
        target_time=datetime(2022, 1, 1, 0, 30), expected_power_generation_megawatts=4
    )
    f = make_fake_forecasts(gsp_ids=[1], session=db_session, forecast_values=[f3, f4])
    forecast_sql = f[0]
    update_forecast_latest(forecast=forecast_sql, session=db_session)

    assert len(db_session.query(ForecastValueSQL).all()) == 4
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 2
    assert len(db_session.query(ForecastSQL).all()) == 3

    # check that for gsp_id the results look right
    forecast_latest_values = (
        db_session.query(ForecastValueLatestSQL).filter(ForecastValueLatestSQL.gsp_id == 1).all()
    )
    assert forecast_latest_values[0].gsp_id == forecast_latest_values[1].gsp_id
    assert forecast_latest_values[0].forecast_id == forecast_latest_values[1].forecast_id


def test_update_one_gsp_wtih_time_step(db_session):
    with freeze_time("2022-01-01") as f:
        db_session.query(ForecastValueSQL).delete()
        db_session.query(ForecastSQL).delete()

        assert len(db_session.query(ForecastSQL).all()) == 0

        f1 = ForecastValueSQL(
            target_time=datetime(2022, 1, 1), expected_power_generation_megawatts=1
        )
        f2 = ForecastValueSQL(
            target_time=datetime(2022, 1, 1, 0, 30), expected_power_generation_megawatts=2
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

    with freeze_time("2022-01-01 00:30") as f:
        # new forecast is made
        # create and add
        f3 = ForecastValueSQL(
            target_time=datetime(2022, 1, 1, 0, 30), expected_power_generation_megawatts=3
        )

        f4 = ForecastValueSQL(
            target_time=datetime(2022, 1, 1, 1), expected_power_generation_megawatts=4
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
            == datetime(2022, 1, 1, 0, 30, tzinfo=timezone.utc).isoformat()
        )

