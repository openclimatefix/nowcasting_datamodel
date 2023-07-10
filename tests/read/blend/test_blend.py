import logging
from datetime import datetime, timezone, timedelta

from freezegun import freeze_time

from nowcasting_datamodel.fake import (
    make_fake_forecasts,
)
from nowcasting_datamodel.models.forecast import ForecastValueLatestSQL, ForecastValueSevenDaysSQL
from nowcasting_datamodel.read.blend.blend import get_blend_forecast_values_latest
from nowcasting_datamodel.read.read import get_model

logger = logging.getLogger(__name__)


@freeze_time("2023-01-01 00:00:01")
def test_get_blend_forecast_values_latest_one_model(db_session):
    model = get_model(session=db_session, name="test_1", version="0.0.1")

    f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
    f1[0].historic = True
    f1[0].forecast_values_latest = [
        ForecastValueLatestSQL(
            gsp_id=1,
            expected_power_generation_megawatts=1,
            target_time=datetime(2023, 1, 1, tzinfo=timezone.utc),
            model_id=model.id,
            properties={"10": 0.9, "90": 1.1},
        ),
        ForecastValueLatestSQL(
            gsp_id=1,
            expected_power_generation_megawatts=1,
            target_time=datetime(2023, 1, 1, 0, 30, tzinfo=timezone.utc),
            model_id=model.id,
        ),
    ]
    db_session.add_all(f1)
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 2

    forecast_values_read = get_blend_forecast_values_latest(
        session=db_session,
        gsp_id=f1[0].location.gsp_id,
        start_datetime=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
        model_names=["test_1"],
        properties_model="test_1",
    )

    assert len(forecast_values_read) == 2
    assert forecast_values_read[0].target_time == f1[0].forecast_values_latest[0].target_time
    assert (
        forecast_values_read[0].expected_power_generation_megawatts
        == f1[0].forecast_values_latest[0].expected_power_generation_megawatts
    )
    assert forecast_values_read[0]._properties == {"10": 0.9, "90": 1.1}


@freeze_time("2023-01-01 00:00:01")
def test_get_blend_forecast_values_latest_two_model_read_one(db_session):
    model_1 = get_model(session=db_session, name="test_1", version="0.0.1")
    model_2 = get_model(session=db_session, name="test_2", version="0.0.1")

    for model in [model_1, model_2]:
        f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
        f1[0].historic = True
        f1[0].forecast_values_latest = [
            ForecastValueLatestSQL(
                gsp_id=1,
                expected_power_generation_megawatts=1,
                target_time=datetime(2023, 1, 1, tzinfo=timezone.utc),
                model_id=model.id,
                properties={"10": 0.9, "90": 1.1},
            ),
            ForecastValueLatestSQL(
                gsp_id=1,
                expected_power_generation_megawatts=2,
                target_time=datetime(2023, 1, 1, 0, 30, tzinfo=timezone.utc),
                model_id=model.id,
                properties={"10": 1.8, "90": 2.2},
            ),
        ]
        db_session.add_all(f1)
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 4

    forecast_values_read = get_blend_forecast_values_latest(
        session=db_session,
        gsp_id=f1[0].location.gsp_id,
        start_datetime=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
        model_names=["test_1"],
        properties_model="test_1",
    )

    assert len(forecast_values_read) == 2
    assert forecast_values_read[0].target_time == f1[0].forecast_values_latest[0].target_time
    assert (
        forecast_values_read[0].expected_power_generation_megawatts
        == f1[0].forecast_values_latest[0].expected_power_generation_megawatts
    )
    assert forecast_values_read[0]._properties == {"10": 0.9, "90": 1.1}


@freeze_time("2023-01-01 00:00:01")
def test_get_blend_forecast_values_latest_two_model_read_two(db_session):
    model_1 = get_model(session=db_session, name="test_1", version="0.0.1")
    model_2 = get_model(session=db_session, name="test_2", version="0.0.1")
    model_3 = get_model(session=db_session, name="test_2", version="0.0.2")

    forecasts = {}
    for model in [model_1, model_2, model_3]:
        f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
        f1[0].historic = True

        if model == model_1:
            power = 1
            adjust = 0
            forecast_horizon_minutes = [-60, -30, 0, 30, 8 * 30, 15 * 30]
            created_utc = datetime(2023, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
            properties = {"10": 0.9, "90": 1.1}
        elif model == model_2:
            power = 2
            adjust = 100
            forecast_horizon_minutes = [0, 30, 8 * 30, 15 * 30, 16 * 30]
            created_utc = datetime(2023, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
            properties = {"10": 1.8, "90": 2.2}
        else:
            power = 3
            adjust = 200
            forecast_horizon_minutes = [0, 30, 8 * 30, 15 * 30, 16 * 30]
            created_utc = datetime(2023, 1, 1, 0, 0, 2, tzinfo=timezone.utc)
            properties = {"10": 2.7, "90": 3.3}

        f1[0].forecast_values_latest = [
            ForecastValueLatestSQL(
                gsp_id=1,
                expected_power_generation_megawatts=power,
                target_time=datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=t),
                model_id=model.id,
                adjust_mw=adjust,
                created_utc=created_utc,
                properties=properties,
            )
            for t in forecast_horizon_minutes
        ]

        db_session.add_all(f1)
        forecasts[model.name] = f1
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 16

    forecast_values_read = get_blend_forecast_values_latest(
        session=db_session,
        gsp_id=f1[0].location.gsp_id,
        start_datetime=datetime(2022, 12, 31, 0, 0, tzinfo=timezone.utc),
        model_names=["test_1", "test_2"],
        properties_model="test_1",
    )

    assert len(forecast_values_read) == 7
    assert (
        forecast_values_read[0].target_time
        == (forecasts["test_1"])[0].forecast_values_latest[0].target_time
    )
    assert forecast_values_read[0].expected_power_generation_megawatts == 1
    assert forecast_values_read[1].expected_power_generation_megawatts == 1
    assert forecast_values_read[2].expected_power_generation_megawatts == 1
    assert forecast_values_read[3].expected_power_generation_megawatts == 1
    assert forecast_values_read[4].expected_power_generation_megawatts == 2
    assert forecast_values_read[5].expected_power_generation_megawatts == 3

    assert forecast_values_read[0]._properties == {"10": 0.9, "90": 1.1}
    assert forecast_values_read[1]._properties == {"10": 0.9, "90": 1.1}
    assert forecast_values_read[2]._properties == {"10": 0.9, "90": 1.1}
    assert forecast_values_read[3]._properties == {"10": 0.9, "90": 1.1}
    assert forecast_values_read[4]._properties == {"10": 1.9, "90": 2.1}
    assert forecast_values_read[5]._properties == {"10": 2.9, "90": 3.1}

    assert forecast_values_read[0]._adjust_mw == 0
    assert forecast_values_read[1]._adjust_mw == 0
    assert forecast_values_read[2]._adjust_mw == 0
    assert forecast_values_read[3]._adjust_mw == 0
    assert forecast_values_read[4]._adjust_mw == 100
    assert forecast_values_read[5]._adjust_mw == 200


#


@freeze_time("2023-01-01 00:00:01")
def test_get_blend_forecast_values_latest_negative(db_session):
    """This test makes sure that the blend function changes negatives to zeros"""

    model_1 = get_model(session=db_session, name="test_1", version="0.0.1")
    model_2 = get_model(session=db_session, name="test_2", version="0.0.1")

    for model in [model_1, model_2]:
        f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
        f1[0].historic = True

        if model == model_1:
            power = -1
            adjust = 1.0
        else:
            power = -2
            adjust = 2.0

        forecast_horizon_minutes = [0, 30, 8 * 30, 15 * 30]
        f1[0].forecast_values_latest = [
            ForecastValueLatestSQL(
                gsp_id=1,
                expected_power_generation_megawatts=power,
                target_time=datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=t),
                model_id=model.id,
                adjust_mw=adjust,
            )
            for t in forecast_horizon_minutes
        ]

        db_session.add_all(f1)
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 8

    forecast_values_read = get_blend_forecast_values_latest(
        session=db_session,
        gsp_id=f1[0].location.gsp_id,
        start_datetime=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
        model_names=["test_1", "test_2"],
        properties_model="test_1",
    )

    assert len(forecast_values_read) == 4
    assert forecast_values_read[0].target_time == f1[0].forecast_values_latest[0].target_time
    assert forecast_values_read[0].expected_power_generation_megawatts == 0
    assert forecast_values_read[1].expected_power_generation_megawatts == 0
    assert forecast_values_read[2].expected_power_generation_megawatts == 0
    assert forecast_values_read[3].expected_power_generation_megawatts == 0

    assert forecast_values_read[0]._adjust_mw == 1.0
    assert forecast_values_read[2]._adjust_mw == 1.5
    assert forecast_values_read[3]._adjust_mw == 2.0


@freeze_time("2023-01-01 00:00:01")
def test_get_blend_forecast_values_latest_negative_two(db_session):
    model_1 = get_model(session=db_session, name="test_1", version="0.0.1")
    model_2 = get_model(session=db_session, name="test_2", version="0.0.1")

    for model in [model_1, model_2]:
        f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
        f1[0].historic = True

        if model == model_1:
            power = -1
        else:
            power = -2

        forecast_horizon_minutes = [0, 30, 8 * 30, 15 * 30]
        f1[0].forecast_values_latest = [
            ForecastValueLatestSQL(
                gsp_id=1,
                expected_power_generation_megawatts=power,
                target_time=datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=t),
                model_id=model.id,
                adjust_mw=1,
            )
            for t in forecast_horizon_minutes
        ]

        db_session.add_all(f1)
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 8

    forecast_values_read = get_blend_forecast_values_latest(
        session=db_session,
        gsp_id=f1[0].location.gsp_id,
        start_datetime=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
        model_names=["test_1", "test_2"],
    )

    assert len(forecast_values_read) == 4
    assert forecast_values_read[0].target_time == f1[0].forecast_values_latest[0].target_time
    assert forecast_values_read[0].expected_power_generation_megawatts == 0
    assert forecast_values_read[1].expected_power_generation_megawatts == 0
    assert forecast_values_read[2].expected_power_generation_megawatts == 0
    assert forecast_values_read[3].expected_power_generation_megawatts == 0


@freeze_time("2023-01-01 00:00:01")
def test_get_blend_forecast_values_latest_forecast_horizon(db_session):
    model_1 = get_model(session=db_session, name="test_1", version="0.0.1")
    model_2 = get_model(session=db_session, name="test_2", version="0.0.1")

    forecasts = {}
    for model in [model_1, model_2]:
        f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
        f1[0].historic = True
        f1[0].model = model

        if model == model_1:
            power = 1
            adjust = 0
            forecast_horizon_minutes = [-60, -30, 0, 30, 8 * 30, 15 * 30]
        else:
            power = 2
            adjust = 100
            forecast_horizon_minutes = [0, 30, 8 * 30, 15 * 30, 16 * 30]

        f1[0].forecast_values_last_seven_days = [
            ForecastValueSevenDaysSQL(
                # gsp_id=1,
                expected_power_generation_megawatts=power,
                target_time=datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=t),
                # model_id=model.id,
                adjust_mw=adjust,
                created_utc=datetime(2023, 1, 1, tzinfo=timezone.utc)
                + timedelta(minutes=t)
                - timedelta(hours=4.1),
            )
            for t in forecast_horizon_minutes
        ]

        db_session.add_all(f1)
        forecasts[model.name] = f1

    fs = db_session.query(ForecastValueSevenDaysSQL).all()
    assert len(fs) == 11
    for f in fs:
        logger.debug(f.forecast.model.name)
        logger.debug(f.target_time)
        logger.debug(f.created_utc)
        logger.debug(f.forecast.location.gsp_id)

    forecast_values_read = get_blend_forecast_values_latest(
        session=db_session,
        gsp_id=f1[0].location.gsp_id,
        start_datetime=datetime(2022, 12, 31, 0, 0, tzinfo=timezone.utc),
        model_names=["test_1", "test_2"],
        forecast_horizon_minutes=4 * 60,
    )

    assert len(forecast_values_read) == 7
    assert (
        forecast_values_read[0].target_time
        == (forecasts["test_1"])[0].forecast_values_last_seven_days[0].target_time
    )
    assert forecast_values_read[0].expected_power_generation_megawatts == 1.0
    assert forecast_values_read[1].expected_power_generation_megawatts == 1.0
    assert forecast_values_read[2].expected_power_generation_megawatts == 1.5
    assert forecast_values_read[3].expected_power_generation_megawatts == 1.5
    assert forecast_values_read[4].expected_power_generation_megawatts == 1.5
    assert forecast_values_read[5].expected_power_generation_megawatts == 1.5
    assert forecast_values_read[6].expected_power_generation_megawatts == 2.0

    assert forecast_values_read[0]._adjust_mw == 0
    assert forecast_values_read[1]._adjust_mw == 0
    assert forecast_values_read[2]._adjust_mw == 50
    assert forecast_values_read[3]._adjust_mw == 50
    assert forecast_values_read[4]._adjust_mw == 50
    assert forecast_values_read[5]._adjust_mw == 50
    assert forecast_values_read[6]._adjust_mw == 100
