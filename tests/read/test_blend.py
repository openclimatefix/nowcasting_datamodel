import logging
from datetime import datetime, timezone, timedelta

from freezegun import freeze_time
from nowcasting_datamodel.fake import (
    make_fake_forecasts,
)
from nowcasting_datamodel.models.forecast import (
    ForecastValueLatestSQL,
)
from nowcasting_datamodel.read.blend import get_blend_forecast_values_latest, make_weights_df
from nowcasting_datamodel.read.read import get_model

logger = logging.getLogger(__name__)


@freeze_time("2023-01-01 00:00:01")
def test_make_weights_df():
    start_datetime = datetime(2023, 1, 1, 0, 30, tzinfo=timezone.utc)

    weights = make_weights_df(model_names=["test_1", "test_2"], weights=None)
    assert len(weights) == 16
    assert "test_1" in weights.columns
    assert "test_2" in weights.columns

    assert weights.index[0].isoformat() == start_datetime.isoformat()
    assert (
        weights.index[15].isoformat()
        == (start_datetime + timedelta(hours=7, minutes=30)).isoformat()
    )

    assert weights["test_1"][0] == 1
    assert weights["test_2"][0] == 0

    assert weights["test_1"][3] == 1
    assert weights["test_2"][3] == 0

    assert weights["test_1"][7] == 0.5
    assert weights["test_2"][7] == 0.5

    assert weights["test_1"][11] == 0
    assert weights["test_2"][11] == 1

    assert weights["test_1"][15] == 0
    assert weights["test_2"][15] == 1


@freeze_time("2023-01-02 00:00:01")
def test_make_weights_df_yesterday():
    start_datetime = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    weights = make_weights_df(
        model_names=["test_1", "test_2"], weights=None, start_datetime=start_datetime
    )
    assert len(weights) == 49 + 16
    assert "test_1" in weights.columns
    assert "test_2" in weights.columns

    assert weights.index[0].isoformat() == start_datetime.isoformat()
    assert (
        weights.index[15].isoformat()
        == (start_datetime + timedelta(hours=7, minutes=30)).isoformat()
    )

    assert weights["test_1"][0] == 1
    assert weights["test_2"][0] == 0

    assert weights["test_1"][49] == 1
    assert weights["test_2"][49] == 0

    assert weights["test_1"][49 + 3] == 1
    assert weights["test_2"][49 + 3] == 0

    assert weights["test_1"][49 + 7] == 0.5
    assert weights["test_2"][49 + 7] == 0.5

    assert weights["test_1"][49 + 11] == 0
    assert weights["test_2"][49 + 11] == 1

    assert weights["test_1"][49 + 15] == 0
    assert weights["test_2"][49 + 15] == 1


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
    )

    assert len(forecast_values_read) == 2
    assert forecast_values_read[0].target_time == f1[0].forecast_values_latest[0].target_time
    assert (
        forecast_values_read[0].expected_power_generation_megawatts
        == f1[0].forecast_values_latest[0].expected_power_generation_megawatts
    )


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
            ),
            ForecastValueLatestSQL(
                gsp_id=1,
                expected_power_generation_megawatts=2,
                target_time=datetime(2023, 1, 1, 0, 30, tzinfo=timezone.utc),
                model_id=model.id,
            ),
        ]
        db_session.add_all(f1)
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 4

    forecast_values_read = get_blend_forecast_values_latest(
        session=db_session,
        gsp_id=f1[0].location.gsp_id,
        start_datetime=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
        model_names=["test_1"],
    )

    assert len(forecast_values_read) == 2
    assert forecast_values_read[0].target_time == f1[0].forecast_values_latest[0].target_time
    assert (
        forecast_values_read[0].expected_power_generation_megawatts
        == f1[0].forecast_values_latest[0].expected_power_generation_megawatts
    )


@freeze_time("2023-01-01 00:00:01")
def test_get_blend_forecast_values_latest_two_model_read_two(db_session):
    model_1 = get_model(session=db_session, name="test_1", version="0.0.1")
    model_2 = get_model(session=db_session, name="test_2", version="0.0.1")

    forecasts = {}
    for model in [model_1, model_2]:
        f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
        f1[0].historic = True

        if model == model_1:
            power = 1
            adjust = 0
            forecast_horizon_minutes = [-60, -30, 0, 30, 8 * 30, 15 * 30]
        else:
            power = 2
            adjust = 100
            forecast_horizon_minutes = [0, 30, 8 * 30, 15 * 30, 16 * 30]

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
        forecasts[model.name] = f1
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 11

    forecast_values_read = get_blend_forecast_values_latest(
        session=db_session,
        gsp_id=f1[0].location.gsp_id,
        start_datetime=datetime(2022, 12, 31, 0, 0, tzinfo=timezone.utc),
        model_names=["test_1", "test_2"],
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
    assert forecast_values_read[4].expected_power_generation_megawatts == 1.5
    assert forecast_values_read[5].expected_power_generation_megawatts == 2

    assert forecast_values_read[0]._adjust_mw == 0
    assert forecast_values_read[1]._adjust_mw == 0
    assert forecast_values_read[2]._adjust_mw == 0
    assert forecast_values_read[3]._adjust_mw == 0
    assert forecast_values_read[4]._adjust_mw == 50
    assert forecast_values_read[5]._adjust_mw == 100


@freeze_time("2023-01-01 00:00:01")
def test_get_blend_forecast_values_latest_negative(db_session):
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

    assert forecast_values_read[0]._adjust_mw == 0
    assert forecast_values_read[2]._adjust_mw == 50
    assert forecast_values_read[3]._adjust_mw == 100


@freeze_time("2023-01-01 00:00:01")
def test_get_blend_forecast_values_latest_negative(db_session):
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
