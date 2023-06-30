import logging
from datetime import datetime, timedelta, timezone

import pytest
from freezegun import freeze_time

from nowcasting_datamodel.fake import (
    N_FAKE_FORECASTS,
    make_fake_forecast,
    make_fake_forecasts,
    make_fake_national_forecast,
    make_fake_pv_system,
)
from nowcasting_datamodel.models import (
    InputDataLastUpdatedSQL,
    LocationSQL,
    MLModel,
    PVSystem,
    Status,
    national_gb_label,
)
from nowcasting_datamodel.models.forecast import (
    Forecast,
    ForecastValue,
    ForecastValueLatestSQL,
    ForecastValueSQL,
)
from nowcasting_datamodel.read.read import (
    get_all_gsp_ids_latest_forecast,
    get_all_locations,
    get_forecast_values,
    get_forecast_values_latest,
    get_latest_forecast,
    get_latest_forecast_created_utc,
    get_latest_input_data_last_updated,
    get_latest_national_forecast,
    get_latest_status,
    get_location,
    get_model,
    get_pv_system,
    update_latest_input_data_last_updated,
)
from nowcasting_datamodel.save.save import save_pv_system

logger = logging.getLogger(__name__)


def test_get_all_location(db_session):
    db_session.add(LocationSQL(label="GSP_1", gsp_id=1))
    db_session.add(LocationSQL(label="GSP_2", gsp_id=2))

    db_session.add(LocationSQL(label=national_gb_label, gsp_id=0))

    locations = get_all_locations(session=db_session, gsp_ids=[0, 1, 2])
    assert len(locations) == 3
    assert locations[0].label == national_gb_label


def test_get_national_location(db_session):
    db_session.add(LocationSQL(label=national_gb_label, gsp_id=0))

    location = get_location(session=db_session, gsp_id=0)
    assert location.label == national_gb_label

    locations = get_all_locations(session=db_session)
    assert len(locations) == 1
    assert locations[0].label == national_gb_label

    with pytest.raises(Exception):
        _ = get_location(session=db_session, gsp_id=0, label="test_label")


def test_get_model(db_session):
    model_read_1 = get_model(session=db_session, name="test_name", version="9.9.9")
    model_read_2 = get_model(session=db_session, name="test_name", version="9.9.9")

    assert model_read_1.name == model_read_2.name
    assert model_read_1.version == model_read_2.version

    _ = MLModel.from_orm(model_read_2)


def test_get_forecast(db_session, forecasts):
    forecast_read = get_latest_forecast(session=db_session)

    assert forecast_read.location.gsp_id == forecasts[-1].location.gsp_id
    assert forecast_read.forecast_values[0] == forecasts[-1].forecast_values[0]

    _ = Forecast.from_orm(forecast_read)


def test_read_gsp_id(db_session, forecasts):
    forecast_read = get_latest_forecast(session=db_session, gsp_id=forecasts[1].location.gsp_id)
    assert forecast_read.location.gsp_id == forecasts[1].location.gsp_id


@freeze_time("2024-01-01")
def test_read_target_time(db_session):
    f1 = ForecastValueLatestSQL(
        target_time=datetime(2024, 1, 1), expected_power_generation_megawatts=1, gsp_id=1
    )
    f2 = ForecastValueLatestSQL(
        target_time=datetime(2024, 1, 1, 1), expected_power_generation_megawatts=3, gsp_id=1
    )
    f3 = ForecastValueLatestSQL(
        target_time=datetime(2024, 1, 1, 0, 30), expected_power_generation_megawatts=2, gsp_id=1
    )
    f = make_fake_forecast(
        gsp_id=1, session=db_session, forecast_values_latest=[f1, f2, f3], historic=True
    )
    db_session.add(f)

    forecast_read = get_latest_forecast(
        session=db_session,
        gsp_id=f.location.gsp_id,
        historic=True,
        start_target_time=datetime(2024, 1, 1, 0, 30),
    )
    assert forecast_read is not None
    assert forecast_read.location.gsp_id == f.location.gsp_id

    assert len(forecast_read.forecast_values_latest) == 2
    assert forecast_read.forecast_values_latest[-1].expected_power_generation_megawatts == 3


def test_get_forecast_values(db_session, forecasts):
    forecast_values_read = get_forecast_values(session=db_session)
    assert len(forecast_values_read) == 10 * N_FAKE_FORECASTS

    # we have order so that the most recent forecast gets returned first
    assert forecast_values_read[0] == forecasts[-1].forecast_values[0]


def test_get_forecast_values_gsp_id(db_session, forecasts):
    forecast_values_read = get_forecast_values(
        session=db_session, gsp_id=forecasts[0].location.gsp_id
    )

    _ = ForecastValue.from_orm(forecast_values_read[0])

    assert len(forecast_values_read) == N_FAKE_FORECASTS

    assert forecast_values_read[0] == forecasts[0].forecast_values[0]


def test_get_forecast_values_latest_gsp_id(db_session):
    f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
    f1[0].historic = True
    f1[0].forecast_values_latest = [
        ForecastValueLatestSQL(
            gsp_id=1,
            expected_power_generation_megawatts=1,
            target_time=datetime(2023, 1, 1, tzinfo=timezone.utc),
        ),
        ForecastValueLatestSQL(
            gsp_id=1,
            expected_power_generation_megawatts=1,
            target_time=datetime(2023, 1, 1, 0, 30, tzinfo=timezone.utc),
        ),
    ]
    db_session.add_all(f1)
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 2

    forecast_values_read = get_forecast_values_latest(
        session=db_session, gsp_id=f1[0].location.gsp_id
    )
    _ = ForecastValue.from_orm(forecast_values_read[0])

    assert len(forecast_values_read) == 2
    assert forecast_values_read[0].gsp_id == f1[0].location.gsp_id
    assert forecast_values_read[0].target_time == f1[0].forecast_values_latest[0].target_time
    assert forecast_values_read[0] == f1[0].forecast_values_latest[0]


def test_get_latest_status(db_session):
    s1 = Status(message="Good", status="ok")
    s1 = s1.to_orm()
    s2 = Status(message="Bad", status="warning").to_orm()
    db_session.add_all([s1])
    db_session.commit()

    db_session.add_all([s2])
    db_session.commit()

    status = get_latest_status(db_session)
    assert status.status == "warning"
    assert status.message == "Bad"


def test_get_latest_forecast_created_utc_gsp(db_session):
    f1 = make_fake_forecast(gsp_id=1, session=db_session)
    f2 = make_fake_forecast(gsp_id=1, session=db_session)
    f3 = make_fake_forecast(gsp_id=2, session=db_session)

    db_session.add_all([f1, f2, f3])
    db_session.commit()

    created_utc = get_latest_forecast_created_utc(session=db_session, gsp_id=1)
    assert created_utc == f2.created_utc


def test_get_latest_forecast_created_utc_national(db_session):
    f1 = make_fake_national_forecast(session=db_session)
    f2 = make_fake_national_forecast(session=db_session)

    db_session.add_all([f1, f2])
    db_session.commit()

    created_utc = get_latest_forecast_created_utc(session=db_session, gsp_id=0)
    assert created_utc == f2.created_utc


@freeze_time("2024-01-01")
def test_get_forecast_values_gsp_id_latest(db_session):
    forecast_1 = make_fake_forecast(
        gsp_id=1, session=db_session, t0_datetime_utc=datetime(2024, 1, 1, tzinfo=timezone.utc)
    )
    forecast_2 = make_fake_forecast(
        gsp_id=1, session=db_session, t0_datetime_utc=datetime(2024, 1, 2, tzinfo=timezone.utc)
    )
    db_session.add_all([forecast_1, forecast_2])

    forecast_values_read = get_forecast_values(
        session=db_session,
        gsp_id=1,
        only_return_latest=True,
        start_datetime=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )

    _ = ForecastValue.from_orm(forecast_values_read[0])

    assert len(forecast_values_read) == 16  # only getting forecast ahead

    assert forecast_values_read[0].target_time == forecast_2.forecast_values[96].target_time


def test_get_all_gsp_ids_latest_forecast(db_session):
    f1 = make_fake_forecasts(gsp_ids=[0, 1], session=db_session)
    db_session.add_all(f1)

    assert len(db_session.query(LocationSQL).all()) == 2

    f2 = make_fake_forecasts(gsp_ids=[0, 1], session=db_session)
    db_session.add_all(f2)

    assert len(db_session.query(LocationSQL).all()) == 2

    forecast_values_read = get_all_gsp_ids_latest_forecast(session=db_session)
    assert len(forecast_values_read) == 2
    assert forecast_values_read[0] == f2[0]
    assert forecast_values_read[1] == f2[1]


def test_get_all_gsp_ids_latest_forecast_not_national(db_session):
    f1 = make_fake_forecasts(gsp_ids=[0, 1], session=db_session)
    db_session.add_all(f1)

    f2 = make_fake_forecasts(gsp_ids=[0, 1], session=db_session)
    db_session.add_all(f2)

    forecast_values_read = get_all_gsp_ids_latest_forecast(
        session=db_session, include_national=False
    )
    assert len(forecast_values_read) == 1
    assert forecast_values_read[0] == f2[1]


def test_get_all_gsp_ids_latest_forecast_historic(db_session):
    f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
    f1[0].historic = True
    f1[0].forecast_values_latest = [
        ForecastValueLatestSQL(
            gsp_id=1, expected_power_generation_megawatts=1, target_time=datetime(2024, 1, 1)
        )
    ]
    db_session.add_all(f1)

    forecast_values_read = get_all_gsp_ids_latest_forecast(session=db_session, historic=True)
    assert len(forecast_values_read) == 1
    assert forecast_values_read[0] == f1[0]


def test_get_all_gsp_ids_latest_forecast_pre_load(db_session):
    f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)

    forecasts = get_all_gsp_ids_latest_forecast(session=db_session, preload_children=True)
    assert len(forecasts) == 2
    assert forecasts[0] == f1[0]
    assert forecasts[1] == f1[1]
    assert len(forecasts[0].forecast_values) == N_FAKE_FORECASTS


@freeze_time("2023-01-01")
def test_get_all_gsp_ids_latest_forecast_filter(db_session):
    f1 = make_fake_forecasts(
        gsp_ids=[1, 2], session=db_session, t0_datetime_utc=datetime(2023, 1, 1)
    )
    db_session.add_all(f1)
    db_session.add_all(f1[0].forecast_values)
    db_session.commit()
    assert len(f1[0].forecast_values) == N_FAKE_FORECASTS

    start_created_utc = datetime.now() - timedelta(days=1)
    target_time = datetime(2020, 1, 1) - timedelta(days=1)
    assert len(f1[0].forecast_values) == N_FAKE_FORECASTS
    forecast_read = get_all_gsp_ids_latest_forecast(
        session=db_session, start_created_utc=start_created_utc, start_target_time=target_time
    )
    assert len(db_session.query(ForecastValueSQL).all()) == 2 * N_FAKE_FORECASTS
    assert len(forecast_read) == 2
    assert forecast_read[0] == f1[0]
    assert forecast_read[1] == f1[1]
    assert len(f1[0].forecast_values) == N_FAKE_FORECASTS
    assert len(forecast_read[0].forecast_values) == N_FAKE_FORECASTS


def test_get_all_gsp_ids_latest_forecast_filter_historic(db_session):
    """Test get all historic forecast from all gsp but filter on starttime"""

    f1 = make_fake_forecasts(
        gsp_ids=[1, 2], session=db_session, t0_datetime_utc=datetime(2022, 9, 1)
    )
    f1[0].historic = True
    f1[0].forecast_values_latest = [
        ForecastValueLatestSQL(
            gsp_id=1, expected_power_generation_megawatts=1, target_time=datetime(2023, 1, 1)
        ),
        ForecastValueLatestSQL(
            gsp_id=1, expected_power_generation_megawatts=3, target_time=datetime(2023, 1, 1, 1)
        ),
        ForecastValueLatestSQL(
            gsp_id=1, expected_power_generation_megawatts=2, target_time=datetime(2023, 1, 1, 0, 30)
        ),
    ]

    db_session.add_all(f1)

    target_time = datetime(2023, 1, 1, 0, 30)
    forecast = get_all_gsp_ids_latest_forecast(
        session=db_session, start_target_time=target_time, historic=True
    )[0]
    assert len(forecast.forecast_values_latest) == 2
    assert forecast.forecast_values_latest[-1].expected_power_generation_megawatts == 3


def test_get_national_latest_forecast(db_session):
    f1 = make_fake_national_forecast(session=db_session)
    db_session.add(f1)

    f2 = make_fake_national_forecast(session=db_session)
    db_session.add(f2)
    forecast_values_read = get_latest_national_forecast(session=db_session)
    assert forecast_values_read == f2


def test_get_pv_system(db_session_pv):
    pv_system = PVSystem.from_orm(make_fake_pv_system())
    save_pv_system(session=db_session_pv, pv_system=pv_system)

    pv_system_get = get_pv_system(
        session=db_session_pv, provider=pv_system.provider, pv_system_id=pv_system.pv_system_id
    )
    # this get defaulted to True when adding to the database
    pv_system.correct_data = True
    assert PVSystem.from_orm(pv_system) == PVSystem.from_orm(pv_system_get)


def test_get_latest_input_data_last_updated_multiple_entries(db_session):
    now = datetime.now(tz=None)
    yesterday = now - timedelta(hours=24)

    input_data_last_updated_1 = InputDataLastUpdatedSQL(
        gsp=yesterday, nwp=yesterday, pv=yesterday, satellite=yesterday
    )
    input_data_last_updated_2 = InputDataLastUpdatedSQL(gsp=now, nwp=now, pv=now, satellite=now)

    db_session.add_all([input_data_last_updated_1, input_data_last_updated_2])
    db_session.commit()

    input_data_last_updated = get_latest_input_data_last_updated(session=db_session)
    assert input_data_last_updated.gsp.replace(tzinfo=None) == now
    assert input_data_last_updated.pv.replace(tzinfo=None) == now


def test_update_latest_input_data_last_updated(db_session):
    now = datetime.now(tz=None)
    yesterday = now - timedelta(hours=24)

    input_data_last_updated_1 = InputDataLastUpdatedSQL(
        gsp=yesterday, nwp=yesterday, pv=yesterday, satellite=yesterday
    )
    db_session.add(input_data_last_updated_1)
    db_session.commit()

    update_latest_input_data_last_updated(session=db_session, component="gsp", update_datetime=now)

    input_data_last_updated = get_latest_input_data_last_updated(session=db_session)
    assert input_data_last_updated.gsp.replace(tzinfo=None) == now
    assert input_data_last_updated.pv.replace(tzinfo=None) == yesterday


@freeze_time("2022-01-01")
def test_update_latest_input_data_last_updated_freeze(db_session):
    yesterday = datetime.now(tz=timezone.utc) - timedelta(hours=24)
    now = datetime.now(tz=timezone.utc)

    input_data_last_updated_1 = InputDataLastUpdatedSQL(
        gsp=yesterday, nwp=yesterday, pv=yesterday, satellite=yesterday
    )
    db_session.add(input_data_last_updated_1)
    db_session.commit()

    update_latest_input_data_last_updated(session=db_session, component="pv")

    input_data_last_updated = get_latest_input_data_last_updated(session=db_session)
    assert input_data_last_updated.pv.replace(tzinfo=None) == now.replace(tzinfo=None)
    assert input_data_last_updated.gsp.replace(tzinfo=None) == yesterday.replace(tzinfo=None)


@freeze_time("2022-01-01")
def test_update_latest_input_data_last_updated_freeze_no_data(db_session):
    yesterday = datetime.now(tz=timezone.utc) - timedelta(hours=24)
    now = datetime.now(tz=timezone.utc)

    update_latest_input_data_last_updated(session=db_session, component="pv")

    input_data_last_updated = get_latest_input_data_last_updated(session=db_session)
    assert input_data_last_updated.pv.replace(tzinfo=None) == now.replace(tzinfo=None)
    assert input_data_last_updated.gsp.replace(tzinfo=None) == datetime(1960, 1, 1)
