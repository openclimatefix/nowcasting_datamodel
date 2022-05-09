import logging
from datetime import datetime, timedelta, timezone

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
    InputDataLastUpdatedSQL,
    LocationSQL,
    MLModel,
    PVSystem,
    national_gb_label,
)
from nowcasting_datamodel.read.read import (
    get_all_gsp_ids_latest_forecast,
    get_all_locations,
    get_forecast_values,
    get_latest_forecast,
    get_latest_forecast_created_utc,
    get_latest_input_data_last_updated,
    get_latest_national_forecast,
    get_location,
    get_model,
    get_pv_system,
    update_latest_input_data_last_updated,
)
from nowcasting_datamodel.save import save_pv_system

logger = logging.getLogger(__name__)


def test_get_all_location(db_session):

    db_session.add(LocationSQL(label="GSP_1", gsp_id=1))
    db_session.add(LocationSQL(label="GSP_2", gsp_id=2))
    db_session.add(LocationSQL(label="fake_location", gsp_id=None))

    db_session.add(LocationSQL(label="fake_national", gsp_id=0))
    db_session.add(LocationSQL(label=national_gb_label, gsp_id=0))
    db_session.add(LocationSQL(label="fake_national", gsp_id=0))

    locations = get_all_locations(session=db_session, gsp_ids=[0, 1, 2])
    assert len(locations) == 3
    assert locations[0].label == national_gb_label


def test_get_national_location(db_session):

    db_session.add(LocationSQL(label=national_gb_label, gsp_id=0))
    db_session.add(LocationSQL(label="not_national", gsp_id=0))

    location = get_location(session=db_session, gsp_id=0)
    assert location.label == national_gb_label

    locations = get_all_locations(session=db_session)
    assert len(locations) == 1
    assert locations[0].label == national_gb_label

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


def test_get_forecast_values(db_session, forecasts):

    forecast_values_read = get_forecast_values(session=db_session)
    assert len(forecast_values_read) == 20

    # we have order so that the most recent forecast gets returned first
    assert forecast_values_read[0] == forecasts[-1].forecast_values[0]


def test_get_forecast_values_gsp_id(db_session, forecasts):

    forecast_values_read = get_forecast_values(
        session=db_session, gsp_id=forecasts[0].location.gsp_id
    )

    _ = ForecastValue.from_orm(forecast_values_read[0])

    assert len(forecast_values_read) == 2

    assert forecast_values_read[0] == forecasts[0].forecast_values[0]


def test_get_latest_forecast_created_utc_gsp(db_session):
    f1 = make_fake_forecast(gsp_id=1, session=db_session)
    f2 = make_fake_forecast(gsp_id=1, session=db_session)
    f3 = make_fake_forecast(gsp_id=2, session=db_session)

    db_session.add_all([f1, f2, f3])
    db_session.commit()

    created_utc = get_latest_forecast_created_utc(session=db_session, gsp_id=1)
    assert created_utc == f2.created_utc


def test_get_latest_forecast_created_utc_national(db_session):
    f1 = make_fake_national_forecast()
    f2 = make_fake_national_forecast()

    db_session.add_all([f2, f1])
    db_session.commit()

    created_utc = get_latest_forecast_created_utc(session=db_session, gsp_id=0)
    assert created_utc == f1.created_utc


def test_get_forecast_values_gsp_id_latest(db_session):
    _ = make_fake_forecast(gsp_id=1, session=db_session, t0_datetime_utc=datetime(2022, 1, 1))
    forecast_2 = make_fake_forecast(
        gsp_id=1, session=db_session, t0_datetime_utc=datetime(2022, 1, 2)
    )

    forecast_values_read = get_forecast_values(
        session=db_session, gsp_id=1, only_return_latest=True, start_datetime=datetime(2022, 1, 2)
    )

    _ = ForecastValue.from_orm(forecast_values_read[0])

    assert len(forecast_values_read) == 2

    assert forecast_values_read[0] == forecast_2.forecast_values[0]


def test_get_all_gsp_ids_latest_forecast(db_session):

    f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
    db_session.add_all(f1)

    assert len(db_session.query(LocationSQL).all()) == 2

    f2 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
    db_session.add_all(f2)

    assert len(db_session.query(LocationSQL).all()) == 2

    forecast_values_read = get_all_gsp_ids_latest_forecast(session=db_session)
    assert len(forecast_values_read) == 2
    assert forecast_values_read[0] == f2[0]
    assert forecast_values_read[1] == f2[1]


def test_get_all_gsp_ids_latest_forecast_filter(db_session):

    f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
    db_session.add_all(f1)

    start_created_utc = datetime.now() - timedelta(days=1)
    forecast_values_read = get_all_gsp_ids_latest_forecast(
        session=db_session, start_created_utc=start_created_utc
    )
    assert len(forecast_values_read) == 2
    assert forecast_values_read[0] == f1[0]
    assert forecast_values_read[1] == f1[1]


def test_get_national_latest_forecast(db_session):

    f1 = make_fake_national_forecast()
    db_session.add(f1)

    f2 = make_fake_national_forecast()
    db_session.add(f2)
    forecast_values_read = get_latest_national_forecast(session=db_session)
    assert forecast_values_read == f2


def test_get_pv_system(db_session_pv):

    pv_system = PVSystem.from_orm(make_fake_pv_system())
    save_pv_system(session=db_session_pv, pv_system=pv_system)

    pv_system_get = get_pv_system(
        session=db_session_pv, provider=pv_system.provider, pv_system_id=pv_system.pv_system_id
    )
    assert PVSystem.from_orm(pv_system) == PVSystem.from_orm(pv_system_get)


def test_get_latest_input_data_last_updated(db_session):

    yesterday = datetime.now(tz=timezone.utc) - timedelta(hours=24)
    now = datetime.now(tz=timezone.utc)

    input_data_last_updated_1 = InputDataLastUpdatedSQL(
        gsp=yesterday, nwp=yesterday, pv=yesterday, satellite=yesterday
    )
    input_data_last_updated_2 = InputDataLastUpdatedSQL(gsp=now, nwp=now, pv=now, satellite=now)

    db_session.add_all([input_data_last_updated_1, input_data_last_updated_2])
    db_session.commit()

    input_data_last_updated = get_latest_input_data_last_updated(session=db_session)
    assert input_data_last_updated.gsp.replace(tzinfo=None) == now.replace(tzinfo=None)


def test_update_latest_input_data_last_updated(db_session):

    yesterday = datetime.now(tz=timezone.utc) - timedelta(hours=24)
    now = datetime.now(tz=timezone.utc)

    input_data_last_updated_1 = InputDataLastUpdatedSQL(
        gsp=yesterday, nwp=yesterday, pv=yesterday, satellite=yesterday
    )
    db_session.add(input_data_last_updated_1)
    db_session.commit()

    update_latest_input_data_last_updated(session=db_session, component="gsp", update_datetime=now)

    input_data_last_updated = get_latest_input_data_last_updated(session=db_session)
    assert input_data_last_updated.gsp.replace(tzinfo=None) == now.replace(tzinfo=None)
    assert input_data_last_updated.pv.replace(tzinfo=None) == yesterday.replace(tzinfo=None)


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
