import logging
from datetime import datetime, timezone

from nowcasting_datamodel.fake import (
    make_fake_forecasts,
    make_fake_national_forecast,
    make_fake_pv_system,
)
from nowcasting_datamodel.models import (
    Forecast,
    ForecastValue,
    LocationSQL,
    MLModel,
    PVSystem,
    PVSystemSQL,
    PVYield,
)
from nowcasting_datamodel.read import (
    get_all_gsp_ids_latest_forecast,
    get_forecast_values,
    get_latest_forecast,
    get_latest_national_forecast,
    get_model,
    get_pv_system,
)
from nowcasting_datamodel.read_pv import get_latest_pv_yield
from nowcasting_datamodel.save import save_pv_system

logger = logging.getLogger(__name__)


def test_get_model(db_session):

    model_read_1 = get_model(session=db_session, name="test_name", version="9.9.9")
    model_read_2 = get_model(session=db_session, name="test_name", version="9.9.9")

    assert model_read_1.name == model_read_2.name
    assert model_read_1.version == model_read_2.version

    _ = MLModel.from_orm(model_read_2)


def test_get_forecast(db_session, forecasts):

    forecast_read = get_latest_forecast(session=db_session)

    assert forecast_read.location.id == forecasts[-1].location.gsp_id
    assert forecast_read.forecast_values[0] == forecasts[-1].forecast_values[0]

    _ = Forecast.from_orm(forecast_read)


def test_read_gsp_id(db_session, forecasts):

    forecast_read = get_latest_forecast(session=db_session, gsp_id=forecasts[1].location.gsp_id)
    assert forecast_read.location.gsp_id == forecasts[1].location.gsp_id


def test_get_forecast_values(db_session, forecasts):

    forecast_values_read = get_forecast_values(session=db_session)
    assert len(forecast_values_read) == 20

    assert forecast_values_read[0] == forecasts[0].forecast_values[0]


def test_get_forecast_values_gsp_id(db_session, forecasts):

    forecast_values_read = get_forecast_values(
        session=db_session, gsp_id=forecasts[0].location.gsp_id
    )

    _ = ForecastValue.from_orm(forecast_values_read[0])

    assert len(forecast_values_read) == 2

    assert forecast_values_read[0] == forecasts[0].forecast_values[0]


def test_get_all_gsp_ids_latest_forecast(db_session):

    f1 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
    db_session.add_all(f1)

    assert len(db_session.query(LocationSQL).all()) == 2

    f2 = make_fake_forecasts(gsp_ids=[1, 2], session=db_session)
    db_session.add_all(f2)

    assert len(db_session.query(LocationSQL).all()) == 2

    forecast_values_read = get_all_gsp_ids_latest_forecast(session=db_session)
    print(forecast_values_read)
    assert len(forecast_values_read) == 2
    assert forecast_values_read[0] == f2[0]
    assert forecast_values_read[1] == f2[1]


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
