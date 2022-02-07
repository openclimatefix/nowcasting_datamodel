import logging

from nowcasting_datamodel.fake import make_fake_forecasts, make_fake_national_forecast
from nowcasting_datamodel.models import Forecast, ForecastValue, LocationSQL
from nowcasting_datamodel.read import (
    get_all_gsp_ids_latest_forecast,
    get_forecast_values,
    get_latest_forecast,
    get_latest_national_forecast,
)

logger = logging.getLogger(__name__)


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
