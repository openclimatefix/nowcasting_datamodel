""" Method to update latest forecast values """
import logging
from datetime import datetime, timezone
from typing import List

from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models.models import ForecastSQL, ForecastValueLatestSQL
from nowcasting_datamodel.read.read import get_latest_forecast, get_model

logger = logging.getLogger(__name__)


def get_historic_forecast(session: Session, forecast: ForecastSQL) -> ForecastSQL:
    """
    Get historic forecast

    :param session:
    :param gsp_id:
    :return:
    """

    gsp_id = forecast.location.gsp_id

    forecast_historic = get_latest_forecast(session=session, gsp_id=gsp_id, historic=True)

    if forecast_historic is None:
        logger.debug("Could not find a historic forecast, so will make one")

        forecast_historic = ForecastSQL(
            historic=True,
            forecast_creation_time=datetime.now(timezone.utc),
            location=forecast.location,
            input_data_last_updated=forecast.input_data_last_updated,
            model=get_model(session=session, name="historic", version="all"),
        )
    else:
        logger.debug("Found historic forecast")

    return forecast_historic


def upsert(session: Session, model, rows: List[dict]):
    """

    Upsert rows into model

    Insert or Update rows into model.
    This functions checks the primary keys, and if duplicate updates them.

    :param session: sqlalchemy Session
    :param model: the model
    :param rows: the rows we are going to update
    """
    table = model.__table__
    stmt = insert(table)
    primary_keys = [key.name for key in inspect(table).primary_key]
    update_dict = {c.name: c for c in stmt.excluded if not c.primary_key}

    if not update_dict:
        raise ValueError("insert_or_update resulted in an empty update_dict")

    stmt = stmt.on_conflict_do_update(index_elements=primary_keys, set_=update_dict)
    session.execute(stmt, rows)


def update_forecast_latest(forecast: ForecastSQL, session: Session):
    """
    Update the forecast_values table

    1. Get the Forecast object that is for the latest,
        and don't load children
    2. create ForecastValueLatestSQL objects
    3. upsert them (update and or insert)

    :param forecast:
    :return:
    """

    # 1. get forecast object
    forecast_historic = get_historic_forecast(session=session, forecast=forecast)

    # 2. create forecast value latest
    forecast_values_dict = []
    for forecast_value in forecast.forecast_values:

        forecast_value_dict = {}
        for v in ["target_time", "expected_power_generation_megawatts"]:
            forecast_value_dict[v] = getattr(forecast_value, v)

        forecast_value_dict["gsp_id"] = forecast.location.gsp_id
        forecast_value_dict["forecast_id"] = forecast_historic.id

        forecast_values_dict.append(ForecastValueLatestSQL(**forecast_value_dict).__dict__)

    # upsert forecast values
    upsert(session=session, model=ForecastValueLatestSQL, rows=forecast_values_dict)

    # update forecast creation time
    forecast_historic.forecast_creation_time = datetime.now(tz=timezone.utc)
    session.commit()


def update_all_forecast_latest(forecasts: List[ForecastSQL], session: Session):
    """
    Update all latest forecasts
    """

    for forecast in forecasts:
        update_forecast_latest(forecast=forecast, session=session)
