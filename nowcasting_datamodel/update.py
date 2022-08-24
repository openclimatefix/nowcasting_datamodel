""" Method to update latest forecast values """
import logging
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm.session import Session

from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.models.models import ForecastSQL, ForecastValueLatestSQL
from nowcasting_datamodel.read.read import (
    get_latest_forecast,
    get_latest_forecast_for_gsps,
    get_model,
)

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


def update_forecast_latest(
    forecast: ForecastSQL, session: Session, forecast_historic: Optional[ForecastSQL] = None
):
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
    if forecast_historic is None:
        forecast_historic = get_historic_forecast(session=session, forecast=forecast)

    # 2. create forecast value latest
    forecast_values = []
    for forecast_value in forecast.forecast_values:

        forecast_value_dict = {}
        for v in ["target_time", "expected_power_generation_megawatts"]:
            forecast_value_dict[v] = getattr(forecast_value, v)

        forecast_value_dict["gsp_id"] = forecast.location.gsp_id
        forecast_value_dict["forecast_id"] = forecast_historic.id

        forecast_values.append(ForecastValueLatestSQL(**forecast_value_dict).__dict__)

    # upsert forecast values
    upsert(session=session, model=ForecastValueLatestSQL, rows=forecast_values)

    # update forecast creation time
    forecast_historic.forecast_creation_time = datetime.now(tz=timezone.utc)
    session.commit()


def update_all_forecast_latest(
    forecasts: List[ForecastSQL],
    session: Session,
    update_national: Optional[bool] = True,
    update_gsp: Optional[bool] = True,
):
    """
    Update all latest forecasts

    :param forecasts: The forecasts that should be updated that should be used to update
        the latest table
    :param session: sqlalmacy session
    :param update_national: Optional (default true), to update the national forecast
    :param update_gsp: Optional (default true), to update all the GSP forecasts
    """

    gsp_ids = get_gsp_ids(include_national=update_national, include_gsps=update_gsp)
    if len(gsp_ids) == 0:
        logger.warning(f"Will not be updating any GSPs as {update_national=} and {update_gsp=}")
        return
    logger.debug(f"Will be update GSPs from {min(gsp_ids)} to {max(gsp_ids)}")

    # get all latest forecasts
    logger.debug("Getting all latest forecasts")
    forecasts_historic_all_gsps = get_latest_forecast_for_gsps(
        session=session, historic=True, preload_children=True, gsp_ids=gsp_ids
    )
    # get all these ids, so we only have to load it once
    historic_gsp_ids = [forecast.location.gsp_id for forecast in forecasts_historic_all_gsps]
    logger.debug(f"Found {len(forecasts_historic_all_gsps)} historic forecasts")

    logger.debug(f"There are {len(forecasts)} forecasts that we will update")

    for forecast in forecasts:

        # chose the correct forecast historic
        logger.debug("Getting gsp")
        gsp_id = forecast.location.gsp_id
        if gsp_id not in gsp_ids:
            logger.debug(
                f"GSP id {gsp_id} will not be updated, "
                f"this is due to the options {update_national=} and {update_gsp=}"
            )
            continue

        logger.debug(f"Updating forecast for gsp_id {gsp_id}")

        forecast_historic = [
            forecast_historic_one_gsps
            for forecast_historic_one_gsps, historic_gsp_id in zip(
                forecasts_historic_all_gsps, historic_gsp_ids
            )
            if historic_gsp_id == gsp_id
        ]
        if len(forecast_historic) == 0:
            forecast_historic = None
            logger.debug(f"Could not find historic, so will be creating one (GSP id{gsp_id})")
        else:
            forecast_historic = forecast_historic[0]

            logger.debug(f"Found historic for GSP id {gsp_id}")

        update_forecast_latest(
            forecast=forecast, session=session, forecast_historic=forecast_historic
        )
        session.commit()


def get_gsp_ids(include_national: bool = True, include_gsps: bool = True) -> List[int]:
    """
    Get list of gsps ids

    :param include_national: option to incldue gsp_id of 0
    :param include_gsps: option to include gsp_ids 1 to N_GSPS
    :return: list of gsps
    """

    gsp_ids = []
    if include_national:
        gsp_ids.append(0)
    if include_gsps:
        gsp_ids = gsp_ids + list(range(1, N_GSP + 1))
    return gsp_ids
