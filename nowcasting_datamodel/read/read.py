""" Read database functions

1. Get the latest forecast
2. get the latest forecasts for all gsp ids
3. get all forecast values
"""
import logging
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import desc
from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models import (
    ForecastSQL,
    ForecastValueSQL,
    InputDataLastUpdatedSQL,
    LocationSQL,
    MLModelSQL,
    PVSystemSQL,
    national_gb_label,
)

logger = logging.getLogger(__name__)


def get_latest_input_data_last_updated(
    session: Session,
) -> InputDataLastUpdatedSQL:
    """
    Read last input data last updated

    :param session: database session

    return: Latest input data object
    """

    # start main query
    query = session.query(InputDataLastUpdatedSQL)

    # this make the newest ones comes to the top
    query = query.order_by(InputDataLastUpdatedSQL.created_utc.desc())

    # get all results
    input_data = query.first()

    logger.debug("Found latest input data")

    return input_data


def update_latest_input_data_last_updated(
    session: Session, component: str, update_datetime: Optional[datetime] = None
):
    """
    Update the table InputDataLastUpdatedSQL with a new valye

    :param session:
    :param component: This should be gsp, pv, nwp or satellite
    :param update_datetime: the datetime is should be updated.
        Default is None, so will be set to now
    :return:
    """

    # For the moment we load the latest, update it and save a new one.
    # This may be a problem if these function is run at the same twice,
    # but for the moment lets ignore this

    if update_datetime is None:
        update_datetime = datetime.now(tz=timezone.utc)

    # get latest
    latest_input_data_last_updated = get_latest_input_data_last_updated(session=session)

    # set new value
    setattr(latest_input_data_last_updated, component, update_datetime)

    # save to database
    session.add(latest_input_data_last_updated)
    session.commit()


def get_latest_forecast(
    session: Session,
    gsp_id: Optional[int] = None,
) -> ForecastSQL:
    """
    Read forecasts

    :param session: database session
    :param gsp_id: optional to gsp id, to filter query on
        If None is given then all are returned.
    :param session: database session

    return: List of forecasts objects from database
    """

    # start main query
    query = session.query(ForecastSQL)
    order_by_items = []

    # filter on gsp_id
    if gsp_id is not None:
        query = query.join(LocationSQL)
        query = query.filter(LocationSQL.gsp_id == gsp_id)
        order_by_items.append(LocationSQL.gsp_id)

    order_by_items.append(ForecastSQL.created_utc.desc())

    # this make the newest ones comes to the top
    query = query.order_by(*order_by_items)

    # get all results
    forecasts = query.first()

    logger.debug(f"Found forecasts for gsp id: {gsp_id}")

    return forecasts


def get_all_gsp_ids_latest_forecast(
    session: Session,
) -> List[ForecastSQL]:
    """
    Read forecasts

    :param session: database session

    return: List of forecasts objects from database
    """

    # start main query
    query = session.query(ForecastSQL)
    query = query.distinct(LocationSQL.gsp_id)
    query = query.join(LocationSQL)
    query = query.order_by(LocationSQL.gsp_id, desc(ForecastSQL.created_utc))

    forecasts = query.all()

    return forecasts


def get_forecast_values(
    session: Session,
    gsp_id: Optional[int] = None,
) -> List[ForecastValueSQL]:
    """
    Get forecast values

    :param session: database session
    :param gsp_id: optional to gsp id, to filter query on
        If None is given then all are returned.

    return: List of forecasts values objects from database

    """

    # start main query
    query = session.query(ForecastValueSQL)

    # filter on gsp_id
    if gsp_id is not None:
        query = query.join(ForecastSQL)
        query = query.join(LocationSQL)
        query = query.filter(LocationSQL.gsp_id == gsp_id)

    # get all results
    forecasts = query.all()

    return forecasts


def get_latest_national_forecast(
    session: Session,
) -> ForecastSQL:
    """
    Get forecast values

    :param session: database session
    :param gsp_id: optional to gsp id, to filter query on
        If None is given then all are returned.

    return: List of forecasts values objects from database

    """

    # start main query
    query = session.query(ForecastSQL)

    # filter on gsp_id
    query = query.join(LocationSQL)
    query = query.filter(LocationSQL.label == national_gb_label)

    # order, so latest is at the top
    query = query.order_by(ForecastSQL.created_utc.desc())

    # get first results
    forecast = query.first()

    return forecast


def get_location(session: Session, gsp_id: int, label: Optional[str] = None) -> LocationSQL:
    """
    Get location object from gsp id

    :param session: database session
    :param gsp_id: gsp id of the location
    :param label: label of the location

    return: List of forecasts values objects from database

    """

    # start main query
    query = session.query(LocationSQL)

    # filter on gsp_id
    query = query.filter(LocationSQL.gsp_id == gsp_id)

    if label is not None:
        query = query.filter(LocationSQL.label == label)

    # get all results
    locations = query.all()

    if len(locations) == 0:
        logger.debug(f"Location for gsp_id {gsp_id} does not exist so going to add it")

        if label is None:
            label = f"GSP_{gsp_id}"

        location = LocationSQL(gsp_id=gsp_id, label=label)
        if gsp_id == 0:
            location.label = national_gb_label
        session.add(location)
        session.commit()

    else:
        location = locations[0]

    return location


def get_all_locations(session: Session, gsp_ids: List[int] = None) -> List[LocationSQL]:
    """
    Get all location object from gsp id

    :param session: database session
    :param gsp_ids: list of gsp id of the location

    return: List of GSP locations

    """

    # start main query
    query = session.query(LocationSQL)
    query = query.distinct(LocationSQL.gsp_id)

    # filter on gsp_id
    if gsp_ids is not None:
        query = query.filter(LocationSQL.gsp_id.in_(gsp_ids))

    query = query.order_by(LocationSQL.gsp_id)

    # get all results
    locations = query.all()

    return locations


def get_model(session: Session, name: str, version: str) -> MLModelSQL:
    """
    Get model object from name and version

    :param session: database session
    :param name: name of the model
    :param version: version of the model

    return: Model object

    """

    # start main query
    query = session.query(MLModelSQL)

    # filter on gsp_id
    query = query.filter(MLModelSQL.name == name)
    query = query.filter(MLModelSQL.version == version)

    # get all results
    models = query.all()

    if len(models) == 0:
        logger.debug(
            f"Model for name {name} and version {version }does not exist so going to add it"
        )

        model = MLModelSQL(name=name, version=version)
        session.add(model)
        session.commit()

    else:
        model = models[0]

    return model


def get_pv_system(
    session: Session, pv_system_id: int, provider: Optional[str] = "pvoutput.org"
) -> PVSystemSQL:
    """
    Get model object from name and version

    :param session: database session
    :param pv_system_id: pv system id
    :param provider: the pv provider, defaulted to pvoutput.org

    return: PVSystem object
    """

    # start main query
    query = session.query(PVSystemSQL)

    # filter on pv_system_id and provider
    query = query.filter(PVSystemSQL.pv_system_id == pv_system_id)
    query = query.filter(PVSystemSQL.provider == provider)

    # get all results
    pv_systems = query.all()

    pv_system = pv_systems[0]

    return pv_system
