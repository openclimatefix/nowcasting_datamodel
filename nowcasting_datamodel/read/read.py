""" Read database functions

1. Get the latest forecast
2. get the latest forecasts for all gsp ids
3. get all forecast values
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from sqlalchemy import desc
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import false, true

from nowcasting_datamodel.models import (
    ForecastSQL,
    ForecastValueLatestSQL,
    ForecastValueSQL,
    InputDataLastUpdatedSQL,
    LocationSQL,
    MLModelSQL,
    PVSystemSQL,
    StatusSQL,
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


def get_latest_status(
    session: Session,
) -> StatusSQL:
    """
    Read last input data last updated

    :param session: database session

    return: Latest input data object
    """

    # start main query
    query = session.query(StatusSQL)

    # this make the newest ones comes to the top
    query = query.order_by(StatusSQL.created_utc.desc())

    # get all results
    input_data = query.first()

    logger.debug("Found latest status")

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

    if latest_input_data_last_updated is None:
        logger.warning("Could not find any 'InputDataLastUpdatedSQL' so will create one.")
        now = datetime(1960, 1, 1, tzinfo=timezone.utc)
        latest_input_data_last_updated = InputDataLastUpdatedSQL(
            gsp=now, nwp=now, pv=now, satellite=now
        )

    # set new value
    setattr(latest_input_data_last_updated, component, update_datetime)

    # save to database
    session.add(latest_input_data_last_updated)
    session.commit()


def get_latest_forecast(
    session: Session,
    gsp_id: Optional[int] = None,
    historic: bool = False,
    start_target_time: Optional[datetime] = None,
) -> ForecastSQL:
    """
    Read forecasts

    :param session: database session
    :param gsp_id: optional to gsp id, to filter query on
        If None is given then all are returned.
    :param session: database session
    :param historic: Option to load historic values or not
    :param start_target_time:
        Filter: forecast values target time should be larger than this datetime

    return: List of forecasts objects from database
    """

    # start main query
    query = session.query(ForecastSQL)
    order_by_items = []

    if historic:
        query = query.filter(ForecastSQL.historic == true())
    else:
        query = query.filter(ForecastSQL.historic == false())

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

    # filter on target time
    if start_target_time is not None:

        # get the correct forecast value table
        if historic:
            data_model_forecast_value = ForecastValueLatestSQL
        else:
            data_model_forecast_value = ForecastValueSQL

        forecast_values = session.query(data_model_forecast_value).\
            filter(data_model_forecast_value.target_time >= start_target_time).\
            filter(forecasts.id == data_model_forecast_value.forecast_id). \
            order_by(data_model_forecast_value.target_time).\
            all()

        forecasts.forecast_values_latest = forecast_values

    # sort list
    if forecasts is not None:
        if forecasts.forecast_values_latest is not None:
            forecasts.forecast_values_latest = sorted(
                forecasts.forecast_values_latest, key=lambda d: d.target_time
            )

    logger.debug(f"Found forecasts for gsp id: {gsp_id} {historic=} {forecasts=}")

    return forecasts


def get_all_gsp_ids_latest_forecast(
    session: Session,
    start_created_utc: Optional[datetime] = None,
    start_target_time: Optional[datetime] = None,
    preload_children: Optional[bool] = False,
    historic: bool = False,
) -> List[ForecastSQL]:
    """
    Read forecasts

    :param session: database session
    :param start_created_utc: Filter: forecast creation time should be larger than this datetime
    :param start_target_time:
        Filter: forecast values target time should be larger than this datetime
    :param preload_children: Option to preload children. This is a speed up, if we need them.
    :param historic: Option to load historic values or not

    return: List of forecasts objects from database
    """

    if historic:
        forecast_value_model = ForecastValueLatestSQL
    else:
        forecast_value_model = ForecastValueSQL

    # start main query
    query = session.query(ForecastSQL)
    query = query.distinct(LocationSQL.gsp_id)
    query = query.join(LocationSQL)
    query = query.join(forecast_value_model)

    query = query.filter(ForecastSQL.historic == historic)

    if start_created_utc is not None:
        query = query.filter(ForecastSQL.created_utc > start_created_utc)

    if start_target_time is not None:
        query = query.filter(forecast_value_model.target_time > start_target_time)

    query = query.order_by(LocationSQL.gsp_id, desc(ForecastSQL.created_utc))

    if preload_children:
        query = query.options(joinedload(ForecastSQL.forecast_values_latest))
        query = query.options(joinedload(ForecastSQL.forecast_values))
        query = query.options(joinedload(ForecastSQL.location))
        query = query.options(joinedload(ForecastSQL.model))
        query = query.options(joinedload(ForecastSQL.input_data_last_updated))

    forecasts = query.all()

    return forecasts


def get_forecast_values(
    session: Session,
    gsp_id: Optional[int] = None,
    start_datetime: Optional[datetime] = None,
    only_return_latest: Optional[bool] = False,
) -> List[ForecastValueSQL]:
    """
    Get forecast values

    :param session: database session
    :param gsp_id: optional to gsp id, to filter query on
        If None is given then all are returned.
    :param start_datetime: optional to filterer target_time by start_datetime
        If None is given then all are returned.
    :param only_return_latest: Optional to only return the latest forecast, not all of them.
        Default is False

    return: List of forecasts values objects from database

    """

    # start main query
    query = session.query(ForecastValueSQL)

    if only_return_latest:
        query = query.distinct(ForecastValueSQL.target_time)

    if start_datetime is not None:
        query = query.filter(ForecastValueSQL.target_time >= start_datetime)

        # also filter on creation time, to speed up things
        created_utc_filter = start_datetime - timedelta(days=1)
        query = query.filter(ForecastValueSQL.created_utc >= created_utc_filter)
        query = query.filter(ForecastSQL.created_utc >= created_utc_filter)

    # filter on gsp_id
    if gsp_id is not None:
        query = query.join(ForecastSQL)
        query = query.join(LocationSQL)
        query = query.filter(LocationSQL.gsp_id == gsp_id)

    # order by target time and created time desc
    query = query.order_by(ForecastValueSQL.target_time, ForecastValueSQL.created_utc.desc())

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


def get_latest_forecast_created_utc(session: Session, gsp_id: int) -> datetime:
    """
    Get the latest forecast created utc value. Can choose for different gsps

    :param session: database session
    :param gsp_id: gsp id to filter query on
    :return:
    """

    # start main query
    query = session.query(ForecastSQL.created_utc)

    # filter on gsp_id
    query = query.join(LocationSQL)
    query = query.filter(LocationSQL.gsp_id == gsp_id)

    # order, so latest is at the top
    query = query.order_by(ForecastSQL.created_utc.desc())

    # get first results
    created_utc = query.first()

    assert len(created_utc) == 1

    return created_utc[0]


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

    get_national = True
    if gsp_ids is not None:
        # see if gsp_id==0 is in list
        # if gsp id 0 is in list, then get national gsp location
        # if gsp id 0 is not in list, then don't get national gsp location
        if 0 in gsp_ids:
            get_national = True
            gsp_ids.remove(0)
        else:
            get_national = False

        # filter on gsp_id
        query = query.filter(LocationSQL.gsp_id.in_(gsp_ids))
    else:
        query = query.filter(LocationSQL.gsp_id != 0)

    # make sure gsp is not null
    query = query.filter(LocationSQL.gsp_id.isnot(None))

    # query
    query = query.order_by(LocationSQL.gsp_id)

    # get all results
    locations = query.all()

    if get_national:
        nation = get_location(session=session, gsp_id=0, label=national_gb_label)
        locations = [nation] + locations

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
