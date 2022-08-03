""" Save forecasts to the database """
import logging
from typing import List, Optional

from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models import ForecastSQL, PVSystem, PVSystemSQL
from nowcasting_datamodel.update import update_all_forecast_latest

logger = logging.getLogger(__name__)


def save(
    forecasts: List[ForecastSQL],
    session: Session,
    update_national: Optional[bool] = True,
    update_gsp: Optional[bool] = True,
):
    """
    Save forecast to database

    1. Add sqlalchemy onjects to database
    2. Saves to 'latest' table aswell

    :param forecasts: list of sql forecasts
    :param session: database session
    :param update_national: Optional (default true), to update the national forecast
    :param update_gsp: Optional (default true), to update all the GSP forecasts
    """

    # save objects to database
    logger.debug("Saving models")
    session.add_all(forecasts)
    session.commit()

    logger.debug("Updating to latest")
    update_all_forecast_latest(
        session=session, forecasts=forecasts, update_national=update_national, update_gsp=update_gsp
    )
    session.commit()


def save_pv_system(session: Session, pv_system: PVSystem) -> PVSystemSQL:
    """
    Get model object from name and version

    :param session: database session
    :param pv_system: pv system sql object

    return: PVSystem object

    """

    # start main query
    query = session.query(PVSystemSQL)

    # filter on pv_system_id and provider
    query = query.filter(PVSystemSQL.pv_system_id == pv_system.pv_system_id)
    query = query.filter(PVSystemSQL.provider == pv_system.provider)

    # get all results
    pv_systems = query.all()

    if len(pv_systems) == 0:
        logger.debug(
            f"Model for provider {pv_system.provider} and pv_system_id {pv_system.pv_system_id }"
            f"does not exist so going to add it"
        )

        session.add(pv_system.to_orm())
        session.commit()

    else:
        logger.debug(
            f"Model for provider {pv_system.provider} and pv_system_id {pv_system.pv_system_id} "
            f"is already there so not going to add it"
        )
        pv_system = pv_systems[0]

    return pv_system
