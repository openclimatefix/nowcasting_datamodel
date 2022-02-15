""" Save forecasts to the database """
import logging
from typing import List

from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models import ForecastSQL, PVSystem, PVSystemSQL

logger = logging.getLogger(__name__)


def save(forecasts: List[ForecastSQL], session: Session):
    """
    Save forecast to database

    1. Change pydantic object to sqlalchemy objects
    2. Add sqlalchemy onjects to database

    :param forecasts: list of pydantic forecasts
    :param session: database session
    """

    # save objects to database
    session.add_all(forecasts)
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
