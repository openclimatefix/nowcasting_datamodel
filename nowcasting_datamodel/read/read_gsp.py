""" Read pv functions """
import logging
from typing import List, Union

from sqlalchemy import desc
from sqlalchemy.orm import Session

from nowcasting_datamodel.models import GSPYieldSQL, LocationSQL

logger = logging.getLogger(__name__)


def get_latest_gsp_yield(
    session: Session, gsps: List[LocationSQL], append_to_gsps: bool = False, regime: str = "in-day"
) -> Union[List[GSPYieldSQL], List[LocationSQL]]:
    """
    Get the last gsp yield data

    :param session: database sessions
    :param gsps: list of gsps
    :param append_to_gsps: append gsp yield to pv systems, or return pv systems.
        If appended the yield is access by 'pv_system.last_gsp_yield'
    :param regime: What regime the data is in, either 'in-day' or 'day-after'
    :return: either list of gsp yields, or pv systems
    """

    gsp_ids = [gsp.gsp_id for gsp in gsps]

    # start main query
    query = session.query(GSPYieldSQL)
    query = query.join(LocationSQL)
    query = query.where(
        LocationSQL.id == GSPYieldSQL.location_id,
    )

    # filter on regime
    query = query.where(GSPYieldSQL.regime == regime)

    # only select on results per pv system
    query = query.distinct(LocationSQL.gsp_id)

    # select only th epv systems we want
    query = query.where(LocationSQL.gsp_id.in_(gsp_ids))

    # order by 'created_utc' desc, so we get the latest one
    query = query.order_by(
        LocationSQL.gsp_id, desc(GSPYieldSQL.datetime_utc), desc(GSPYieldSQL.created_utc)
    )

    # get all results
    gsp_yields: List[GSPYieldSQL] = query.all()

    logger.debug(f"Found {len(gsp_yields)}  latest gsp yields")

    if not append_to_gsps:
        return gsp_yields
    else:
        # get list of gsp with last pv yields
        gsp_systems_with_gsp_yields = []
        for gsp_yield in gsp_yields:
            gsp = gsp_yield.location
            gsp.last_gsp_yield = gsp_yield

            gsp_systems_with_gsp_yields.append(gsp)

        logger.debug(f"Found {len(gsp_systems_with_gsp_yields)} gsps with yields")

        # add pv systems that dont have any pv yields
        gsp_systems_with_gsp_yields_ids = [gsp.id for gsp in gsp_systems_with_gsp_yields]

        gsp_systems_with_no_gsp_yields = []
        for gsp in gsps:
            if gsp.id not in gsp_systems_with_gsp_yields_ids:
                gsp.last_gsp_yield = None

                gsp_systems_with_no_gsp_yields.append(gsp)

        logger.debug(f"Found {len(gsp_systems_with_no_gsp_yields)} gsps with no yields")

        all_gsp_systems = gsp_systems_with_gsp_yields + gsp_systems_with_no_gsp_yields

        return all_gsp_systems
