""" Read pv functions """
from typing import List, Union

from sqlalchemy import desc
from sqlalchemy.orm import Session

from nowcasting_datamodel.models import GSPYieldSQL, LocationSQL


def get_latest_gsp_yield(
    session: Session, gsps: List[LocationSQL], append_to_gsps: bool = False
) -> Union[List[GSPYieldSQL], List[LocationSQL]]:
    """
    Get the last gsp yield data

    :param session: database sessions
    :param gsps: list of gsps
    :param append_to_gsps: append gsp yield to pv systems, or return pv systems.
        If appended the yield is access by 'pv_system.last_gsp_yield'
    :return: either list of gsp yields, or pv systems
    """

    gsp_ids = [gsp.id for gsp in gsps]

    # start main query
    query = session.query(GSPYieldSQL)
    query = query.join(LocationSQL)
    query = query.where(
        LocationSQL.id == GSPYieldSQL.location_id,
    )

    # only select on results per pv system
    query = query.distinct(LocationSQL.id)

    # select only th epv systems we want
    query = query.where(LocationSQL.id.in_(gsp_ids))

    # order by 'created_utc' desc, so we get the latest one
    query = query.order_by(
        LocationSQL.id, desc(GSPYieldSQL.datetime_utc), desc(GSPYieldSQL.created_utc)
    )

    # get all results
    gsp_yields: List[GSPYieldSQL] = query.all()

    if not append_to_gsps:
        return gsp_yields
    else:
        # get list of pvsystems with last pv yields
        gsp_systems_with_gsp_yields = []
        for gsp_yield in gsp_yields:
            gsp = gsp_yield.location
            gsp.last_gsp_yield = gsp_yield

            gsp_systems_with_gsp_yields.append(gsp)

        # add pv systems that dont have any pv yields
        gsp_systems_with_gsp_yields_ids = [
            pv_system.id for pv_system in gsp_systems_with_gsp_yields
        ]

        gsp_systems_with_no_gsp_yields = []
        for gsp in gsps:
            if gsp.id not in gsp_systems_with_gsp_yields_ids:
                gsp.last_gsp_yield = None

                gsp_systems_with_no_gsp_yields.append(gsp)

        all_gsp_systems = gsp_systems_with_gsp_yields_ids + gsp_systems_with_no_gsp_yields

        return all_gsp_systems