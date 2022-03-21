""" Read pv functions """
from typing import List, Optional, Union

from sqlalchemy import desc
from sqlalchemy.orm import Session

from nowcasting_datamodel.models import PVSystemSQL, PVYieldSQL


def get_pv_systems(
    session: Session,
    pv_systems_ids: Optional[List[int]] = None,
    provider: Optional[str] = "pvoutput.org",
) -> List[PVSystemSQL]:
    """
    Get all pv systems

    :param session:
    :param pv_systems_ids: optional list of ids
    :param provider: optional provider name
    :return: list of pv systems
    """

    # start main query
    query = session.query(PVSystemSQL)
    query = query.distinct(PVSystemSQL.id)

    # filter on pv_system_id and provider
    if pv_systems_ids is not None:
        query = query.filter(PVSystemSQL.pv_system_id.in_(pv_systems_ids))
        query = query.filter(PVSystemSQL.provider == provider)

    # order by 'created_utc' desc, so we get the latest one
    query = query.order_by(PVSystemSQL.id, desc(PVSystemSQL.created_utc))

    # get all results
    pv_systems = query.all()

    return pv_systems


def get_latest_pv_yield(
    session: Session, pv_systems: List[PVSystemSQL], append_to_pv_systems: bool = False
) -> Union[List[PVYieldSQL], List[PVSystemSQL]]:
    """
    Get the last pv yield data

    :param session: database sessions
    :param pv_systems: list of pv systems
    :param append_to_pv_systems: append pv yield to pv systems, or return pv systems.
        If appended the yield is access by 'pv_system.last_pv_yield'
    :return: either list of pv yields, or pv systems
    """

    pv_systems_ids = [pv_system.id for pv_system in pv_systems]

    # start main query
    query = session.query(PVYieldSQL)
    query = query.join(PVSystemSQL)
    query = query.where(
        PVSystemSQL.id == PVYieldSQL.pv_system_id,
    )

    # only select on results per pv system
    query = query.distinct(PVSystemSQL.id)

    # select only th epv systems we want
    query = query.where(PVSystemSQL.id.in_(pv_systems_ids))

    # order by 'created_utc' desc, so we get the latest one
    query = query.order_by(
        PVSystemSQL.id, desc(PVYieldSQL.datetime_utc), desc(PVYieldSQL.created_utc)
    )

    # get all results
    pv_yields: List[PVYieldSQL] = query.all()

    if not append_to_pv_systems:
        return pv_yields
    else:
        # get list of pvsystems with last pv yields
        pv_systems_with_pv_yields = []
        for pv_yield in pv_yields:
            pv_system = pv_yield.pv_system
            pv_system.last_pv_yield = pv_yield

            pv_systems_with_pv_yields.append(pv_system)

        # add pv systems that dont have any pv yields
        pv_systems_with_pv_yields_ids = [pv_system.id for pv_system in pv_systems_with_pv_yields]

        pv_systems_with_no_pv_yields = []
        for pv_system in pv_systems:
            if pv_system.id not in pv_systems_with_pv_yields_ids:
                pv_system.last_pv_yield = None

                pv_systems_with_no_pv_yields.append(pv_system)

        all_pv_systems = pv_systems_with_pv_yields + pv_systems_with_no_pv_yields

        return all_pv_systems
