import logging
from datetime import datetime

from nowcasting_datamodel.models import (
    PVSystem,
PVSystemSQL,
PVYield
)
from nowcasting_datamodel.read_pv import (
    get_latest_pv_yield, get_pv_systems
)
from nowcasting_datamodel.read_pv import get_latest_pv_yield
from nowcasting_datamodel.save import save_pv_system

logger = logging.getLogger(__name__)


def test_get_pv_system(db_session_pv):
    pv_system_sql_1: PVSystemSQL = PVSystem(
        pv_system_id=1, provider="pvoutput.org", status_interval_minutes=5
    )
    pv_system_sql_2: PVSystemSQL = PVSystem(
        pv_system_id=2, provider="pvoutput.org", status_interval_minutes=5
    )
    save_pv_system(session=db_session_pv, pv_system=pv_system_sql_1)
    save_pv_system(session=db_session_pv, pv_system=pv_system_sql_2)

    pv_systems_get = get_pv_systems(
        session=db_session_pv
    )
    assert len(pv_systems_get) == 2

    pv_systems_get = get_pv_systems(
        session=db_session_pv, pv_systems_ids=[pv_systems_get[0].id]
    )

    assert len(pv_systems_get) == 1


def test_get_latest_pv_yield(db_session_pv):

    pv_yield_1 = PVYield(datetime_utc=datetime(2022, 1, 2), solar_generation_kw=1)
    pv_yield_1_sql = pv_yield_1.to_orm()

    pv_yield_2 = PVYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=2)
    pv_yield_2_sql = pv_yield_2.to_orm()

    pv_yield_3 = PVYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=2)
    pv_yield_3_sql = pv_yield_3.to_orm()

    pv_system_sql_1: PVSystemSQL = PVSystem(
        pv_system_id=1, provider="pvoutput.org", status_interval_minutes=5
    ).to_orm()
    pv_system_sql_2: PVSystemSQL = PVSystem(
        pv_system_id=2, provider="pvoutput.org", status_interval_minutes=5
    ).to_orm()

    # add pv system to yield object
    pv_yield_1_sql.pv_system = pv_system_sql_1
    pv_yield_2_sql.pv_system = pv_system_sql_1
    pv_yield_3_sql.pv_system = pv_system_sql_2

    # add to database
    db_session_pv.add(pv_yield_1_sql)
    db_session_pv.add(pv_yield_2_sql)
    db_session_pv.add(pv_system_sql_1)
    db_session_pv.add(pv_system_sql_2)

    db_session_pv.commit()

    pv_yields = get_latest_pv_yield(session=db_session_pv, pv_systems=[pv_system_sql_1, pv_system_sql_2])

    # read database
    assert len(pv_yields) == 2

    assert pv_yields[0].datetime_utc == datetime(2022, 1, 2)
    assert pv_yields[1].datetime_utc == datetime(2022, 1, 1)

    pv_systems = db_session_pv.query(PVSystemSQL).order_by(PVSystemSQL.created_utc).all()
    pv_yields[0].pv_system.id = pv_systems[0].id


def test_get_latest_pv_yield_append_no_yields(db_session_pv):

    pv_system_sql_1: PVSystemSQL = PVSystem(
        pv_system_id=1, provider="pvoutput.org", status_interval_minutes=5
    ).to_orm()
    pv_system_sql_2: PVSystemSQL = PVSystem(
        pv_system_id=2, provider="pvoutput.org", status_interval_minutes=5
    ).to_orm()

    # add to database
    db_session_pv.add(pv_system_sql_1)
    db_session_pv.add(pv_system_sql_2)

    db_session_pv.commit()

    pv_systems = get_latest_pv_yield(session=db_session_pv, pv_systems=[pv_system_sql_1, pv_system_sql_2],append_to_pv_systems=True)
    assert len(pv_systems) == 2


def test_get_latest_pv_yield_append(db_session_pv):
    pv_yield_1 = PVYield(datetime_utc=datetime(2022, 1, 2), solar_generation_kw=1)
    pv_yield_1_sql = pv_yield_1.to_orm()

    pv_yield_2 = PVYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=2)
    pv_yield_2_sql = pv_yield_2.to_orm()

    pv_yield_3 = PVYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=2)
    pv_yield_3_sql = pv_yield_3.to_orm()

    pv_system_sql_1: PVSystemSQL = PVSystem(
        pv_system_id=1, provider="pvoutput.org", status_interval_minutes=5
    ).to_orm()
    pv_system_sql_2: PVSystemSQL = PVSystem(
        pv_system_id=2, provider="pvoutput.org", status_interval_minutes=5
    ).to_orm()

    # add pv system to yield object
    pv_yield_1_sql.pv_system = pv_system_sql_1
    pv_yield_2_sql.pv_system = pv_system_sql_1
    pv_yield_3_sql.pv_system = pv_system_sql_2

    # add to database
    db_session_pv.add(pv_yield_1_sql)
    db_session_pv.add(pv_yield_2_sql)
    db_session_pv.add(pv_system_sql_1)
    db_session_pv.add(pv_system_sql_2)

    db_session_pv.commit()


    pv_systems = get_latest_pv_yield(session=db_session_pv, pv_systems=[pv_system_sql_1, pv_system_sql_2],append_to_pv_systems=True)
    assert len(pv_systems) == 2




