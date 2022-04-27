import pytest

from datetime import datetime
from nowcasting_datamodel.models import PVSystem, PVSystemSQL, PVYield


@pytest.fixture()
def pv_systems(db_session_pv):

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

    return [pv_system_sql_1, pv_system_sql_2]


@pytest.fixture()
def pv_yields_and_systems(db_session_pv):

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

    return {
        "pv_yields": [pv_yield_1_sql, pv_yield_2_sql, pv_yield_3_sql],
        "pv_systems": [pv_system_sql_1, pv_system_sql_1],
    }
