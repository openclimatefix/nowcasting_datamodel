import pytest

from datetime import datetime, timezone


from nowcasting_datamodel.fake import make_fake_pv_system
from nowcasting_datamodel.models import PVSystem, PVSystemSQL, PVYield, PVYieldSQL


def test_make_fake_pv_system():
    pv_system_sql: PVSystemSQL = make_fake_pv_system()
    pv_system = PVSystem.from_orm(pv_system_sql)
    _ = PVSystem.to_orm(pv_system)


def test_pv_system_error():
    with pytest.raises(Exception):
        _ = PVSystem(pv_system_id=1, provider="fake.com", latitude=55, longitude=0)


def test_make_fake_pv_yield():

    pv_yield = PVYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=-1)
    assert pv_yield.solar_generation_kw == 0

    _ = pv_yield.to_orm()


def test_latest_datetime(db_session_pv):
    pv_yield_1 = PVYield(datetime_utc=datetime(2022, 1, 2), solar_generation_kw=1)
    pv_yield_1_sql = pv_yield_1.to_orm()

    pv_yield_2 = PVYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=2)
    pv_yield_2_sql = pv_yield_2.to_orm()

    pv_system_sql: PVSystemSQL = make_fake_pv_system()

    # add pv system to yield object
    pv_yield_1_sql.pv_system = pv_system_sql
    pv_yield_2_sql.pv_system = pv_system_sql

    # add to database
    db_session_pv.add(pv_yield_1_sql)
    db_session_pv.add(pv_yield_2_sql)
    db_session_pv.add(pv_system_sql)

    # read database
    assert len(db_session_pv.query(PVYieldSQL).all()) == 2
    pv_systems_read = db_session_pv.query(PVSystemSQL).all()
    assert len(pv_systems_read) == 1

    assert pv_systems_read[0].last_pv_yield.datetime_utc == datetime(2022, 1, 2, tzinfo=timezone.utc)


def test_latest_datetime_mulitple_pv_systems(db_session_pv):
    pv_yield_1 = PVYield(datetime_utc=datetime(2022, 1, 2), solar_generation_kw=1)
    pv_yield_1_sql = pv_yield_1.to_orm()

    pv_yield_2 = PVYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=2)
    pv_yield_2_sql = pv_yield_2.to_orm()

    pv_yield_3 = PVYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=2)
    pv_yield_3_sql = pv_yield_3.to_orm()

    pv_system_sql_1: PVSystemSQL = PVSystem(pv_system_id=1, provider="pvoutput.org", status_interval_minutes=5).to_orm()
    pv_system_sql_2: PVSystemSQL = PVSystem(pv_system_id=2, provider="pvoutput.org", status_interval_minutes=5).to_orm()

    # add pv system to yield object
    pv_yield_1_sql.pv_system = pv_system_sql_1
    pv_yield_2_sql.pv_system = pv_system_sql_1
    pv_yield_3_sql.pv_system = pv_system_sql_2

    # add to database
    db_session_pv.add(pv_yield_1_sql)
    db_session_pv.add(pv_yield_2_sql)
    db_session_pv.add(pv_system_sql_1)
    db_session_pv.add(pv_system_sql_2)

    # read database
    assert len(db_session_pv.query(PVYieldSQL).all()) == 3
    pv_systems_read = db_session_pv.query(PVSystemSQL).all()
    assert len(pv_systems_read) == 2

    assert pv_systems_read[0].last_pv_yield.datetime_utc == datetime(2022, 1, 2, tzinfo=timezone.utc)

