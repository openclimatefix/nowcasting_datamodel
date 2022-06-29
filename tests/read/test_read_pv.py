import logging
from datetime import datetime

from nowcasting_datamodel.models import PVSystem, PVSystemSQL, PVYield, pv_output, sheffield_solar_passiv
from nowcasting_datamodel.read.read_pv import get_latest_pv_yield, get_pv_systems, get_pv_yield
from nowcasting_datamodel.save import save_pv_system

logger = logging.getLogger(__name__)


def test_save_pv_system(db_session_pv):
    pv_system_1 = PVSystem(pv_system_id=1, provider="pvoutput.org", status_interval_minutes=5)

    save_pv_system(session=db_session_pv, pv_system=pv_system_1)

    pv_systems_get = get_pv_systems(session=db_session_pv)
    assert len(pv_systems_get) == 1


def test_save_pv_system_repeat(db_session_pv, pv_systems):

    # add extra systems that is already there
    save_pv_system(session=db_session_pv, pv_system=pv_systems[0])

    pv_systems_get = get_pv_systems(session=db_session_pv)
    assert len(pv_systems_get) == 2


def test_save_pv_system_correct_data(db_session_pv, pv_systems):

    pv_systems[0].correct_data = False
    pv_systems_get = get_pv_systems(session=db_session_pv, correct_data=True)
    assert len(pv_systems_get) == 1

    pv_systems_get = get_pv_systems(
        session=db_session_pv, pv_systems_ids=[pv_systems_get[0].id], correct_data=True
    )

    assert len(pv_systems_get) == 1


def test_get_pv_system(db_session_pv, pv_systems):

    pv_systems_get = get_pv_systems(session=db_session_pv)
    assert len(pv_systems_get) == 2

    pv_systems_get = get_pv_systems(session=db_session_pv, pv_systems_ids=[pv_systems_get[0].id])

    assert len(pv_systems_get) == 1


def test_get_latest_pv_yield(db_session_pv, pv_yields_and_systems):
    [pv_system_sql_1, pv_system_sql_2] = pv_yields_and_systems["pv_systems"]

    pv_yields = get_latest_pv_yield(
        session=db_session_pv, pv_systems=[pv_system_sql_1, pv_system_sql_2]
    )

    # read database
    # this is 3 for when using sqlite as 'distinct' does work
    assert len(pv_yields) == 2

    assert pv_yields[0].datetime_utc == datetime(2022, 1, 2)
    assert pv_yields[1].datetime_utc == datetime(2022, 1, 1)

    pv_systems = db_session_pv.query(PVSystemSQL).order_by(PVSystemSQL.created_utc).all()
    pv_yields[0].pv_system.id = pv_systems[0].id


def test_get_latest_pv_yield_correct_data(db_session_pv, pv_yields_and_systems):
    [pv_system_sql_1, pv_system_sql_2] = pv_yields_and_systems["pv_systems"]
    pv_system_sql_1.correct_data = False

    pv_yields = get_latest_pv_yield(
        session=db_session_pv, pv_systems=[pv_system_sql_1, pv_system_sql_2], correct_data=True
    )

    # read database
    # this is 2 for when using sqlite as 'distinct' does work
    assert len(pv_yields) == 1


def test_get_latest_pv_yield_filter(db_session_pv, pv_yields_and_systems):
    [pv_system_sql_1, pv_system_sql_2] = pv_yields_and_systems["pv_systems"]

    pv_yields = get_latest_pv_yield(
        session=db_session_pv,
        pv_systems=[pv_system_sql_1, pv_system_sql_2],
        start_datetime_utc=datetime(2022, 1, 2),
        start_created_utc=datetime(2022, 1, 2),
    )

    # read database
    assert len(pv_yields) == 1

    assert pv_yields[0].datetime_utc == datetime(2022, 1, 2)

    pv_systems = db_session_pv.query(PVSystemSQL).order_by(PVSystemSQL.created_utc).all()
    pv_yields[0].pv_system.id = pv_systems[0].id


def test_get_latest_pv_yield_append_no_yields(db_session_pv, pv_systems):
    [pv_system_sql_1, pv_system_sql_2] = pv_systems

    pv_systems = get_latest_pv_yield(
        session=db_session_pv,
        pv_systems=[pv_system_sql_1, pv_system_sql_2],
        append_to_pv_systems=True,
    )

    assert pv_systems[0].last_pv_yield is None
    assert len(pv_systems) == 2


def test_get_latest_pv_yield_append(db_session_pv, pv_yields_and_systems):
    [pv_system_sql_1, pv_system_sql_2] = pv_yields_and_systems["pv_systems"]

    assert pv_system_sql_1.pv_system_id == 1
    assert pv_system_sql_2.pv_system_id == 2

    pv_systems = get_latest_pv_yield(
        session=db_session_pv,
        pv_systems=[pv_system_sql_1, pv_system_sql_2],
        append_to_pv_systems=True,
    )
    assert pv_systems[0].last_pv_yield is not None
    # this is 3 for when using sqlite as 'distinct' does work
    assert len(pv_systems) == 2


def test_read_pv_yield(db_session_pv, pv_yields_and_systems):

    assert len(get_pv_yield(session=db_session_pv, pv_systems_ids=[1])) == 2
    assert len(get_pv_yield(session=db_session_pv, pv_systems_ids=[1, 2])) == 3


def test_read_pv_yield_providers(db_session_pv, pv_yields_and_systems):
    assert len(get_pv_yield(session=db_session_pv, providers=[pv_output])) == 3
    assert len(get_pv_yield(session=db_session_pv, providers=[sheffield_solar_passiv])) == 0


def test_read_pv_yield_correct_data(db_session_pv, pv_yields_and_systems):
    pv_yields_and_systems["pv_systems"][0].correct_data = False

    assert len(get_pv_yield(session=db_session_pv, pv_systems_ids=[1], correct_data=True)) == 0
    assert len(get_pv_yield(session=db_session_pv, pv_systems_ids=[1, 2], correct_data=True)) == 1


def test_read_pv_yield_start_utc(db_session_pv, pv_yields_and_systems):

    assert (
        len(get_pv_yield(session=db_session_pv, pv_systems_ids=[1], start_utc=datetime(2022, 1, 2)))
        == 1
    )


def test_read_pv_yield_end_utc(db_session_pv, pv_yields_and_systems):

    assert (
        len(
            get_pv_yield(session=db_session_pv, pv_systems_ids=[1, 2], end_utc=datetime(2022, 1, 2))
        )
        == 2
    )
