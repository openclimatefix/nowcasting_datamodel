import logging
from datetime import datetime

from nowcasting_datamodel.models import GSPYield, Location, LocationSQL
from nowcasting_datamodel.read.read_gsp import get_gsp_yield, get_latest_gsp_yield

logger = logging.getLogger(__name__)


def test_get_latest_gsp_yield(db_session):

    gsp_yield_1 = GSPYield(datetime_utc=datetime(2022, 1, 2), solar_generation_kw=1)
    gsp_yield_1_sql = gsp_yield_1.to_orm()

    gsp_yield_2 = GSPYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=2)
    gsp_yield_2_sql = gsp_yield_2.to_orm()

    gsp_yield_3 = GSPYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=2)
    gsp_yield_3_sql = gsp_yield_3.to_orm()

    gsp_sql_1: LocationSQL = Location(gsp_id=1, label="GSP_1", status_interval_minutes=5).to_orm()
    gsp_sql_2: LocationSQL = Location(gsp_id=2, label="GSP_2", status_interval_minutes=5).to_orm()

    # add pv system to yield object
    gsp_yield_1_sql.location = gsp_sql_1
    gsp_yield_2_sql.location = gsp_sql_1
    gsp_yield_3_sql.location = gsp_sql_2

    # add to database
    db_session.add(gsp_yield_1_sql)
    db_session.add(gsp_yield_2_sql)
    db_session.add(gsp_yield_3_sql)
    db_session.add(gsp_sql_1)
    db_session.add(gsp_sql_2)

    db_session.commit()

    gsp_yields = get_latest_gsp_yield(session=db_session, gsps=[gsp_sql_1, gsp_sql_2])

    # read database
    assert len(gsp_yields) == 2

    assert gsp_yields[0].datetime_utc == datetime(2022, 1, 2)
    assert gsp_yields[1].datetime_utc == datetime(2022, 1, 1)

    gsps = db_session.query(LocationSQL).order_by(LocationSQL.gsp_id).all()
    gsp_yields[0].location.id = gsps[0].id


#
def test_get_latest_gsp_yield_append_no_yields(db_session):

    gsp_sql_1: LocationSQL = Location(gsp_id=1, label="GSP_1", status_interval_minutes=5).to_orm()
    gsp_sql_2: LocationSQL = Location(gsp_id=2, label="GSP_2", status_interval_minutes=5).to_orm()

    # add to database
    db_session.add(gsp_sql_1)
    db_session.add(gsp_sql_2)

    db_session.commit()

    pv_systems = get_latest_gsp_yield(
        session=db_session,
        gsps=[gsp_sql_1, gsp_sql_2],
        append_to_gsps=True,
    )

    assert pv_systems[0].last_gsp_yield is None
    assert len(pv_systems) == 2


def test_get_latest_gsp_yield_append(db_session):
    gsp_yield_1 = GSPYield(datetime_utc=datetime(2022, 1, 2), solar_generation_kw=1)
    gsp_yield_1_sql = gsp_yield_1.to_orm()

    gsp_yield_2 = GSPYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=2)
    gsp_yield_2_sql = gsp_yield_2.to_orm()

    gsp_yield_3 = GSPYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=2)
    gsp_yield_3_sql = gsp_yield_3.to_orm()

    gsp_sql_1: LocationSQL = Location(gsp_id=1, label="GSP_1", status_interval_minutes=5).to_orm()
    gsp_sql_2: LocationSQL = Location(gsp_id=2, label="GSP_2", status_interval_minutes=5).to_orm()

    # add pv system to yield object
    gsp_yield_1_sql.location = gsp_sql_1
    gsp_yield_2_sql.location = gsp_sql_1
    gsp_yield_3_sql.location = gsp_sql_2

    # add to database
    db_session.add(gsp_yield_1_sql)
    db_session.add(gsp_yield_2_sql)
    db_session.add(gsp_yield_3_sql)
    db_session.add(gsp_sql_1)
    db_session.add(gsp_sql_2)

    db_session.commit()

    gsps = get_latest_gsp_yield(
        session=db_session,
        gsps=[gsp_sql_1, gsp_sql_2],
        append_to_gsps=True,
    )
    assert len(gsps) == 2


def test_get_gsp_yield(db_session):

    gsp_yield_1 = GSPYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=1)
    gsp_yield_1_sql = gsp_yield_1.to_orm()

    gsp_yield_2 = GSPYield(datetime_utc=datetime(2022, 1, 1, 12), solar_generation_kw=2)
    gsp_yield_2_sql = gsp_yield_2.to_orm()

    gsp_yield_3 = GSPYield(datetime_utc=datetime(2022, 1, 2), solar_generation_kw=3)
    gsp_yield_3_sql = gsp_yield_3.to_orm()

    gsp_sql_1: LocationSQL = Location(gsp_id=1, label="GSP_1", status_interval_minutes=5).to_orm()

    # add pv system to yield object
    gsp_yield_1_sql.location = gsp_sql_1
    gsp_yield_2_sql.location = gsp_sql_1
    gsp_yield_3_sql.location = gsp_sql_1

    # add to database
    db_session.add(gsp_yield_1_sql)
    db_session.add(gsp_yield_2_sql)
    db_session.add(gsp_yield_3_sql)
    db_session.add(gsp_sql_1)

    db_session.commit()

    gsps = get_gsp_yield(session=db_session, gsp_ids=[1], start_datetime_utc=datetime(2022, 1, 1))
    assert len(gsps) == 3

    gsps = get_gsp_yield(
        session=db_session,
        gsp_ids=[1],
        start_datetime_utc=datetime(2022, 1, 1),
        end_datetime_utc=datetime(2022, 1, 1, 12),
    )
    assert len(gsps) == 2


def test_get_gsp_yield_regime(db_session):

    gsp_yield_1 = GSPYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=1,regime='in-day')
    gsp_yield_1_sql = gsp_yield_1.to_orm()

    gsp_yield_2 = GSPYield(datetime_utc=datetime(2022, 1, 1, 12), solar_generation_kw=2,regime='day-after')
    gsp_yield_2_sql = gsp_yield_2.to_orm()

    gsp_yield_3 = GSPYield(datetime_utc=datetime(2022, 1, 2), solar_generation_kw=3)
    gsp_yield_3_sql = gsp_yield_3.to_orm()

    gsp_sql_1: LocationSQL = Location(gsp_id=1, label="GSP_1", status_interval_minutes=5).to_orm()

    # add pv system to yield object
    gsp_yield_1_sql.location = gsp_sql_1
    gsp_yield_2_sql.location = gsp_sql_1
    gsp_yield_3_sql.location = gsp_sql_1

    # add to database
    db_session.add(gsp_yield_1_sql)
    db_session.add(gsp_yield_2_sql)
    db_session.add(gsp_yield_3_sql)
    db_session.add(gsp_sql_1)

    db_session.commit()

    gsps = get_gsp_yield(session=db_session, gsp_ids=[1], start_datetime_utc=datetime(2022, 1, 1))
    assert len(gsps) == 3

    gsps = get_gsp_yield(
        session=db_session,
        gsp_ids=[1],
        start_datetime_utc=datetime(2022, 1, 1),
        end_datetime_utc=datetime(2022, 1, 1, 12),
        regime='in-day'
    )
    assert len(gsps) == 1

