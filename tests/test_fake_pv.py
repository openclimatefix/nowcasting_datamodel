from datetime import datetime, timezone

import pytest

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
