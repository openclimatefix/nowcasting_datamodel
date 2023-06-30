from datetime import datetime

from nowcasting_datamodel.models.gsp import GSPYield


def test_gsp_validation():
    gsp_yield_1 = GSPYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=-10)

    assert gsp_yield_1.solar_generation_kw == 0
