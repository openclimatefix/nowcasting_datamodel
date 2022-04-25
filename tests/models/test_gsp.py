from datetime import datetime

import numpy as np

from nowcasting_datamodel.models.gsp import GSPYield


def test_gsp_validation():

    gsp_yield = GSPYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=np.nan)

    assert gsp_yield.solar_generation_kw == -1
