import numpy as np

from nowcasting_datamodel.read.blend.utils import convert_df_to_list_forecast_values
import pandas as pd
from datetime import datetime, timezone


def test_convert_df_to_list_forecast_values():

    data = pd.DataFrame(columns=["target_time", "expected_power_generation_megawatts", "adjust_mw", "properties"],
                        data=[["2020-01-01 00:00:00", 1, 2, {}],
                        ["2020-01-01 00:00:00", 1, 2, {}]])

    results = convert_df_to_list_forecast_values(forecast_values_blended=data)

    assert len(results) == 2
    assert results[0].expected_power_generation_megawatts == 1
    assert results[0]._adjust_mw == 2
    assert results[0]._properties == {}
    assert results[0].target_time == datetime(2020,1,1,0,0,0, tzinfo=timezone.utc)


def test_convert_df_to_list_forecast_values_whit_nan():
    data = pd.DataFrame(columns=["target_time", "expected_power_generation_megawatts", "adjust_mw", "properties"],
                        data=[["2020-01-01 00:00:00", 1, 2, {}],
                              ["2020-01-01 00:00:00", np.nan, 2, {}]])

    results = convert_df_to_list_forecast_values(forecast_values_blended=data)

    assert len(results) == 1
