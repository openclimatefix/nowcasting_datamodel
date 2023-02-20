from datetime import datetime

import pandas as pd

from nowcasting_datamodel.fake import make_fake_forecasts
from nowcasting_datamodel.models import ForecastValueSevenDaysSQL
from nowcasting_datamodel.models.convert import (
    convert_df_to_national_forecast,
    convert_list_forecast_value_seven_days_sql_to_list_forecast,
)
from nowcasting_datamodel.save import save_all_forecast_values_seven_days


def test_convert_list_forecast_value_seven_days_sql_to_list_forecast(db_session):
    # set up
    forecasts = make_fake_forecasts(gsp_ids=range(0, 10), session=db_session)
    db_session.add_all(forecasts)
    db_session.commit()

    save_all_forecast_values_seven_days(session=db_session, forecasts=forecasts)
    forecast_values = db_session.query(ForecastValueSevenDaysSQL).all()

    forecast_new = convert_list_forecast_value_seven_days_sql_to_list_forecast(forecast_values)

    assert len(forecast_new) == 10


def test_convert_df_to_national_forecast(db_session):
    # set up
    forecast_values_df = pd.DataFrame(
        columns=["target_datetime_utc", "forecast_mw"],
        data=[[datetime(2023, 1, 1), 0.0], [datetime(2023, 1, 1, 1), 1.0]],
    )

    forecast = convert_df_to_national_forecast(
        session=db_session,
        forecast_values_df=forecast_values_df,
        model_name="test_model",
        version="0.0.1",
    )

    assert len(forecast.forecast_values) == 2
