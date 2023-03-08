""" Methods for adding adjust values to the forecast"""
import logging
from datetime import datetime, timedelta
from typing import List

import pandas as pd

from nowcasting_datamodel.models import ForecastSQL, MetricValue, MetricValueSQL
from nowcasting_datamodel.read.read_metric import read_latest_me_national

logger = logging.getLogger()


def add_adjust_to_forecasts(forecasts_sql: List[ForecastSQL], session):

    logger.debug("Adding Adjusts to National forecast")

    # get the national forecast only
    forecast_national = [f for f in forecasts_sql if f.location.gsp_id == 0]

    if len(forecast_national) != 1:
        logger.debug("Could not find single national forecast, tehre fore not adding adjust")

    datetime_now = forecast_national[0].forecast_values[0].target_time

    add_adjust_to_national_forecast(
        forecast=forecast_national[0], session=session, datetime_now=datetime_now
    )


def add_adjust_to_national_forecast(forecast: ForecastSQL, session, datetime_now: datetime):

    # read metric values
    latest_me = read_latest_me_national(session=session)
    assert len(latest_me) > 0

    # filter value down to now onwards
    # change to dataframe
    latest_me_df = reduce_metric_values_to_correct_forecast_horizon(
        latest_me=latest_me, datetime_now=datetime_now
    )
    assert len(latest_me_df) > 0

    # add values to forecast_values
    for forecast_value in forecast.forecast_values:
        target_time = forecast_value.target_time
        try:
            # get value from df
            value = latest_me_df[latest_me_df["datetime"] == forecast_value.target_time]
            value = value.iloc[0].value

            # add value to ForecastValueSQL
            forecast_value.adjust_mw = value

        except Exception:
            logger.debug(f"Could not find ME value for {target_time} in {latest_me_df}")


def reduce_metric_values_to_correct_forecast_horizon(
    latest_me: List[MetricValueSQL], datetime_now: datetime, hours_ahead=8
) -> pd.DataFrame:

    latest_me_df = pd.DataFrame([MetricValue.from_orm(m).dict() for m in latest_me])

    # Let now big a dataframe of datetimes from now onwards. Lets say the time is 04.30, then
    # time forecast_horizon value
    # 04.30 0  0.1
    # 05.00 30 0.2
    # 05.30 60 0.6
    # .....

    results_df = pd.DataFrame(
        index=pd.date_range(
            start=datetime_now, end=datetime_now + timedelta(hours=hours_ahead), freq="30T"
        ),
    )
    print(results_df)
    results_df["datetime"] = results_df.index
    results_df["time_of_day"] = results_df.index.time
    results_df["forecast_horizon_minutes"] = range(0, 30 * len(results_df), 30)
    latest_me_df["forecast_horizon_minutes"] = latest_me_df["forecast_horizon_minutes"].astype(int)
    results_df["forecast_horizon_minutes"] = results_df["forecast_horizon_minutes"].astype(int)
    latest_me_df["value"] = latest_me_df["value"].astype(float)

    all_df = pd.merge(
        results_df,
        latest_me_df,
        how="left",
        on=["time_of_day", "forecast_horizon_minutes"],
    )

    all_df = all_df[["datetime", "time_of_day", "value", "forecast_horizon_minutes"]]

    return all_df
