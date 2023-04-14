""" Methods for adding adjust values to the forecast"""
import logging
from datetime import datetime, timedelta
from typing import List

import pandas as pd

from nowcasting_datamodel.models import ForecastSQL, MetricValue, MetricValueSQL
from nowcasting_datamodel.read.read_metric import read_latest_me_national

logger = logging.getLogger()


def add_adjust_to_forecasts(forecasts_sql: List[ForecastSQL], session):
    """
    Adjust National Forecast by ME over the last week

    Note that no objects are returned as sqlalchemy objects can be adjust on the spot

    :param forecasts_sql: list of forecasts
    :param session: database sessions
    """
    logger.debug("Adding Adjusts to National forecast")

    # get the national forecast only
    forecast_national = [f for f in forecasts_sql if f.location.gsp_id == 0]

    if len(forecast_national) != 1:
        logger.debug("Could not find single national forecast, tehre fore not adding adjust")

    add_adjust_to_national_forecast(forecast=forecast_national[0], session=session)


def add_adjust_to_national_forecast(forecast: ForecastSQL, session):
    """
    Add adjust to national forecast.

    1. Get latest me results for time_of_day and forecast horizont
    2. Reduce latest me results relevant for now
    3. Add Me to adjust_mw property in ForecastValueSQL

    :param forecast: national forecast
    :param session:
    :return:
    """

    # get the target time and model name
    datetime_now = forecast.forecast_values[0].target_time
    model_name = forecast.model.name

    # 1. read metric values
    latest_me = read_latest_me_national(session=session, model_name=model_name)
    assert len(latest_me) > 0

    # 2. filter value down to now onwards
    # change to dataframe
    latest_me_df = reduce_metric_values_to_correct_forecast_horizon(
        latest_me=latest_me, datetime_now=datetime_now
    )
    assert len(latest_me_df) > 0

    # 3. add values to forecast_values
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
    """
    Change all latest ME results 2D to 1D array

    Start:
    # time forecast_horizon value
    # 00.00 0  0.1
    # 00.00 30 0.2
    # 00.00 60 0.6
    # .....
    # 00.30 0  0.11
    # 00.30 30 0.21
    # 00.30 60 0.61
    # .....
    # 23.30 0  0.11
    # 23.30 30 0.21
    # 23.30 60 0.61

    Using datetime_now of 04:30

    End:
    # time forecast_horizon value
    # 04.30 0  0.1
    # 05.00 30 0.2
    # 05.30 60 0.6
    # .....

    :param latest_me: list of latest me results for all time_of_day and all forecast_horizons
    :param datetime_now: the dateteim now, so we know where to use forecast_horizons of 0
    :param hours_ahead: how many hours ahead
    :return: dataframe containing
        'datetime'
        'time_of_day'
        'forecast_horizon'
        'value'
    """

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
