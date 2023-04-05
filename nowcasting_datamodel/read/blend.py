""" Blends forecasts together

1. creates weights for blending
2. get forecast values for each model
3. blends them together

"""
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import pandas as pd
from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models.forecast import (
    ForecastValue,
)
from nowcasting_datamodel.read.read import get_forecast_values_latest

logger = logging.getLogger(__name__)


def get_blend_forecast_values_latest(
    session: Session,
    gsp_id: int,
    start_datetime: Optional[datetime] = None,
    model_names: Optional[List[str]] = None,
    weights: Optional[List[float]] = None,
) -> List[ForecastValue]:
    """
    Get forecast values

    :param session: database session
    :param gsp_id: gsp id, to filter query on
    :param start_datetime: optional to filterer target_time by start_datetime
        If None is given then all are returned.
    :param model_names: list of model names to use for blending
    :param weights: list of weights to use for blending, see structure in make_weights_df

    return: List of forecasts values blended from different models
    """

    logger.info(f"Getting blend forecast for gsp_id {gsp_id} and start_datetime {start_datetime}")

    if model_names is None:
        model_names = ["cnn", "National_xg"]
    if len(model_names) > 1:
        weights_df = make_weights_df(model_names, weights, start_datetime)

    # get forecast for the different models
    forecast_values_all_model = []
    one_forecast_created_within_timedelta = False
    for model_name in model_names:
        forecast_values_one_model = get_forecast_values_latest(
            session=session, gsp_id=gsp_id, start_datetime=start_datetime, model_name=model_name
        )

        if len(forecast_values_one_model) == 0:
            logger.debug(
                f"No forecast values for {model_name} for gsp_id {gsp_id} "
                f"and start_datetime {start_datetime}"
            )
        else:
            logger.debug(
                f"Found {len(forecast_values_one_model)} values for {model_name} "
                f"for gsp_id {gsp_id} and start_datetime {start_datetime}"
            )
            forecast_values_all_model.append([model_name, forecast_values_one_model])

    # if all forecasts are later than 2 hours, then we use the blend,
    # otherwise we only use forecast less than 2 hours
    if one_forecast_created_within_timedelta:
        # remove all forecasts that are older than 2 hours
        forecast_values_all_model_valid = []
        for model_name, forecast_values_one_model in forecast_values_all_model:

            one_forecast_created_within_timedelta = forecast_values_one_model[
                0
            ].created_utc < datetime.now(timezone.utc) - timedelta(hours=2)

            if one_forecast_created_within_timedelta:
                logger.debug(f"Will be using forecast {model_name} as it is newer than 2 hours")
                forecast_values_all_model_valid.append([model_name, forecast_values_one_model])
            else:
                logger.debug(f"forecast {model_name} is older than 2 hours, so not using it")
    else:
        # use all forecast:
        logger.debug("using all forecasts as all are older than 2 hours")
        forecast_values_all_model_valid = forecast_values_all_model

    # merge into pandas dataframe with columns
    # - "expected_power_generation_megawatts",
    # - "adjust_mw",
    # - "created_utc",
    # - "model_name",
    logger.debug(f"Getting values from {len(forecast_values_all_model_valid)} models")
    forecast_values_all_model_df = []
    for model_name, forecast_values_one_model in forecast_values_all_model_valid:

        logger.debug(f"Making dataframe for {model_name}")

        values_list = [
            [
                value.target_time,
                value.expected_power_generation_megawatts,
                value.adjust_mw,
                value.created_utc,
                model_name,
            ]
            for value in forecast_values_one_model
        ]
        value_df = pd.DataFrame(
            values_list,
            columns=[
                "target_time",
                "expected_power_generation_megawatts",
                "adjust_mw",
                "created_utc",
                "model_name",
            ],
        )
        value_df.reset_index(inplace=True, drop=False)
        forecast_values_all_model_df.append(value_df)

    # join into one dataframe
    forecast_values_all_model = pd.concat(forecast_values_all_model_df, axis=0)

    logger.debug(forecast_values_all_model)

    # blend together
    # lets deal with unique target times first
    logger.debug(forecast_values_all_model["target_time"])

    # get all unique target times
    all_target_times = forecast_values_all_model["target_time"].unique()
    logger.debug(f"Found in total {len(all_target_times)} target times")

    # get the duplicated target times
    duplicated_target_times = forecast_values_all_model[
        forecast_values_all_model["target_time"].duplicated()
    ]["target_time"].tolist()
    logger.debug(f"Found {len(duplicated_target_times)} duplicated target times")

    unique_target_times = [x for x in all_target_times if x not in duplicated_target_times]
    logger.debug(f"Found in {len(unique_target_times)} unique target times")

    # get the index of the duplicated and unique target times
    duplicated_target_times_idx = forecast_values_all_model[
        forecast_values_all_model["target_time"].isin(duplicated_target_times)
    ].index
    unique_target_times_idx = forecast_values_all_model[
        forecast_values_all_model["target_time"].isin(unique_target_times)
    ].index

    # get the unique forecast values
    forecast_values_blended = forecast_values_all_model.loc[unique_target_times_idx]

    # now lets deal with the weights
    duplicated = forecast_values_all_model.loc[duplicated_target_times_idx]
    duplicated = duplicated.drop_duplicates()

    # only do this if there are duplicated
    if len(duplicated) > 0:

        logger.debug(f"Now blending the duplicated target times using {weights_df}")

        pd.DataFrame()
        # unstack the weights
        weights_one_df = weights_df.stack()
        weights_one_df.name = "weight"
        weights_one_df = weights_one_df.reset_index()
        weights_one_df = weights_one_df.rename(columns={"level_1": "model_name"})
        weights_one_df = weights_one_df.rename(columns={"level_0": "target_time"})

        # join the weights to the duplicated
        duplicated = pd.merge(
            duplicated,
            weights_one_df,
            how="left",
            left_on=["model_name", "target_time"],
            right_on=["model_name", "target_time"],
        )

        # multiply the expected power generation by the weight
        duplicated["expected_power_generation_megawatts"] = (
            duplicated["expected_power_generation_megawatts"] * duplicated["weight"]
        )
        duplicated["adjust_mw"] = duplicated["adjust_mw"] * duplicated["weight"]
        duplicated.drop(columns=["created_utc"], inplace=True)

        # sum the weights
        duplicated = duplicated.groupby(["target_time"]).sum()

        # make sure target_time is a columns
        duplicated["target_time"] = duplicated.index
        duplicated.reset_index(inplace=True, drop=True)

        # divide by the sum of the weights
        duplicated["expected_power_generation_megawatts"] /= duplicated["weight"]
        duplicated["adjust_mw"] /= duplicated["weight"]

        logger.debug(duplicated)

    # join unique and duplicates together
    forecast_values_blended = pd.concat([forecast_values_blended, duplicated], axis=0)

    # sort by target time
    forecast_values_blended.sort_values(by="target_time", inplace=True)

    # change in to list of ForecastValue objects
    forecast_values = []
    logger.debug(forecast_values_blended)
    for i, row in forecast_values_blended.iterrows():
        print(row)
        forecast_value = ForecastValue(
            target_time=row.target_time,
            expected_power_generation_megawatts=row.expected_power_generation_megawatts,
        )
        forecast_value._adjust_mw = row.adjust_mw
        forecast_values.append(forecast_value)

    return forecast_values


def make_weights_df(model_names, weights, start_datetime_now=None):
    """Makes weights to half an hour and blocks

    A pd data frame like
    target_time weight cnn National_xg
    2020-01-01 00:00:00 1.0 0.0
    2020-01-01 00:30:00 0.5 0.5
    2020-01-01 01:00:00 0.0 1.0

    :param model_names:
    :param start_datetime:
    :param weights:
    :return:
    """
    if weights is None:
        weights = [
            {
                "end_horizon_hour": 2,
                "end_weight": [1, 0],
            },
            {
                "start_horizon_hour": 2,
                "end_horizon_hour": 6,
                "start_weight": [1, 0],
                "end_weight": [0, 1],
            },
            {
                "start_horizon_hour": 6,
                "start_weight": [0, 1],
            },
        ]
    # get time now rounded up to 30 mins
    if start_datetime_now is None:
        start_datetime_now = datetime.now().replace(second=0, microsecond=0)
        if start_datetime_now.minute >= 30:
            start_datetime_now += timedelta(hours=1)
            start_datetime_now = start_datetime_now.replace(minute=00)
        else:
            start_datetime_now = start_datetime_now.replace(minute=30)

    # make dataframe of 8 hours in 30 minutes chunks from now
    weights_all_df = []
    for weight in weights:
        if "start_horizon_hour" not in weight:
            start_horizon_hour = 0
        else:
            start_horizon_hour = weight["start_horizon_hour"]
        if "end_horizon_hour" not in weight:
            end_horizon_hour = 8
        else:
            end_horizon_hour = weight["end_horizon_hour"]

        if "start_weight" not in weight:
            start_weight = weight["end_weight"]
        else:
            start_weight = weight["start_weight"]
        if "end_weight" not in weight:
            end_weight = weight["start_weight"]
        else:
            end_weight = weight["end_weight"]

        logger.debug(
            f"Making weights for {start_horizon_hour} to {end_horizon_hour} "
            f"hours with weights {start_weight} to {end_weight}"
        )

        start_datetime = (
            start_datetime_now + timedelta(hours=start_horizon_hour) - timedelta(minutes=30)
        )
        if start_horizon_hour == 0:
            start_datetime += timedelta(minutes=30)
        end_datetime = (
            start_datetime_now + timedelta(hours=end_horizon_hour) - timedelta(minutes=30)
        )
        if end_horizon_hour == 8:
            end_datetime += timedelta(minutes=30)

        weights_df = pd.DataFrame(
            index=pd.date_range(start=start_datetime, end=end_datetime, freq="30min")
        )
        # get rid of last timestamp
        weights_df = weights_df[:-1]

        assert len(model_names) == len(start_weight)
        assert len(model_names) == len(end_weight)
        for model_name, start_weight, end_weight in zip(model_names, start_weight, end_weight):
            weights_df[model_name] = start_weight + (end_weight - start_weight) * (
                weights_df.index - start_datetime
            ) / (end_datetime - start_datetime)

        weights_all_df.append(weights_df)

    weights_all_df = pd.concat(weights_all_df)
    weights_all_df = weights_all_df[~weights_all_df.index.duplicated(keep="first")]

    # only keep from now
    weights_all_df = weights_all_df[weights_all_df.index >= start_datetime_now]

    return weights_all_df
