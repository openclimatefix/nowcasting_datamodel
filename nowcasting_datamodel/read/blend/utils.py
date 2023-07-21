"""Utils for blending forecasts together"""
from datetime import datetime, timedelta, timezone
from typing import List, Union

import numpy as np
import pandas as pd
import structlog

from nowcasting_datamodel.models import ForecastValue

logger = structlog.stdlib.get_logger()


def check_forecast_created_utc(forecast_values_all_model) -> List[Union[str, List[ForecastValue]]]:
    """
    Check if forecasts are valid.

    We only consider forecast less than 2 hours old.
    If all forecast are older than 2 hours, we used both.

    :param forecast_values_all_model: list of forecast
    :return: list of valid forecasts
    """
    one_forecast_created_within_timedelta = False

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
                logger.debug(f"Will be using forecast {model_name} " f"as it is newer than 2 hours")
                forecast_values_all_model_valid.append([model_name, forecast_values_one_model])
            else:
                logger.debug(f"forecast {model_name} is older than 2 hours, so not using it")
    else:
        # use all forecast:
        logger.debug("using all forecasts as all are older than 2 hours")
        forecast_values_all_model_valid = forecast_values_all_model
    return forecast_values_all_model_valid


def convert_list_forecast_values_to_df(forecast_values_all_model_valid):
    """
    Convert list of forecast values to a pandas dataframe

    :param forecast_values_all_model_valid:
    :return: merged into pandas dataframe with columns
     - "expected_power_generation_megawatts",
     - "adjust_mw",
     - "created_utc",
     - "model_name",
    """
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
                value.properties,
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
                "properties",
                "model_name",
            ],
        )
        value_df.reset_index(inplace=True, drop=False)
        forecast_values_all_model_df.append(value_df)
    # join into one dataframe
    forecast_values_all_model = pd.concat(forecast_values_all_model_df, axis=0)
    forecast_values_all_model.reset_index(inplace=True)

    # sort by target time
    forecast_values_all_model.sort_values(by=["target_time"], inplace=True)

    return forecast_values_all_model


def convert_df_to_list_forecast_values(forecast_values_blended: pd.DataFrame):
    """
    Convert the blended dataframe to a list of ForecastValue objects

    :param forecast_values_blended: Needs to have the columns 'target_time'
        and 'expected_power_generation_megawatts
    :return:
    """
    forecast_values = []
    logger.debug(forecast_values_blended)
    for i, row in forecast_values_blended.iterrows():
        # make sure we don't have negative values
        expected_power_generation_megawatts = row.expected_power_generation_megawatts
        if np.isnan(expected_power_generation_megawatts):
            continue
        if expected_power_generation_megawatts < 0:
            expected_power_generation_megawatts = 0

        forecast_value = ForecastValue(
            target_time=row.target_time,
            expected_power_generation_megawatts=expected_power_generation_megawatts,
        )
        forecast_value._adjust_mw = row.adjust_mw
        forecast_value._properties = row.properties
        forecast_values.append(forecast_value)
    return forecast_values


def blend_forecasts_together(forecast_values_all_model, weights_df):
    """
    Blend the forecasts together using the weights_df

    :param forecast_values_all_model: Dataframe containing the columns 'target_time',
        'expected_power_generation_megawatts', 'adjust_mw', 'model_name'
    :param weights_df: Dataframe of weights with columns of the model name,
        and index of target times
    :return: Dataframe with the columns
        'target_time',
        'expected_power_generation_megawatts',
        'adjust_mw'
    """

    # drop of the "properties" column
    if "properties" in forecast_values_all_model.columns:
        forecast_values_all_model = forecast_values_all_model.drop(columns=["properties"]).copy()

    # get all unique target times
    all_target_times = forecast_values_all_model["target_time"].unique()
    if len(all_target_times) == len(forecast_values_all_model):
        # if we have the same number of unique target times as we have rows,
        # then we only have one model, so just return that
        return forecast_values_all_model
    logger.debug(f"Found in total {len(all_target_times)} target times")

    # unstack weights, this makes a dataframe with columns "model_name", "target_time", "weight"
    weights_one_df = weights_df.stack()
    weights_one_df.name = "weight"
    weights_one_df = weights_one_df.reset_index()
    weights_one_df = weights_one_df.rename(columns={"level_1": "model_name"})
    weights_one_df = weights_one_df.rename(columns={"level_0": "target_time"})

    # create a dataframe of weights that dont repeat,
    # this is useful for search for a different weight to use
    weights_trim_df = weights_df.drop_duplicates(subset=weights_df.columns)

    # join together the forecast values and the weights
    forecast_values_all_model = forecast_values_all_model.merge(
        weights_one_df, on=["target_time", "model_name"], how="left"
    )

    # take all the forecasts that have a weight of 1.0
    forecast_values_blended = forecast_values_all_model[forecast_values_all_model["weight"] == 1.0]

    # find the other target times than needed to be blended
    target_times_to_blend = [
        x for x in all_target_times if x not in forecast_values_blended["target_time"].tolist()
    ]

    forecast_values_blended = [forecast_values_blended]
    # loop over datetimes, we could do this without looping,
    # but I found it hard to read the code, and keep track of things
    for target_time in target_times_to_blend:
        logger.debug(f"Blending forecasts for {target_time}")

        # get the forecast values for this target time
        forecast_values_one_target_time = forecast_values_all_model[
            forecast_values_all_model["target_time"] == target_time
        ]
        logger.debug(f"Found {len(forecast_values_one_target_time)} forecasts for {target_time}")

        # merge the forecast values and weights together
        forecast_values_one_target_time = forecast_values_one_target_time[
            ["model_name", "expected_power_generation_megawatts", "adjust_mw", "weight"]
        ]

        forecast_values_one_target_time_blend = blend_together_one_target_time(
            forecast_values_one_target_time, target_time
        )

        total_weights_used = forecast_values_one_target_time_blend["weight"].iloc[0]
        if total_weights_used == 0:
            # the total weight is 0, which means the forecast is not available for the target time
            # Therefore we find the model which model to use

            logger.debug(
                f"Trying to blend forecast for {target_time} "
                f"but the weights for the forecasts with weights are not available, or add to 0"
            )

            if len(forecast_values_one_target_time) == 1:
                logger.debug(f"Only found one forecast for {target_time} so using that")
                forecast_values_one_target_time_blend = forecast_values_one_target_time
                forecast_values_one_target_time_blend["target_time"] = target_time
            else:
                weights_target_time = weights_trim_df[weights_trim_df.index > target_time]

                logger.debug(
                    f"Found more than one forecast available for {target_time}"
                    f"Going to try at the next weights "
                )
                for i, row in weights_target_time.iterrows():
                    # format row into dataframe
                    weights = pd.DataFrame(row)
                    weights.index.name = "model_name"
                    weights.reset_index(inplace=True)
                    weights.columns = ["model_name", "weight"]

                    logger.debug(f"Trying {weights}")

                    # replace weights
                    forecast_values_one_target_time.drop(columns=["weight"], inplace=True)
                    forecast_values_one_target_time = forecast_values_one_target_time.merge(
                        weights, on=["model_name"], how="left"
                    )
                    logger.debug(forecast_values_all_model)

                    # blend together
                    forecast_values_one_target_time_blend = blend_together_one_target_time(
                        forecast_values_one_target_time, target_time
                    )
                    total_weights_used = forecast_values_one_target_time_blend["weight"].iloc[0]

                    # keep looping unless the total weights used > 0. This means we have blended the
                    # forecast together using the next weights possible
                    if total_weights_used > 0:
                        logger.debug(f"Using weights {weights}")
                        break

        # append to the blended dataframe
        forecast_values_blended.append(forecast_values_one_target_time_blend)

    forecast_values_blended = pd.concat(forecast_values_blended, axis=0)

    # sort by target time
    forecast_values_blended.sort_values(by=["target_time"], inplace=True)

    return forecast_values_blended


def blend_together_one_target_time(
    forecast_values_one_target_time: pd.DataFrame,
    target_time: datetime,
):
    """
    Blend forecasts for one target time together using the weights

    :param forecast_values_one_target_time: dataframe with columns 'model_name',
        'expected_power_generation_megawatts', 'adjust_mw', 'weight'.
        The rows are the different models
    :param target_time: the target time of this forecast
    :return: blended dataframe with columns 'target_time', 'expected_power_generation_megawatts',
        'adjust_mw'. The column weight shows how much total weight was used for each model
    """

    # calculate the blended forecast
    weight = np.sum(forecast_values_one_target_time["weight"])
    expected_power_generation_megawatts = (
        np.sum(
            forecast_values_one_target_time["expected_power_generation_megawatts"]
            * forecast_values_one_target_time["weight"]
        )
        / weight
    )
    adjust_mw = (
        np.sum(
            forecast_values_one_target_time["adjust_mw"] * forecast_values_one_target_time["weight"]
        )
        / weight
    )

    # make into dataframe
    forecast_values_one_target_time_blend = pd.DataFrame(
        data=[[expected_power_generation_megawatts, adjust_mw, weight]],
        columns=["expected_power_generation_megawatts", "adjust_mw", "weight"],
    )
    # make sure target_time is a columns
    forecast_values_one_target_time_blend["target_time"] = target_time
    forecast_values_one_target_time_blend.reset_index(inplace=True, drop=True)

    return forecast_values_one_target_time_blend
