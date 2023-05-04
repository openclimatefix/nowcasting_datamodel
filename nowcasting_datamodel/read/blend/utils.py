"""Utils for blending forecasts together"""
from datetime import datetime, timedelta, timezone
from typing import List, Union

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
    Conert list of forecast values to a pandas dataframe

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
    forecast_values_all_model.reset_index(inplace=True)
    return forecast_values_all_model


def convert_df_to_list_forecast_values(forecast_values_blended):
    """
    Convert the blended dataframe to a list of ForecastValue objects

    :param forecast_values_blended: Needs to have the columns 'target_time'
        and 'expected_power_generation_megawatts
    :return:
    """
    # change in to list of ForecastValue objects
    forecast_values = []
    logger.debug(forecast_values_blended)
    for i, row in forecast_values_blended.iterrows():
        # make sure we don't have negative values
        expected_power_generation_megawatts = row.expected_power_generation_megawatts
        if expected_power_generation_megawatts < 0:
            expected_power_generation_megawatts = 0

        forecast_value = ForecastValue(
            target_time=row.target_time,
            expected_power_generation_megawatts=expected_power_generation_megawatts,
        )
        forecast_value._adjust_mw = row.adjust_mw
        forecast_values.append(forecast_value)
    return forecast_values


def blend_forecasts_together(forecast_values_all_model, weights_df):
    """
    Blend the forecasts together using the weights_df

    :param forecast_values_all_model: Dataframe containing the columns 'target_time',
        'expected_power_generation_megawatts', 'adjust_mw', 'model_name'
    :param weights_df: Dataframe of weights with columns 'model_name' and 'weight'
    :return: Dataframe with the columns
        'target_time',
        'expected_power_generation_megawatts',
        'adjust_mw'
    """
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

        # divide by the sum of the weights, # TODO should we be worried about dividing by zero?
        duplicated["expected_power_generation_megawatts"] /= duplicated["weight"]
        duplicated["adjust_mw"] /= duplicated["weight"]

        logger.debug(duplicated)
    # join unique and duplicates together
    forecast_values_blended = pd.concat([forecast_values_blended, duplicated], axis=0)
    # sort by target time
    forecast_values_blended.sort_values(by="target_time", inplace=True)
    return forecast_values_blended
