"""Functions to make weights for blending"""
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import numpy as np
import pandas as pd
import structlog

logger = structlog.stdlib.get_logger()

default_weights = [
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


def make_weights_df(
    model_names, weights, start_datetime=None, forecast_horizon_minutes: Optional[int] = None
):
    """Makes weights to half an hour and blocks

    A pd data frame like
    target_time weight cnn National_xg
    2020-01-01 00:00:00 1.0 0.0
    2020-01-01 00:30:00 0.5 0.5
    2020-01-01 01:00:00 0.0 1.0

    :param model_names: list of model names to use for blending
    :param start_datetime: start datetime to make weights from
    :param weights: dictionary of weights, see default for structure
    :param forecast_horizon_minutes: optional, if given then the weights are the same for the
    :return: pd dataframe of weights
    """
    if weights is None:
        weights = default_weights
    # get time now rounded up to 30 mins
    start_datetime_now = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    if start_datetime_now.minute >= 30:
        start_datetime_now += timedelta(hours=1)
        start_datetime_now = start_datetime_now.replace(minute=00)
    else:
        start_datetime_now = start_datetime_now.replace(minute=30)

    if start_datetime is None:
        start_datetime = start_datetime_now

    if forecast_horizon_minutes is not None:
        weights_for_forecast_horizon = get_weights_for_forecast_horizon(
            weights=weights, forecast_horizon_hours=int(forecast_horizon_minutes / 60)
        )
        logger.debug(
            f"weights for forecast_horizon {weights_for_forecast_horizon}"
            f" {forecast_horizon_minutes=}"
        )

    # make dataframe of 8 hours in 30 minutes chunks from now
    weights_all_df = []
    for i in range(len(weights)):
        weight = weights[i]
        end_horizon_hour, end_weight, start_horizon_hour, start_weight = extract_weight_variables(
            weight
        )

        if forecast_horizon_minutes is not None:
            end_weight = weights_for_forecast_horizon
            start_weight = weights_for_forecast_horizon

        # add the first weight to the dataframe, from  start_datetime to start_datetime_now.
        # This could be for the two days before now
        if i == 0:
            weights_df = pd.DataFrame(
                index=pd.date_range(start=start_datetime, end=start_datetime_now, freq="30min")
            )
            weights_df[model_names] = start_weight
            weights_all_df.append(weights_df)

        logger.debug(
            f"Making weights for {start_horizon_hour} to {end_horizon_hour} "
            f"hours with weights {start_weight} to {end_weight}"
        )

        start_datetime_one_weight = (
            start_datetime_now + timedelta(hours=start_horizon_hour) - timedelta(minutes=30)
        )
        if start_horizon_hour == 0:
            start_datetime_one_weight += timedelta(minutes=30)

        end_datetime = (
            start_datetime_now + timedelta(hours=end_horizon_hour) - timedelta(minutes=30)
        )
        if end_horizon_hour == 8:
            end_datetime += timedelta(hours=1)

        logger.debug(f"Making weights from {start_datetime_one_weight} to {end_datetime}")
        weights_df = pd.DataFrame(
            index=pd.date_range(start=start_datetime_one_weight, end=end_datetime, freq="30min")
        )

        # get rid of last timestamp
        weights_df = weights_df[:-1]

        assert len(model_names) == len(start_weight)
        assert len(model_names) == len(end_weight)
        for model_name, start_weight, end_weight in zip(model_names, start_weight, end_weight):
            weights_df[model_name] = start_weight + (end_weight - start_weight) * (
                weights_df.index - start_datetime_one_weight
            ) / (end_datetime - start_datetime_one_weight)

        weights_all_df.append(weights_df)

    weights_all_df = pd.concat(weights_all_df)
    weights_all_df = weights_all_df[~weights_all_df.index.duplicated(keep="first")]

    # only keep from now
    logger.debug(weights_all_df)
    logger.debug(start_datetime)
    weights_all_df = weights_all_df[weights_all_df.index >= start_datetime]

    return weights_all_df


def extract_weight_variables(weight):
    """
    Extra weight variables from dictionary

    :param weight: dictionary of weights
    :return:
    """
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
    return end_horizon_hour, end_weight, start_horizon_hour, start_weight


def get_weights_for_forecast_horizon(forecast_horizon_hours: int, weights) -> List[float]:
    """
    Get weights for a specific forecast horizon

    :param forecast_horizon_hours:
    :param weights: list of weights
    :return: for the one forecast horizon, the return weights for the different models
    """
    logger.debug(f"Getting weights for forecast horizon {forecast_horizon_hours} from {weights}")
    if weights is None:
        weights = default_weights

    for i in range(len(weights)):
        weight = weights[i]
        end_horizon_hour, end_weight, start_horizon_hour, start_weight = extract_weight_variables(
            weight
        )
        logger.debug(
            f"Checking {start_horizon_hour} to {end_horizon_hour} hours with weights "
            f"{start_weight} to {end_weight}"
        )

        if start_horizon_hour < forecast_horizon_hours < end_horizon_hour:
            logger.debug(
                f"Interpolating between {start_weight} and {end_weight} for forecast horizon "
                f"{forecast_horizon_hours}"
            )
            end_weight = np.array(end_weight)
            start_weight = np.array(start_weight)
            f = (start_horizon_hour - forecast_horizon_hours) / (
                start_horizon_hour - end_horizon_hour
            )
            logger.debug(f)
            return (f * end_weight + (1 - f) * start_weight).tolist()
        elif forecast_horizon_hours <= start_horizon_hour and (i == 0):
            logger.debug(
                f"Using end weight {end_weight} for forecast horizon "
                f"{forecast_horizon_hours} {end_horizon_hour=}"
            )
            return end_weight
        elif forecast_horizon_hours >= end_horizon_hour and (i == len(weights) - 1):
            logger.debug(
                f"Using start weight {start_weight} for forecast horizon "
                f"{forecast_horizon_hours} {start_horizon_hour=}"
            )
            return start_weight

    logger.debug(f"Did not find weights for forecast horizon {forecast_horizon_hours}")
