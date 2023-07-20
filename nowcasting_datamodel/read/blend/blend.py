""" Blends forecasts together

1. creates weights for blending
2. get forecast values for each model
3. blends them together

"""

import json
from datetime import datetime
from typing import List, Optional

import pandas as pd
import structlog
from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models.forecast import ForecastValue, ForecastValueSevenDaysSQL
from nowcasting_datamodel.read.blend.utils import (
    blend_forecasts_together,
    check_forecast_created_utc,
    convert_df_to_list_forecast_values,
    convert_list_forecast_values_to_df,
)
from nowcasting_datamodel.read.blend.weights import (
    make_weights_df,
)
from nowcasting_datamodel.read.read import get_forecast_values, get_forecast_values_latest

logger = structlog.stdlib.get_logger()


def get_blend_forecast_values_latest(
    session: Session,
    gsp_id: int,
    start_datetime: Optional[datetime] = None,
    model_names: Optional[List[str]] = None,
    weights: Optional[List[float]] = None,
    forecast_horizon_minutes: Optional[int] = None,
    properties_model: Optional[str] = None,
) -> List[ForecastValue]:
    """
    Get forecast values

    :param session: database session
    :param gsp_id: gsp id, to filter query on
    :param start_datetime: optional to filterer target_time by start_datetime
        If None is given then all are returned.
    :param model_names: list of model names to use for blending
    :param weights: list of weights to use for blending, see structure in make_weights_df
    :param properties_model: the model to use for the properties

    return: List of forecasts values blended from different models
    """

    logger.info(f"Getting blend forecast for gsp_id {gsp_id} and start_datetime {start_datetime}")

    if model_names is None:
        model_names = ["cnn", "National_xg"]
    if len(model_names) > 1:
        weights_df = make_weights_df(
            model_names, weights, start_datetime, forecast_horizon_minutes=forecast_horizon_minutes
        )
    else:
        weights_df = None

    if properties_model is not None:
        assert (
            properties_model in model_names
        ), f"properties_model must be in model_names {model_names}"

    # get forecast for the different models
    forecast_values_all_model = []
    for model_name in model_names:
        if forecast_horizon_minutes is None:
            forecast_values_one_model = get_forecast_values_latest(
                session=session, gsp_id=gsp_id, start_datetime=start_datetime, model_name=model_name
            )
        else:
            forecast_values_one_model = get_forecast_values(
                session=session,
                gsp_ids=[gsp_id],
                start_datetime=start_datetime,
                only_return_latest=True,
                forecast_horizon_minutes=forecast_horizon_minutes,
                model=ForecastValueSevenDaysSQL,
                model_name=model_name,
            )

        if len(forecast_values_one_model) == 0:
            logger.debug(
                f"No forecast values for {model_name} for gsp_id {gsp_id} "
                f"and start_datetime {start_datetime}"
            )
        else:
            logger.debug(
                f"Found {len(forecast_values_one_model)} values for {model_name} "
                f"for gsp_id {gsp_id} and start_datetime {start_datetime}. "
                f"First value is {forecast_values_one_model[0].target_time}"
            )
            forecast_values_all_model.append([model_name, forecast_values_one_model])

    # check the created_utc is valid for each forecast
    forecast_values_all_model_valid = check_forecast_created_utc(forecast_values_all_model)

    # make into dataframe
    forecast_values_all_model = convert_list_forecast_values_to_df(forecast_values_all_model_valid)

    # blend together
    forecast_values_blended = blend_forecasts_together(forecast_values_all_model, weights_df)

    # add properties
    forecast_values_df = add_properties_to_forecast_values(
        blended_df=forecast_values_blended,
        properties_model=properties_model,
        all_model_df=forecast_values_all_model,
    )

    # convert back to list of forecast values
    forecast_values = convert_df_to_list_forecast_values(forecast_values_df)

    return forecast_values


def add_properties_to_forecast_values(
    blended_df: pd.DataFrame,
    all_model_df: pd.DataFrame,
    properties_model: Optional[str] = None,
):
    """
    Add properties to blended forecast values, we just take it from one model.

    We normalize all properties by the "expected_power_generation_megawatts" value,
    and renormalize by the blended "expected_power_generation_megawatts" value.
    This makes sure that plevels 10 and 90 surround the blended value.

    :param blended_df: dataframe of blended forecast values
    :param properties_model: which model to use for properties
    :param all_model_df: dataframe of all forecast values for all models
    :return:
    """

    logger.debug(
        f"Adding properties to blended forecast values for properties_model {properties_model}"
    )

    if properties_model is None:
        blended_df["properties"] = None
        return blended_df

    # get properties

    properties_df = all_model_df[all_model_df["model_name"] == properties_model]
    properties_df.reset_index(inplace=True, drop=True)

    # adjust "properties" to be relative to the expected_power_generation_megawatts
    # this is a bit tricky becasue the "properties" column is a list of dictionaries
    # below we add "expected_power_generation_megawatts" value back to this.
    # We do this so that plevels are relative to the blended values.
    properties_only_df = pd.json_normalize(properties_df["properties"])
    for c in properties_only_df.columns:
        properties_only_df[c] -= properties_df["expected_power_generation_megawatts"]
    properties_df["properties"] = properties_only_df.apply(
        lambda x: json.loads(x.to_json()), axis=1
    )

    # reduce columns
    properties_df = properties_df[["target_time", "properties"]]

    # add properties to blended forecast values
    blended_df = blended_df.merge(properties_df, on=["target_time"], how="left")

    # add "expected_power_generation_megawatts" to the properties
    properties_only_df = pd.json_normalize(blended_df["properties"])
    for c in properties_only_df.columns:
        properties_only_df[c] += blended_df["expected_power_generation_megawatts"]
    blended_df["properties"] = properties_only_df.apply(lambda x: json.loads(x.to_json()), axis=1)

    assert "properties" in blended_df.columns

    return blended_df
