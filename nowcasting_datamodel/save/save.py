""" Save forecasts to the database """

import logging
import os
from typing import List, Optional

from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models import MLModelSQL, PVSystem, PVSystemSQL
from nowcasting_datamodel.models.forecast import ForecastSQL, ForecastValueSevenDaysSQL
from nowcasting_datamodel.save.adjust import add_adjust_to_forecasts
from nowcasting_datamodel.save.update import (
    add_forecast_last_7_days_and_remove_old_data,
    change_forecast_value_to_forecast_last_7_days,
    update_all_forecast_latest,
)

logger = logging.getLogger(__name__)


def save(
    forecasts: List[ForecastSQL],
    session: Session,
    update_national: Optional[bool] = True,
    update_gsp: Optional[bool] = True,
    apply_adjuster: Optional[bool] = True,
    save_to_last_seven_days: Optional[bool] = True,
    save_distinct_last_seven_days: bool = True,
):
    """
    Save forecast to database

    1. Add sqlalchemy onjects to database
    2. Saves to 'latest' table aswell

    Note that apply_adjuster=True can be overwritten by "USE_ADJUSTER" env var

    :param forecasts: list of sql forecasts
    :param session: database session
    :param update_national: Optional (default true), to update the national forecast
    :param update_gsp: Optional (default true), to update all the GSP forecasts
    :param apply_adjuster: Optional (default true), to apply the adjuster
    :param save_to_last_seven_days: Optional (default true), to save to the last seven days table
    :param save_distinct_last_seven_days: Optional (default True), to only save distinct
        forecast values in the forecast_value_last_seven_days table
    """

    use_adjuster_env_var = bool(os.getenv("USE_ADJUSTER", "True").lower() in ["true", "1"])
    if apply_adjuster & (not use_adjuster_env_var):
        logger.warning(
            "USE_ADJUSTER is set to False, but apply_adjuster is set to True. "
            "Therefore the adjuster will not be applied to the forecasts."
        )
        apply_adjuster = False

    if apply_adjuster:
        logger.debug("Add Adjust to forecasts")
        add_adjust_to_forecasts(session=session, forecasts_sql=forecasts)

    # save objects to database
    logger.debug("Saving models")
    session.add_all(forecasts)
    session.commit()

    logger.debug("Updating to latest")
    update_all_forecast_latest(
        session=session, forecasts=forecasts, update_national=update_national, update_gsp=update_gsp
    )
    session.commit()

    if save_to_last_seven_days:
        logger.debug("Saving to last seven days table")
        save_all_forecast_values_seven_days(
            session=session, forecasts=forecasts, save_distinct=save_distinct_last_seven_days
        )
        session.commit()


def save_pv_system(session: Session, pv_system: PVSystem) -> PVSystemSQL:
    """
    Get model object from name and version

    :param session: database session
    :param pv_system: pv system sql object

    return: PVSystem object

    """

    # start main query
    query = session.query(PVSystemSQL)

    # filter on pv_system_id and provider
    query = query.filter(PVSystemSQL.pv_system_id == pv_system.pv_system_id)
    query = query.filter(PVSystemSQL.provider == pv_system.provider)

    # get all results
    pv_systems = query.all()

    if len(pv_systems) == 0:
        logger.debug(
            f"Model for provider {pv_system.provider} and pv_system_id {pv_system.pv_system_id }"
            f"does not exist so going to add it"
        )

        session.add(pv_system.to_orm())
        session.commit()

    else:
        logger.debug(
            f"Model for provider {pv_system.provider} and pv_system_id {pv_system.pv_system_id} "
            f"is already there so not going to add it"
        )
        pv_system = pv_systems[0]

    return pv_system


def save_all_forecast_values_seven_days(
    session: Session, forecasts: List[ForecastSQL], save_distinct: bool = True
):
    """
    Save all the forecast values in the last seven days table

    :param session: database sessions
    :param forecasts: list of forecasts
    :param save_distinct: Optional (default True), to only save distinct forecast values
        If the last saved forecast value is the same as the current one, it will not be saved
    """

    # get all values together
    forecast_values_last_7_days = []
    for forecast in forecasts:
        for forecast_value in forecast.forecast_values:
            forecast_value_last_7_days = change_forecast_value_to_forecast_last_7_days(
                forecast_value
            )

            if save_distinct:
                # only keep it if its unique
                # this does take extra time, but it keeps the database smaller
                forecast_is_new = is_new_forecast_values_distinct(
                    forecast_value=forecast_value_last_7_days,
                    session=session,
                    location_id=forecast.location_id,
                    model_name=forecast.model.name,
                )
                if forecast_is_new:
                    forecast_values_last_7_days.append(forecast_value_last_7_days)
            else:
                forecast_values_last_7_days.append(forecast_value_last_7_days)

    # add them to the database
    add_forecast_last_7_days_and_remove_old_data(
        session=session, forecast_values=forecast_values_last_7_days
    )


def is_new_forecast_values_distinct(forecast_value, session, location_id, model_name):
    """
    Keep only new forecast values, that are different from current ones in the database

    The forecast values are matched on
    - location
    - target_time
    - model name

    :param forecast_value: forecast value
    :param session: database session
    :param location_id: location id
    :param model_name: model name
    :return: list of new forecast values, different from current ones in the database
    """

    # make order by and disntict columns
    distinct_columns = [
        ForecastSQL.location_id,
        ForecastValueSevenDaysSQL.target_time,
        ForecastSQL.model_id,
    ]
    order_by_columns = distinct_columns + [ForecastValueSevenDaysSQL.created_utc.desc()]

    # start qiery
    query = session.query(ForecastValueSevenDaysSQL)
    query = query.distinct(*distinct_columns)
    query = query.join(ForecastSQL)
    query = query.join(MLModelSQL)

    # filters
    query = query.filter(ForecastValueSevenDaysSQL.target_time == forecast_value.target_time)
    query = query.filter(ForecastSQL.location_id == location_id)
    query = query.filter(MLModelSQL.name == model_name)

    # order
    query = query.order_by(*order_by_columns)

    # get value
    last_forecast_value = query.first()

    if last_forecast_value is not None:
        if (
            last_forecast_value.expected_power_generation_megawatts
            != forecast_value.expected_power_generation_megawatts
        ):
            return True
        else:
            return False
    else:
        return True
