""" Functions to convert objects """

from typing import List

from nowcasting_datamodel.models import Forecast, ForecastValue, ForecastValueSevenDaysSQL


def convert_list_forecast_value_seven_days_sql_to_list_forecast(
    forecast_values_sql=List[ForecastValueSevenDaysSQL],
) -> List[Forecast]:
    """
    Convert a list of sql ForecastValueSevenDaysSQL to pydantic Forecast objects

    :param forecast_values_sql: forecast value objects
    :return: list of Forecast pydantic objects
    """

    # dictionary of forecasts by gsp_id
    forecasts_by_gsp = {}
    for forecast_value_sql in forecast_values_sql:

        gsp_id = forecast_value_sql.forecast.location.gsp_id

        forecast_value: ForecastValue = ForecastValue.from_orm(forecast_value_sql)

        if gsp_id in forecasts_by_gsp.keys():
            forecasts_by_gsp[gsp_id].forecast_values.append(forecast_value)
        else:
            forecast = Forecast.from_orm(forecast_value_sql.forecast)
            forecasts_by_gsp[gsp_id] = forecast

    forecasts = [forecast for forecast in forecasts_by_gsp.values()]

    return forecasts
