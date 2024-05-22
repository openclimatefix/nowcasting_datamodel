""" Read functions for models"""

from typing import Optional
from datetime import datetime
from nowcasting_datamodel.models.models import MLModelSQL
from nowcasting_datamodel.models.forecast import ForecastSQL


def get_models(
    session, with_forecasts: Optional[bool] = False, forecast_created_utc: Optional[datetime] = None
) -> MLModelSQL:
    """
    Get all models from the database, distinct on name

    Can also filter if the model has forecasts, and if they are made after a certain time

    :param session: sql session
    :param with_forecasts: only get models with forecasts
    :param forecast_created_utc: only look at forecast created after this time
    :return:
    """

    query = session.query(MLModelSQL)
    query = query.distinct(MLModelSQL.name)

    if with_forecasts:
        query = query.join(ForecastSQL)

        if forecast_created_utc:
            query = query.filter(ForecastSQL.created_utc > forecast_created_utc)

    # order by
    query = query.order_by(MLModelSQL.name)

    # get all results
    models = query.all()

    return models
