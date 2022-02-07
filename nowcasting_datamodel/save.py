""" Save forecasts to the database """
from typing import List

from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models import ForecastSQL


def save(forecasts: List[ForecastSQL], session: Session):
    """
    Save forecast to database

    1. Change pydantic object to sqlalchemy objects
    2. Add sqlalchemy onjects to database

    :param forecasts: list of pydantic forecasts
    :param session: database session
    """

    # save objects to database
    session.add_all(forecasts)
    session.commit()
