""" Database Connection class"""
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
import architect

from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models.forecast import ForecastValueSQL, ForecastSQL
from nowcasting_datamodel.models.models import MLModelSQL

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Database connection class"""

    def __init__(self, url, base=Base_Forecast, echo: bool = True):
        """
        Set up database connection

        url: the database url, used for connecting
        """
        self.url = url
        self.base = base

        self.engine = create_engine(self.url, echo=echo)

        self.Session = sessionmaker(bind=self.engine)

        self.partitions = []

        assert self.url is not None, Exception("Need to set url for database connection")

    def create_all(self):
        """Create all tables"""

        self.base.metadata.create_all(self.engine)

        # TODO add more
        self.partitions.append(ForecastValueSQL.create_partition('2021_01', '2021_02'))
        self.partitions.append(ForecastValueSQL.create_partition('2022_01', '2022_02'))

        # make partitions
        for partition in self.partitions:
            if not self.engine.dialect.has_table(connection=self.engine.connect(), table_name=partition.__table__.name):
                partition.__table__.create(bind=self.engine)

    def drop_all(self):

        # drop partitions
        for partition in self.partitions:
            if not self.engine.dialect.has_table(connection=self.engine.connect(), table_name=partition.__table__.name):
                partition.__table__.drop(bind=self.engine)

        Base_Forecast.metadata.drop_all(self.engine)

    def get_session(self) -> Session:
        """Get sqlalamcy session"""
        return self.Session()
