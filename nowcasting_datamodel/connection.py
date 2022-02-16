""" Database Connection class"""
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.orm.session import Session

Base_Forecast = declarative_base()
Base_PV = declarative_base()

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Database connection class"""

    def __init__(self, url, base=Base_Forecast):
        """
        Set up database connection

        url: the database url, used for connecting
        """
        self.url = url

        self.engine = create_engine(self.url, echo=True)

        # quick and easy way to make sure the database table names are made.
        # This should be moved, or done separate from here.
        try:
            base.metadata.create_all(self.engine)
        except Exception as e:
            logger.debug(
                f'Try to run "create all" on database, but failed. ' f"Will pass this anyway {e}"
            )

        self.Session = sessionmaker(bind=self.engine)

    def get_session(self) -> Session:
        """Get sqlalamcy session"""
        return self.Session()
