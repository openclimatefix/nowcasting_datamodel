""" Read user"""
import logging
from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models.api import APIRequestSQL, UserSQL

logger = logging.getLogger(__name__)


def get_user(session: Session, email: str) -> UserSQL:
    """
    Get metric object from database

    :param session: database session
    :param email: email of user

    return: One Metric SQl object

    """

    # start main query
    query = session.query(UserSQL)

    # filter on name
    query = query.filter(UserSQL.email == email)

    # get all results
    users = query.all()

    if len(users) == 0:
        logger.debug(f"User for name {email} does not exist so going to add it")

        user = UserSQL(email=email)

        session.add(user)
        session.commit()

    else:
        user = users[0]

    return user


def get_all_last_api_request(session: Session) -> List[APIRequestSQL]:
    """
    Get all last api requests for all users

    :param session: database session
    :return:
    """

    last_requests_sql = (
        session.query(APIRequestSQL)
        .distinct(APIRequestSQL.user_uuid)
        .join(UserSQL)
        .order_by(APIRequestSQL.user_uuid, APIRequestSQL.created_utc.desc())
        .all()
    )

    return last_requests_sql


def get_api_requests_for_one_user(
    session: Session,
    email: str,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
) -> List[APIRequestSQL]:
    """
    Get all api requests for one user

    :param session: database session
    :param email: user email
    :param start_datetime: only get api requests after start datetime
    :param end_datetime: only get api requests before end datetime
    """

    query = session.query(APIRequestSQL).join(UserSQL).filter(UserSQL.email == email)

    if start_datetime is not None:
        query = query.filter(APIRequestSQL.created_utc >= start_datetime)

    if end_datetime is not None:
        query = query.filter(APIRequestSQL.created_utc <= end_datetime)

    api_requests = query.order_by(APIRequestSQL.created_utc.desc()).all()

    return api_requests
