""" Read user"""
import logging

from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models.api import UserSQL

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
