""" Read database functions

1. Get the one metric
2. get datetimer interval
"""
import logging
from typing import List, Optional

from sqlalchemy.orm.session import Session


from nowcasting_datamodel.models.metric import MetricSQL, DatetimeIntervalSQL

logger = logging.getLogger(__name__)


def get_metric(session: Session, name: str) -> MetricSQL:
    """
    Get metric object from name

    :param session: database session
    :param name: name of the metric

    return: One Metric SQl object

    """

    # start main query
    query = session.query(MetricSQL)

    # filter on gsp_id
    query = query.filter(MetricSQL.name == name)

    # get all results
    metrics = query.all()

    if len(metrics) == 0:
        logger.debug(f"Metric for name {name} does not exist so going to add it")

        metric = MetricSQL(name=name, description="TODO need to update")

        session.add(metric)
        session.commit()

    else:
        metric = metrics[0]

    return metric
