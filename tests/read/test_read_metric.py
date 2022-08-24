import pytest

from nowcasting_datamodel.models import MetricSQL
from nowcasting_datamodel.read.read_metric import get_metric


def test_get_metric(db_session):

    db_session.add(MetricSQL(name="test"))

    metric = get_metric(session=db_session, name="test")
    assert metric.name == "test"
    assert len(db_session.query(MetricSQL).all()) == 1


def test_get_new_metric(db_session):
    assert len(db_session.query(MetricSQL).all()) == 0

    _ = get_metric(session=db_session, name="test")
    metric = get_metric(session=db_session, name="test")
    assert metric.name == "test"
    assert len(db_session.query(MetricSQL).all()) == 1
