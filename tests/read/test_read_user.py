from nowcasting_datamodel.models import UserSQL
from nowcasting_datamodel.read.read_user import get_user


def test_get_user(db_session):
    db_session.add(UserSQL(email="test@test.com"))

    user = get_user(session=db_session, email="test@test.com")
    assert user.email == "test@test.com"
    assert len(db_session.query(UserSQL).all()) == 1


def test_get_new_metric(db_session):
    assert len(db_session.query(UserSQL).all()) == 0

    _ = get_user(session=db_session, email="test@test.com")
    user = get_user(session=db_session, email="test@test.com")
    assert user.email == "test@test.com"
    assert len(db_session.query(UserSQL).all()) == 1
