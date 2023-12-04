from nowcasting_datamodel.models import UserSQL, APIRequestSQL
from nowcasting_datamodel.read.read_user import (
    get_user,
    get_all_last_api_request,
    get_api_requests_for_one_user,
)


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


def test_get_all_last_api_request(db_session):
    user = get_user(session=db_session, email="test@test.com")
    db_session.add(APIRequestSQL(user_uuid=user.uuid, url="test"))
    db_session.add(APIRequestSQL(user_uuid=user.uuid, url="test2"))

    last_requests_sql = get_all_last_api_request(session=db_session)
    assert len(last_requests_sql) == 1
    assert last_requests_sql[0].url == "test2"
    assert last_requests_sql[0].user_uuid == user.uuid


def test_get_api_requests_for_one_user(db_session):
    user = get_user(session=db_session, email="test@test.com")
    db_session.add(APIRequestSQL(user_uuid=user.uuid, url="test"))

    requests_sql = get_api_requests_for_one_user(session=db_session, email=user.email)
    assert len(requests_sql) == 1
    assert requests_sql[0].url == "test"
