from nowcasting_datamodel.fake import make_fake_forecast

from datetime import datetime, timedelta, timezone

from nowcasting_datamodel.read.read_models import get_models
from nowcasting_datamodel.read.read import get_model


def test_get_models(db_session):

    _ = make_fake_forecast(session=db_session, gsp_id=1)

    models = get_models(session=db_session)
    assert len(models) == 1


def test_get_models_no_models(db_session):
    models = get_models(session=db_session)
    assert len(models) == 0


def test_get_models_no_forecasts(db_session):
    get_model(session=db_session, name="test")

    models = get_models(session=db_session, with_forecasts=True)
    assert len(models) == 0


def test_get_models_multiple_models(db_session):

    _ = make_fake_forecast(session=db_session, gsp_id=1, model_name="test_1")
    _ = make_fake_forecast(session=db_session, gsp_id=1, model_name="test_2")

    models = get_models(session=db_session)
    assert len(models) == 2


def testget_models_after_created(db_session):
    _ = make_fake_forecast(session=db_session, gsp_id=1, model_name="test_1")

    models = get_models(
        session=db_session,
        with_forecasts=True,
        forecast_created_utc=datetime.now(tz=timezone.utc) + timedelta(days=1),
    )
    assert len(models) == 0
