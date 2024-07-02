from nowcasting_datamodel.fake import make_fake_forecast

from datetime import datetime, timedelta, timezone

from nowcasting_datamodel.models import MLModel
from nowcasting_datamodel.read.read_models import get_models, get_model


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


def test_get_model(db_session):
    model_read_1 = get_model(session=db_session, name="test_name", version="9.9.9")
    model_read_2 = get_model(session=db_session, name="test_name", version="9.9.9")

    assert model_read_1.name == model_read_2.name
    assert model_read_1.version == model_read_2.version

    _ = MLModel.model_validate(model_read_2, from_attributes=True)
