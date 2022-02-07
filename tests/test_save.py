from nowcasting_datamodel.fake import make_fake_forecasts
from nowcasting_datamodel.save import save


def test_save(db_session):
    forecasts = make_fake_forecasts(gsp_ids=range(0, 10), session=db_session)

    save(session=db_session, forecasts=forecasts)
