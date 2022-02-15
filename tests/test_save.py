from nowcasting_datamodel.fake import make_fake_forecasts, make_fake_pv_system
from nowcasting_datamodel.save import save, save_pv_system


def test_save(db_session):
    forecasts = make_fake_forecasts(gsp_ids=range(0, 10), session=db_session)

    save(session=db_session, forecasts=forecasts)


def test_save_pv_system(db_session):
    pv_system = make_fake_pv_system()

    save_pv_system(session=db_session, pv_system=pv_system)
