from nowcasting_datamodel.fake import make_fake_forecasts, make_fake_pv_system
from nowcasting_datamodel.models.pv import PVSystem, PVSystemSQL
from nowcasting_datamodel.models.models import ForecastSQL
from nowcasting_datamodel.save import save, save_pv_system


def test_save(db_session):
    forecasts = make_fake_forecasts(gsp_ids=range(0, 10), session=db_session)

    save(session=db_session, forecasts=forecasts)

    # 10 forecast, + 10 historic ones
    assert len(db_session.query(ForecastSQL).all()) == 20

    forecasts = make_fake_forecasts(gsp_ids=range(0, 10), session=db_session)
    save(session=db_session, forecasts=forecasts)

    # 20 forecast, + 10 historic ones
    assert len(db_session.query(ForecastSQL).all()) == 30


def test_save_pv_system(db_session_pv):

    pv_systems = db_session_pv.query(PVSystemSQL).all()
    assert len(pv_systems) == 0

    pv_system = PVSystem(pv_system_id=2, provider="pvoutput.org", latitude=55, longitude=0)

    save_pv_system(session=db_session_pv, pv_system=pv_system)

    pv_systems = db_session_pv.query(PVSystemSQL).all()
    assert len(pv_systems) == 1

    save_pv_system(session=db_session_pv, pv_system=pv_system)

    pv_systems = db_session_pv.query(PVSystemSQL).all()
    assert len(pv_systems) == 1
