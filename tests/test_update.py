from datetime import datetime

import pytest

from nowcasting_datamodel.models.models import ForecastSQL, ForecastValueLatestSQL, ForecastValueSQL
from nowcasting_datamodel.update import update_forecast_latest


def test_model_duplicate_key(db_session):

    f1 = ForecastValueLatestSQL(
        gsp_id=1, target_time=datetime(2022, 1, 1), expected_power_generation_megawatts=1
    )

    f2 = ForecastValueLatestSQL(
        gsp_id=1, target_time=datetime(2022, 1, 1), expected_power_generation_megawatts=2
    )

    db_session.add(f1)
    db_session.commit()
    with pytest.raises(Exception):
        db_session.add(f2)
        db_session.commit()


def test_update_one_gsp(forecast_sql, db_session):

    db_session.query(ForecastValueSQL).delete()

    forecast_sql = forecast_sql[0]

    f1 = ForecastValueSQL(target_time=datetime(2022, 1, 1), expected_power_generation_megawatts=1)

    f2 = ForecastValueSQL(
        target_time=datetime(2022, 1, 1, 0, 30), expected_power_generation_megawatts=2
    )

    forecast_sql.forecast_values = [f1, f2]

    update_forecast_latest(forecast=forecast_sql, session=db_session)

    assert len(db_session.query(ForecastValueSQL).all()) == 2
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 2
    assert len(db_session.query(ForecastSQL).all()) == 2

    # new forecast is made

    f3 = ForecastValueSQL(target_time=datetime(2022, 1, 1), expected_power_generation_megawatts=3)

    f4 = ForecastValueSQL(
        target_time=datetime(2022, 1, 1, 0, 30), expected_power_generation_megawatts=4
    )

    forecast_sql.forecast_values = [f3, f4]

    update_forecast_latest(forecast=forecast_sql, session=db_session)

    latest = db_session.query(ForecastValueLatestSQL).all()
    import logging

    logger = logging.getLogger(__name__)
    for f in latest:
        logger.info(f.__dict__)

    assert len(db_session.query(ForecastValueSQL).all()) == 4
    assert len(db_session.query(ForecastValueLatestSQL).all()) == 2
    assert len(db_session.query(ForecastSQL).all()) == 2
