""" Functions used to make fake forecasts"""
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import numpy as np
from sqlalchemy.orm import Session

from nowcasting_datamodel.models import (
    ForecastSQL,
    ForecastValueSQL,
    InputDataLastUpdatedSQL,
    MLModelSQL,
    PVSystemSQL,
    national_gb_label,
)
from nowcasting_datamodel.models.gsp import LocationSQL
from nowcasting_datamodel.read.read import get_location, get_model


def make_fake_location(gsp_id: int) -> LocationSQL:
    """Make fake location with gsp id"""
    return LocationSQL(label=f"GSP_{gsp_id}", gsp_id=gsp_id)


def make_fake_pv_system() -> PVSystemSQL:
    """Make fake location with gsp id"""
    return PVSystemSQL(pv_system_id=1, provider="pvoutput.org", latitude=55, longitude=0)


def make_fake_input_data_last_updated() -> InputDataLastUpdatedSQL:
    """Make fake input data last updated"""
    now = datetime.now(tz=timezone.utc)

    return InputDataLastUpdatedSQL(gsp=now, nwp=now, pv=now, satellite=now)


def make_fake_forecast_value(target_time) -> ForecastValueSQL:
    """Make fake fprecast value"""
    return ForecastValueSQL(
        target_time=target_time, expected_power_generation_megawatts=np.random.random()
    )


def make_fake_forecast(
    gsp_id: int, session: Session, t0_datetime_utc: Optional[datetime] = None
) -> ForecastSQL:
    """Make one fake forecast"""
    location = get_location(gsp_id=gsp_id, session=session)
    model = get_model(name="fake_model", session=session, version="0.1.2")
    input_data_last_updated = make_fake_input_data_last_updated()

    if t0_datetime_utc is None:
        t0_datetime_utc = datetime(2022, 1, 1, tzinfo=timezone.utc)

    # create
    forecast_values = []
    for target_datetime_utc in [t0_datetime_utc, t0_datetime_utc + timedelta(minutes=30)]:
        f = make_fake_forecast_value(target_time=target_datetime_utc)
        forecast_values.append(f)

    forecast = ForecastSQL(
        model=model,
        forecast_creation_time=t0_datetime_utc,
        location=location,
        input_data_last_updated=input_data_last_updated,
        forecast_values=forecast_values,
    )

    return forecast


def make_fake_forecasts(
    gsp_ids: List[int], session: Session, t0_datetime_utc: Optional[datetime] = None
) -> List[ForecastSQL]:
    """Make many fake forecast"""
    forecasts = []
    for gsp_id in gsp_ids:
        forecasts.append(
            make_fake_forecast(gsp_id=gsp_id, t0_datetime_utc=t0_datetime_utc, session=session)
        )

    return forecasts


def make_fake_national_forecast(t0_datetime_utc: Optional[datetime] = None) -> ForecastSQL:
    """Make national fake forecast"""
    location = LocationSQL(label=national_gb_label)
    model = MLModelSQL(name="fake_model_national", version="0.1.2")
    input_data_last_updated = make_fake_input_data_last_updated()

    if t0_datetime_utc is None:
        t0_datetime_utc = datetime(2022, 1, 1, tzinfo=timezone.utc)

    # create
    forecast_values = []
    for target_datetime_utc in [t0_datetime_utc, t0_datetime_utc + timedelta(minutes=30)]:
        f = make_fake_forecast_value(target_time=target_datetime_utc)
        forecast_values.append(f)

    forecast = ForecastSQL(
        model=model,
        forecast_creation_time=t0_datetime_utc,
        location=location,
        input_data_last_updated=input_data_last_updated,
        forecast_values=forecast_values,
    )

    return forecast
