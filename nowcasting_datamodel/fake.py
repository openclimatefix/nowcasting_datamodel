""" Functions used to make fake forecasts"""
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import numpy as np
from sqlalchemy.orm import Session

from nowcasting_datamodel.models import (
    InputDataLastUpdatedSQL,
    MLModelSQL,
    PVSystemSQL,
    national_gb_label,
)
from nowcasting_datamodel.models.forecast import ForecastSQL, ForecastValueSQL
from nowcasting_datamodel.models.gsp import GSPYieldSQL, LocationSQL
from nowcasting_datamodel.read.read import get_location, get_model
from nowcasting_datamodel.update import change_forecast_value_to_latest

# 2 days in the past + 8 hours forward at 30 mins interval
N_FAKE_FORECASTS = (24 * 2 + 8) * 2


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


def make_fake_forecast_value(target_time, forecast_maximum: Optional[int] = 1) -> ForecastValueSQL:
    """Make fake fprecast value"""
    return ForecastValueSQL(
        target_time=target_time,
        expected_power_generation_megawatts=np.random.random() * forecast_maximum,
    )


def make_fake_forecast(
    gsp_id: int,
    session: Session,
    t0_datetime_utc: Optional[datetime] = None,
    forecast_values: Optional = None,
    forecast_values_latest: Optional = None,
    add_latest: Optional[bool] = False,
    historic: Optional[bool] = False,
) -> ForecastSQL:
    """Make one fake forecast"""

    if gsp_id == 0:
        # national capacity
        installed_capacity_mw = 13000
    else:
        # gsp capacity (roughly)
        installed_capacity_mw = 10

    location = get_location(
        gsp_id=gsp_id, session=session, installed_capacity_mw=installed_capacity_mw
    )

    model = get_model(name="fake_model", session=session, version="0.1.2")
    input_data_last_updated = make_fake_input_data_last_updated()

    if t0_datetime_utc is None:
        t0_datetime_utc = datetime(2023, 1, 1, tzinfo=timezone.utc)

    # create
    if forecast_values is None:
        forecast_values = []
        # 2 days in the past + 8 hours forward
        for i in range(N_FAKE_FORECASTS):
            target_datetime_utc = t0_datetime_utc + timedelta(minutes=i * 30) - timedelta(days=2)
            f = make_fake_forecast_value(
                target_time=target_datetime_utc, forecast_maximum=location.installed_capacity_mw
            )
            forecast_values.append(f)

    if forecast_values_latest is None:
        forecast_values_latest = []

        if add_latest:
            for forecast_value in forecast_values:
                forecast_values_latest.append(
                    change_forecast_value_to_latest(forecast_value, gsp_id=location.gsp_id)
                )

    forecast = ForecastSQL(
        model=model,
        forecast_creation_time=t0_datetime_utc,
        location=location,
        input_data_last_updated=input_data_last_updated,
        forecast_values=forecast_values,
        forecast_values_latest=forecast_values_latest,
        historic=historic,
    )

    return forecast


def make_fake_forecasts(
    gsp_ids: List[int],
    session: Session,
    t0_datetime_utc: Optional[datetime] = None,
    forecast_values: Optional = None,
    add_latest: Optional[bool] = False,
    historic: Optional[bool] = False,
) -> List[ForecastSQL]:
    """Make many fake forecast"""
    forecasts = []
    for gsp_id in gsp_ids:
        forecasts.append(
            make_fake_forecast(
                gsp_id=gsp_id,
                t0_datetime_utc=t0_datetime_utc,
                session=session,
                forecast_values=forecast_values,
                add_latest=add_latest,
                historic=historic,
            )
        )

    session.add_all(forecasts)

    return forecasts


def make_fake_national_forecast(
    session: Session, t0_datetime_utc: Optional[datetime] = None
) -> ForecastSQL:
    """Make national fake forecast"""
    location = get_location(gsp_id=0, session=session, label=national_gb_label)
    model = MLModelSQL(name="fake_model_national", version="0.1.2")
    input_data_last_updated = make_fake_input_data_last_updated()

    if t0_datetime_utc is None:
        t0_datetime_utc = datetime(2023, 1, 1, tzinfo=timezone.utc)

    # create
    forecast_values = []
    for i in range(N_FAKE_FORECASTS):
        target_datetime_utc = t0_datetime_utc + timedelta(minutes=i * 30) - timedelta(days=2)
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


def make_fake_gsp_yields_for_one_location(
    gsp_id: int, session: Session, t0_datetime_utc: Optional[datetime] = None
):
    """
    Make GSP yields for one locations

    :param gsp_id: the gsp id you want gsp yields for
    :param session: database sessions
    :param t0_datetime_utc: the time now.
    """

    if gsp_id == 0:
        # national capacity
        installed_capacity_mw = 13000
    else:
        # gsp capacity (roughly)
        installed_capacity_mw = 10

    if t0_datetime_utc is None:
        t0_datetime_utc = datetime(2022, 1, 1, tzinfo=timezone.utc)

    location = get_location(
        session=session, gsp_id=gsp_id, installed_capacity_mw=installed_capacity_mw
    )

    # make 2 days of fake data
    for i in range(48 * 2):
        datetime_utc = t0_datetime_utc - timedelta(days=2) + timedelta(minutes=i * 30)
        gsp_yield = GSPYieldSQL(
            datetime_utc=datetime_utc,
            solar_generation_kw=np.random.random() * installed_capacity_mw,
            regime="in-day",
        )
        gsp_yield.location = location

        session.add(gsp_yield)

        gsp_yield = GSPYieldSQL(
            datetime_utc=datetime_utc, solar_generation_kw=3, regime="day-after"
        )
        gsp_yield.location = location

        session.add(gsp_yield)


def make_fake_gsp_yields(
    session: Session, gsp_ids: List[int], t0_datetime_utc: Optional[datetime] = None
):
    """
    Make GSP yields for many locations

    :param gsp_ids: the gsp ids you want gsp yields for
    :param session: database sessions
    :param t0_datetime_utc: the time now.
    """

    for gsp_id in gsp_ids:
        make_fake_gsp_yields_for_one_location(
            gsp_id=gsp_id, session=session, t0_datetime_utc=t0_datetime_utc
        )
