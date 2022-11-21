""" Pydantic and Sqlalchemy models for the database

2. Location objects, where the forecast is for
8. GSP yield for storing GSP yield data

"""
import logging
from datetime import datetime
from typing import List, Optional

from pydantic import Field, validator
from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import relationship

from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models.utils import CreatedMixin, EnhancedBaseModel
from nowcasting_datamodel.utils import datetime_must_have_timezone

logger = logging.getLogger(__name__)

########
# 2. Location
########


class LocationSQL(Base_Forecast):
    """Location that the forecast is for"""

    __tablename__ = "location"

    id = Column(Integer, primary_key=True)
    label = Column(String)
    gsp_id = Column(Integer, index=True, unique=True, nullable=False)
    gsp_name = Column(String, nullable=True)
    gsp_group = Column(String, nullable=True)
    region_name = Column(String, nullable=True)
    installed_capacity_mw = Column(Integer, nullable=True)

    forecast = relationship("ForecastSQL", back_populates="location")
    gsp_yields = relationship("GSPYieldSQL", back_populates="location")
    metric_value = relationship("MetricValueSQL", back_populates="location")


class Location(EnhancedBaseModel):
    """Location that the forecast is for"""

    label: str = Field(..., description="")
    gsp_id: Optional[int] = Field(None, description="The Grid Supply Point (GSP) id", index=True)
    gsp_name: Optional[str] = Field(None, description="The GSP name")
    gsp_group: Optional[str] = Field(None, description="The GSP group name")
    region_name: Optional[str] = Field(None, description="The GSP region name")
    installed_capacity_mw: Optional[int] = Field(
        None, description="The installed capacity of the GSP in MW"
    )

    rm_mode = True

    def to_orm(self) -> LocationSQL:
        """Change model to LocationSQL"""

        if isinstance(self, LocationSQL):
            return self

        return LocationSQL(
            label=self.label,
            gsp_id=self.gsp_id,
            gsp_name=self.gsp_name,
            gsp_group=self.gsp_group,
            region_name=self.region_name,
            installed_capacity_mw=self.installed_capacity_mw,
        )


class GSPYieldSQL(Base_Forecast, CreatedMixin):
    """GSP Yield data"""

    __tablename__ = "gsp_yield"

    id = Column(Integer, primary_key=True)
    datetime_utc = Column(DateTime, index=True)
    solar_generation_kw = Column(Float)
    regime = Column(String, nullable=True)

    # many (gsp_yields) to one (location)
    location = relationship("LocationSQL", back_populates="gsp_yields")
    location_id = Column(Integer, ForeignKey("location.id"), index=True)

    Index("ix_gsp_yield_datetime_utc_desc", datetime_utc.desc())


class GSPYield(EnhancedBaseModel):
    """GSP Yield data"""

    datetime_utc: datetime = Field(..., description="The timestamp of the gsp yield")
    solar_generation_kw: float = Field(..., description="The amount of solar generation")
    regime: str = Field(
        "in-day", description="When the GSP data is pulled, can be 'in-day' or 'day-after'"
    )

    _normalize_target_time = validator("datetime_utc", allow_reuse=True)(
        datetime_must_have_timezone
    )

    gsp: Optional[Location] = Field(
        None,
        description="The GSP associated with this model",
    )

    @validator("solar_generation_kw")
    def validate_solar_generation_kw(cls, v):
        """Validate the solar_generation_kw field"""
        if v < 0:
            logger.debug(f"Changing solar_generation_kw ({v}) to 0")
            v = 0
        return v

    @validator("regime")
    def validate_regime(cls, v):
        """Validate the solar_generation_kw field"""
        if v not in ["day-after", "in-day"]:
            message = f"Regime ({v}) not in 'day-after', 'in-day'"
            logger.debug(message)
            raise Exception(message)
        return v

    def to_orm(self) -> GSPYieldSQL:
        """Change model to GSPYieldSQL"""
        return GSPYieldSQL(
            datetime_utc=self.datetime_utc,
            solar_generation_kw=self.solar_generation_kw,
            regime=self.regime,
        )


class LocationWithGSPYields(Location):
    """Location object with GSPYields"""

    gsp_yields: Optional[List[GSPYield]] = Field([], description="List of gsp yields")
