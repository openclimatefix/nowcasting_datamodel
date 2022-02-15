""" Pydantic and Sqlalchemy models for the database

7. PV system for storing PV data (pv.py)
8. PV yield for storing PV data (pv.py)

"""
import logging
from typing import Optional
from datetime import datetime

from pydantic import Field, validator
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from nowcasting_datamodel.connection import Base
from nowcasting_datamodel.models.utils import CreatedMixin, EnhancedBaseModel
from nowcasting_datamodel.utils import datetime_must_have_timezone

logger = logging.getLogger(__name__)

providers = ["pvoutput.org"]

########
# 7. PV Metadata
########


class PVSystemSQL(Base, CreatedMixin):
    """Metadata for PV data"""

    __tablename__ = "pv_system"

    id = Column(Integer, primary_key=True)
    pv_system_id = Column(Integer, index=True)
    provider = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    name = Column(String, nullable=True)
    orientation = Column(Float, nullable=True)

    pv_yield = relationship("PVYieldSQL", back_populates="pv_system")


class PVSystem(EnhancedBaseModel):
    """Metadata for PV data"""

    pv_system_id: int = Field(..., description="The PV system id")
    provider: str = Field(..., description="The provider of the PV system")
    latitude: float = Field(..., description="The latitude of the PV system")
    longitude: float = Field(..., description="The longitude of the PV system")
    name: Optional[str] = Field(None, description="The PV system name")
    orientation: Optional[float] = Field(None, description="The orientation of the PV system")

    rm_mode = True

    @validator("provider")
    def validate_provider(cls, v):
        """Validate the provider"""
        if v not in providers:
            raise Exception(f"Provider ({v}) must be in {providers}")
        return v

    def to_orm(self) -> PVSystemSQL:
        """Change model to PVSystemSQL"""
        return PVSystemSQL(
            pv_system_id=self.pv_system_id,
            provider=self.provider,
            latitude=self.latitude,
            longitude=self.longitude,
            name=self.name,
            orientation=self.orientation,
        )


########
# 8. PV Yield
########


class PVYieldSQL(Base, CreatedMixin):
    """PV Yield data"""

    __tablename__ = "pv_yield"

    id = Column(Integer, primary_key=True)
    datetime_utc = Column(DateTime, index=True)
    solar_generation_kw = Column(String)

    # many (forecasts) to one (location)
    pv_system = relationship("PVSystemSQL", back_populates="pv_yield")
    pv_system_id = Column(Integer, ForeignKey("pv_system.id"), index=True)


class PVYield(EnhancedBaseModel):
    """PV Yield data"""

    datetime_utc: datetime = Field(..., description="The timestamp of the pv system")
    solar_generation_kw: float = Field(..., description="The provider of the PV system")

    _normalize_target_time = validator("datetime_utc", allow_reuse=True)(
        datetime_must_have_timezone
    )

    @validator("solar_generation_kw")
    def validate_solar_generation_kw(cls, v):
        """Validate the solar_generation_kw field"""
        if v < 0:
            logger.debug(f"Changing solar_generation_kw ({v}) to 0")
            v = 0
        return v

    rm_mode = True

    def to_orm(self) -> PVYieldSQL:
        """Change model to PVYieldSQL"""
        return PVYieldSQL(
            datetime_utc=self.datetime_utc,
            solar_generation_kw=self.solar_generation_kw,
        )