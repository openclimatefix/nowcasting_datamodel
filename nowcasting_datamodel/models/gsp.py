""" Pydantic and Sqlalchemy models for the database

1. GSP for storing GSP data (https://www.solar.sheffield.ac.uk/pvlive/)
2. GSP yield for storing GSP data (https://www.solar.sheffield.ac.uk/pvlive/)

"""
import logging
from datetime import datetime
from typing import Optional

from pydantic import Field, validator
from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, and_, select
from sqlalchemy.orm import relationship

from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models.utils import CreatedMixin, EnhancedBaseModel
from nowcasting_datamodel.utils import datetime_must_have_timezone

logger = logging.getLogger(__name__)

########
# 1. GSP Metadata
########


class GSPSQL(Base_Forecast, CreatedMixin):
    """Metadata for GSP data"""

    __tablename__ = "gsp"

    pk = Column(Integer, primary_key=True)
    label = Column(String)
    id = Column(Integer)
    name = Column(String, nullable=True)
    group = Column(String, nullable=True)
    region_name = Column(String, nullable=True)
    status_interval_minutes = Column(Integer, nullable=True, default=30)

    gsp_yield = relationship("GSPYieldSQL", back_populates="gsp")
    forecast = relationship("ForecastSQL", back_populates="location")


class GSP(EnhancedBaseModel):
    """Metadata for GSP data"""

    label: str = Field(..., description="")
    id: Optional[int] = Field(None, description="The Grid Supply Point (GSP) id", index=True)
    name: Optional[str] = Field(None, description="The GSP name")
    group: Optional[str] = Field(None, description="The GSP group name")
    region_name: Optional[str] = Field(None, description="The GSP region name")
    status_interval_minutes: Optional[float] = Field(
        None, description="The number of minutes for the pv data to be refreshed"
    )

    def to_orm(self) -> GSPSQL:
        """Change model to PVSystemSQL"""
        return GSPSQL(
            id=self.id,
            label=self.label,
            name=self.name,
            group=self.group,
            region_name=self.region_name,
            status_interval_minutes=self.status_interval_minutes,
        )


########
# 8. GSP Yield
########


class GSPYieldSQL(Base_Forecast, CreatedMixin):
    """PV Yield data"""

    __tablename__ = "gsp_yield"

    id = Column(Integer, primary_key=True)
    datetime_utc = Column(DateTime, index=True)
    solar_generation_kw = Column(String)

    # many (forecasts) to one (location)
    gsp = relationship("GSPSQL", back_populates="gsp_yield")
    gsp_pk = Column(Integer, ForeignKey("gsp.pk"), index=True)

    Index("ix_datetime_utc", datetime_utc.desc())


class GSPYield(EnhancedBaseModel):
    """GSP Yield data"""

    datetime_utc: datetime = Field(..., description="The timestamp of the pv system")
    solar_generation_kw: float = Field(..., description="The provider of the PV system")

    _normalize_target_time = validator("datetime_utc", allow_reuse=True)(
        datetime_must_have_timezone
    )

    gsp: Optional[GSP] = Field(
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

    def to_orm(self) -> GSPSQL:
        """Change model to GSPSQL"""
        return GSPSQL(
            datetime_utc=self.datetime_utc,
            solar_generation_kw=self.solar_generation_kw,
        )


# Add the last yield value asociated with a pv system.
# This means we can just load the gsp and know the last gsp yield value.
# Helpful advice on
# https://groups.google.com/g/sqlalchemy/c/Vw1iBXSLibI
GSPSQL.last_gsp_yield = relationship(
    GSPYieldSQL,
    primaryjoin=and_(
        GSPSQL.pk == GSPYieldSQL.gsp_pk,
        GSPYieldSQL.datetime_utc
        == (
            select([GSPYieldSQL.datetime_utc])
            .where(GSPSQL.pk == GSPYieldSQL.gsp_pk)
            .order_by(GSPYieldSQL.datetime_utc.desc())
            .limit(1)
            .scalar_subquery()
        ),
    ),
    viewonly=True,
    uselist=False,
)
