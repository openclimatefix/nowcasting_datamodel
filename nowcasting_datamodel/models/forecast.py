""" Pydantic and Sqlalchemy models for the database

The following class are made
1. ForecastValue objects, specific values of a forecast and time
2. Forecasts, a forecast that is made for one gsp, for several time steps into the future

"""

import logging
from datetime import datetime
from typing import List

from pydantic import Field, validator
from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Index, Integer, event, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import DeclarativeMeta, declared_attr
from sqlalchemy.orm import relationship
from sqlalchemy.sql.ddl import DDL

from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models.gsp import Location
from nowcasting_datamodel.models.models import InputDataLastUpdated, MLModel
from nowcasting_datamodel.models.utils import CreatedMixin, EnhancedBaseModel
from nowcasting_datamodel.utils import datetime_must_have_timezone

logger = logging.getLogger(__name__)

"""
Tried to follow the example here
https://stackoverflow.com/questions/61545680/postgresql-partition-and-sqlalchemy
"""


class PartitionByMeta(DeclarativeMeta):
    """Parition table meta object"""

    def __new__(cls, clsname, bases, attrs, *, partition_by, partition_type: str = "RANGE"):
        """Make new partition"""

        @classmethod
        def get_partition_name(cls_, suffix):
            """Get the name of the partition table"""
            return f"{cls_.__tablename__}_{suffix}"

        @classmethod
        def create_partition(
            cls_, suffix, year_month_end, subpartition_by=None, subpartition_type=None
        ):
            """Create new partitions"""
            if suffix not in cls_.partitions:
                partition = PartitionByMeta(
                    f"{clsname}{suffix}",
                    bases,
                    {"__tablename__": cls_.get_partition_name(suffix)},
                    partition_type=subpartition_type,
                    partition_by=subpartition_by,
                )

                partition.__table__.add_is_dependent_on(cls_.__table__)

                event.listen(
                    partition.__table__,
                    "after_create",
                    DDL(
                        # For non-year ranges, modify the FROM and TO below
                        # LIST: IN ('first', 'second');
                        # RANGE: FROM ('{key}-01-01') TO ('{key+1}-01-01')
                        f"""
                        ALTER TABLE {cls_.__tablename__}
                        ATTACH PARTITION {partition.__tablename__}
                        FOR VALUES FROM ('{suffix}-01') TO ('{year_month_end}-01');
                        """
                    ),
                )

                cls_.partitions[suffix] = partition

            return cls_.partitions[suffix]

        if partition_by is not None:
            attrs.update(
                {
                    "__table_args__": attrs.get("__table_args__", ())
                    + (dict(postgresql_partition_by=f"{partition_type.upper()}({partition_by})"),),
                    "partitions": {},
                    "partitioned_by": partition_by,
                    "get_partition_name": get_partition_name,
                    "create_partition": create_partition,
                }
            )

        return super().__new__(cls, clsname, bases, attrs)


class ForecastValueSQLMixin(CreatedMixin):
    """One Forecast of generation at one timestamp

    This Mixin is used to create partition tables
    """

    uuid = Column(UUID, primary_key=True, server_default=func.gen_random_uuid())
    target_time = Column(DateTime(timezone=True), index=True, nullable=False, primary_key=True)
    expected_power_generation_megawatts = Column(Float)

    @declared_attr
    def forecast_id(self):
        """Link with Forecast table"""
        return Column(Integer, ForeignKey("forecast.id"), index=True)


class ForecastValueSQL(
    ForecastValueSQLMixin, Base_Forecast, metaclass=PartitionByMeta, partition_by="target_time"
):
    """One Forecast of generation at one timestamp

    This Mixin is used to creat partition tables
    """

    __tablename__ = "forecast_value"


def get_partitions(start_year: int, end_year: int):
    """Make partitions"""
    partitions = []
    for year in range(start_year, end_year):
        for month in range(1, 13):
            if month == 12:
                year_end = year + 1
                month_end = 1
            else:
                year_end = year
                month_end = month + 1

            if month < 10:
                month = f"0{month}"
            if month_end < 10:
                month_end = f"0{month_end}"

            partitions.append(
                ForecastValueSQL.create_partition(f"{year}_{month}", f"{year_end}_{month_end}")
            )

    return partitions


class ForecastValueLatestSQL(Base_Forecast, CreatedMixin):
    """One Forecast of generation at one timestamp

    Thanks for the help from
    https://www.johbo.com/2016/creating-a-partial-unique-index-with-sqlalchemy-in-postgresql.html
    """

    __tablename__ = "forecast_value_latest"

    # add a unique condition on 'gsp_id' and 'target_time'
    __table_args__ = (
        Index(
            "uix_1",  # Index name
            "gsp_id",
            "target_time",  # Columns which are part of the index
            unique=True,
            postgresql_where=Column("is_primary"),  # The condition
        ),
    )

    target_time = Column(DateTime(timezone=True), index=True, primary_key=True)
    expected_power_generation_megawatts = Column(Float)
    gsp_id = Column(Integer, index=True, primary_key=True)
    is_primary = Column(Boolean, default=True)

    forecast_id = Column(Integer, ForeignKey("forecast.id"), index=True)
    forecast_latest = relationship("ForecastSQL", back_populates="forecast_values_latest")

    Index("index_forecast_value_latest", CreatedMixin.created_utc.desc())


class ForecastValue(EnhancedBaseModel):
    """One Forecast of generation at one timestamp"""

    target_time: datetime = Field(
        ...,
        description="The target time that the forecast is produced for",
    )
    expected_power_generation_megawatts: float = Field(
        ..., ge=0, description="The forecasted value in MW"
    )

    expected_power_generation_normalized: float = Field(
        None, ge=0, description="The forecasted value divided by the gsp capacity [%]"
    )

    _normalize_target_time = validator("target_time", allow_reuse=True)(datetime_must_have_timezone)

    def to_orm(self) -> ForecastValueSQL:
        """Change model to ForecastValueSQL"""
        return ForecastValueSQL(
            target_time=self.target_time,
            expected_power_generation_megawatts=self.expected_power_generation_megawatts,
        )

    def normalize(self, installed_capacity_mw):
        """Normalize forecasts by installed capacity mw"""
        if installed_capacity_mw in [0, None]:
            logger.warning(
                f"Could not normalize ForecastValue object {installed_capacity_mw}"
                f"So will set to zero"
            )
            self.expected_power_generation_normalized = 0
        else:
            self.expected_power_generation_normalized = (
                self.expected_power_generation_megawatts / installed_capacity_mw
            )

        return self


class ForecastSQL(Base_Forecast, CreatedMixin):
    """Forecast SQL model"""

    __tablename__ = "forecast"

    id = Column(Integer, primary_key=True)
    forecast_creation_time = Column(DateTime(timezone=True))

    # Two distinuise between
    # 1. a Forecast with some forecast values, for that moment,
    # 2. a Forecast with all the historic latest value.
    # we use this boolean.
    # This make it easier to load the forecast showing historic values very easily
    historic = Column(Boolean, default=False)

    model = relationship("MLModelSQL", back_populates="forecast")
    model_id = Column(Integer, ForeignKey("model.id"), index=True)

    # many (forecasts) to one (location)
    location = relationship("LocationSQL", back_populates="forecast")
    location_id = Column(Integer, ForeignKey("location.id"), index=True)

    # one (forecasts) to many (forecast_value)
    forecast_values = relationship("ForecastValueSQL")
    forecast_values_latest = relationship(
        "ForecastValueLatestSQL", back_populates="forecast_latest"
    )

    # many (forecasts) to one (input_data_last_updated)
    input_data_last_updated = relationship("InputDataLastUpdatedSQL", back_populates="forecast")
    input_data_last_updated_id = Column(
        Integer, ForeignKey("input_data_last_updated.id"), index=True
    )

    Index("index_forecast", CreatedMixin.created_utc.desc())


class Forecast(EnhancedBaseModel):
    """A single Forecast"""

    location: Location = Field(..., description="The location object for this forecaster")
    model: MLModel = Field(..., description="The name of the model that made this forecast")
    forecast_creation_time: datetime = Field(
        ..., description="The time when the forecaster was made"
    )
    historic: bool = Field(
        False,
        description="if False, the forecast is just the latest forecast. "
        "If True, historic values are also given",
    )
    forecast_values: List[ForecastValue] = Field(
        ...,
        description="List of forecasted value objects. Each value has the datestamp and a value",
    )
    input_data_last_updated: InputDataLastUpdated = Field(
        ...,
        description="Information about the input data that was used to create the forecast",
    )

    _normalize_forecast_creation_time = validator("forecast_creation_time", allow_reuse=True)(
        datetime_must_have_timezone
    )

    def to_orm(self) -> ForecastSQL:
        """Change model to ForecastSQL"""
        return ForecastSQL(
            model=self.model.to_orm(),
            forecast_creation_time=self.forecast_creation_time,
            location=self.location.to_orm(),
            input_data_last_updated=self.input_data_last_updated.to_orm(),
            forecast_values=[forecast_value.to_orm() for forecast_value in self.forecast_values],
            historic=self.historic,
        )

    @classmethod
    def from_orm_latest(cls, forecast_sql: ForecastSQL):
        """Method to make Forecast object from ForecastSQL,

        but move 'forecast_values_latest' to 'forecast_values'
        This is useful as we want the API to still present a Forecast object.
        """
        # do normal transform
        forecast = cls.from_orm(forecast_sql)

        # move 'forecast_values_latest' to 'forecast_values'
        forecast.forecast_values = [
            ForecastValue.from_orm(forecast_value)
            for forecast_value in forecast_sql.forecast_values_latest
        ]

        return forecast

    def normalize(self):
        """Normalize forecasts by installed capacity mw"""
        self.forecast_values = [
            forecast_value.normalize(self.location.installed_capacity_mw)
            for forecast_value in self.forecast_values
        ]

        return self


class ManyForecasts(EnhancedBaseModel):
    """Many Forecasts"""

    forecasts: List[Forecast] = Field(
        ...,
        description="List of forecasts for different GSPs",
    )

    def normalize(self):
        """Normalize forecasts by installed capacity mw"""
        self.forecasts = [forecast.normalize() for forecast in self.forecasts]
