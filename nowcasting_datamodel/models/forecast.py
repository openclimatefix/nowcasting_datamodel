""" Pydantic and Sqlalchemy models for the database

The following class are made
1. ForecastValue objects, specific values of a forecast and time
2. Forecasts, a forecast that is made for one gsp, for several time steps into the future

"""

import logging
from datetime import datetime
from typing import List

from pydantic import Field, validator
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    event,
    func,
)
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
    target_time = Column(DateTime(timezone=True), nullable=False, primary_key=True)
    expected_power_generation_megawatts = Column(Float(precision=6))
    adjust_mw = Column(Float, default=0.0)

    @declared_attr
    def forecast_id(self):
        """Link with Forecast table"""
        return Column(Integer, ForeignKey("forecast.id"))


class ForecastValueSQL(
    ForecastValueSQLMixin, Base_Forecast, metaclass=PartitionByMeta, partition_by="target_time"
):
    """One Forecast of generation at one timestamp

    This Mixin is used to creat partition tables
    """

    __tablename__ = "forecast_value"

    __table_args__ = (
        Index(
            "ix_forecast_value_forecast_id",  # Index name
            "forecast_id",  # Columns which are part of the index
        ),
        Index(
            "ix_forecast_created_utc",  # Index name
            "created_utc",  # Columns which are part of the index
        ),
        Index(
            "ix_forecast_value_target_time",  # Index name
            "target_time",  # Columns which are part of the index
        ),
    )

    forecast = relationship("ForecastSQL", back_populates="forecast_values")


def get_partitions(start_year: int, start_month: int, end_year: int, end_month: int):
    """Make partitions"""
    partitions = []
    for year in range(start_year, end_year + 1):
        if year != start_year:
            start_month = 1
        if year == end_year:
            end_month_loop = end_month
        else:
            end_month_loop = 13

        for month in range(start_month, end_month_loop):
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


def make_partitions(start_year: int, start_month: int, end_year: int):
    """
    Make partitions

    :param start_year: year to start
    :param start_month: month to end
    :param end_year: end year (exclusive)
    """
    for year in range(start_year, end_year):
        if year != start_year:
            start_month = 1

        for month in range(start_month, 13):
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

            class ForecastValueYearMonth(ForecastValueSQLMixin, Base_Forecast):
                __tablename__ = f"forecast_value_{year}_{month}"

                __table_args__ = (
                    Index(
                        f"forecast_value_{year}_{month}_created_utc_idx",  # Index name
                        "created_utc",  # Columns which are part of the index
                    ),
                    Index(
                        f"forecast_value_{year}_{month}_target_time_idx",  # Index name
                        "target_time",  # Columns which are part of the index
                    ),
                    Index(
                        f"forecast_value_{year}_{month}_forecast_id_idx",  # Index name
                        "forecast_id",  # Columns which are part of the index
                    ),
                )

            ForecastValueYearMonth.__table__.add_is_dependent_on(ForecastValueSQL.__table__)

            event.listen(
                ForecastValueYearMonth.__table__,
                "after_create",
                DDL(
                    f"ALTER TABLE forecast_value ATTACH PARTITION forecast_value_{year}_{month} "
                    f"for VALUES FROM ('{year}-{month}-01') TO ('{year_end}-{month_end}-01');"
                ),
            )


make_partitions(2022, 8, 2025)


# legacy table, this means migration still work
class ForecastValueOld(ForecastValueSQLMixin, Base_Forecast):
    """Old ForecastValue table"""

    __tablename__ = "forecast_value_old"

    __table_args__ = (
        Index(
            "ix_forecast_value_forecast_id_old",  # Index name
            "forecast_id",  # Columns which are part of the index
        ),
        Index(
            "ix_forecast_value_target_time_old",  # Index name
            "target_time",  # Columns which are part of the index
        ),
    )


class ForecastValueLatestSQL(Base_Forecast, CreatedMixin):
    """One Forecast of generation at one timestamp

    Thanks for the help from
    https://www.johbo.com/2016/creating-a-partial-unique-index-with-sqlalchemy-in-postgresql.html
    """

    __tablename__ = "forecast_value_latest"

    # add a unique condition on 'gsp_id',  'target_time' and 'model_id'
    __table_args__ = (
        Index(
            "uix_1",  # Index name
            "gsp_id",  # Columns which are part of the index
            "target_time",
            "model_id",
            unique=True,
            postgresql_where=Column("is_primary"),  # The condition
        ),
    )

    target_time = Column(DateTime(timezone=True), index=True, primary_key=True)
    expected_power_generation_megawatts = Column(Float(precision=6))
    gsp_id = Column(Integer, index=True, primary_key=True)
    model_id = Column(Integer, index=True, primary_key=True, default=-1)
    is_primary = Column(Boolean, default=True)
    adjust_mw = Column(Float, default=0.0)

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

    _adjust_mw: float = Field(
        0.0,
        description="The amount that the forecast should be adjusted by, "
        "due to persistence errors. This way we keep the original ML prediction. "
        "The _ at the start means it is not expose in the API",
    )

    _normalize_target_time = validator("target_time", allow_reuse=True)(datetime_must_have_timezone)

    def to_orm(self) -> ForecastValueSQL:
        """Change model to ForecastValueSQL"""
        return ForecastValueSQL(
            target_time=self.target_time,
            expected_power_generation_megawatts=self.expected_power_generation_megawatts,
            adjust_mw=self._adjust_mw,
        )

    @classmethod
    def from_orm(cls, obj: ForecastValueSQL):
        """Make sure _adjust_mw is transfered also"""
        m = super().from_orm(obj=obj)

        # this is because from orm doesnt copy over '_' variables.
        # But we don't want to expose this in the API
        if hasattr(obj, "adjust_mw"):
            m._adjust_mw = obj.adjust_mw
        else:
            m._adjust_mw = 0.0

        return m

    def normalize(self, installed_capacity_mw):
        """Normalize forecasts by installed capacity mw"""
        if installed_capacity_mw in [0, None]:
            self.expected_power_generation_normalized = 0
        else:
            self.expected_power_generation_normalized = (
                self.expected_power_generation_megawatts / installed_capacity_mw
            )

        return self

    def adjust(self, limit: float = 0.0):
        """Adjust forecasts"""
        if isinstance(self._adjust_mw, float):
            adjust_mw = self._adjust_mw
            if adjust_mw > limit:
                adjust_mw = limit
            elif adjust_mw < -limit:
                adjust_mw = -limit

            self.expected_power_generation_megawatts = (
                self.expected_power_generation_megawatts - adjust_mw
            )

            if self.expected_power_generation_megawatts < 0:
                self.expected_power_generation_megawatts = 0.0

        return self


class ForecastSQL(Base_Forecast, CreatedMixin):
    """Forecast SQL model"""

    __tablename__ = "forecast"

    __table_args__ = (
        Index(
            "idx_forecast_created_utc",  # Index name
            "created_utc",  # Columns which are part of the index
        ),
    )

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
    forecast_values_last_seven_days = relationship(
        "ForecastValueSevenDaysSQL", back_populates="forecast"
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

    def normalize(self, adjust: bool = False):
        """Normalize forecasts by installed capacity mw"""

        self.forecast_values = [
            forecast_value.normalize(self.location.installed_capacity_mw)
            for forecast_value in self.forecast_values
        ]

        return self

    def adjust(self, limit: float = 0.0):
        """Adjust forecasts by adjust values, useful for time persistence errors"""

        print("start main")
        self.forecast_values = [
            forecast_value.adjust(limit=limit) for forecast_value in self.forecast_values
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

    def adjust(self, limit: float = 0.0):
        """Adjust forecasts"""
        self.forecasts = [forecast.adjust(limit=limit) for forecast in self.forecasts]


class ForecastValueSevenDaysSQL(ForecastValueSQLMixin, Base_Forecast):
    """
    One Forecast of generation at one timestamp

    This table will only save the last week of data.
    """

    __tablename__ = "forecast_value_last_seven_days"

    __table_args__ = (
        Index(
            "idx_forecast_value_last_seven_days_created_utc",  # Index name
            "created_utc",  # Columns which are part of the index
        ),
        Index(
            "idx_forecast_value_last_seven_days_target_time",  # Index name
            "target_time",  # Columns which are part of the index
        ),
    )

    forecast = relationship("ForecastSQL", back_populates="forecast_values_last_seven_days")
