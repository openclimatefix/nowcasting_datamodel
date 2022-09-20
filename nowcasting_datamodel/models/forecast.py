from datetime import datetime

from pydantic import Field, validator
from sqlalchemy import Column, Integer, DateTime, Float, ForeignKey, Index, Boolean
from sqlalchemy.orm import relationship

from nowcasting_datamodel.models import Base_Forecast, CreatedMixin, EnhancedBaseModel, logger
from nowcasting_datamodel.utils import datetime_must_have_timezone

from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.sql.ddl import DDL
from sqlalchemy import event


"""
Tried to follow the example here
https://stackoverflow.com/questions/61545680/postgresql-partition-and-sqlalchemy
"""


class PartitionByYearMonthMeta(DeclarativeMeta):
    def __new__(cls, clsname, bases, attrs, *, partition_by):
        @classmethod
        def get_partition_name(cls_, year_month: str):
            # 'forecast_value' -> 'forecast_value_2020_01'
            return f"{cls_.__tablename__}_{year_month}"

        @classmethod
        def create_partition(cls_, year_month_start, year_month_end):
            """
            Create partion

            :param year_month_start: start month e.g. 2020-01
            :param year_month_end: end month e.g. 2020-02
            """
            if year_month_start not in cls_.partitions:
                Partition = type(
                    f"{clsname}{year_month_start}",  # Class name, only used internally
                    bases,
                    {"__tablename__": cls_.get_partition_name(year_month_start)},
                )

                Partition.__table__.add_is_dependent_on(cls_.__table__)

                event.listen(
                    Partition.__table__,
                    "after_create",
                    DDL(
                        # For non-year ranges, modify the FROM and TO below
                        f"""
                        ALTER TABLE {cls_.__tablename__}
                        ATTACH PARTITION {Partition.__tablename__}
                        FOR VALUES FROM ('{year_month_start}-01') TO ('{year_month_end}-01');
                        """
                    ),
                )

                cls_.partitions[year_month_start] = Partition

            return cls_.partitions[year_month_start]

        attrs.update(
            {
                # For non-RANGE partitions, modify the `postgresql_partition_by` key below
                "__table_args__": attrs.get("__table_args__", ())
                + (dict(postgresql_partition_by=f"RANGE({partition_by})"),),
                "partitions": {},
                "partitioned_by": partition_by,
                "get_partition_name": get_partition_name,
                "create_partition": create_partition,
            }
        )

        return super().__new__(cls, clsname, bases, attrs)


class ForecastValueSQLMixin(CreatedMixin):
    """One Forecast of generation at one timestamp

    This Mixin is used to creat partition tables
    """
    # __tablename__ = "forecast_value"

    id = Column(Integer, primary_key=True)
    target_time = Column(DateTime(timezone=True), index=True)
    expected_power_generation_megawatts = Column(Float)

    forecast_id = Column(Integer, ForeignKey("forecast.id"), index=True)
    forecast = relationship("ForecastSQL", back_populates="forecast_values")

    Index("index_forecast_value", CreatedMixin.created_utc.desc())


class ForecastValueSQL(
    ForecastValueSQLMixin,
    Base_Forecast,
    metaclass=PartitionByYearMonthMeta,
    partition_by="target_time",
):
    """One Forecast of generation at one timestamp"""

    __tablename__ = "forecast_value"


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
