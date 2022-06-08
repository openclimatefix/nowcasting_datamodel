""" Pydantic and Sqlalchemy models for the database

The following class are made
1. Reusable classes
3. Model object, what forecast model is used
4. ForecastValue objects, specific values of a forecast and time
5. Input data status, shows when the data was collected
6. Forecasts, a forecast that is made for one gsp, for several time steps into the future

"""
import logging
from datetime import datetime
from typing import List, Optional

from pydantic import Field, validator
from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import relationship

from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models.gsp import Location
from nowcasting_datamodel.models.utils import CreatedMixin, EnhancedBaseModel
from nowcasting_datamodel.utils import datetime_must_have_timezone

national_gb_label = "National-GB"

logger = logging.getLogger(__name__)
# TODO #3 Add forecast latest table, this make it easy to load the latest forecast


########
# 2. Location
########


########
# 3. Model
########
class MLModelSQL(Base_Forecast):
    """ML model that is being used"""

    __tablename__ = "model"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    version = Column(String)

    forecast = relationship("ForecastSQL", back_populates="model")


class MLModel(EnhancedBaseModel):
    """ML model that is being used"""

    name: Optional[str] = Field(..., description="The name of the model", index=True)
    version: Optional[str] = Field(..., description="The version of the model")

    def to_orm(self) -> MLModelSQL:
        """Change model to MLModelSQL"""
        return MLModelSQL(
            name=self.name,
            version=self.version,
        )


########
# 4. ForecastValue
# A. SQL table saves all forecast values
# B. SQL table that save the latest forecast
# C. Pydantic object
########


class ForecastValueSQL(Base_Forecast, CreatedMixin):
    """One Forecast of generation at one timestamp"""

    __tablename__ = "forecast_value"

    id = Column(Integer, primary_key=True)
    target_time = Column(DateTime(timezone=True), index=True)
    expected_power_generation_megawatts = Column(Float)

    forecast_id = Column(Integer, ForeignKey("forecast.id"), index=True)
    forecast = relationship("ForecastSQL", back_populates="forecast_values")

    Index("index_forecast_value", CreatedMixin.created_utc.desc())


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


########
# 5. Input data status
########
class InputDataLastUpdatedSQL(Base_Forecast, CreatedMixin):
    """Information about the input data that was used to create the forecast"""

    __tablename__ = "input_data_last_updated"

    id = Column(Integer, primary_key=True)
    gsp = Column(DateTime(timezone=True))
    nwp = Column(DateTime(timezone=True))
    pv = Column(DateTime(timezone=True))
    satellite = Column(DateTime(timezone=True))

    forecast = relationship("ForecastSQL", back_populates="input_data_last_updated")

    Index("index_input_data", CreatedMixin.created_utc.desc())


class InputDataLastUpdated(EnhancedBaseModel):
    """Information about the input data that was used to create the forecast"""

    gsp: datetime = Field(..., description="The time when the input GSP data was last updated")
    nwp: datetime = Field(..., description="The time when the input NWP data was last updated")
    pv: datetime = Field(..., description="The time when the input PV data was last updated")
    satellite: datetime = Field(
        ..., description="The time when the input satellite data was last updated"
    )

    _normalize_gsp = validator("gsp", allow_reuse=True)(datetime_must_have_timezone)
    _normalize_nwp = validator("nwp", allow_reuse=True)(datetime_must_have_timezone)
    _normalize_pv = validator("pv", allow_reuse=True)(datetime_must_have_timezone)
    _normalize_satellite = validator("satellite", allow_reuse=True)(datetime_must_have_timezone)

    def to_orm(self) -> InputDataLastUpdatedSQL:
        """Change model to InputDataLastUpdatedSQL"""
        return InputDataLastUpdatedSQL(
            gsp=self.gsp,
            nwp=self.nwp,
            pv=self.pv,
            satellite=self.satellite,
        )


########
# 6. Forecasts
########
# TODO add model_name to forecast, or add model table #13
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
    forecast_values = relationship("ForecastValueSQL", back_populates="forecast")
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


########
# 7. Status
########


class StatusSQL(Base_Forecast, CreatedMixin):
    """Status SQL Model"""

    __tablename__ = "status"

    id = Column(Integer, primary_key=True)
    status = Column(String)
    message = Column(String)


class Status(EnhancedBaseModel):
    """Status Model for a single message"""

    status: str = Field(..., description="Status description")
    message: str = Field(..., description="Status Message")

    def to_orm(self) -> StatusSQL:
        """Change model to SQL"""
        return StatusSQL(status=self.status, message=self.message)

    @validator("status")
    def validate_solar_generation_kw(cls, v):
        """Validate the solar_generation_kw field"""
        if v not in ["ok", "warning", "error"]:
            message = f"Status is {v}, not one of 'ok', 'warning', 'error'"
            logger.debug(message)
            raise Exception(message)
        return v
