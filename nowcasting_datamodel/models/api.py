""" Pydantic and Sqlalchemy models for the database

The following class are made
1. User
2. APIRequest

"""

from typing import Optional

from pydantic import Field
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models.utils import CreatedMixin, EnhancedBaseModel


########
# 1. User
########
class UserSQL(Base_Forecast):
    """ML model that is being used"""

    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    email = Column(String)


class User(EnhancedBaseModel):
    """ML model that is being used"""

    email: Optional[str] = Field(..., description="The name of the model", index=True)
    api_request = relationship("APIRequestSQL", back_populates="user")

    def to_orm(self) -> UserSQL:
        """Change model to MLModelSQL"""
        return UserSQL(
            email=self.email,
        )


########
# 2. APIRequest
########
class APIRequestSQL(CreatedMixin):
    """Information about what API route was called"""

    __tablename__ = "api_request"

    uuid = Column(UUID, primary_key=True)
    url = Column(Integer, primary_key=True)

    user = relationship("UserSQL", back_populates="api_request")


class APIRequest(EnhancedBaseModel):
    """Information about the input data that was used to create the forecast"""

    url: str = Field(..., description="The url that was called")
    user: Optional[User] = Field(
        None,
        description="The user associated with this api call",
    )

    def to_orm(self) -> APIRequestSQL:
        """Change model to APIRequestSQL"""
        return APIRequestSQL(
            url=self.url,
            user=self.user.to_orm() if self.user is not None else None,
        )