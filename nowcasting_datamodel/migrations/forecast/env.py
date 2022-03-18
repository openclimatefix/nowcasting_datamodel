""" run migrations on Forecast Database """
import os
from logging.config import fileConfig

from alembic import context

from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.models import Base_Forecast

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base_Forecast.metadata


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    connection = DatabaseConnection(os.getenv("DB_URL"))
    connectable = connection.engine

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=Base_Forecast.metadata)

        with context.begin_transaction():
            context.run_migrations()


# need to have this code running when called
run_migrations_online()
