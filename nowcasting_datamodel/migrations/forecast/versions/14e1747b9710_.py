"""empty message

Revision ID: 14e1747b9710
Revises:
Create Date: 2022-03-11 13:39:18.824128

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "14e1747b9710"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():  # noqa 103
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "input_data_last_updated",
        sa.Column("created_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("gsp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("nwp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pv", sa.DateTime(timezone=True), nullable=True),
        sa.Column("satellite", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "location",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("label", sa.String(), nullable=True),
        sa.Column("gsp_id", sa.Integer(), nullable=True),
        sa.Column("gsp_name", sa.String(), nullable=True),
        sa.Column("gsp_group", sa.String(), nullable=True),
        sa.Column("region_name", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "model",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("version", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "forecast",
        sa.Column("created_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("forecast_creation_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("model_id", sa.Integer(), nullable=True),
        sa.Column("location_id", sa.Integer(), nullable=True),
        sa.Column("input_data_last_updated_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["input_data_last_updated_id"],
            ["input_data_last_updated.id"],
        ),
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["location.id"],
        ),
        sa.ForeignKeyConstraint(
            ["model_id"],
            ["model.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_forecast_input_data_last_updated_id"),
        "forecast",
        ["input_data_last_updated_id"],
        unique=False,
    )
    op.create_index(op.f("ix_forecast_location_id"), "forecast", ["location_id"], unique=False)
    op.create_index(op.f("ix_forecast_model_id"), "forecast", ["model_id"], unique=False)
    op.create_table(
        "forecast_value",
        sa.Column("created_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("target_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expected_power_generation_megawatts", sa.Float(), nullable=True),
        sa.Column("forecast_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["forecast_id"],
            ["forecast.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_forecast_value_forecast_id"), "forecast_value", ["forecast_id"], unique=False
    )
    # ### end Alembic commands ###


def downgrade():  # noqa 103
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_forecast_value_forecast_id"), table_name="forecast_value")
    op.drop_table("forecast_value")
    op.drop_index(op.f("ix_forecast_model_id"), table_name="forecast")
    op.drop_index(op.f("ix_forecast_location_id"), table_name="forecast")
    op.drop_index(op.f("ix_forecast_input_data_last_updated_id"), table_name="forecast")
    op.drop_table("forecast")
    op.drop_table("model")
    op.drop_table("location")
    op.drop_table("input_data_last_updated")
    # ### end Alembic commands ###