"""Add horizon minutes to forecast_values tables

Revision ID: 3a8ad17b57a3
Revises: 820c16a0524c
Create Date: 2025-06-05 19:13:15.137091

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3a8ad17b57a3"
down_revision = "820c16a0524c"
branch_labels = None
depends_on = None


def upgrade():
    """Upgrades the database schema to the next revision."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("forecast_value", sa.Column("horizon_minutes", sa.Integer(), nullable=True))
    op.add_column(
        "forecast_value_2022_08", sa.Column("horizon_minutes", sa.Integer(), nullable=True)
    )
    op.create_index(
        op.f("ix_forecast_value_2022_08_horizon_minutes"),
        "forecast_value_2022_08",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2022_09_horizon_minutes"),
        "forecast_value_2022_09",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2022_10_horizon_minutes"),
        "forecast_value_2022_10",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2022_11_horizon_minutes"),
        "forecast_value_2022_11",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2022_12_horizon_minutes"),
        "forecast_value_2022_12",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2023_01_horizon_minutes"),
        "forecast_value_2023_01",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2023_02_horizon_minutes"),
        "forecast_value_2023_02",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2023_03_horizon_minutes"),
        "forecast_value_2023_03",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2023_04_horizon_minutes"),
        "forecast_value_2023_04",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2023_05_horizon_minutes"),
        "forecast_value_2023_05",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2023_06_horizon_minutes"),
        "forecast_value_2023_06",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2023_07_horizon_minutes"),
        "forecast_value_2023_07",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2023_08_horizon_minutes"),
        "forecast_value_2023_08",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2023_09_horizon_minutes"),
        "forecast_value_2023_09",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2023_10_horizon_minutes"),
        "forecast_value_2023_10",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2023_11_horizon_minutes"),
        "forecast_value_2023_11",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2023_12_horizon_minutes"),
        "forecast_value_2023_12",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2024_01_horizon_minutes"),
        "forecast_value_2024_01",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2024_02_horizon_minutes"),
        "forecast_value_2024_02",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2024_03_horizon_minutes"),
        "forecast_value_2024_03",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2024_04_horizon_minutes"),
        "forecast_value_2024_04",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2024_05_horizon_minutes"),
        "forecast_value_2024_05",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2024_06_horizon_minutes"),
        "forecast_value_2024_06",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2024_07_horizon_minutes"),
        "forecast_value_2024_07",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2024_08_horizon_minutes"),
        "forecast_value_2024_08",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2024_09_horizon_minutes"),
        "forecast_value_2024_09",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2024_10_horizon_minutes"),
        "forecast_value_2024_10",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2024_11_horizon_minutes"),
        "forecast_value_2024_11",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2024_12_horizon_minutes"),
        "forecast_value_2024_12",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2025_01_horizon_minutes"),
        "forecast_value_2025_01",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2025_02_horizon_minutes"),
        "forecast_value_2025_02",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2025_03_horizon_minutes"),
        "forecast_value_2025_03",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2025_04_horizon_minutes"),
        "forecast_value_2025_04",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2025_05_horizon_minutes"),
        "forecast_value_2025_05",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2025_06_horizon_minutes"),
        "forecast_value_2025_06",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2025_07_horizon_minutes"),
        "forecast_value_2025_07",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2025_08_horizon_minutes"),
        "forecast_value_2025_08",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2025_09_horizon_minutes"),
        "forecast_value_2025_09",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2025_10_horizon_minutes"),
        "forecast_value_2025_10",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2025_11_horizon_minutes"),
        "forecast_value_2025_11",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2025_12_horizon_minutes"),
        "forecast_value_2025_12",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2026_01_horizon_minutes"),
        "forecast_value_2026_01",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2026_02_horizon_minutes"),
        "forecast_value_2026_02",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2026_03_horizon_minutes"),
        "forecast_value_2026_03",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2026_04_horizon_minutes"),
        "forecast_value_2026_04",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2026_05_horizon_minutes"),
        "forecast_value_2026_05",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2026_06_horizon_minutes"),
        "forecast_value_2026_06",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2026_07_horizon_minutes"),
        "forecast_value_2026_07",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2026_08_horizon_minutes"),
        "forecast_value_2026_08",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2026_09_horizon_minutes"),
        "forecast_value_2026_09",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2026_10_horizon_minutes"),
        "forecast_value_2026_10",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2026_11_horizon_minutes"),
        "forecast_value_2026_11",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2026_12_horizon_minutes"),
        "forecast_value_2026_12",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2027_01_horizon_minutes"),
        "forecast_value_2027_01",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2027_02_horizon_minutes"),
        "forecast_value_2027_02",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2027_03_horizon_minutes"),
        "forecast_value_2027_03",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2027_04_horizon_minutes"),
        "forecast_value_2027_04",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2027_05_horizon_minutes"),
        "forecast_value_2027_05",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2027_06_horizon_minutes"),
        "forecast_value_2027_06",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2027_07_horizon_minutes"),
        "forecast_value_2027_07",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2027_08_horizon_minutes"),
        "forecast_value_2027_08",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2027_09_horizon_minutes"),
        "forecast_value_2027_09",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2027_10_horizon_minutes"),
        "forecast_value_2027_10",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2027_11_horizon_minutes"),
        "forecast_value_2027_11",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2027_12_horizon_minutes"),
        "forecast_value_2027_12",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2028_01_horizon_minutes"),
        "forecast_value_2028_01",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2028_02_horizon_minutes"),
        "forecast_value_2028_02",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2028_03_horizon_minutes"),
        "forecast_value_2028_03",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2028_04_horizon_minutes"),
        "forecast_value_2028_04",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2028_05_horizon_minutes"),
        "forecast_value_2028_05",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2028_06_horizon_minutes"),
        "forecast_value_2028_06",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2028_07_horizon_minutes"),
        "forecast_value_2028_07",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2028_08_horizon_minutes"),
        "forecast_value_2028_08",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2028_09_horizon_minutes"),
        "forecast_value_2028_09",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2028_10_horizon_minutes"),
        "forecast_value_2028_10",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2028_11_horizon_minutes"),
        "forecast_value_2028_11",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2028_12_horizon_minutes"),
        "forecast_value_2028_12",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2029_01_horizon_minutes"),
        "forecast_value_2029_01",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2029_02_horizon_minutes"),
        "forecast_value_2029_02",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2029_03_horizon_minutes"),
        "forecast_value_2029_03",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2029_04_horizon_minutes"),
        "forecast_value_2029_04",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2029_05_horizon_minutes"),
        "forecast_value_2029_05",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2029_06_horizon_minutes"),
        "forecast_value_2029_06",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2029_07_horizon_minutes"),
        "forecast_value_2029_07",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2029_08_horizon_minutes"),
        "forecast_value_2029_08",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2029_09_horizon_minutes"),
        "forecast_value_2029_09",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2029_10_horizon_minutes"),
        "forecast_value_2029_10",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2029_11_horizon_minutes"),
        "forecast_value_2029_11",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_2029_12_horizon_minutes"),
        "forecast_value_2029_12",
        ["horizon_minutes"],
        unique=False,
    )
    op.add_column(
        "forecast_value_last_seven_days", sa.Column("horizon_minutes", sa.Integer(), nullable=True)
    )
    op.create_index(
        op.f("ix_forecast_value_last_seven_days_horizon_minutes"),
        "forecast_value_last_seven_days",
        ["horizon_minutes"],
        unique=False,
    )
    op.add_column("forecast_value_old", sa.Column("horizon_minutes", sa.Integer(), nullable=True))
    op.create_index(
        op.f("ix_forecast_value_old_horizon_minutes"),
        "forecast_value_old",
        ["horizon_minutes"],
        unique=False,
    )
    op.create_index(
        op.f("ix_forecast_value_horizon_minutes"),
        "forecast_value",
        ["horizon_minutes"],
        unique=False,
    )
    # ### end Alembic commands ###


def downgrade():
    """Downgrades the database schema to the previous revision."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_forecast_value_old_horizon_minutes"), table_name="forecast_value_old")
    op.drop_column("forecast_value_old", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_last_seven_days_horizon_minutes"),
        table_name="forecast_value_last_seven_days",
    )
    op.drop_column("forecast_value_last_seven_days", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2029_12_horizon_minutes"), table_name="forecast_value_2029_12"
    )
    op.drop_column("forecast_value_2029_12", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2029_11_horizon_minutes"), table_name="forecast_value_2029_11"
    )
    op.drop_column("forecast_value_2029_11", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2029_10_horizon_minutes"), table_name="forecast_value_2029_10"
    )
    op.drop_column("forecast_value_2029_10", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2029_09_horizon_minutes"), table_name="forecast_value_2029_09"
    )
    op.drop_column("forecast_value_2029_09", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2029_08_horizon_minutes"), table_name="forecast_value_2029_08"
    )
    op.drop_column("forecast_value_2029_08", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2029_07_horizon_minutes"), table_name="forecast_value_2029_07"
    )
    op.drop_column("forecast_value_2029_07", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2029_06_horizon_minutes"), table_name="forecast_value_2029_06"
    )
    op.drop_column("forecast_value_2029_06", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2029_05_horizon_minutes"), table_name="forecast_value_2029_05"
    )
    op.drop_column("forecast_value_2029_05", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2029_04_horizon_minutes"), table_name="forecast_value_2029_04"
    )
    op.drop_column("forecast_value_2029_04", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2029_03_horizon_minutes"), table_name="forecast_value_2029_03"
    )
    op.drop_column("forecast_value_2029_03", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2029_02_horizon_minutes"), table_name="forecast_value_2029_02"
    )
    op.drop_column("forecast_value_2029_02", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2029_01_horizon_minutes"), table_name="forecast_value_2029_01"
    )
    op.drop_column("forecast_value_2029_01", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2028_12_horizon_minutes"), table_name="forecast_value_2028_12"
    )
    op.drop_column("forecast_value_2028_12", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2028_11_horizon_minutes"), table_name="forecast_value_2028_11"
    )
    op.drop_column("forecast_value_2028_11", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2028_10_horizon_minutes"), table_name="forecast_value_2028_10"
    )
    op.drop_column("forecast_value_2028_10", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2028_09_horizon_minutes"), table_name="forecast_value_2028_09"
    )
    op.drop_column("forecast_value_2028_09", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2028_08_horizon_minutes"), table_name="forecast_value_2028_08"
    )
    op.drop_column("forecast_value_2028_08", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2028_07_horizon_minutes"), table_name="forecast_value_2028_07"
    )
    op.drop_column("forecast_value_2028_07", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2028_06_horizon_minutes"), table_name="forecast_value_2028_06"
    )
    op.drop_column("forecast_value_2028_06", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2028_05_horizon_minutes"), table_name="forecast_value_2028_05"
    )
    op.drop_column("forecast_value_2028_05", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2028_04_horizon_minutes"), table_name="forecast_value_2028_04"
    )
    op.drop_column("forecast_value_2028_04", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2028_03_horizon_minutes"), table_name="forecast_value_2028_03"
    )
    op.drop_column("forecast_value_2028_03", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2028_02_horizon_minutes"), table_name="forecast_value_2028_02"
    )
    op.drop_column("forecast_value_2028_02", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2028_01_horizon_minutes"), table_name="forecast_value_2028_01"
    )
    op.drop_column("forecast_value_2028_01", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2027_12_horizon_minutes"), table_name="forecast_value_2027_12"
    )
    op.drop_column("forecast_value_2027_12", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2027_11_horizon_minutes"), table_name="forecast_value_2027_11"
    )
    op.drop_column("forecast_value_2027_11", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2027_10_horizon_minutes"), table_name="forecast_value_2027_10"
    )
    op.drop_column("forecast_value_2027_10", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2027_09_horizon_minutes"), table_name="forecast_value_2027_09"
    )
    op.drop_column("forecast_value_2027_09", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2027_08_horizon_minutes"), table_name="forecast_value_2027_08"
    )
    op.drop_column("forecast_value_2027_08", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2027_07_horizon_minutes"), table_name="forecast_value_2027_07"
    )
    op.drop_column("forecast_value_2027_07", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2027_06_horizon_minutes"), table_name="forecast_value_2027_06"
    )
    op.drop_column("forecast_value_2027_06", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2027_05_horizon_minutes"), table_name="forecast_value_2027_05"
    )
    op.drop_column("forecast_value_2027_05", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2027_04_horizon_minutes"), table_name="forecast_value_2027_04"
    )
    op.drop_column("forecast_value_2027_04", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2027_03_horizon_minutes"), table_name="forecast_value_2027_03"
    )
    op.drop_column("forecast_value_2027_03", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2027_02_horizon_minutes"), table_name="forecast_value_2027_02"
    )
    op.drop_column("forecast_value_2027_02", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2027_01_horizon_minutes"), table_name="forecast_value_2027_01"
    )
    op.drop_column("forecast_value_2027_01", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2026_12_horizon_minutes"), table_name="forecast_value_2026_12"
    )
    op.drop_column("forecast_value_2026_12", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2026_11_horizon_minutes"), table_name="forecast_value_2026_11"
    )
    op.drop_column("forecast_value_2026_11", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2026_10_horizon_minutes"), table_name="forecast_value_2026_10"
    )
    op.drop_column("forecast_value_2026_10", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2026_09_horizon_minutes"), table_name="forecast_value_2026_09"
    )
    op.drop_column("forecast_value_2026_09", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2026_08_horizon_minutes"), table_name="forecast_value_2026_08"
    )
    op.drop_column("forecast_value_2026_08", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2026_07_horizon_minutes"), table_name="forecast_value_2026_07"
    )
    op.drop_column("forecast_value_2026_07", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2026_06_horizon_minutes"), table_name="forecast_value_2026_06"
    )
    op.drop_column("forecast_value_2026_06", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2026_05_horizon_minutes"), table_name="forecast_value_2026_05"
    )
    op.drop_column("forecast_value_2026_05", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2026_04_horizon_minutes"), table_name="forecast_value_2026_04"
    )
    op.drop_column("forecast_value_2026_04", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2026_03_horizon_minutes"), table_name="forecast_value_2026_03"
    )
    op.drop_column("forecast_value_2026_03", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2026_02_horizon_minutes"), table_name="forecast_value_2026_02"
    )
    op.drop_column("forecast_value_2026_02", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2026_01_horizon_minutes"), table_name="forecast_value_2026_01"
    )
    op.drop_column("forecast_value_2026_01", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2025_12_horizon_minutes"), table_name="forecast_value_2025_12"
    )
    op.drop_column("forecast_value_2025_12", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2025_11_horizon_minutes"), table_name="forecast_value_2025_11"
    )
    op.drop_column("forecast_value_2025_11", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2025_10_horizon_minutes"), table_name="forecast_value_2025_10"
    )
    op.drop_column("forecast_value_2025_10", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2025_09_horizon_minutes"), table_name="forecast_value_2025_09"
    )
    op.drop_column("forecast_value_2025_09", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2025_08_horizon_minutes"), table_name="forecast_value_2025_08"
    )
    op.drop_column("forecast_value_2025_08", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2025_07_horizon_minutes"), table_name="forecast_value_2025_07"
    )
    op.drop_column("forecast_value_2025_07", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2025_06_horizon_minutes"), table_name="forecast_value_2025_06"
    )
    op.drop_column("forecast_value_2025_06", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2025_05_horizon_minutes"), table_name="forecast_value_2025_05"
    )
    op.drop_column("forecast_value_2025_05", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2025_04_horizon_minutes"), table_name="forecast_value_2025_04"
    )
    op.drop_column("forecast_value_2025_04", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2025_03_horizon_minutes"), table_name="forecast_value_2025_03"
    )
    op.drop_column("forecast_value_2025_03", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2025_02_horizon_minutes"), table_name="forecast_value_2025_02"
    )
    op.drop_column("forecast_value_2025_02", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2025_01_horizon_minutes"), table_name="forecast_value_2025_01"
    )
    op.drop_column("forecast_value_2025_01", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2024_12_horizon_minutes"), table_name="forecast_value_2024_12"
    )
    op.drop_column("forecast_value_2024_12", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2024_11_horizon_minutes"), table_name="forecast_value_2024_11"
    )
    op.drop_column("forecast_value_2024_11", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2024_10_horizon_minutes"), table_name="forecast_value_2024_10"
    )
    op.drop_column("forecast_value_2024_10", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2024_09_horizon_minutes"), table_name="forecast_value_2024_09"
    )
    op.drop_column("forecast_value_2024_09", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2024_08_horizon_minutes"), table_name="forecast_value_2024_08"
    )
    op.drop_column("forecast_value_2024_08", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2024_07_horizon_minutes"), table_name="forecast_value_2024_07"
    )
    op.drop_column("forecast_value_2024_07", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2024_06_horizon_minutes"), table_name="forecast_value_2024_06"
    )
    op.drop_column("forecast_value_2024_06", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2024_05_horizon_minutes"), table_name="forecast_value_2024_05"
    )
    op.drop_column("forecast_value_2024_05", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2024_04_horizon_minutes"), table_name="forecast_value_2024_04"
    )
    op.drop_column("forecast_value_2024_04", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2024_03_horizon_minutes"), table_name="forecast_value_2024_03"
    )
    op.drop_column("forecast_value_2024_03", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2024_02_horizon_minutes"), table_name="forecast_value_2024_02"
    )
    op.drop_column("forecast_value_2024_02", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2024_01_horizon_minutes"), table_name="forecast_value_2024_01"
    )
    op.drop_column("forecast_value_2024_01", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2023_12_horizon_minutes"), table_name="forecast_value_2023_12"
    )
    op.drop_column("forecast_value_2023_12", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2023_11_horizon_minutes"), table_name="forecast_value_2023_11"
    )
    op.drop_column("forecast_value_2023_11", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2023_10_horizon_minutes"), table_name="forecast_value_2023_10"
    )
    op.drop_column("forecast_value_2023_10", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2023_09_horizon_minutes"), table_name="forecast_value_2023_09"
    )
    op.drop_column("forecast_value_2023_09", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2023_08_horizon_minutes"), table_name="forecast_value_2023_08"
    )
    op.drop_column("forecast_value_2023_08", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2023_07_horizon_minutes"), table_name="forecast_value_2023_07"
    )
    op.drop_column("forecast_value_2023_07", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2023_06_horizon_minutes"), table_name="forecast_value_2023_06"
    )
    op.drop_column("forecast_value_2023_06", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2023_05_horizon_minutes"), table_name="forecast_value_2023_05"
    )
    op.drop_column("forecast_value_2023_05", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2023_04_horizon_minutes"), table_name="forecast_value_2023_04"
    )
    op.drop_column("forecast_value_2023_04", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2023_03_horizon_minutes"), table_name="forecast_value_2023_03"
    )
    op.drop_column("forecast_value_2023_03", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2023_02_horizon_minutes"), table_name="forecast_value_2023_02"
    )
    op.drop_column("forecast_value_2023_02", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2023_01_horizon_minutes"), table_name="forecast_value_2023_01"
    )
    op.drop_column("forecast_value_2023_01", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2022_12_horizon_minutes"), table_name="forecast_value_2022_12"
    )
    op.drop_column("forecast_value_2022_12", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2022_11_horizon_minutes"), table_name="forecast_value_2022_11"
    )
    op.drop_column("forecast_value_2022_11", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2022_10_horizon_minutes"), table_name="forecast_value_2022_10"
    )
    op.drop_column("forecast_value_2022_10", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2022_09_horizon_minutes"), table_name="forecast_value_2022_09"
    )
    op.drop_column("forecast_value_2022_09", "horizon_minutes")
    op.drop_index(
        op.f("ix_forecast_value_2022_08_horizon_minutes"), table_name="forecast_value_2022_08"
    )
    op.drop_column("forecast_value_2022_08", "horizon_minutes")
    op.drop_index(op.f("ix_forecast_value_horizon_minutes"), table_name="forecast_value")
    op.drop_column("forecast_value", "horizon_minutes")
    # ### end Alembic commands ###
