"""Add partitions for forecast_value table

Revision ID: 06ced43c1321
Revises: ea79f7472cc5
Create Date: 2024-12-30

"""

import pandas as pd
from alembic import op

# revision identifiers, used by Alembic.
revision = "06ced43c1321"
down_revision = "ea79f7472cc5"
branch_labels = None
depends_on = None


def upgrade():  # noqa

    months = pd.date_range(start="2025-01-01", end="2029-12-01", freq="MS")
    for month in months:
        partition_name = f"forecast_value_{month.strftime('%Y_%m')}"
        month_start = month.strftime("%Y-%m-%d")
        month_end = (month + pd.DateOffset(months=1)).strftime("%Y-%m-%d")
        op.execute(
            f"ALTER TABLE forecast_value ATTACH PARTITION {partition_name} FOR VALUES FROM ('{month_start}') TO ('{month_end}');"  # noqa
        )
    # ### end Alembic commands ###


def downgrade():  # noqa
    pass
