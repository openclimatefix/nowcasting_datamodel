""" Add columns to pv systems

They are
- ml capacity
- ocf_id internal id

Revision ID: 5268d6f39e84
Revises: c061c49cbf37
Create Date: 2022-07-07 10:40:36.952739

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5268d6f39e84"
down_revision = "c061c49cbf37"
branch_labels = None
depends_on = None


def upgrade():  # noqa
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("pv_system", sa.Column("ml_capacity_kw", sa.Float(), nullable=True))
    op.add_column("pv_system", sa.Column("ocf_id", sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade():  # noqa
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("pv_system", "ocf_id")
    op.drop_column("pv_system", "ml_capacity_kw")
    # ### end Alembic commands ###