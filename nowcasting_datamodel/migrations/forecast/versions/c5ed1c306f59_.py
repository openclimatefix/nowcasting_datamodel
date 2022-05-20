"""
Make location.gsp_id unique and not nullable

Revision ID: c5ed1c306f59
Revises: 870afe3b2b8d
Create Date: 2022-05-09 21:42:38.179441

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c5ed1c306f59"
down_revision = "870afe3b2b8d"
branch_labels = None
depends_on = None


def upgrade():  # noqa
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column("location", "gsp_id", existing_type=sa.INTEGER(), nullable=False)
    op.drop_index("ix_location_gsp_id", table_name="location")
    op.create_index(op.f("ix_location_gsp_id"), "location", ["gsp_id"], unique=True)
    # ### end Alembic commands ###


def downgrade():  # noqa
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_location_gsp_id"), table_name="location")
    op.create_index("ix_location_gsp_id", "location", ["gsp_id"], unique=False)
    op.alter_column("location", "gsp_id", existing_type=sa.INTEGER(), nullable=True)
    # ### end Alembic commands ###