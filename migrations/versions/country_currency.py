from alembic import op
import sqlalchemy as sa

# revision identifiers:
revision = "ref_country_currency"
down_revision = None  # set this to your latest revision id
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "ref_country_currency",
        sa.Column("alpha2", sa.String(2), primary_key=True),   # e.g. "ZA"
        sa.Column("currency", sa.String(3), nullable=False),   # e.g. "ZAR"
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )
    # minimal seed so UI looks sane on day one
    op.execute(
        "INSERT INTO ref_country_currency (alpha2, currency) VALUES "
        "('ZA','ZAR'),('US','USD'),('GB','GBP'),('IN','INR'),('IE','EUR')"
    )


def downgrade():
    op.drop_table("ref_country_currency")
