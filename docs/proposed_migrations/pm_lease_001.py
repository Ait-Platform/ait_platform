"""PROPOSED ONLY: outside Alembic discovery until schema/backup/rehearsal approval."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB
revision="pm_lease_001"
down_revision=None
branch_labels=("pm_lease",)
depends_on=None

def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!="postgresql":raise RuntimeError("PostgreSQL required")
    inspector=sa.inspect(bind)
    for table in ("bil_lease_draft","bil_lease_document"):
        if inspector.has_table(table):raise RuntimeError("Existing lease draft objects require review")
    for table,columns in {"user":{"id"},"bil_property":{"id","manager_id"}}.items():
        if not inspector.has_table(table) or not columns<={c['name'] for c in inspector.get_columns(table)}:
            raise RuntimeError("Existing property/user foundation required")
    op.create_table("bil_lease_draft",
        sa.Column("id",UUID(as_uuid=True),primary_key=True),
        sa.Column("property_id",sa.Integer,sa.ForeignKey("bil_property.id",ondelete="RESTRICT"),nullable=False),
        sa.Column("manager_id",sa.Integer,sa.ForeignKey("user.id",ondelete="RESTRICT"),nullable=False),
        sa.Column("registration_token",UUID(as_uuid=True),nullable=False),
        sa.Column("source_version",sa.String(12),nullable=False),
        sa.Column("items",JSONB,nullable=False,server_default=sa.text("'{}'::jsonb")),
        sa.Column("version",sa.Integer,nullable=False,server_default="1"),
        sa.Column("status",sa.String(16),nullable=False,server_default="draft"),
        sa.Column("created_at",sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.Column("updated_at",sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.CheckConstraint("status='draft' AND version>0",name="ck_pm_lease_draft_only"),
        sa.CheckConstraint("jsonb_typeof(items)='object'",name="ck_pm_lease_items_object"),
        sa.UniqueConstraint("manager_id","registration_token",name="uq_pm_lease_registration"),
        sa.UniqueConstraint("id","property_id","manager_id",name="uq_pm_lease_draft_scope"))
    op.create_index("ix_pm_lease_draft_scope","bil_lease_draft",["manager_id","property_id"])
    op.create_table("bil_lease_document",
        sa.Column("id",UUID(as_uuid=True),primary_key=True),
        sa.Column("draft_id",UUID(as_uuid=True),nullable=False),
        sa.Column("property_id",sa.Integer,nullable=False),
        sa.Column("manager_id",sa.Integer,nullable=False),
        sa.Column("item_key",sa.String(80),nullable=False),
        sa.Column("storage_key",sa.String(500),nullable=False,unique=True),
        sa.Column("filename",sa.String(200),nullable=False),
        sa.Column("mime_type",sa.String(100),nullable=False),
        sa.Column("size_bytes",sa.Integer,nullable=False),
        sa.Column("sha256",sa.String(64),nullable=False),
        sa.Column("state",sa.String(16),nullable=False,server_default="pending"),
        sa.Column("created_at",sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.Column("verified_at",sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["draft_id","property_id","manager_id"],
            ["bil_lease_draft.id","bil_lease_draft.property_id","bil_lease_draft.manager_id"],ondelete="RESTRICT"),
        sa.CheckConstraint("state IN ('pending','available','failed')",name="ck_pm_lease_document_state"),
        sa.CheckConstraint("size_bytes>0 AND size_bytes<=10485760",name="ck_pm_lease_document_size"),
        sa.CheckConstraint("sha256 ~ '^[0-9a-f]{64}$'",name="ck_pm_lease_document_digest"))
    op.create_index("ix_pm_lease_document_draft","bil_lease_document",["draft_id","property_id","manager_id"])

def downgrade():
    raise RuntimeError("Destructive downgrade disabled; disable feature and preserve draft/document records")
