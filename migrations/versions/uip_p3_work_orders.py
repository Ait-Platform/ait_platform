"""UIP Phase 3: explicit external work lifecycle. No legacy ancestry or backfill.

Existing work orders are deliberately refused: dispatch history cannot be inferred.
Downgrade is refused; use a separately reviewed preservation plan.
"""
from alembic import op
import sqlalchemy as sa

revision = "uip_p3_work_orders"
down_revision = "a27c9e4b6102"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    # Refuse interpretation of legacy work orders, even terminal-looking rows.
    if connection.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM uip_work_order)")):
        raise RuntimeError("UIP Phase 3 refused: existing work orders require a reviewed historical preservation plan")
    inspector = sa.inspect(connection)
    if set(inspector.get_table_names()) & {"uip_provider_capability", "uip_provider_user", "uip_work_order_action"}:
        raise RuntimeError("UIP Phase 3 refused: unexpected pre-existing Phase 3 tables")
    additions = {
        "uip_provider": {"availability", "version", "updated_at"},
        "uip_work_order": {"organization_id", "created_by", "version", "state_changed_at", "service_location"},
        "core_task": {"uip_version", "uip_terminal_actor_id", "uip_cancelled_at", "uip_cancellation_reason"},
    }
    for table, fields in additions.items():
        if fields & {c["name"] for c in inspector.get_columns(table)}:
            raise RuntimeError("UIP Phase 3 refused: unexpected pre-existing columns on " + table)
    op.create_unique_constraint("uq_uip_interaction_id_org", "core_interaction", ["id", "organization_id"])
    op.create_unique_constraint("uq_uip_provider_id_org", "uip_provider", ["id", "organization_id"])
    op.create_unique_constraint("uq_uip_audit_id_org", "uip_audit_event", ["id", "organization_id"])
    op.add_column("uip_provider", sa.Column("availability", sa.String(20), nullable=False, server_default="UNKNOWN"))
    op.add_column("uip_provider", sa.Column("version", sa.Integer(), nullable=False, server_default="1"))
    op.add_column("uip_provider", sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()))
    op.create_check_constraint("ck_uip_provider_availability", "uip_provider", "availability IN ('UNKNOWN','AVAILABLE','UNAVAILABLE')")
    op.add_column("core_task", sa.Column("uip_version", sa.Integer(), nullable=True))
    op.add_column("core_task", sa.Column("uip_terminal_actor_id", sa.Integer(), nullable=True))
    op.create_foreign_key("fk_uip_task_terminal_actor", "core_task", "user", ["uip_terminal_actor_id"], ["id"])
    op.add_column("core_task", sa.Column("uip_cancelled_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("core_task", sa.Column("uip_cancellation_reason", sa.Text(), nullable=True))
    op.add_column("uip_work_order", sa.Column("organization_id", sa.Integer(), nullable=False))
    op.create_foreign_key("fk_uip_order_org", "uip_work_order", "core_organization", ["organization_id"], ["id"])
    op.add_column("uip_work_order", sa.Column("created_by", sa.Integer(), nullable=False))
    op.create_foreign_key("fk_uip_order_creator", "uip_work_order", "user", ["created_by"], ["id"])
    op.add_column("uip_work_order", sa.Column("version", sa.Integer(), nullable=False))
    op.add_column("uip_work_order", sa.Column("state_changed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()))
    op.add_column("uip_work_order", sa.Column("service_location", sa.String(500), nullable=False))
    op.alter_column("uip_work_order", "status", existing_type=sa.String(50), nullable=False)
    op.create_unique_constraint("uq_uip_order_id_org", "uip_work_order", ["id", "organization_id"])
    op.create_foreign_key("fk_uip_order_issue_org", "uip_work_order", "core_interaction", ["interaction_id", "organization_id"], ["id", "organization_id"])
    op.create_foreign_key("fk_uip_order_provider_org", "uip_work_order", "uip_provider", ["provider_id", "organization_id"], ["id", "organization_id"])
    op.create_check_constraint("ck_uip_order_state", "uip_work_order", "status IN ('CREATED','DISPATCHED','ACCEPTED','IN_PROGRESS','COMPLETED','VERIFIED','CLOSED','CANCELLED','REJECTED','FAILED')")
    op.create_check_constraint("ck_uip_order_version", "uip_work_order", "version > 0")
    op.create_index("uq_uip_order_open_issue", "uip_work_order", ["interaction_id"], unique=True, postgresql_where=sa.text("status NOT IN ('CLOSED','CANCELLED','REJECTED','FAILED')"))
    op.create_index("ix_uip_order_org_provider_state", "uip_work_order", ["organization_id", "provider_id", "status"])
    op.execute('CREATE TABLE uip_provider_capability (\n\tid SERIAL NOT NULL, \n\torganization_id INTEGER NOT NULL, \n\tprovider_id INTEGER NOT NULL, \n\tcategory VARCHAR(100) NOT NULL, \n\tPRIMARY KEY (id), \n\tCONSTRAINT fk_uip_capability_provider_org FOREIGN KEY(provider_id, organization_id) REFERENCES uip_provider (id, organization_id), \n\tCONSTRAINT uq_uip_provider_category UNIQUE (provider_id, category)\n)')
    op.execute('CREATE TABLE uip_provider_user (\n\tid SERIAL NOT NULL, \n\torganization_id INTEGER NOT NULL, \n\tprovider_id INTEGER NOT NULL, \n\tmembership_id INTEGER NOT NULL, \n\tis_active BOOLEAN NOT NULL, \n\tlinked_by INTEGER NOT NULL, \n\tlinked_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\trevoked_by INTEGER, \n\trevoked_at TIMESTAMP WITH TIME ZONE, \n\tPRIMARY KEY (id), \n\tCONSTRAINT fk_uip_provider_user_provider_org FOREIGN KEY(provider_id, organization_id) REFERENCES uip_provider (id, organization_id), \n\tCONSTRAINT fk_uip_provider_user_member_org FOREIGN KEY(membership_id, organization_id) REFERENCES core_organization_member (id, organization_id), \n\tFOREIGN KEY(linked_by) REFERENCES "user" (id), \n\tFOREIGN KEY(revoked_by) REFERENCES "user" (id)\n)')
    op.execute('CREATE UNIQUE INDEX uq_uip_provider_user_active ON uip_provider_user (provider_id, membership_id) WHERE is_active')
    op.execute('CREATE TABLE uip_work_order_action (\n\tid SERIAL NOT NULL, \n\torganization_id INTEGER NOT NULL, \n\twork_order_id INTEGER NOT NULL, \n\tactor_user_id INTEGER NOT NULL, \n\taction VARCHAR(40) NOT NULL, \n\tprevious_state VARCHAR(50), \n\tnew_state VARCHAR(50) NOT NULL, \n\tresulting_version INTEGER NOT NULL, \n\toccurred_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\trequest_key VARCHAR(36) NOT NULL, \n\tfingerprint VARCHAR(64) NOT NULL, \n\treason_code VARCHAR(40), \n\tnote TEXT, \n\tdispatch_method VARCHAR(30), \n\taudit_event_id INTEGER NOT NULL, \n\tPRIMARY KEY (id), \n\tCONSTRAINT fk_uip_action_order_org FOREIGN KEY(work_order_id, organization_id) REFERENCES uip_work_order (id, organization_id), \n\tCONSTRAINT fk_uip_action_audit_org FOREIGN KEY(audit_event_id, organization_id) REFERENCES uip_audit_event (id, organization_id), \n\tCONSTRAINT uq_uip_action_request UNIQUE (organization_id, actor_user_id, request_key), \n\tCONSTRAINT uq_uip_action_version UNIQUE (work_order_id, resulting_version), \n\tFOREIGN KEY(actor_user_id) REFERENCES "user" (id)\n)')
    op.execute('CREATE INDEX ix_uip_action_order_time ON uip_work_order_action (organization_id, work_order_id, occurred_at)')
    op.execute("CREATE TRIGGER uip_work_order_action_append_only BEFORE UPDATE OR DELETE ON uip_work_order_action FOR EACH ROW EXECUTE FUNCTION uip_audit_reject_mutation()")


def downgrade():
    raise RuntimeError("UIP Phase 3 downgrade refused: a reviewed historical preservation plan is required")
