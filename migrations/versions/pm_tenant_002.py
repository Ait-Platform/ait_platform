"""Repurpose existing tenant identity table for representatives. Prepared only."""
from pathlib import Path
from alembic import op
revision='pm_tenant_002'
down_revision='pm_tenant_001'
branch_labels=None
depends_on=None

def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    sql=(Path(__file__).resolve().parents[2]/'docs'/'sql'/'pm_tenant_002.pgadmin.sql').read_text(encoding='utf-8')
    ddl=sql.split('-- BEGIN MIGRATION DDL\n',1)[1].split('-- END MIGRATION DDL',1)[0]
    bind.exec_driver_sql(ddl)

def downgrade():
    raise RuntimeError('No automatic downgrade: review tenant and representative data before reversal')
