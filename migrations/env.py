# migrations/env.py
from __future__ import annotations
from logging.config import fileConfig
from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import make_url

# Load model metadata only. Never execute application startup maintenance.
import os
from app.extensions import db
import app.models  # noqa: F401 -- registers mapped tables, not the app factory

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)
target_metadata = db.metadata
# A supplied connection is used by isolated tests and programmatic migrations.
if config.attributes.get("connection") is None:
    db_url = os.environ.get("DATABASE_URL") or config.get_main_option("sqlalchemy.url")
    if not db_url:
        raise RuntimeError("Set DATABASE_URL explicitly before running migrations.")
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    if make_url(db_url).get_backend_name() != "postgresql":
        raise RuntimeError("Migrations require an explicit PostgreSQL DATABASE_URL.")
    config.set_main_option("sqlalchemy.url", db_url.replace("%", "%%"))

def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    supplied = config.attributes.get("connection")
    if supplied is not None:
        context.configure(connection=supplied, target_metadata=target_metadata,
                          compare_type=True, compare_server_default=True)
        with context.begin_transaction():
            context.run_migrations()
        return
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
