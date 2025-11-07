# migrations/env.py
from __future__ import annotations
from logging.config import fileConfig
from alembic import context
from sqlalchemy import engine_from_config, pool

# --- Load Flask app & db metadata ---
from app import create_app
from app.extensions import db  # db = SQLAlchemy()

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

app = create_app()

with app.app_context():
    target_metadata = db.metadata
    db_url = app.config.get("SQLALCHEMY_DATABASE_URI")
    if not db_url:
        raise RuntimeError("SQLALCHEMY_DATABASE_URI is not configured on the Flask app.")
    config.set_main_option("sqlalchemy.url", db_url)

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
