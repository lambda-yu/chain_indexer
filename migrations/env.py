# migrations/env.py
from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from core.config.models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Allow env override; strip async driver portion if present
url = os.environ.get("ALEMBIC_DATABASE_URL") or config.get_main_option("sqlalchemy.url")
assert url is not None
# Alembic uses sync drivers; map aiosqlite→sqlite, asyncpg→postgresql, asyncmy→mysql+pymysql
sync_url = (
    url.replace("+aiosqlite", "")
    .replace("+asyncpg", "")
    .replace("+asyncmy", "+pymysql")
)
config.set_main_option("sqlalchemy.url", sync_url)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=sync_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
