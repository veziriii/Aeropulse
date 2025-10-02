import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# --- Load .env so DATABASE_URL / POSTGRES_DSN / PG_SCHEMA are available ---
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

# --- Alembic Config object ---
config = context.config

# If you keep an .ini logging config, this wires it up
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# --- Import your models' metadata (editable install: pip install -e .) ---
from aeropulse.models import Base  # exported in src/aeropulse/models/__init__.py

target_metadata = Base.metadata

# --- Pick the DB URL from env (prefer DSN with explicit driver) ---
db_url = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError(
        "Set POSTGRES_DSN or DATABASE_URL in your environment/.env "
        "(e.g., postgresql+psycopg2://postgres:postgres@localhost:5432/Aeropulse)"
    )

# Force Alembic to use the env var instead of any placeholder in alembic.ini
config.set_main_option("sqlalchemy.url", db_url)

# Optional: where to store Alembic's version table (defaults to 'public')
version_table_schema = os.getenv("PG_SCHEMA")  # e.g., 'public' (your .env sets this)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table_schema=version_table_schema,
        compare_type=True,  # autogenerate picks up type changes
        compare_server_default=True,  # and server defaults
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema=version_table_schema,
            compare_type=True,
            compare_server_default=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
