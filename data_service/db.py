# data_service/db.py

from sqlalchemy import create_engine
from config import settings

def get_sqlalchemy_engine():
    """
    Retorna um engine SQLAlchemy conectado ao SQLite ou ao Postgres,
    conforme configuração em settings.DATABASE_TYPE.
    """
    if settings.DATABASE_TYPE == "sqlite":
        uri = f"sqlite:///{settings.SQLITE_PATH}"
    else:
        uri = settings.postgres_dsn
    return create_engine(uri, echo=False, future=True)
