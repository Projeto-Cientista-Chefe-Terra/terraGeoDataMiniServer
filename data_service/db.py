# data_service/db.py

from sqlalchemy import create_engine, event
from config import settings, DatabaseType

def get_sqlalchemy_engine():
    if settings.DATABASE_TYPE == DatabaseType.SQLITE:
        uri = f"sqlite:///{settings.SQLITE_PATH}"
        engine = create_engine(uri, echo=False, future=True)

        # 👇 Carrega a extensão SpatiaLite em cada conexão
        @event.listens_for(engine, "connect")
        def load_spatialite(dbapi_connection, connection_record):
            # habilita carga de extensões
            dbapi_connection.enable_load_extension(True)
            # o nome aqui pode variar: 'mod_spatialite', 'libspatialite.so', ...
            dbapi_connection.load_extension("mod_spatialite")

        return engine

    # Postgres segue normal
    return create_engine(settings.postgres_dsn, echo=False, future=True)

