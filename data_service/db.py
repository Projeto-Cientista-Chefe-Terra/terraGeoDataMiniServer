# # data_service/db.py

# import os
# import sqlite3
# from sqlalchemy import create_engine
# from dotenv import load_dotenv

# # Carrega variáveis do arquivo .env (caso exista)
# load_dotenv()

# # Se o usuário colocou o caminho em .env→SQLITE_PATH, usa; senão, default:
# SQLITE_PATH = os.getenv("SQLITE_PATH", os.path.join(os.getcwd(), "data/terra_data.sqlite"))

# def get_sqlalchemy_engine():
#     """
#     Retorna um engine SQLAlchemy conectado ao SQLite (SpatiaLite).
#     """
#     uri = f"sqlite:///{SQLITE_PATH}"
#     return create_engine(uri, echo=False)

# def get_sqlite_connection():
#     """
#     Retorna uma conexão sqlite3 pura e tenta carregar a extensão SpatiaLite
#     (mod_spatialite ou libspatialite.so).
#     """
#     conn = sqlite3.connect(SQLITE_PATH)
#     conn.enable_load_extension(True)
#     try:
#         conn.load_extension("mod_spatialite")
#     except Exception:
#         try:
#             conn.load_extension("libspatialite.so")
#         except Exception:
#             pass
#     return conn

# from sqlalchemy import create_engine
# from config import settings

# def get_sqlalchemy_engine():
#     if settings.DATABASE_TYPE == "sqlite":
#         uri = f"sqlite:///{settings.SQLITE_PATH}"
#     else:
#         uri = settings.postgres_dsn
#     return create_engine(uri, echo=False, future=True)

# db.py
from sqlalchemy import create_engine
from config import settings

def get_sqlalchemy_engine():
    if settings.DATABASE_TYPE == "sqlite":
        uri = f"sqlite:///{settings.SQLITE_PATH}"
    else:
        uri = settings.postgres_dsn
    return create_engine(uri, echo=False, future=True)
