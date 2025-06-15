# config.py
from enum import Enum
from pydantic_settings import BaseSettings, SettingsConfigDict

class DatabaseType(str, Enum):
    SQLITE = "sqlite"
    POSTGRES = "postgres"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    DATABASE_TYPE: DatabaseType
    SQLITE_PATH: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str

    # defaults seguros
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str   = "geodata"

    TABLE_GEOM_MUNICIPIOS: str   =  "municipios_ceara"
    TABLE_DADOS_FUNDIARIOS: str   = "malha_fundiaria_ceara"

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:"
            f"{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:"
            f"{self.POSTGRES_PORT}/"
            f"{self.POSTGRES_DB}"
        )

settings = Settings()
