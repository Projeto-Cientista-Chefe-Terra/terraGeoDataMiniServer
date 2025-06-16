# config.py
from enum import Enum
from typing import List
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import os

class DatabaseType(str, Enum):
    SQLITE = "sqlite"
    POSTGRES = "postgres"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Configurações principais do banco de dados
    DATABASE_TYPE: DatabaseType
    SQLITE_PATH: str = "data/terra_data.sqlite"
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "geodata"

    # Nomes das tabelas
    TABLE_GEOM_MUNICIPIOS: str = "municipios_ceara"
    TABLE_DADOS_FUNDIARIOS: str = "malha_fundiaria_ceara"

    # Configurações de CORS
    ALLOWED_ORIGINS: List[str] = ["*"]
    
    # Configurações de performance
    GEOMETRY_TOLERANCE: float = 0.0001
    GEOMETRY_DECIMALS: int = 6
    PREPROCESS_START_HOUR: int = 2
    PREPROCESS_START_MINUTE: int = 0

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:"
            f"{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:"
            f"{self.POSTGRES_PORT}/"
            f"{self.POSTGRES_DB}"
        )
    
    @property
    def sqlite_dsn(self) -> str:
        return f"sqlite:///{os.path.abspath(self.SQLITE_PATH)}"

    @field_validator("ALLOWED_ORIGINS", mode="before")
    def parse_allowed_origins(cls, v):
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")] if v != "*" else ["*"]
        return v

    @field_validator("DATABASE_TYPE", mode="before")
    def validate_db_type(cls, v):
        try:
            return DatabaseType(v.lower())
        except ValueError:
            raise ValueError(f"Tipo de banco de dados inválido. Use 'sqlite' ou 'postgres'")

settings = Settings()