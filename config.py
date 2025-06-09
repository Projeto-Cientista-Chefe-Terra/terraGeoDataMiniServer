from enum import Enum
from pydantic_settings import BaseSettings


class DatabaseType(str, Enum):
    SQLITE = "sqlite"
    POSTGRES = "postgres"

class Settings(BaseSettings):
    DATABASE_TYPE: DatabaseType = DatabaseType.POSTGRES
    SQLITE_PATH: str = "data/terra_data.sqlite"
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "terra"
    POSTGRES_PASSWORD: str = "Asdsee;30"
    POSTGRES_DB: str = "geodata"

    @property
    def postgres_dsn(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

settings = Settings()
