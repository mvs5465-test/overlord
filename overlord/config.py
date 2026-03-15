from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = Field(default="Overlord", alias="OVERLORD_APP_NAME")
    host: str = Field(default="127.0.0.1", alias="HOST")
    port: int = Field(default=8080, alias="PORT")
    data_dir: Path = Field(default=Path("data"), alias="OVERLORD_DATA_DIR")
    default_environment: str = Field(default="local", alias="OVERLORD_DEFAULT_ENVIRONMENT")
    default_workspace: str = Field(default="default", alias="OVERLORD_DEFAULT_WORKSPACE")
    worker_write_token: str | None = Field(default=None, alias="OVERLORD_WORKER_WRITE_TOKEN")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
    )
