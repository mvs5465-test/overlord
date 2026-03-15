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
    allowed_repo_roots_raw: str = Field(
        default="~/projects",
        alias="OVERLORD_ALLOWED_REPO_ROOTS",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
    )

    @property
    def allowed_repo_roots(self) -> list[Path]:
        roots: list[Path] = []
        for raw_value in self.allowed_repo_roots_raw.split(","):
            stripped = raw_value.strip()
            if stripped:
                roots.append(Path(stripped).expanduser().resolve())
        return roots
