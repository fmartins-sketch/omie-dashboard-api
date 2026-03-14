from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List


class Settings(BaseSettings):
    database_url: str = "sqlite:///./omie_dashboard.db"
    omie_app_key: str = ""
    omie_app_secret: str = ""
    jwt_secret: str = "trocar-por-chave-forte"
    admin_username: str = "admin"
    admin_password: str = "admin123"
    cors_origins: str = "http://localhost:3000"
    environment: str = "development"
    sync_interval_minutes: int = 30
    access_token_expire_hours: int = 12

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    @property
    def cors_origins_list(self) -> List[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


settings = Settings()
