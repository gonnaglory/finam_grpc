from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # --- Keys / Secrets ---
    FINAM_HOST: str = ""
    FINAM_TOKEN: str = ""
    ACCOUNTID: str = ""
    
    # --- Redis ---
    DB_HOST: str = ""
    DB_PASSWORD: str = ""

    model_config = SettingsConfigDict(env_file='.env')

settings = Settings()
