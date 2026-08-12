from datetime import date
from typing import Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class GlobalConfig(BaseSettings):
    """Global configurations."""

    model_config = SettingsConfigDict(extra="ignore")

    USER: Optional[str] = Field(default=None, validation_alias="CTC_USERNAME")

    # static attributes

    PROJECT_ROOT_DIR: Optional[str] = None
    HIVE_DEFAULT_DB: Optional[str] = None
    HIVE_STAGING_DB: Optional[str] = None
    HIVE_DEFAULT_DB_HDFS_DIR: Optional[str] = None
    HIVE_STAGING_DB_HDFS_DIR: Optional[str] = None
    HIVE_TABLE_PREFIX: Optional[str] = ''
    ENV_HANDLE: Optional[str] = None
    PROJECT_DIR: Optional[str] = None
    PATTERN_DIR: Optional[str] = None

    # derived attributes

    INPUT_DIR: Optional[str] = None

    @model_validator(mode="after")
    def set_pattern_dir(self):
        if self.PATTERN_DIR is None and self.PROJECT_DIR is not None:
            self.PATTERN_DIR = f"{self.PROJECT_DIR}/hql_patterns"
        return self

