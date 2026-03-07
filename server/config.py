"""Configuration loader — reads config.yaml and validates with Pydantic."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, ConfigDict


class AwsConfig(BaseModel):
    region: str = "us-east-1"
    access_key_id: str = ""
    secret_access_key: str = ""
    athena_output_location: str = ""
    athena_workgroup: str = "primary"


class PartitionColumnConfig(BaseModel):
    name: str
    type: str = "string"


class TableConfig(BaseModel):
    name: str
    s3_path: str
    format: str = "parquet"
    partition_columns: List[PartitionColumnConfig] = Field(default_factory=list)
    description: str = ""

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        if v not in ("parquet", "iceberg"):
            raise ValueError(f"format must be 'parquet' or 'iceberg', got: {v}")
        return v

    @field_validator("s3_path")
    @classmethod
    def validate_s3_path(cls, v: str) -> str:
        if not v.startswith("s3://"):
            raise ValueError(f"s3_path must start with s3://, got: {v}")
        return v.rstrip("/") + "/"


class EngineConfig(BaseModel):
    duckdb_max_scan_bytes: int = 10 * 1024 ** 3   # 10 GB
    default_row_limit: int = 1000
    max_row_limit: int = 50_000
    worker_pool_size: int = 4
    query_timeout_seconds: int = 120


class CacheConfig(BaseModel):
    db_path: str = "/tmp/s3_mcp_schema_cache.db"
    stale_threshold_hours: float = 24.0
    auto_refresh: bool = True


class CostGatesConfig(BaseModel):
    warn_threshold_usd: float = 0.10
    block_threshold_usd: float = 1.00


class LoggingConfig(BaseModel):
    level: str = "INFO"
    format: str = "json"
    query_log: bool = True


class Config(BaseModel):
    model_config = ConfigDict(extra="ignore")

    aws: AwsConfig = Field(default_factory=AwsConfig)
    tables: List[TableConfig] = Field(default_factory=list)
    engine: EngineConfig = Field(default_factory=EngineConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    cost_gates: CostGatesConfig = Field(default_factory=CostGatesConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    def get_table(self, name: str) -> Optional[TableConfig]:
        return next((t for t in self.tables if t.name == name), None)

    @property
    def table_names(self) -> List[str]:
        return [t.name for t in self.tables]


def load_config(path: str = "config/config.yaml") -> Config:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}. "
            f"Copy config/config.example.yaml to {config_path} and fill in your values."
        )
    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}
    return Config(**raw)
