"""
SQLite-backed schema and partition metadata cache.

Stores table metadata so the server doesn't re-scan S3 on every request.
Metadata is considered stale after config.cache.stale_threshold_hours.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import List, Optional


@dataclass
class ColumnMeta:
    name: str
    dtype: str
    estimated_bytes_per_row: float = 8.0
    null_fraction: float = 0.0
    distinct_count: Optional[int] = None


@dataclass
class PartitionMeta:
    name: str
    dtype: str
    known_values: List[str] = field(default_factory=list)


@dataclass
class TableMeta:
    table_name: str
    s3_path: str
    format: str
    columns: List[ColumnMeta]
    partition_columns: List[PartitionMeta]
    total_rows: int
    total_bytes: int
    total_files: int
    total_partitions: int
    avg_row_groups_per_file: int
    description: str = ""
    last_refreshed: Optional[datetime] = None
    bytes_per_row_estimate: Optional[float] = None

    @property
    def freshness_hours(self) -> float:
        if not self.last_refreshed:
            return float("inf")
        delta = datetime.now(tz=timezone.utc) - self.last_refreshed.replace(
            tzinfo=timezone.utc
        )
        return delta.total_seconds() / 3600

    @property
    def size_human(self) -> str:
        b = self.total_bytes
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if b < 1024:
                return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} PB"


class SchemaCache:
    def __init__(self, db_path: str = "/tmp/s3_mcp_schema_cache.db"):
        self.db_path = db_path
        self._con = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self._con.executescript("""
            CREATE TABLE IF NOT EXISTS table_meta (
                table_name          TEXT PRIMARY KEY,
                s3_path             TEXT NOT NULL,
                format              TEXT NOT NULL,
                columns_json        TEXT NOT NULL,
                partition_cols_json TEXT NOT NULL,
                total_rows          INTEGER NOT NULL DEFAULT 0,
                total_bytes         INTEGER NOT NULL DEFAULT 0,
                total_files         INTEGER NOT NULL DEFAULT 0,
                total_partitions    INTEGER NOT NULL DEFAULT 0,
                avg_row_groups      INTEGER NOT NULL DEFAULT 4,
                description         TEXT NOT NULL DEFAULT '',
                last_refreshed      TEXT NOT NULL
            );
        """)
        self._con.commit()
        # Migration: add bytes_per_row_estimate column for flat file formats
        try:
            self._con.execute(
                "ALTER TABLE table_meta ADD COLUMN bytes_per_row_estimate REAL"
            )
            self._con.commit()
        except sqlite3.OperationalError:
            pass  # column already exists

    def upsert(self, meta: TableMeta) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        self._con.execute(
            """
            INSERT OR REPLACE INTO table_meta VALUES
            (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                meta.table_name,
                meta.s3_path,
                meta.format,
                json.dumps([asdict(c) for c in meta.columns]),
                json.dumps([asdict(p) for p in meta.partition_columns]),
                meta.total_rows,
                meta.total_bytes,
                meta.total_files,
                meta.total_partitions,
                meta.avg_row_groups_per_file,
                meta.description,
                now,
                meta.bytes_per_row_estimate,
            ),
        )
        self._con.commit()

    def get(self, table_name: str) -> Optional[TableMeta]:
        row = self._con.execute(
            "SELECT * FROM table_meta WHERE table_name = ?", (table_name,)
        ).fetchone()
        if not row:
            return None
        return TableMeta(
            table_name=row[0],
            s3_path=row[1],
            format=row[2],
            columns=[ColumnMeta(**c) for c in json.loads(row[3])],
            partition_columns=[PartitionMeta(**p) for p in json.loads(row[4])],
            total_rows=row[5],
            total_bytes=row[6],
            total_files=row[7],
            total_partitions=row[8],
            avg_row_groups_per_file=row[9],
            description=row[10],
            last_refreshed=datetime.fromisoformat(row[11]),
            bytes_per_row_estimate=row[12] if len(row) > 12 else None,
        )

    def list_tables(self) -> List[str]:
        rows = self._con.execute(
            "SELECT table_name FROM table_meta ORDER BY table_name"
        ).fetchall()
        return [r[0] for r in rows]

    def delete(self, table_name: str) -> None:
        self._con.execute("DELETE FROM table_meta WHERE table_name = ?", (table_name,))
        self._con.commit()

    def close(self) -> None:
        self._con.close()
