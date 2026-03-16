"""Tests for catalog.iceberg — Iceberg metadata reader."""

from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import MagicMock

import pytest

from catalog.iceberg import (
    read_iceberg_metadata,
    _parse_s3_path,
    _read_version_hint,
    _parse_schema,
    _parse_partition_spec,
)


def test_parse_s3_path():
    assert _parse_s3_path("s3://my-bucket/path/to/table") == (
        "my-bucket",
        "path/to/table/",
    )
    assert _parse_s3_path("s3://my-bucket/") == ("my-bucket", "")
    assert _parse_s3_path("s3://my-bucket") == ("my-bucket", "")


def test_read_version_hint_from_text_file():
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": BytesIO(b"5\n")}

    version = _read_version_hint(s3, "bucket", "prefix/")
    assert version == 5
    s3.get_object.assert_called_with(Bucket="bucket", Key="prefix/version-hint.text")


def test_read_version_hint_from_listing_fallback():
    s3 = MagicMock()
    # version-hint.text missing
    s3.get_object.side_effect = Exception("404")
    # v1, v2, v3 metadata files exist
    s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "prefix/v1.metadata.json"},
            {"Key": "prefix/v2.metadata.json"},
            {"Key": "prefix/v3.metadata.json"},
            {"Key": "prefix/not-a-meta-file.txt"},
        ]
    }

    version = _read_version_hint(s3, "bucket", "prefix/")
    assert version == 3


def test_read_version_hint_not_found_raises():
    s3 = MagicMock()
    s3.get_object.side_effect = Exception("404")
    s3.list_objects_v2.return_value = {"Contents": []}

    with pytest.raises(FileNotFoundError, match="No Iceberg metadata found"):
        _read_version_hint(s3, "bucket", "prefix/")


def test_parse_schema_basic():
    schema_json = {
        "fields": [
            {"id": 1, "name": "id", "type": "int", "required": True},
            {"id": 2, "name": "data", "type": "string", "required": False},
            {"id": 3, "name": "ts", "type": "timestamp", "required": True},
        ]
    }
    columns = _parse_schema(schema_json)

    assert len(columns) == 3
    assert columns[0].name == "id"
    assert columns[0].dtype == "INTEGER"
    assert columns[0].required is True
    assert columns[1].name == "data"
    assert columns[1].dtype == "VARCHAR"
    assert columns[2].dtype == "TIMESTAMP"


def test_parse_partition_spec():
    spec_json = {
        "fields": [
            {
                "source-id": 1,
                "field-id": 1000,
                "name": "id_bucket",
                "transform": "bucket[16]",
            },
            {"source-id": 3, "field-id": 1001, "name": "ts_day", "transform": "day"},
        ]
    }
    parts = _parse_partition_spec(spec_json)

    assert len(parts) == 2
    assert parts[0].name == "id_bucket"
    assert parts[0].transform == "bucket[16]"
    assert parts[1].name == "ts_day"
    assert parts[1].transform == "day"


def test_read_iceberg_metadata_full_flow():
    s3 = MagicMock()

    # 1. Mock version hint
    s3.get_object.side_effect = [
        {"Body": BytesIO(b"1")},  # version-hint.text
        {
            "Body": BytesIO(
                json.dumps(
                    {
                        "table-uuid": "test-uuid",
                        "format-version": 2,
                        "location": "s3://bucket/table/",
                        "current-schema-id": 0,
                        "schemas": [
                            {
                                "schema-id": 0,
                                "fields": [
                                    {"id": 1, "name": "id", "type": "int"},
                                    {"id": 2, "name": "val", "type": "float"},
                                ],
                            }
                        ],
                        "default-spec-id": 0,
                        "partition-specs": [{"spec-id": 0, "fields": []}],
                        "current-snapshot-id": 123,
                        "snapshots": [
                            {
                                "snapshot-id": 123,
                                "timestamp-ms": 1640995200000,
                                "manifest-list": "s3://bucket/table/metadata/snap-123.avro",
                                "summary": {
                                    "total-data-files": "5",
                                    "total-records": "1000",
                                    "total-files-size": "1048576",
                                },
                            }
                        ],
                    }
                ).encode()
            )
        },  # v1.metadata.json
    ]

    meta = read_iceberg_metadata("s3://bucket/table/", s3_client=s3)

    assert meta.table_uuid == "test-uuid"
    assert len(meta.schema_columns) == 2
    assert meta.total_files == 5
    assert meta.total_rows == 1000
    assert meta.total_bytes == 1048576
    assert meta.current_snapshot.snapshot_id == 123
    assert (
        meta.current_snapshot.manifest_list
        == "s3://bucket/table/metadata/snap-123.avro"
    )
