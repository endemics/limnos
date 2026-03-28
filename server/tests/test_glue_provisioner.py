"""Tests for catalog.glue.GlueProvisioner."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from catalog.glue import GlueProvisioner
from catalog.schema_cache import ColumnMeta, PartitionMeta


def _make_config(glue_database="test_db", region="us-east-1"):
    cfg = MagicMock()
    cfg.aws.region = region
    cfg.aws.glue_database = glue_database
    return cfg


def _make_table_cfg(
    name="orders_csv",
    fmt="csv",
    s3_path="s3://bucket/orders/",
    delimiter=",",
    has_header=True,
):
    t = MagicMock()
    t.name = name
    t.format = fmt
    t.s3_path = s3_path
    t.delimiter = delimiter
    t.has_header = has_header
    return t


def _columns():
    return [
        ColumnMeta(name="id", dtype="BIGINT"),
        ColumnMeta(name="name", dtype="VARCHAR"),
    ]


def _partition_cols():
    return [PartitionMeta(name="dt", dtype="string")]


class TestGlueProvisionerSyncTable:
    def _make_provisioner(self, glue_mock, cfg=None):
        cfg = cfg or _make_config()
        with patch("catalog.glue.boto3") as mock_boto3:
            mock_boto3.client.return_value = glue_mock
            prov = GlueProvisioner(cfg)
        prov._glue = glue_mock
        return prov

    def test_csv_calls_create_table_with_lazy_serde(self):
        glue = MagicMock()
        prov = self._make_provisioner(glue)
        table_cfg = _make_table_cfg(fmt="csv")

        prov.sync_table(table_cfg, _columns(), _partition_cols())

        glue.create_table.assert_called_once()
        call_kwargs = glue.create_table.call_args[1]
        sd = call_kwargs["TableInput"]["StorageDescriptor"]
        assert "LazySimpleSerDe" in sd["SerdeInfo"]["SerializationLibrary"]

    def test_json_uses_openx_serde(self):
        glue = MagicMock()
        prov = self._make_provisioner(glue)
        table_cfg = _make_table_cfg(fmt="json", s3_path="s3://bucket/events/")

        prov.sync_table(table_cfg, _columns(), [])

        sd = glue.create_table.call_args[1]["TableInput"]["StorageDescriptor"]
        assert "jsonserde" in sd["SerdeInfo"]["SerializationLibrary"].lower()

    def test_ndjson_uses_openx_serde(self):
        glue = MagicMock()
        prov = self._make_provisioner(glue)
        table_cfg = _make_table_cfg(fmt="ndjson", s3_path="s3://bucket/logs/")

        prov.sync_table(table_cfg, _columns(), [])

        sd = glue.create_table.call_args[1]["TableInput"]["StorageDescriptor"]
        assert "jsonserde" in sd["SerdeInfo"]["SerializationLibrary"].lower()

    def test_csv_delimiter_in_serde_params(self):
        glue = MagicMock()
        prov = self._make_provisioner(glue)
        table_cfg = _make_table_cfg(fmt="csv", delimiter="|")

        prov.sync_table(table_cfg, _columns(), [])

        params = glue.create_table.call_args[1]["TableInput"]["StorageDescriptor"][
            "SerdeInfo"
        ]["Parameters"]
        assert params["field.delim"] == "|"

    def test_csv_has_header_adds_skip_param(self):
        glue = MagicMock()
        prov = self._make_provisioner(glue)
        table_cfg = _make_table_cfg(fmt="csv", has_header=True)

        prov.sync_table(table_cfg, _columns(), [])

        params = glue.create_table.call_args[1]["TableInput"]["StorageDescriptor"][
            "SerdeInfo"
        ]["Parameters"]
        assert params.get("skip.header.line.count") == "1"

    def test_csv_no_header_omits_skip_param(self):
        glue = MagicMock()
        prov = self._make_provisioner(glue)
        table_cfg = _make_table_cfg(fmt="csv", has_header=False)

        prov.sync_table(table_cfg, _columns(), [])

        params = glue.create_table.call_args[1]["TableInput"]["StorageDescriptor"][
            "SerdeInfo"
        ]["Parameters"]
        assert "skip.header.line.count" not in params

    def test_already_exists_calls_update_table(self):
        glue = MagicMock()
        error_response = {"Error": {"Code": "AlreadyExistsException", "Message": ""}}
        glue.create_table.side_effect = ClientError(error_response, "CreateTable")

        prov = self._make_provisioner(glue)
        table_cfg = _make_table_cfg(fmt="csv")

        prov.sync_table(table_cfg, _columns(), [])

        glue.update_table.assert_called_once()

    def test_other_client_error_reraises(self):
        glue = MagicMock()
        error_response = {"Error": {"Code": "AccessDeniedException", "Message": ""}}
        glue.create_table.side_effect = ClientError(error_response, "CreateTable")

        prov = self._make_provisioner(glue)
        table_cfg = _make_table_cfg(fmt="csv")

        with pytest.raises(ClientError):
            prov.sync_table(table_cfg, _columns(), [])

    def test_type_mapping_bigint(self):
        glue = MagicMock()
        prov = self._make_provisioner(glue)
        table_cfg = _make_table_cfg(fmt="csv")

        prov.sync_table(table_cfg, _columns(), [])

        glue_cols = glue.create_table.call_args[1]["TableInput"]["StorageDescriptor"][
            "Columns"
        ]
        bigint_col = next(c for c in glue_cols if c["Name"] == "id")
        assert bigint_col["Type"] == "bigint"

    def test_partition_keys_included(self):
        glue = MagicMock()
        prov = self._make_provisioner(glue)
        table_cfg = _make_table_cfg(fmt="csv")

        prov.sync_table(table_cfg, _columns(), _partition_cols())

        part_keys = glue.create_table.call_args[1]["TableInput"]["PartitionKeys"]
        assert len(part_keys) == 1
        assert part_keys[0]["Name"] == "dt"

    def test_table_name_hyphens_replaced(self):
        glue = MagicMock()
        prov = self._make_provisioner(glue)
        table_cfg = _make_table_cfg(name="my-table", fmt="csv")

        prov.sync_table(table_cfg, _columns(), [])

        name = glue.create_table.call_args[1]["TableInput"]["Name"]
        assert name == "my_table"

    def test_database_name_passed_correctly(self):
        glue = MagicMock()
        cfg = _make_config(glue_database="my_lake")
        prov = self._make_provisioner(glue, cfg=cfg)
        table_cfg = _make_table_cfg(fmt="json")

        prov.sync_table(table_cfg, _columns(), [])

        assert glue.create_table.call_args[1]["DatabaseName"] == "my_lake"

    def test_parquet_uses_parquet_serde(self):
        glue = MagicMock()
        prov = self._make_provisioner(glue)
        table_cfg = _make_table_cfg(fmt="parquet", s3_path="s3://bucket/parquet/")

        prov.sync_table(table_cfg, _columns(), _partition_cols())

        glue.create_table.assert_called_once()
        call_kwargs = glue.create_table.call_args[1]
        sd = call_kwargs["TableInput"]["StorageDescriptor"]
        assert "ParquetHiveSerDe" in sd["SerdeInfo"]["SerializationLibrary"]
        assert "MapredParquetInputFormat" in sd["InputFormat"]
        assert "MapredParquetOutputFormat" in sd["OutputFormat"]
