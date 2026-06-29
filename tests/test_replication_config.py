"""Tests for replication configuration in orchestration_config.py."""

from __future__ import annotations

from pathlib import Path

from dbt_dagsterizer.orchestration_config import (
    ReplicationEntry,
    index as index_orch,
    load_or_create,
    set_replication_entry,
)


def test_load_or_create_defaults_include_replication(tmp_path: Path):
    cfg_path = tmp_path / "dagsterization.yml"
    data = load_or_create(cfg_path)
    assert "replication" in data
    assert data["replication"]["enabled"] is False
    assert data["replication"]["entries"] == []


def test_load_or_create_with_none_data_includes_replication(tmp_path: Path):
    cfg_path = tmp_path / "dagsterization.yml"
    cfg_path.write_text("null\n", encoding="utf-8")
    data = load_or_create(cfg_path)
    assert "replication" in data
    assert data["replication"]["enabled"] is False
    assert data["replication"]["entries"] == []


def test_load_or_create_normalizes_missing_replication_keys(tmp_path: Path):
    cfg_path = tmp_path / "dagsterization.yml"
    cfg_path.write_text("version: 1\nreplication: {}\n", encoding="utf-8")
    data = load_or_create(cfg_path)
    assert data["replication"]["enabled"] is False
    assert data["replication"]["entries"] == []


def test_set_replication_entry_adds_new(tmp_path: Path):
    data = load_or_create(tmp_path / "dagsterization.yml")
    set_replication_entry(
        data=data,
        model="orders",
        enabled=True,
        destination_table=None,
        destination_schema=None,
        write_disposition=None,
        partition_column=None,
    )
    entries = data["replication"]["entries"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["model"] == "orders"
    assert entry["enabled"] is True
    assert entry["destination_table"] == "orders"
    assert entry["destination_schema"] == "dbo"
    assert entry["write_disposition"] == "replace"
    assert "partition_column" not in entry


def test_set_replication_entry_upserts_existing(tmp_path: Path):
    data = load_or_create(tmp_path / "dagsterization.yml")
    set_replication_entry(
        data=data,
        model="orders",
        enabled=True,
        destination_table=None,
        destination_schema=None,
        write_disposition=None,
        partition_column=None,
    )
    set_replication_entry(
        data=data,
        model="orders",
        enabled=False,
        destination_table="orders_copy",
        destination_schema="replication",
        write_disposition="append",
        partition_column="order_date",
    )
    entries = data["replication"]["entries"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["model"] == "orders"
    assert entry["enabled"] is False
    assert entry["destination_table"] == "orders_copy"
    assert entry["destination_schema"] == "replication"
    assert entry["write_disposition"] == "append"
    assert entry["partition_column"] == "order_date"


def test_set_replication_entry_preserves_other_entries(tmp_path: Path):
    data = load_or_create(tmp_path / "dagsterization.yml")
    set_replication_entry(
        data=data,
        model="orders",
        enabled=True,
        destination_table=None,
        destination_schema=None,
        write_disposition=None,
        partition_column=None,
    )
    set_replication_entry(
        data=data,
        model="customers",
        enabled=True,
        destination_table=None,
        destination_schema=None,
        write_disposition=None,
        partition_column="customer_date",
    )
    entries = data["replication"]["entries"]
    assert len(entries) == 2
    models = {e["model"] for e in entries}
    assert models == {"orders", "customers"}


def test_set_replication_entry_invalid_write_disposition(tmp_path: Path):
    data = load_or_create(tmp_path / "dagsterization.yml")
    try:
        set_replication_entry(
            data=data,
            model="orders",
            enabled=True,
            destination_table=None,
            destination_schema=None,
            write_disposition="upsert",
            partition_column=None,
        )
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "write_disposition" in str(e)


def test_set_replication_entry_accepts_merge(tmp_path: Path):
    """Verify that 'merge' is accepted as a valid write disposition."""
    data = load_or_create(tmp_path / "dagsterization.yml")
    set_replication_entry(
        data=data,
        model="orders",
        enabled=True,
        destination_table=None,
        destination_schema=None,
        write_disposition="merge",
        partition_column=None,
    )
    entries = data["replication"]["entries"]
    assert len(entries) == 1
    assert entries[0]["model"] == "orders"
    assert entries[0]["write_disposition"] == "merge"


def test_set_replication_entry_empty_model(tmp_path: Path):
    data = load_or_create(tmp_path / "dagsterization.yml")
    try:
        set_replication_entry(
            data=data,
            model="  ",
            enabled=True,
            destination_table=None,
            destination_schema=None,
            write_disposition=None,
            partition_column=None,
        )
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "model" in str(e).lower()


def test_index_replication_disabled():
    data = {
        "version": 1,
        "replication": {"enabled": False, "entries": [{"model": "orders", "enabled": True}]},
    }
    idx = index_orch(data)
    assert idx.replication_enabled is False
    assert "orders" in idx.replication_entries


def test_index_replication_enabled_with_entries():
    data = {
        "version": 1,
        "replication": {
            "enabled": True,
            "entries": [
                {
                    "model": "orders",
                    "enabled": True,
                    "destination_table": "orders",
                    "destination_schema": "dbo",
                    "write_disposition": "replace",
                    "partition_column": "order_date",
                },
                {
                    "model": "customers",
                    "enabled": False,
                    "destination_table": "customers",
                    "destination_schema": "dbo",
                    "write_disposition": "append",
                },
            ],
        },
    }
    idx = index_orch(data)
    assert idx.replication_enabled is True
    assert len(idx.replication_entries) == 2

    orders = idx.replication_entries["orders"]
    assert orders.enabled is True
    assert orders.destination_table == "orders"
    assert orders.destination_schema == "dbo"
    assert orders.write_disposition == "replace"
    assert orders.partition_column == "order_date"

    customers = idx.replication_entries["customers"]
    assert customers.enabled is False
    assert customers.write_disposition == "append"
    assert customers.partition_column is None


def test_index_replication_defaults():
    data = {"version": 1}
    idx = index_orch(data)
    assert idx.replication_enabled is False
    assert idx.replication_entries == {}


def test_index_replication_entry_defaults():
    data = {
        "version": 1,
        "replication": {
            "enabled": True,
            "entries": [{"model": "orders"}],
        },
    }
    idx = index_orch(data)
    entry = idx.replication_entries["orders"]
    assert entry.enabled is True  # default
    assert entry.destination_table == "orders"  # defaults to model name
    assert entry.destination_schema == "dbo"  # default
    assert entry.write_disposition == "replace"  # default
    assert entry.partition_column is None  # not set


def test_replication_entry_dataclass_is_frozen():
    entry = ReplicationEntry(
        model="orders",
        enabled=True,
        destination_table="orders",
        destination_schema="dbo",
        write_disposition="replace",
        partition_column=None,
    )
    try:
        entry.enabled = False  # type: ignore[misc]
        assert False, "Should have raised FrozenInstanceError"
    except AttributeError:
        pass
