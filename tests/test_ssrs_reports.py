from __future__ import annotations

import json
from pathlib import Path

import dagster as dg
import pytest

from dbt_dagsterizer.orchestration_config import (
    delete_ssrs_report,
    load_or_create,
    set_ssrs_report,
)

SUBSCRIPTION_DESCRIPTION = "Daily sales report subscription"
AGENT_JOB_NAME = "0A1B2C3D-4E5F-6789-ABCD-EF0123456789"


def test_load_or_create_defaults_include_ssrs_reports(tmp_path: Path):
    data = load_or_create(tmp_path / "dagsterization.yml")
    assert data["ssrs_reports"] == []


def test_set_and_delete_ssrs_report(tmp_path: Path):
    data = load_or_create(tmp_path / "dagsterization.yml")

    set_ssrs_report(
        data=data,
        name="daily_sales",
        model="fct_sales_daily",
        subscription_description=SUBSCRIPTION_DESCRIPTION,
        enabled=True,
    )

    assert data["ssrs_reports"] == [
        {
            "name": "daily_sales",
            "model": "fct_sales_daily",
            "subscription_description": SUBSCRIPTION_DESCRIPTION,
            "enabled": True,
        }
    ]

    # Re-setting the same name replaces the entry instead of duplicating it
    set_ssrs_report(
        data=data,
        name="daily_sales",
        model="fct_sales_daily",
        subscription_description="Weekly sales report subscription",
        enabled=False,
    )
    assert len(data["ssrs_reports"]) == 1
    assert data["ssrs_reports"][0]["subscription_description"] == "Weekly sales report subscription"
    assert data["ssrs_reports"][0]["enabled"] is False

    assert delete_ssrs_report(data=data, name="daily_sales") is True
    assert data["ssrs_reports"] == []
    assert delete_ssrs_report(data=data, name="daily_sales") is False


def test_set_ssrs_report_requires_subscription_description(tmp_path: Path):
    data = load_or_create(tmp_path / "dagsterization.yml")

    with pytest.raises(ValueError, match="subscription_description must be non-empty"):
        set_ssrs_report(
            data=data,
            name="broken",
            model="fct_sales_daily",
            subscription_description="  ",
            enabled=True,
        )


def _manifest_with_model(name: str) -> dict:
    return {
        "nodes": {
            f"model.demo.{name}": {
                "resource_type": "model",
                "name": name,
                "fqn": ["demo", "marts", name],
                "tags": [],
                "meta": {},
                "database": "dwh",
                "schema": "dws",
                "identifier": name,
            }
        }
    }


def test_build_auto_ssrs_report_specs(monkeypatch, tmp_path: Path):
    from dbt_dagsterizer.assets.ssrs import auto_config

    (tmp_path / "dagsterization.yml").write_text(
        json.dumps(
            {
                "ssrs_reports": [
                    {
                        "name": "daily_sales",
                        "model": "fct_sales_daily",
                        "subscription_description": SUBSCRIPTION_DESCRIPTION,
                        "enabled": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(auto_config, "load_manifest", lambda: _manifest_with_model("fct_sales_daily"))
    monkeypatch.setattr(auto_config, "get_dbt_project_dir", lambda: tmp_path)

    specs = auto_config.build_auto_ssrs_report_specs()
    assert specs == [
        {
            "name": "daily_sales",
            "model": "fct_sales_daily",
            "upstream_relation": ["dbt", "dwh", "dws", "fct_sales_daily"],
            "subscription_description": SUBSCRIPTION_DESCRIPTION,
            "enabled": True,
        }
    ]


def test_build_auto_ssrs_report_specs_rejects_missing_model(monkeypatch, tmp_path: Path):
    from dbt_dagsterizer.assets.ssrs import auto_config

    (tmp_path / "dagsterization.yml").write_text(
        json.dumps(
            {
                "ssrs_reports": [
                    {
                        "name": "r",
                        "model": "missing_model",
                        "subscription_description": SUBSCRIPTION_DESCRIPTION,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(auto_config, "load_manifest", lambda: _manifest_with_model("fct_sales_daily"))
    monkeypatch.setattr(auto_config, "get_dbt_project_dir", lambda: tmp_path)

    with pytest.raises(ValueError, match="missing dbt model 'missing_model'"):
        auto_config.build_auto_ssrs_report_specs()


def test_build_auto_ssrs_report_specs_requires_subscription_description(monkeypatch, tmp_path: Path):
    from dbt_dagsterizer.assets.ssrs import auto_config

    (tmp_path / "dagsterization.yml").write_text(
        json.dumps(
            {
                "ssrs_reports": [
                    {
                        "name": "daily_sales",
                        "model": "fct_sales_daily",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(auto_config, "load_manifest", lambda: _manifest_with_model("fct_sales_daily"))
    monkeypatch.setattr(auto_config, "get_dbt_project_dir", lambda: tmp_path)

    with pytest.raises(ValueError, match="subscription_description"):
        auto_config.build_auto_ssrs_report_specs()


def _report_spec(**overrides) -> dict:
    spec = {
        "name": "daily_sales",
        "model": "fct_sales_daily",
        "upstream_relation": ["dbt", "dwh", "dws", "fct_sales_daily"],
        "subscription_description": SUBSCRIPTION_DESCRIPTION,
        "enabled": True,
    }
    spec.update(overrides)
    return spec


def test_build_ssrs_report_assets_definition_shape():
    from dbt_dagsterizer.assets.ssrs.factory import build_ssrs_report_assets

    assets = build_ssrs_report_assets(specs=[_report_spec()])
    assert len(assets) == 1
    asset_def = assets[0]

    assert asset_def.key == dg.AssetKey(["ssrs", "daily_sales"])
    assert dg.AssetKey(["dbt", "dwh", "dws", "fct_sales_daily"]) in asset_def.dependency_keys
    assert asset_def.get_asset_spec(asset_def.key).automation_condition is not None
    assert asset_def.group_names_by_key[asset_def.key] == "reports"
    assert "ssrs_agent" in asset_def.required_resource_keys

    disabled_assets = build_ssrs_report_assets(specs=[_report_spec(enabled=False)])
    disabled = disabled_assets[0]
    assert disabled.get_asset_spec(disabled.key).automation_condition is None


def test_ssrs_report_asset_starts_subscription_job():
    from dbt_dagsterizer.assets.ssrs.factory import build_ssrs_report_assets

    started_calls: list[dict] = []

    class FakeAgent:
        def start_subscription_job(self, **kwargs):
            started_calls.append(kwargs)
            return AGENT_JOB_NAME

    assets = build_ssrs_report_assets(specs=[_report_spec()])
    result = dg.materialize(
        assets,
        resources={"ssrs_agent": FakeAgent()},
    )
    assert result.success

    assert started_calls == [{"subscription_description": SUBSCRIPTION_DESCRIPTION}]


class _FakeCursor:
    def __init__(self, lookup_rows: list[tuple]):
        self.lookup_rows = lookup_rows
        self.executed: list[tuple] = []

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.lookup_rows


class _FakeConnection:
    def __init__(self, lookup_rows: list[tuple]):
        self.cursor_obj = _FakeCursor(lookup_rows)
        self.committed: list[bool] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed.append(True)


def _make_client(lookup_rows: list[tuple]):
    from dbt_dagsterizer.resources.ssrs_agent import SsrsAgentJobClient

    connection = _FakeConnection(lookup_rows)
    client = SsrsAgentJobClient(
        host="sql.example.com",
        connection_factory=lambda: connection,
    )
    return client, connection


def test_ssrs_agent_job_client_looks_up_job_name_and_starts_it():
    client, connection = _make_client([(AGENT_JOB_NAME,)])

    job_name = client.start_subscription_job(
        subscription_description=f" {SUBSCRIPTION_DESCRIPTION} "
    )

    assert job_name == AGENT_JOB_NAME
    lookup_sql, lookup_params = connection.cursor_obj.executed[0]
    assert "FROM ReportServer.dbo.Catalog c" in lookup_sql
    assert "INNER JOIN ReportServer.dbo.Subscriptions s ON c.ItemID = s.Report_OID" in lookup_sql
    assert "INNER JOIN ReportServer.dbo.ReportSchedule rs ON s.SubscriptionID = rs.SubscriptionID" in lookup_sql
    assert "INNER JOIN msdb.dbo.sysjobs j ON CAST(rs.ScheduleID AS VARCHAR(100)) = j.name" in lookup_sql
    assert "WHERE s.Description = %s" in lookup_sql
    assert lookup_params == (SUBSCRIPTION_DESCRIPTION,)
    assert connection.cursor_obj.executed[1] == (
        "EXEC msdb.dbo.sp_start_job @job_name = %s",
        (AGENT_JOB_NAME,),
    )
    assert connection.committed == [True]


def test_ssrs_agent_job_client_raises_when_no_job_found():
    client, connection = _make_client([])

    with pytest.raises(RuntimeError, match="No SQL Server Agent job found"):
        client.start_subscription_job(subscription_description=SUBSCRIPTION_DESCRIPTION)
    assert len(connection.cursor_obj.executed) == 1
    assert connection.committed == []


def test_ssrs_agent_job_client_raises_on_ambiguous_description():
    client, connection = _make_client([("job-a",), ("job-b",)])

    with pytest.raises(RuntimeError, match="matches 2 SQL Server Agent jobs"):
        client.start_subscription_job(subscription_description=SUBSCRIPTION_DESCRIPTION)
    assert len(connection.cursor_obj.executed) == 1
    assert connection.committed == []


def test_ssrs_agent_job_client_requires_host_and_subscription_description():
    from dbt_dagsterizer.resources.ssrs_agent import SsrsAgentJobClient

    with pytest.raises(ValueError, match="SSRS_DB_HOST"):
        SsrsAgentJobClient(host="").start_subscription_job(subscription_description="x")
    with pytest.raises(ValueError, match="subscription_description"):
        SsrsAgentJobClient(host="sql.example.com").start_subscription_job(
            subscription_description="  "
        )
