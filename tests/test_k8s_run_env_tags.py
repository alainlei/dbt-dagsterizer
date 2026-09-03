from __future__ import annotations

import json

import dagster as dg
import pytest

from dbt_dagsterizer.jobs.dbt.factory import build_dbt_asset_jobs
from dbt_dagsterizer.jobs.sources.jobs import get_observe_sources_job


def _get_dbt_asset_job_tags(*, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    jobs = build_dbt_asset_jobs(
        [
            {
                "name": "my_job",
                "type": "asset",
                "selection": {"type": "asset_keys", "keys": [["a"]]},
            }
        ]
    )
    return jobs["my_job"].tags


def test_dbt_asset_job_injects_configmap_env_from(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LUBAN_RUN_ENV_CONFIGMAP", "my-config")
    monkeypatch.delenv("LUBAN_RUN_ENV_SECRET", raising=False)

    tags = _get_dbt_asset_job_tags(monkeypatch=monkeypatch)
    raw = tags["dagster-k8s/config"]
    payload = json.loads(raw)
    assert payload["container_config"]["envFrom"] == [{"configMapRef": {"name": "my-config"}}]


def test_dbt_asset_job_injects_secret_env_from(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("LUBAN_RUN_ENV_CONFIGMAP", raising=False)
    monkeypatch.setenv("LUBAN_RUN_ENV_SECRET", "my-secret")

    tags = _get_dbt_asset_job_tags(monkeypatch=monkeypatch)
    raw = tags["dagster-k8s/config"]
    payload = json.loads(raw)
    assert payload["container_config"]["envFrom"] == [{"secretRef": {"name": "my-secret"}}]


def test_dbt_asset_job_no_injection_when_unset(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("LUBAN_RUN_ENV_CONFIGMAP", raising=False)
    monkeypatch.delenv("LUBAN_RUN_ENV_SECRET", raising=False)

    tags = _get_dbt_asset_job_tags(monkeypatch=monkeypatch)
    assert "dagster-k8s/config" not in tags


def test_dbt_asset_job_does_not_override_existing_dagster_k8s_config(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LUBAN_RUN_ENV_CONFIGMAP", "my-config")
    monkeypatch.delenv("LUBAN_RUN_ENV_SECRET", raising=False)

    jobs = build_dbt_asset_jobs(
        [
            {
                "name": "my_job",
                "type": "asset",
                "selection": {"type": "asset_keys", "keys": [["a"]]},
                "tags": {"dagster-k8s/config": '{"container_config": {"envFrom": []}}'},
            }
        ]
    )
    assert jobs["my_job"].tags["dagster-k8s/config"] == '{"container_config": {"envFrom": []}}'


def test_observe_sources_job_injects_tags(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LUBAN_RUN_ENV_CONFIGMAP", "my-config")
    monkeypatch.delenv("LUBAN_RUN_ENV_SECRET", raising=False)

    monkeypatch.setattr("dbt_dagsterizer.assets.dbt.assets.get_dbt_assets", lambda: None)
    monkeypatch.setattr(
        "dbt_dagsterizer.assets.sources.automation.load_filtered_observable_sources",
        lambda: [{"source": "ods", "table": "orders", "watermark_column": "updated_at"}],
    )
    monkeypatch.setattr(
        "dbt_dagsterizer.assets.sources.factory.build_observable_source_assets",
        lambda **_: [dg.AssetKey("obs")],
    )

    job = get_observe_sources_job()
    assert job is not None
    payload = json.loads(job.tags["dagster-k8s/config"])
    assert payload["container_config"]["envFrom"] == [{"configMapRef": {"name": "my-config"}}]
