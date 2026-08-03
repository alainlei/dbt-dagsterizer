import os
from pathlib import Path

from dbt_dagsterizer.api import build_definitions


def test_build_definitions_minimal_dbt_project(tmp_path: Path):
    dbt_project_dir = tmp_path / "dbt_project"
    (dbt_project_dir / "models").mkdir(parents=True)

    (dbt_project_dir / "dbt_project.yml").write_text(
        "name: demo\nprofile: demo\nconfig-version: 2\nversion: '1.0.0'\nmodel-paths: ['models']\n",
        encoding="utf-8",
    )
    (dbt_project_dir / "profiles.yml").write_text(
        "demo:\n  target: dev\n  outputs:\n    dev:\n      type: starrocks\n      host: localhost\n      port: 9030\n      user: root\n      password: ''\n      schema: demo\n",
        encoding="utf-8",
    )
    (dbt_project_dir / "packages.yml").write_text("packages: []\n", encoding="utf-8")
    (dbt_project_dir / "models" / "one.sql").write_text("select 1 as one\n", encoding="utf-8")

    os.environ["LUBAN_DBT_PREPARE_ON_LOAD"] = "1"
    os.environ["STARROCKS_HOST"] = "mock_host"
    os.environ["STARROCKS_PORT"] = "9030"
    os.environ["STARROCKS_USER"] = "mock_user"
    os.environ["STARROCKS_PASSWORD"] = "mock_pass"

    defs = build_definitions(dbt_project_dir=dbt_project_dir, default_dbt_target="dev")
    assert defs is not None
    assert "dbt" in defs.resources
    assert "starrocks" in defs.resources
    assert "mssql" in defs.resources

    sensor_names = {s.name for s in defs.sensors}
    assert "default_automation_condition_sensor" in sensor_names

