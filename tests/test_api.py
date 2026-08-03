from pathlib import Path

from dbt_dagsterizer.api import build_definitions


def test_build_definitions_skeleton(tmp_path: Path):
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
    defs = build_definitions(dbt_project_dir=dbt_project_dir)
    assert defs is not None
    assert len(defs.assets) == 2  # version_info + project_ready
