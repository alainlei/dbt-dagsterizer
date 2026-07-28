from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from dbt_dagsterizer.cli import cli


def test_project_gen_gitops_env_generates_kustomize_tree(tmp_path: Path):
    runner = CliRunner()
    out_dir = tmp_path / "out"

    result = runner.invoke(
        cli,
        [
            "project",
            "init",
            "--output-dir",
            str(out_dir),
            "--project-name",
            "Demo App",
            "--author-name",
            "Demo",
            "--author-email",
            "demo@example.com",
        ],
    )
    assert result.exit_code == 0, result.output

    project_root = out_dir / "demo-app"
    env_example = (project_root / ".env.example").read_text(encoding="utf-8")
    env_text = env_example.replace("STARROCKS_PASSWORD=\n", "STARROCKS_PASSWORD=supersecret\n")
    (project_root / ".env").write_text(env_text, encoding="utf-8")

    result = runner.invoke(
        cli,
        [
            "project",
            "gen-gitops-env",
            "--project-dir",
            str(project_root),
        ],
    )
    assert result.exit_code == 0, result.output
    out_path = Path(result.output.strip())

    base_configmap = out_path / "app" / "base" / "configmap.yaml"
    base_secret = out_path / "app" / "base" / "secret.yaml"
    prd_configmap = out_path / "app" / "overlays" / "prd" / "configmap.yaml"
    snd_configmap = out_path / "app" / "overlays" / "snd" / "configmap.yaml"
    assert base_configmap.exists()
    assert base_secret.exists()
    assert prd_configmap.exists()
    assert snd_configmap.exists()

    base_text = base_configmap.read_text(encoding="utf-8")
    assert "name: demo-app-config" in base_text
    assert 'DAGSTER_HOME: "/tmp/dagster_home"' in base_text
    assert "LUBAN_PARTITION_CHANGE_PROPAGATOR_MODE" in base_text
    assert "LUBAN_OTEL_DBT_RUN_RESULTS_MODE" in base_text
    assert "OTEL_TRACES_EXPORTER" not in base_text
    assert "LUBAN_REPO_ROOT" not in base_text

    secret_text = base_secret.read_text(encoding="utf-8")
    assert "name: demo-app-secret" in secret_text
    assert 'STARROCKS_PASSWORD: ""' in secret_text
    assert 'SSRS_DB_PASSWORD: ""' in secret_text
    assert "supersecret" not in secret_text

    prd_text = prd_configmap.read_text(encoding="utf-8")
    assert 'APP_ENV: "prd"' in prd_text
    assert 'DBT_TARGET: "production"' in prd_text
    assert "_prd" in prd_text

    snd_text = snd_configmap.read_text(encoding="utf-8")
    assert 'APP_ENV: "snd"' in snd_text
    assert 'DBT_TARGET: "sandbox"' in snd_text
    assert "_snd" in snd_text

    gitignore_text = (project_root / ".gitignore").read_text(encoding="utf-8")
    assert ".gitops-env/" in gitignore_text
