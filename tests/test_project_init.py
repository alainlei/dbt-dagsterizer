from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from dbt_dagsterizer.cli import cli


def _make_local_dbt_dagsterizer_checkout(path: Path, *, project_name: str = "dbt-dagsterizer") -> Path:
    path.mkdir(parents=True, exist_ok=True)
    (path / "pyproject.toml").write_text(
        f'[project]\nname = "{project_name}"\nversion = "0.0.0"\n',
        encoding="utf-8",
    )
    return path


def test_project_init_renders_into_output_dir(tmp_path: Path):
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
    assert project_root.exists()
    assert (project_root / "pyproject.toml").exists()
    assert (project_root / "dbt_project" / "dbt_project.yml").exists()
    assert list((project_root / "dbt_project" / "models").rglob("*.sql")) == []
    assert (project_root / "dbt_project" / ".dbt_dagsterizer_template").exists()
    assert (project_root / "dbt_project" / "macros" / "dbt_dagsterizer").exists()
    assert any((project_root / "dbt_project" / "macros" / "dbt_dagsterizer").rglob("*.sql"))

    env_example = (project_root / ".env.example").read_text(encoding="utf-8")
    assert "DAGSTER_MAX_CONCURRENT_RUNS=2" in env_example
    assert f"DAGSTER_HOME={project_root / 'dagster_home'}" in env_example
    assert f"LUBAN_REPO_ROOT={project_root}" in env_example
    assert (project_root / "dagster_home" / "dagster.yaml").exists()
    assert not (project_root / "docker").exists()

    definitions_py = project_root / "src" / "demo_app" / "definitions.py"
    assert definitions_py.exists()
    content = definitions_py.read_text(encoding="utf-8")
    assert "REPO_ROOT = Path(__file__).resolve().parents[2]" in content
    assert 'os.environ.setdefault("LUBAN_REPO_ROOT", str(REPO_ROOT))' in content
    assert 'os.environ.setdefault("DBT_PROJECT_DIR", str(DBT_PROJECT_DIR))' in content
    assert 'os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR))' in content
    assert "dbt_project_dir=DBT_PROJECT_DIR" in content


def test_project_init_namespace_affects_env_defaults(tmp_path: Path):
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
            "--namespace",
            "Demo Project",
        ],
    )
    assert result.exit_code == 0, result.output

    project_root = out_dir / "demo-app"
    env_example = (project_root / ".env.example").read_text(encoding="utf-8")
    assert "OTEL_SERVICE_NAME=demo_project/demo_app" in env_example
    assert "service.namespace=demo_project" in env_example
    assert "STARROCKS_DWS_DB=demo_project_demo_app_dws_" in env_example


def test_project_init_output_name_overrides_folder_name(tmp_path: Path):
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
            "--output-name",
            "custom-dir",
        ],
    )
    assert result.exit_code == 0, result.output
    project_root = out_dir / "custom-dir"
    assert project_root.exists()
    assert (project_root / "pyproject.toml").exists()
    assert (project_root / "src" / "demo_app").exists()


def test_project_init_errors_when_output_dir_exists_without_force(tmp_path: Path):
    runner = CliRunner()
    out_dir = tmp_path / "out"
    (out_dir / "demo-app").mkdir(parents=True, exist_ok=True)

    result = runner.invoke(
        cli,
        [
            "project",
            "init",
            "--output-dir",
            str(out_dir),
            "--project-name",
            "Demo App",
        ],
    )
    assert result.exit_code != 0
    assert "Output directory already exists" in result.output


def test_project_init_local_path_renders_file_url_dependency(tmp_path: Path):
    runner = CliRunner()
    out_dir = tmp_path / "out"
    checkout = _make_local_dbt_dagsterizer_checkout(tmp_path / "dbt-dagsterizer")

    result = runner.invoke(
        cli,
        [
            "project",
            "init",
            "--output-dir",
            str(out_dir),
            "--project-name",
            "Demo App",
            "--local-dbt-dagsterizer-path",
            str(checkout),
        ],
    )

    assert result.exit_code == 0, result.output
    pyproject = (out_dir / "demo-app" / "pyproject.toml").read_text(encoding="utf-8")
    assert f'"dbt-dagsterizer @ {checkout.resolve().as_uri()}"' in pyproject


def test_project_init_local_path_requires_dbt_dagsterizer_checkout(tmp_path: Path):
    runner = CliRunner()
    out_dir = tmp_path / "out"
    checkout = _make_local_dbt_dagsterizer_checkout(tmp_path / "not-dagsterizer", project_name="other")

    result = runner.invoke(
        cli,
        [
            "project",
            "init",
            "--output-dir",
            str(out_dir),
            "--project-name",
            "Demo App",
            "--local-dbt-dagsterizer-path",
            str(checkout),
        ],
    )

    assert result.exit_code != 0
    assert "must point to a dbt-dagsterizer checkout" in result.output


def test_project_init_local_path_is_mutually_exclusive_with_version_flags(tmp_path: Path):
    runner = CliRunner()
    checkout = _make_local_dbt_dagsterizer_checkout(tmp_path / "dbt-dagsterizer")

    result_with_version = runner.invoke(
        cli,
        [
            "project",
            "init",
            "--output-dir",
            str(tmp_path / "out-version"),
            "--project-name",
            "Demo App",
            "--local-dbt-dagsterizer-path",
            str(checkout),
            "--dbt-dagsterizer-version",
            "0.3.1",
        ],
    )
    assert result_with_version.exit_code != 0
    assert "mutually exclusive" in result_with_version.output

    result_with_no_pin = runner.invoke(
        cli,
        [
            "project",
            "init",
            "--output-dir",
            str(tmp_path / "out-no-pin"),
            "--project-name",
            "Demo App",
            "--local-dbt-dagsterizer-path",
            str(checkout),
            "--no-pin-dbt-dagsterizer",
        ],
    )
    assert result_with_no_pin.exit_code != 0
    assert "mutually exclusive" in result_with_no_pin.output
