from __future__ import annotations

from pathlib import Path

from dagster import Definitions, asset

from .env_utils import temporary_env


def _has_any_dbt_models(dbt_project_dir: Path) -> bool:
    models_dir = dbt_project_dir / "models"
    return models_dir.exists() and any(models_dir.rglob("*.sql"))


def build_definitions(
    *,
    dbt_project_dir: str | Path | None = None,
    dbt_profiles_dir: str | Path | None = None,
    default_dbt_target: str | None = None,
) -> Definitions:
    from .resources.dbt import get_dbt_project_dir

    if dbt_project_dir is None:
        resolved_dbt_project_dir = get_dbt_project_dir()
    else:
        candidate = Path(dbt_project_dir).expanduser()
        resolved_dbt_project_dir = (
            candidate.resolve() if candidate.is_absolute() else (Path.cwd() / candidate).resolve()
        )

    resolved_dbt_profiles_dir: Path | None
    if dbt_profiles_dir is None:
        resolved_dbt_profiles_dir = None
    else:
        candidate = Path(dbt_profiles_dir).expanduser()
        resolved_dbt_profiles_dir = (
            candidate.resolve() if candidate.is_absolute() else (Path.cwd() / candidate).resolve()
        )
    env: dict[str, str] = {}
    env["DBT_PROJECT_DIR"] = str(resolved_dbt_project_dir)
    if resolved_dbt_profiles_dir is not None:
        env["DBT_PROFILES_DIR"] = str(resolved_dbt_profiles_dir)
    if default_dbt_target is not None:
        env["LUBAN_DEFAULT_DBT_TARGET"] = default_dbt_target

    with temporary_env(env):
        if not _has_any_dbt_models(resolved_dbt_project_dir):
            @asset(name="project_ready")
            def project_ready(_context) -> None:
                return None

            from .assets.version_info import build_version_info_asset
            from .resources import get_resources

            return Definitions(
                assets=[build_version_info_asset(), project_ready],
                jobs=[],
                schedules=[],
                sensors=[],
                resources=get_resources(),
            )

        from .assets import get_assets
        from .jobs import get_jobs
        from .resources import get_resources
        from .schedules import get_schedules
        from .sensors import get_sensors

        # Build jobs first to ensure they're cached before schedules reference them
        jobs = get_jobs()

        return Definitions(
            assets=get_assets(),
            jobs=jobs,
            schedules=get_schedules(),
            sensors=get_sensors(),
            resources=get_resources(),
        )
