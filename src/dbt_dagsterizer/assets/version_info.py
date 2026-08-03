from __future__ import annotations

import importlib.metadata
import os

import dagster as dg
from ruamel.yaml import YAML

from .. import __version__


def _dagster_version() -> str:
    try:
        return importlib.metadata.version("dagster")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _dagster_dbt_version() -> str:
    try:
        return importlib.metadata.version("dagster-dbt")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _resolve_dbt_project_name() -> str:
    """Read the dbt project name from *dbt_project.yml*."""
    project_dir = os.environ.get("DBT_PROJECT_DIR")
    if project_dir:
        yml_path = os.path.join(project_dir, "dbt_project.yml")
        try:
            with open(yml_path, encoding="utf-8") as f:
                data = YAML().load(f)
            name = data.get("name", "") if data else ""
            if name:
                return str(name)
        except Exception:
            pass
    return "unknown"


def build_version_info_asset():
    """Create a static asset that displays dbt_dagsterizer version information in the Dagster UI.

    * The asset key is ``versions/<dbt_dagsterizer_version>/<dbt_project_name>_version``
      so each code location gets a **unique** entry inside a version-specific
      subfolder of the *versions* folder.
    * Version numbers are returned as ``dg.Output`` metadata so they are
      recorded at materialization time and visible in the Dagster UI.
    """
    project_name = _resolve_dbt_project_name()
    asset_name = f"{project_name}_version"

    versions = {
        "dbt_dagsterizer": __version__,
        "dagster": _dagster_version(),
        "dagster_dbt": _dagster_dbt_version(),
    }

    @dg.asset(
        key_prefix=["versions", __version__],
        name=asset_name,
        group_name="versions",
        description=(
            f"dbt_dagsterizer={__version__}; dagster={_dagster_version()}; dagster_dbt={_dagster_dbt_version()}"
        ),
    )
    def version_info() -> dg.Output:
        return dg.Output(metadata=versions)

    return version_info
