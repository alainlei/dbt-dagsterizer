import os
from pathlib import Path

from dbt_dagsterizer.api import build_definitions
from dbt_dagsterizer.env_utils import dotenv_paths_for_dbt_project, parse_dotenv_file
from dbt_dagsterizer.otel import configure_otel


REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"

for dotenv_path in dotenv_paths_for_dbt_project(DBT_PROJECT_DIR):
    for key, value in parse_dotenv_file(dotenv_path).items():
        os.environ.setdefault(key, value)


os.environ.setdefault("LUBAN_REPO_ROOT", str(REPO_ROOT))
os.environ.setdefault("DBT_PROJECT_DIR", str(DBT_PROJECT_DIR))
os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR))

configure_otel()


defs = build_definitions(
    dbt_project_dir=DBT_PROJECT_DIR,
    dbt_profiles_dir=DBT_PROJECT_DIR,
    default_dbt_target="{{ cookiecutter.default_env }}",
)
