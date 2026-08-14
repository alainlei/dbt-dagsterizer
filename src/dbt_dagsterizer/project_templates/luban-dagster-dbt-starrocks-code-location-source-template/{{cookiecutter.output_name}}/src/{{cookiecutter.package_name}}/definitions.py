import os
from pathlib import Path

from dbt_dagsterizer.api import build_definitions
from dbt_dagsterizer.otel import configure_otel


REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"

def _load_dotenv_if_preset(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return  
    
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        os.environ.setdefault(key, value)

_load_dotenv_if_preset(REPO_ROOT / ".env")


os.environ.setdefault("LUBAN_REPO_ROOT", str(REPO_ROOT))
os.environ.setdefault("DBT_PROJECT_DIR", str(DBT_PROJECT_DIR))
os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR))

configure_otel()


defs = build_definitions(
    dbt_project_dir=DBT_PROJECT_DIR,
    dbt_profiles_dir=DBT_PROJECT_DIR,
    default_dbt_target="{{ cookiecutter.default_env }}",
)
