from pathlib import Path

from ...assets.dbt.translator import relation_asset_key_path
from ...dbt.manifest import iter_models, load_manifest
from ...orchestration_config import (
    default_orchestration_path,
    resolve_orchestration_path,
)
from ...orchestration_config import (
    index as index_orch,
)
from ...orchestration_config import (
    load_or_create as load_orch,
)
from ...resources.dbt import get_dbt_project_dir
from ..dbt_config import DBT_JOB_SPECS
from .auto_config import build_auto_dbt_job_specs
from .factory import build_dbt_asset_jobs

_dbt_jobs_by_name = None


def _build_model_relation_index() -> dict[str, list[str]]:
    manifest = load_manifest()
    return {
        m.name: relation_asset_key_path(
            database=m.database,
            schema=m.schema,
            identifier=m.identifier,
        )
        for m in iter_models(manifest)
    }


def _normalize_manual_job_specs(job_specs: list[dict]) -> list[dict]:
    model_relations = _build_model_relation_index()
    normalized_specs: list[dict] = []

    for spec in job_specs:
        job_type = spec.get("type", "asset")
        selection = spec.get("selection")
        if job_type != "asset" or not isinstance(selection, dict) or selection.get("type") != "asset_keys":
            normalized_specs.append(spec)
            continue

        keys = selection.get("keys")
        if not isinstance(keys, list):
            normalized_specs.append(spec)
            continue

        normalized_keys: list[list[str]] = []
        changed = False
        for key in keys:
            if (
                isinstance(key, list)
                and len(key) == 2
                and key[0] == "dbt"
                and isinstance(key[1], str)
                and key[1] in model_relations
            ):
                normalized_keys.append(model_relations[key[1]])
                changed = True
            else:
                normalized_keys.append(key)

        if not changed:
            normalized_specs.append(spec)
            continue

        spec_copy = dict(spec)
        selection_copy = dict(selection)
        selection_copy["keys"] = normalized_keys
        spec_copy["selection"] = selection_copy
        normalized_specs.append(spec_copy)

    return normalized_specs


def get_dbt_jobs_by_name():
    global _dbt_jobs_by_name
    if _dbt_jobs_by_name is None:
        dbt_project_dir = get_dbt_project_dir()

        # Read include_current_day_partition from orchestration config
        orch_cfg_path = resolve_orchestration_path(
            dbt_project_dir=dbt_project_dir,
            path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
        )
        orch_cfg = load_orch(orch_cfg_path)
        orch_index = index_orch(orch_cfg)

        manual_specs = _normalize_manual_job_specs(DBT_JOB_SPECS)
        _dbt_jobs_by_name = build_dbt_asset_jobs(
            build_auto_dbt_job_specs() + manual_specs,
            include_current_day_partition=orch_index.daily_include_current_day_partition,
        )
    return _dbt_jobs_by_name


def get_dbt_jobs():
    return list(get_dbt_jobs_by_name().values())
