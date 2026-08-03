from __future__ import annotations

from pathlib import Path

from ...assets.dbt.translator import relation_asset_key_path
from ...dbt.manifest import iter_models, load_manifest
from ...orchestration_config import (
    default_orchestration_path,
    derive_job_name_for_model,
    resolve_orchestration_path,
)
from ...orchestration_config import (
    index as index_orch,
)
from ...orchestration_config import (
    load_or_create as load_orch,
)
from ...resources.dbt import get_dbt_project_dir
from .presets import models_job


def build_auto_dbt_job_specs() -> list[dict]:
    manifest = load_manifest()
    models = iter_models(manifest)
    existing_models = {m.name for m in models}
    model_relations = {m.name: relation_asset_key_path(database=m.database, schema=m.schema, identifier=m.identifier) for m in models}

    dbt_project_dir = get_dbt_project_dir()
    cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    cfg = load_orch(cfg_path)
    idx = index_orch(cfg)

    specs: list[dict] = []

    jobs = cfg.get("jobs")
    if isinstance(jobs, dict):
        for job_name in sorted(jobs.keys()):
            job_cfg = jobs.get(job_name)
            if not isinstance(job_name, str) or not job_name.strip() or not isinstance(job_cfg, dict):
                continue
            models_list = [m for m in job_cfg.get("models", []) if isinstance(m, str) and m.strip()]
            models_list = sorted(set([m.strip() for m in models_list]))
            for m in models_list:
                if m not in existing_models:
                    raise ValueError(f"Orchestration job '{job_name}' references missing dbt model '{m}'")

            include_upstream = bool(job_cfg.get("include_upstream", False))
            partitions = job_cfg.get("partitions")
            partitions_value = None
            if partitions is not None:
                # Validate partition spec: daily or unpartitioned
                if partitions not in {"daily", "unpartitioned"}:
                    raise ValueError(
                        f"Orchestration job '{job_name}' partitions must be daily|unpartitioned"
                    )
                partitions_value = partitions
            else:
                inferred = {idx.partitions_by_model.get(m) for m in models_list}
                inferred.discard(None)
                if len(inferred) == 1:
                    partitions_value = inferred.pop()
                elif len(inferred) > 1:
                    raise ValueError(
                        f"Orchestration job '{job_name}' cannot mix different partition types. "
                        f"Models {models_list} have conflicting partitions: {sorted(inferred)}. "
                        f"Either: (1) Set explicit 'partitions' in the job config, or "
                        f"(2) Split into separate jobs for each partition type"
                    )

            specs.append(
                models_job(
                    name=str(job_name),
                    models=models_list,
                    model_relations=model_relations,
                    include_upstream=include_upstream,
                    partitions=partitions_value,
                )
            )

    for model in sorted(idx.asset_job_models):
        if model not in existing_models:
            raise ValueError(f"Orchestration asset_jobs references missing dbt model '{model}'")
        partitions_value = idx.partitions_by_model.get(model)
        specs.append(
            models_job(
                name=str(derive_job_name_for_model(idx, model=model)),
                models=[model],
                model_relations=model_relations,
                include_upstream=False,
                partitions=partitions_value,
            )
        )

    return specs
