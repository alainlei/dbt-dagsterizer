from __future__ import annotations

from collections import defaultdict
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
from .detector.presets import daily_partition_change, monthly_partition_change
from .propagator.presets import partition_change_propagation


def _build_model_relation_index(manifest: dict) -> dict[str, list[str]]:
    """Map dbt model names to their relation-based AssetKey path components."""
    models = iter_models(manifest)
    return {m.name: relation_asset_key_path(database=m.database, schema=m.schema, identifier=m.identifier) for m in models}


def build_auto_partition_change_detection_specs() -> list[dict]:
    manifest = load_manifest()
    existing_models = {m.name for m in iter_models(manifest)}
    dbt_project_dir = get_dbt_project_dir()
    cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    cfg = load_orch(cfg_path)
    idx = index_orch(cfg)

    specs: list[dict] = []
    pc = cfg.get("partition_change")
    detectors = pc.get("detectors") if isinstance(pc, dict) else None
    if isinstance(detectors, list):
        for detector in detectors:
            if not isinstance(detector, dict):
                continue
            model = detector.get("model")
            if not isinstance(model, str) or not model.strip():
                continue
            model = model.strip()
            if model not in existing_models:
                raise ValueError(f"Partition-change detector references missing dbt model '{model}'")

            enabled = bool(detector.get("enabled", False))
            if not enabled:
                continue

            job_name = detector.get("job_name")
            if not isinstance(job_name, str) or not job_name.strip():
                derived = derive_job_name_for_model(idx, model=model)
                if not derived:
                    raise ValueError(
                        f"Partition-change detector on model '{model}' could not derive job_name; add model to asset_jobs or jobs"
                    )
                job_name = derived

            lookback_days = int(detector.get("lookback_days", 7))
            offset_days = int(detector.get("offset_days", 1))
            minimum_interval_seconds = int(detector.get("minimum_interval_seconds", 60))
            sensor_name = detector.get("name") or f"{model}_partition_change_sensor"

            detector_meta: dict = {
                "partition_date_expr": detector.get("partition_date_expr"),
                "updated_at_expr": detector.get("updated_at_expr"),
            }
            if detector.get("detect_relation"):
                detector_meta["detect_relation"] = detector.get("detect_relation")
            if detector.get("detect_source"):
                detector_meta["detect_source"] = detector.get("detect_source")
            if detector.get("impact"):
                detector_meta["impact"] = detector.get("impact")

            # Only models explicitly marked monthly get a month-granular detector; hourly and
            # unpartitioned models keep the existing daily-granular behaviour.
            if idx.partitions_by_model.get(model) == "monthly":
                lookback_months = int(detector.get("lookback_months", 3))
                offset_months = int(detector.get("offset_months", 0))
                specs.append(
                    monthly_partition_change(
                        name=str(sensor_name),
                        job_name=str(job_name),
                        detector_model=model,
                        lookback_months=lookback_months,
                        offset_months=offset_months,
                        enabled=True,
                        minimum_interval_seconds=minimum_interval_seconds,
                        meta=detector_meta,
                    )
                )
                continue

            specs.append(
                daily_partition_change(
                    name=str(sensor_name),
                    job_name=str(job_name),
                    detector_model=model,
                    lookback_days=lookback_days,
                    offset_days=offset_days,
                    enabled=True,
                    minimum_interval_seconds=minimum_interval_seconds,
                    meta=detector_meta,
                )
            )

    return sorted(specs, key=lambda s: str(s.get("name", "")))


def build_auto_partition_change_propagation_specs() -> list[dict]:
    manifest = load_manifest()
    existing_models = {m.name for m in iter_models(manifest)}
    model_relations = _build_model_relation_index(manifest)
    dbt_project_dir = get_dbt_project_dir()
    cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    cfg = load_orch(cfg_path)

    specs_by_name: dict[str, dict] = {}
    duplicates: dict[str, list[str]] = defaultdict(list)

    pc = cfg.get("partition_change")
    propagations = pc.get("propagators") if isinstance(pc, dict) else None
    if isinstance(propagations, list):
        for propagate in propagations:
            if not isinstance(propagate, dict):
                continue
            upstream_model = propagate.get("upstream_model")
            if not isinstance(upstream_model, str) or not upstream_model.strip():
                continue
            upstream_model = upstream_model.strip()
            if upstream_model not in existing_models:
                raise ValueError(f"Partition-change propagation references missing dbt model '{upstream_model}'")

            enabled = bool(propagate.get("enabled", False))
            targets = propagate.get("targets")
            if not isinstance(targets, list) or not targets:
                raise ValueError(f"Partition-change propagation on '{upstream_model}' requires non-empty targets")

            minimum_interval_seconds = int(propagate.get("minimum_interval_seconds", 30))
            name = propagate.get("name")

            for target in targets:
                if not isinstance(target, dict):
                    raise ValueError(f"Partition-change propagation target must be dict (model '{upstream_model}')")
                job_name = target.get("job_name")
                if not isinstance(job_name, str) or not job_name.strip():
                    raise ValueError(f"Partition-change propagation target missing job_name (model '{upstream_model}')")

                spec_name = name or f"{upstream_model}_partition_change_to_{job_name}"

                upstream_model_relation = model_relations.get(upstream_model)
                if upstream_model_relation is None:
                    raise ValueError(f"Partition-change propagation references missing dbt model '{upstream_model}'")

                spec = partition_change_propagation(
                    name=str(spec_name),
                    upstream_dbt_model=upstream_model,
                    upstream_model_relation=upstream_model_relation,
                    job_name=str(job_name),
                    enabled=enabled,
                    minimum_interval_seconds=minimum_interval_seconds,
                )

                if spec["name"] in specs_by_name:
                    duplicates[spec["name"]].append(upstream_model)
                specs_by_name[spec["name"]] = spec

    if duplicates:
        details = ", ".join([f"{name}: {sorted(models)}" for name, models in sorted(duplicates.items())])
        raise ValueError(f"Duplicate propagation spec names in dagsterization.yml partition_change.propagators: {details}")

    return [specs_by_name[name] for name in sorted(specs_by_name.keys())]
