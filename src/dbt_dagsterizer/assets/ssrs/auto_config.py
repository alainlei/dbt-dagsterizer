from __future__ import annotations

from pathlib import Path

from ...assets.dbt.translator import relation_asset_key_path
from ...dbt.manifest import iter_models, load_manifest
from ...orchestration_config import (
    default_orchestration_path,
    resolve_orchestration_path,
)
from ...orchestration_config import (
    load_or_create as load_orch,
)
from ...resources.dbt import get_dbt_project_dir


def build_auto_ssrs_report_specs() -> list[dict]:
    """Build SSRS report specs from the ssrs_reports section of dagsterization.yml."""
    manifest = load_manifest()
    models = iter_models(manifest)
    existing_models = {m.name for m in models}
    model_relations = {
        m.name: relation_asset_key_path(database=m.database, schema=m.schema, identifier=m.identifier)
        for m in models
    }

    dbt_project_dir = get_dbt_project_dir()
    cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    cfg = load_orch(cfg_path)

    specs: list[dict] = []
    seen_names: set[str] = set()
    reports = cfg.get("ssrs_reports")
    if isinstance(reports, list):
        for report in reports:
            if not isinstance(report, dict):
                continue
            name = report.get("name")
            if not isinstance(name, str) or not name.strip():
                raise ValueError("ssrs_reports entry requires a non-empty 'name'")
            name = name.strip()
            if name in seen_names:
                raise ValueError(f"Duplicate ssrs_reports name '{name}'")
            seen_names.add(name)

            model = report.get("model")
            if not isinstance(model, str) or not model.strip():
                raise ValueError(f"ssrs_reports '{name}' requires a non-empty 'model'")
            model = model.strip()
            if model not in existing_models:
                raise ValueError(f"ssrs_reports '{name}' references missing dbt model '{model}'")

            subscription_description = report.get("subscription_description")
            if not isinstance(subscription_description, str) or not subscription_description.strip():
                raise ValueError(f"ssrs_reports '{name}' requires a non-empty 'subscription_description'")

            specs.append(
                {
                    "name": name,
                    "model": model,
                    "upstream_relation": model_relations[model],
                    "subscription_description": subscription_description.strip(),
                    "enabled": bool(report.get("enabled", True)),
                }
            )

    return sorted(specs, key=lambda s: str(s.get("name", "")))
