from ..assets.dbt.translator import relation_asset_key_path
from ..dbt.manifest import iter_models, load_manifest


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


def _normalize_manual_propagation_specs(specs: list[dict]) -> list[dict]:
    model_relations = _build_model_relation_index()
    normalized_specs: list[dict] = []

    for spec in specs:
        upstream_model_relation = spec.get("upstream_model_relation")
        upstream_dbt_model = spec.get("upstream_dbt_model")
        if upstream_model_relation or not isinstance(upstream_dbt_model, str):
            normalized_specs.append(spec)
            continue

        relation = model_relations.get(upstream_dbt_model)
        if relation is None:
            normalized_specs.append(spec)
            continue

        spec_copy = dict(spec)
        spec_copy["upstream_model_relation"] = relation
        normalized_specs.append(spec_copy)

    return normalized_specs


def get_sensors():
    import os

    import dagster as dg

    from ..jobs.dbt.jobs import get_dbt_jobs_by_name
    from .dbt_config import PARTITION_CHANGE_DETECTION_SPECS, PARTITION_CHANGE_PROPAGATION_SPECS
    from .partition_change.auto_config import (
        build_auto_partition_change_detection_specs,
        build_auto_partition_change_propagation_specs,
    )
    from .partition_change.detector.factory import build_dbt_partition_change_sensors
    from .partition_change.propagator.factory import build_partition_propagation_sensors
    from .replication import get_replication_trigger_sensors

    automation_condition_sensor = dg.AutomationConditionSensorDefinition(
        "default_automation_condition_sensor",
        target=dg.AssetSelection.all(),
        default_status=dg.DefaultSensorStatus.RUNNING,
    )

    sensors = [automation_condition_sensor]

    sensors += build_dbt_partition_change_sensors(
        specs=build_auto_partition_change_detection_specs() + PARTITION_CHANGE_DETECTION_SPECS,
    )

    propagator_mode = os.getenv("LUBAN_PARTITION_CHANGE_PROPAGATOR_MODE", "sensor").strip().lower()
    if propagator_mode != "eager":
        manual_specs = _normalize_manual_propagation_specs(PARTITION_CHANGE_PROPAGATION_SPECS)
        sensors += build_partition_propagation_sensors(
            specs=build_auto_partition_change_propagation_specs() + manual_specs,
            jobs_by_name=get_dbt_jobs_by_name(),
        )

    # Replication trigger sensors: watch upstream dbt materializations and
    # trigger replication jobs per partition.
    sensors += get_replication_trigger_sensors()

    return sensors
