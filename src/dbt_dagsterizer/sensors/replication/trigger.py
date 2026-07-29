"""Build replication trigger sensors.

Each sensor watches materialization events of an upstream dbt model asset and
triggers the replication job:

- Partitioned entries: emits ``RunRequest(partition_key=...)`` for every newly
  materialized partition.  This ensures that the replication job runs for
  **each** partition independently, solving the problem where ``deps``-based
  auto-materialization only triggers one partition when multiple partitions
  are materialized simultaneously.
- Unpartitioned entries: emits a single ``RunRequest`` (no partition key) so
  the replication executes for the entire table whenever the upstream model
  materializes.

The pattern mirrors the partition-change propagator sensor but targets the
replication job instead of a dbt job.
"""
from __future__ import annotations

import dagster as dg


def build_replication_trigger_sensors(
    *, specs: list[dict], jobs_by_name: dict
) -> list[dg.SensorDefinition]:
    """Build one sensor per enabled replication entry.

    Args:
        specs: List of sensor spec dicts from ``auto_config``.
        jobs_by_name: Map of job name → job definition (from
            ``get_replication_jobs_by_name``).
    """
    _validate_unique_names(specs)

    sensors: list[dg.SensorDefinition] = []
    for spec in specs:
        sensors.append(_build_sensor(spec, jobs_by_name))
    return sensors


def _validate_unique_names(specs: list[dict]) -> None:
    seen: set[str] = set()
    duplicated: set[str] = set()
    for spec in specs:
        name = spec.get("name")
        if not name:
            continue
        if name in seen:
            duplicated.add(name)
        else:
            seen.add(name)
    if duplicated:
        raise ValueError(f"Duplicate replication trigger sensor names: {sorted(duplicated)}")


def _build_sensor(spec: dict, jobs_by_name: dict) -> dg.SensorDefinition:
    name = spec["name"]
    job_name = spec["job_name"]
    upstream_model_relation = spec["upstream_model_relation"]
    upstream_model_name = spec["upstream_model_name"]
    partition_type = spec.get("partition_type", "unpartitioned")
    unpartitioned = partition_type in ("unpartitioned", None, "")
    enabled = bool(spec.get("enabled", True))
    minimum_interval_seconds = int(spec.get("minimum_interval_seconds", 30))

    default_status = dg.DefaultSensorStatus.RUNNING if enabled else dg.DefaultSensorStatus.STOPPED

    job = jobs_by_name[job_name]
    upstream_asset_key = dg.AssetKey(upstream_model_relation)

    @dg.sensor(
        name=name,
        job=job,
        default_status=default_status,
        minimum_interval_seconds=minimum_interval_seconds,
    )
    def _sensor(context: dg.SensorEvaluationContext):
        """Watch upstream dbt model materializations → trigger replication per partition."""
        cursor = (context.cursor or "").strip()

        # --- cursor bootstrap (same logic as propagator) ---
        after_cursor: int | None = None

        if cursor:
            try:
                after_cursor = int(cursor)
            except ValueError:
                latest = context.instance.fetch_materializations(
                    upstream_asset_key,
                    limit=1,
                    ascending=False,
                ).records
                # Seed to "0" when no history exists so the first-ever
                # materialization is processed instead of re-entering the
                # bootstrap and being swallowed as the seed.
                context.update_cursor(str(latest[0].storage_id) if latest else "0")
                yield dg.SkipReason("Reset invalid replication trigger cursor")
                return
        else:
            # First evaluation: seed cursor to latest event so we don't
            # re-trigger historical materializations.  When the upstream
            # asset has never materialized, seed to "0" so the very first
            # materialization (e.g. a partition first materialized via
            # observable-source detection) triggers replication on the next
            # tick instead of being consumed as the bootstrap seed.
            latest = context.instance.fetch_materializations(
                upstream_asset_key,
                limit=1,
                ascending=False,
            ).records
            context.update_cursor(str(latest[0].storage_id) if latest else "0")
            yield dg.SkipReason(
                f"Initialized replication trigger cursor for '{name}'"
            )
            return

        # --- fetch new materialization events since cursor ---
        records = context.instance.fetch_materializations(
            dg.AssetRecordsFilter(
                asset_key=upstream_asset_key,
                after_storage_id=after_cursor,
            ),
            limit=1000,
            ascending=True,
        ).records

        if unpartitioned:
            # Unpartitioned replication: any new materialization of the
            # upstream model triggers one full-table replication run.
            if not records:
                yield dg.SkipReason(
                    f"No new materialization events for '{upstream_model_name}'"
                )
                return

            latest_record = max(records, key=lambda r: r.storage_id)
            run_key = f"{name}:{latest_record.run_id}:{latest_record.storage_id}"
            yield dg.RunRequest(
                run_key=run_key,
                tags={
                    "luban/replication_trigger": name,
                    "luban/upstream_dbt_model": upstream_model_name,
                    "luban/upstream_asset_key": upstream_asset_key.to_user_string(),
                },
            )
            context.update_cursor(str(latest_record.storage_id))
            return

        # Deduplicate: keep only the latest event per partition key.
        latest_by_partition: dict[str, object] = {}
        for r in records:
            entry = r.event_log_entry
            mat = getattr(entry, "asset_materialization", None)
            partition_key = getattr(mat, "partition", None)
            if not partition_key:
                continue
            latest_by_partition[str(partition_key)] = r

        if not latest_by_partition:
            if records:
                context.update_cursor(str(max(r.storage_id for r in records)))
                yield dg.SkipReason(
                    f"No partitioned materializations for '{upstream_model_name}'"
                )
                return
            yield dg.SkipReason(
                f"No new materialization events for '{upstream_model_name}'"
            )
            return

        max_storage_id = max(r.storage_id for r in latest_by_partition.values())
        for partition_key, r in sorted(latest_by_partition.items()):
            run_key = f"{name}:{partition_key}:{r.run_id}:{r.storage_id}"
            yield dg.RunRequest(
                partition_key=partition_key,
                run_key=run_key,
                tags={
                    "luban/replication_trigger": name,
                    "luban/upstream_dbt_model": upstream_model_name,
                    "luban/upstream_asset_key": upstream_asset_key.to_user_string(),
                },
            )

        context.update_cursor(str(max_storage_id))

    return _sensor
