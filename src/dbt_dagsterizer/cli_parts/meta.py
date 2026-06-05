from __future__ import annotations

import os
from pathlib import Path

import click

from ..dbt.manifest_prepare import (
    load_manifest as load_dbt_manifest,
)
from ..dbt.manifest_prepare import (
    manifest_path as dbt_manifest_path,
)
from ..dbt.manifest_prepare import (
    run_dbt_parse,
)
from ..orchestration_config import (
    delete_group_job,
    derive_job_name_for_model,
    resolve_orchestration_path,
    set_group_job,
)
from ..orchestration_config import (
    index as index_orch,
)
from ..orchestration_config import (
    load_or_create as load_orch,
)
from ..orchestration_config import (
    set_asset_job as orch_set_asset_job,
)
from ..orchestration_config import (
    set_partition as orch_set_partition,
)
from ..orchestration_config import (
    set_partition_change_detector as orch_set_detector,
)
from ..orchestration_config import (
    set_partition_change_propagation as orch_set_propagation,
)
from ..orchestration_config import (
    set_schedule as orch_set_schedule,
)
from .common import orchestration_path, resolve_dir_arg, select_models, split_csv
from .validation import (
    save_orchestration_with_validation,
    validate_orchestration,
    validate_orchestration_structure,
)


def _default_dbt_target() -> str:
    return os.getenv("DBT_TARGET") or os.getenv("LUBAN_DEFAULT_DBT_TARGET") or "development"


def build_meta_group() -> click.Group:
    @click.group()
    def meta() -> None:
        pass

    @meta.command("init")
    @click.option("--dbt-project-dir", default="./dbt_project", show_default=True)
    @click.option("--path", "path_", default="dagsterization.yml", show_default=True)
    @click.option("--force/--no-force", default=False, show_default=True)
    @click.option("--parse/--no-parse", default=False, show_default=True)
    def meta_init(dbt_project_dir: str, path_: str, force: bool, parse: bool) -> None:
        dbt_project_path = resolve_dir_arg(dbt_project_dir)
        if not dbt_project_path.exists():
            raise click.ClickException(f"dbt project dir does not exist: {dbt_project_path}")

        target = orchestration_path(dbt_project_dir=dbt_project_path, path_=path_)
        if target.exists() and not force:
            raise click.ClickException(f"File already exists: {target} (use --force to overwrite)")

        data = load_orch(target)
        save_orchestration_with_validation(target=target, data=data, dbt_project_dir=dbt_project_path, prepare=False)
        if parse:
            run_dbt_parse(dbt_project_dir=dbt_project_path, dbt_profiles_dir=dbt_project_path, dbt_target=_default_dbt_target())
        click.echo(str(target))

    @meta.command("job")
    @click.option("--dbt-project-dir", default="./dbt_project", show_default=True)
    @click.option("--path", "path_", default="dagsterization.yml", show_default=True)
    @click.option("--models", default="", help="Comma-separated model names")
    @click.option("--tag", "tag_", default="", help="Select models by existing dbt tag")
    @click.option("--name", "job_name", required=True)
    @click.option("--include-upstream/--no-include-upstream", default=False, show_default=True)
    @click.option("--partitions", default="", help="daily|unpartitioned|none")
    @click.option("--prepare/--no-prepare", default=True, show_default=True)
    @click.option("--parse/--no-parse", default=False, show_default=True)
    def meta_job(
        dbt_project_dir: str,
        path_: str,
        models: str,
        tag_: str,
        job_name: str,
        include_upstream: bool,
        partitions: str,
        prepare: bool,
        parse: bool,
    ) -> None:
        dbt_project_path = resolve_dir_arg(dbt_project_dir)
        if not dbt_project_path.exists():
            raise click.ClickException(f"dbt project dir does not exist: {dbt_project_path}")

        manifest = load_dbt_manifest(
            dbt_project_dir=dbt_project_path,
            dbt_profiles_dir=dbt_project_path,
            dbt_target=_default_dbt_target(),
            prepare=prepare,
        ) if (tag_ or prepare) else None
        selected = select_models(dbt_project_dir=dbt_project_path, manifest=manifest, models_csv=models, tag_=tag_)
        if not selected:
            raise click.ClickException("No models selected (use --models or --tag)")

        partitions_value = partitions.strip().lower() or None
        if partitions_value is not None and partitions_value not in {"daily", "unpartitioned", "none"}:
            raise click.ClickException("--partitions must be one of daily|unpartitioned|none")
        job_partitions = None if partitions_value in {None, "none"} else partitions_value

        target = orchestration_path(dbt_project_dir=dbt_project_path, path_=path_)
        data = load_orch(target)
        set_group_job(
            data=data,
            job_name=job_name,
            models=selected,
            include_upstream=include_upstream,
            partitions=job_partitions,
        )
        if job_partitions is not None:
            for m in selected:
                orch_set_partition(data=data, model=m, partition=job_partitions)
        save_orchestration_with_validation(target=target, data=data, dbt_project_dir=dbt_project_path, prepare=prepare)
        if parse:
            run_dbt_parse(dbt_project_dir=dbt_project_path, dbt_profiles_dir=dbt_project_path, dbt_target=_default_dbt_target())
        click.echo(str(target))

    @meta.command("job-delete")
    @click.option("--dbt-project-dir", default="./dbt_project", show_default=True)
    @click.option("--path", "path_", default="dagsterization.yml", show_default=True)
    @click.option("--name", "job_name", required=True)
    @click.option("--force/--no-force", default=False, show_default=True)
    @click.option("--prepare/--no-prepare", default=True, show_default=True)
    @click.option("--parse/--no-parse", default=False, show_default=True)
    def meta_job_delete(
        dbt_project_dir: str,
        path_: str,
        job_name: str,
        force: bool,
        prepare: bool,
        parse: bool,
    ) -> None:
        dbt_project_path = resolve_dir_arg(dbt_project_dir)
        if not dbt_project_path.exists():
            raise click.ClickException(f"dbt project dir does not exist: {dbt_project_path}")

        target = orchestration_path(dbt_project_dir=dbt_project_path, path_=path_)
        data = load_orch(target)

        schedules = data.get("schedules")
        schedule_refs: list[str] = []
        if isinstance(schedules, dict):
            for name, cfg in schedules.items():
                if isinstance(cfg, dict) and cfg.get("job_name") == job_name:
                    schedule_refs.append(str(name))

        pc = data.get("partition_change")
        propagators = pc.get("propagators") if isinstance(pc, dict) else None
        propagator_refs: list[str] = []
        if isinstance(propagators, list):
            for p in propagators:
                if not isinstance(p, dict):
                    continue
                upstream_model = p.get("upstream_model")
                targets = p.get("targets")
                if not isinstance(targets, list):
                    continue
                for t in targets:
                    if isinstance(t, dict) and t.get("job_name") == job_name:
                        propagator_refs.append(str(upstream_model))
                        break

        if (schedule_refs or propagator_refs) and not force:
            details = []
            if schedule_refs:
                details.append(f"schedules: {sorted(set(schedule_refs))}")
            if propagator_refs:
                details.append(f"propagators targeting job: {sorted(set(propagator_refs))}")
            raise click.ClickException(
                f"Job '{job_name}' is still referenced ({'; '.join(details)}). Re-run with --force to remove references."
            )

        if isinstance(schedules, dict) and schedule_refs:
            for name in schedule_refs:
                schedules.pop(name, None)

        if isinstance(propagators, list) and propagator_refs:
            new_props: list[dict] = []
            for p in propagators:
                if not isinstance(p, dict):
                    continue
                targets = p.get("targets")
                if not isinstance(targets, list):
                    new_props.append(p)
                    continue
                filtered = [t for t in targets if not (isinstance(t, dict) and t.get("job_name") == job_name)]
                if not filtered:
                    continue
                p["targets"] = filtered
                new_props.append(p)
            pc["propagators"] = new_props

        deleted = delete_group_job(data=data, job_name=job_name)
        if not deleted:
            raise click.ClickException(f"Job not found: {job_name}")
        save_orchestration_with_validation(target=target, data=data, dbt_project_dir=dbt_project_path, prepare=prepare)
        if parse:
            run_dbt_parse(dbt_project_dir=dbt_project_path, dbt_profiles_dir=dbt_project_path, dbt_target=_default_dbt_target())
        click.echo(str(target))

    @meta.command("partition")
    @click.option("--dbt-project-dir", default="./dbt_project", show_default=True)
    @click.option("--path", "path_", default="dagsterization.yml", show_default=True)
    @click.option("--models", default="", help="Comma-separated model names")
    @click.option("--tag", "tag_", default="", help="Select models by existing dbt tag")
    @click.option("--type", "partition_type", required=True, help="daily|unpartitioned")
    @click.option("--prepare/--no-prepare", default=True, show_default=True)
    @click.option("--parse/--no-parse", default=False, show_default=True)
    def meta_partition(
        dbt_project_dir: str,
        path_: str,
        models: str,
        tag_: str,
        partition_type: str,
        prepare: bool,
        parse: bool,
    ) -> None:
        if partition_type not in {"daily", "unpartitioned"}:
            raise click.ClickException("--type must be one of daily|unpartitioned")

        dbt_project_path = resolve_dir_arg(dbt_project_dir)
        if not dbt_project_path.exists():
            raise click.ClickException(f"dbt project dir does not exist: {dbt_project_path}")

        manifest = load_dbt_manifest(
            dbt_project_dir=dbt_project_path,
            dbt_profiles_dir=dbt_project_path,
            dbt_target=_default_dbt_target(),
            prepare=prepare,
        ) if (tag_ or prepare) else None
        selected = select_models(dbt_project_dir=dbt_project_path, manifest=manifest, models_csv=models, tag_=tag_)
        if not selected:
            raise click.ClickException("No models selected (use --models or --tag)")

        target = orchestration_path(dbt_project_dir=dbt_project_path, path_=path_)
        data = load_orch(target)
        for m in selected:
            orch_set_partition(data=data, model=m, partition=partition_type)
        save_orchestration_with_validation(target=target, data=data, dbt_project_dir=dbt_project_path, prepare=prepare)
        if parse:
            run_dbt_parse(dbt_project_dir=dbt_project_path, dbt_profiles_dir=dbt_project_path, dbt_target=_default_dbt_target())
        click.echo(str(target))

    @meta.command("asset-job")
    @click.option("--dbt-project-dir", default="./dbt_project", show_default=True)
    @click.option("--path", "path_", default="dagsterization.yml", show_default=True)
    @click.option("--models", default="", help="Comma-separated model names")
    @click.option("--tag", "tag_", default="", help="Select models by existing dbt tag")
    @click.option("--prepare/--no-prepare", default=True, show_default=True)
    @click.option("--parse/--no-parse", default=False, show_default=True)
    def meta_asset_job(
        dbt_project_dir: str,
        path_: str,
        models: str,
        tag_: str,
        prepare: bool,
        parse: bool,
    ) -> None:
        dbt_project_path = resolve_dir_arg(dbt_project_dir)
        if not dbt_project_path.exists():
            raise click.ClickException(f"dbt project dir does not exist: {dbt_project_path}")
        manifest = load_dbt_manifest(
            dbt_project_dir=dbt_project_path,
            dbt_profiles_dir=dbt_project_path,
            dbt_target=_default_dbt_target(),
            prepare=prepare,
        ) if (tag_ or prepare) else None
        selected = select_models(dbt_project_dir=dbt_project_path, manifest=manifest, models_csv=models, tag_=tag_)
        if not selected:
            raise click.ClickException("No models selected (use --models or --tag)")

        target = orchestration_path(dbt_project_dir=dbt_project_path, path_=path_)
        data = load_orch(target)
        for m in selected:
            orch_set_asset_job(data=data, model=m, enabled=True)
        save_orchestration_with_validation(target=target, data=data, dbt_project_dir=dbt_project_path, prepare=prepare)
        if parse:
            run_dbt_parse(dbt_project_dir=dbt_project_path, dbt_profiles_dir=dbt_project_path, dbt_target=_default_dbt_target())
        click.echo(str(target))

    @meta.command("asset-job-delete")
    @click.option("--dbt-project-dir", default="./dbt_project", show_default=True)
    @click.option("--path", "path_", default="dagsterization.yml", show_default=True)
    @click.option("--models", default="", help="Comma-separated model names")
    @click.option("--tag", "tag_", default="", help="Select models by existing dbt tag")
    @click.option("--force/--no-force", default=False, show_default=True)
    @click.option("--prepare/--no-prepare", default=True, show_default=True)
    @click.option("--parse/--no-parse", default=False, show_default=True)
    def meta_asset_job_delete(
        dbt_project_dir: str,
        path_: str,
        models: str,
        tag_: str,
        force: bool,
        prepare: bool,
        parse: bool,
    ) -> None:
        dbt_project_path = resolve_dir_arg(dbt_project_dir)
        if not dbt_project_path.exists():
            raise click.ClickException(f"dbt project dir does not exist: {dbt_project_path}")
        manifest = load_dbt_manifest(
            dbt_project_dir=dbt_project_path,
            dbt_profiles_dir=dbt_project_path,
            dbt_target=_default_dbt_target(),
            prepare=prepare,
        ) if (tag_ or prepare) else None
        selected = select_models(dbt_project_dir=dbt_project_path, manifest=manifest, models_csv=models, tag_=tag_)
        if not selected:
            raise click.ClickException("No models selected (use --models or --tag)")
        target = orchestration_path(dbt_project_dir=dbt_project_path, path_=path_)
        data = load_orch(target)

        schedules = data.get("schedules")
        schedule_refs: list[str] = []
        if isinstance(schedules, dict):
            job_names = {f"dbt_{m}_asset_job" for m in selected}
            for name, cfg in schedules.items():
                if not isinstance(cfg, dict):
                    continue
                if cfg.get("job_name") in job_names:
                    schedule_refs.append(str(name))

        if schedule_refs and not force:
            raise click.ClickException(
                f"Asset job(s) still referenced by schedules {sorted(set(schedule_refs))}. Re-run with --force to delete schedules."
            )
        if isinstance(schedules, dict) and schedule_refs:
            for name in schedule_refs:
                schedules.pop(name, None)

        for m in selected:
            orch_set_asset_job(data=data, model=m, enabled=False)
        save_orchestration_with_validation(target=target, data=data, dbt_project_dir=dbt_project_path, prepare=prepare)
        if parse:
            run_dbt_parse(dbt_project_dir=dbt_project_path, dbt_profiles_dir=dbt_project_path, dbt_target=_default_dbt_target())
        click.echo(str(target))

    @meta.command("schedule")
    @click.option("--dbt-project-dir", default="./dbt_project", show_default=True)
    @click.option("--path", "path_", default="dagsterization.yml", show_default=True)
    @click.option("--models", default="", help="Comma-separated model names")
    @click.option("--tag", "tag_", default="", help="Select models by existing dbt tag")
    @click.option("--name", required=True)
    @click.option("--hour", type=int, required=True)
    @click.option("--minute", type=int, required=True)
    @click.option("--lookback-days", type=int, default=0, show_default=True)
    @click.option("--offset-days", type=int, default=1, show_default=True)
    @click.option("--enabled/--disabled", default=True, show_default=True)
    @click.option("--prepare/--no-prepare", default=True, show_default=True)
    @click.option("--parse/--no-parse", default=False, show_default=True)
    def meta_schedule(
        dbt_project_dir: str,
        path_: str,
        models: str,
        tag_: str,
        name: str,
        hour: int,
        minute: int,
        lookback_days: int,
        offset_days: int,
        enabled: bool,
        prepare: bool,
        parse: bool,
    ) -> None:
        if hour < 0 or hour > 23:
            raise click.ClickException("--hour must be 0..23")
        if minute < 0 or minute > 59:
            raise click.ClickException("--minute must be 0..59")
        if lookback_days < 0:
            raise click.ClickException("--lookback-days must be >= 0")
        if offset_days < 0:
            raise click.ClickException("--offset-days must be >= 0")

        dbt_project_path = resolve_dir_arg(dbt_project_dir)
        if not dbt_project_path.exists():
            raise click.ClickException(f"dbt project dir does not exist: {dbt_project_path}")

        manifest = load_dbt_manifest(
            dbt_project_dir=dbt_project_path,
            dbt_profiles_dir=dbt_project_path,
            dbt_target=_default_dbt_target(),
            prepare=prepare,
        ) if (tag_ or prepare) else None
        selected = select_models(dbt_project_dir=dbt_project_path, manifest=manifest, models_csv=models, tag_=tag_)
        if not selected:
            raise click.ClickException("No models selected (use --models or --tag)")

        target = orchestration_path(dbt_project_dir=dbt_project_path, path_=path_)
        data = load_orch(target)
        idx = index_orch(data)

        schedule_job_name = derive_job_name_for_model(idx, model=selected[0])
        if schedule_job_name is None:
            schedule_job_name = f"dbt_{selected[0]}_asset_job"
            orch_set_asset_job(data=data, model=selected[0], enabled=True)

        orch_set_schedule(
            data=data,
            name=name,
            job_name=schedule_job_name,
            schedule_type="daily_at",
            hour=hour,
            minute=minute,
            lookback_days=lookback_days,
            offset_days=offset_days,
            enabled=enabled,
        )
        save_orchestration_with_validation(target=target, data=data, dbt_project_dir=dbt_project_path, prepare=prepare)
        if parse:
            run_dbt_parse(dbt_project_dir=dbt_project_path, dbt_profiles_dir=dbt_project_path, dbt_target=_default_dbt_target())
        click.echo(str(target))

    @meta.group("partition-change")
    def meta_partition_change() -> None:
        pass

    @meta_partition_change.command("detector")
    @click.option("--dbt-project-dir", default="./dbt_project", show_default=True)
    @click.option("--path", "path_", default="dagsterization.yml", show_default=True)
    @click.option("--model", required=True)
    @click.option("--enabled/--disabled", default=False, show_default=True)
    @click.option("--name", default="")
    @click.option("--job-name", default="")
    @click.option("--detect-relation", default="")
    @click.option("--detect-source", default="", help="source.table")
    @click.option("--partition-date-expr", required=True)
    @click.option("--updated-at-expr", required=True)
    @click.option("--lookback-days", type=int, default=0, show_default=True)
    @click.option("--offset-days", type=int, default=0, show_default=True)
    @click.option("--minimum-interval-seconds", type=int, default=60, show_default=True)
    @click.option("--prepare/--no-prepare", default=True, show_default=True)
    @click.option("--parse/--no-parse", default=False, show_default=True)
    def meta_partition_change_detector(
        dbt_project_dir: str,
        path_: str,
        model: str,
        enabled: bool,
        name: str,
        job_name: str,
        detect_relation: str,
        detect_source: str,
        partition_date_expr: str,
        updated_at_expr: str,
        lookback_days: int,
        offset_days: int,
        minimum_interval_seconds: int,
        prepare: bool,
        parse: bool,
    ) -> None:
        if lookback_days < 0:
            raise click.ClickException("--lookback-days must be >= 0")
        if offset_days < 0:
            raise click.ClickException("--offset-days must be >= 0")
        if minimum_interval_seconds <= 0:
            raise click.ClickException("--minimum-interval-seconds must be > 0")
        if bool(detect_relation.strip()) == bool(detect_source.strip()):
            raise click.ClickException("Specify exactly one of --detect-relation or --detect-source")

        detect_source_dict = None
        if detect_source.strip():
            if "." not in detect_source:
                raise click.ClickException("--detect-source must be in format source.table")
            source, table = detect_source.split(".", 1)
            detect_source_dict = {"source": source.strip(), "table": table.strip()}

        dbt_project_path = resolve_dir_arg(dbt_project_dir)
        if not dbt_project_path.exists():
            raise click.ClickException(f"dbt project dir does not exist: {dbt_project_path}")

        target = orchestration_path(dbt_project_dir=dbt_project_path, path_=path_)
        data = load_orch(target)
        idx = index_orch(data)

        job_name_value = job_name.strip() or derive_job_name_for_model(idx, model=model) or None
        orch_set_detector(
            data=data,
            model=model,
            enabled=enabled,
            name=name.strip() or None,
            job_name=job_name_value,
            detect_relation=detect_relation.strip() or None,
            detect_source=detect_source_dict,
            partition_date_expr=partition_date_expr,
            updated_at_expr=updated_at_expr,
            lookback_days=lookback_days,
            offset_days=offset_days,
            minimum_interval_seconds=minimum_interval_seconds,
        )
        save_orchestration_with_validation(target=target, data=data, dbt_project_dir=dbt_project_path, prepare=prepare)
        if parse:
            run_dbt_parse(dbt_project_dir=dbt_project_path, dbt_profiles_dir=dbt_project_path, dbt_target=_default_dbt_target())
        click.echo(str(target))

    def _propagator_impl(
        *,
        dbt_project_dir: str,
        path_: str,
        model: str,
        enabled: bool,
        name: str,
        minimum_interval_seconds: int,
        targets: str,
        prepare: bool,
        parse: bool,
    ) -> None:
        if minimum_interval_seconds <= 0:
            raise click.ClickException("--minimum-interval-seconds must be > 0")
        targets_list = split_csv(targets)
        if not targets_list:
            raise click.ClickException("--targets must be a non-empty comma-separated list")

        dbt_project_path = resolve_dir_arg(dbt_project_dir)
        if not dbt_project_path.exists():
            raise click.ClickException(f"dbt project dir does not exist: {dbt_project_path}")

        target = orchestration_path(dbt_project_dir=dbt_project_path, path_=path_)
        data = load_orch(target)
        orch_set_propagation(
            data=data,
            upstream_model=model,
            enabled=enabled,
            name=name.strip() or None,
            minimum_interval_seconds=minimum_interval_seconds,
            targets=targets_list,
        )
        save_orchestration_with_validation(target=target, data=data, dbt_project_dir=dbt_project_path, prepare=prepare)
        if parse:
            run_dbt_parse(dbt_project_dir=dbt_project_path, dbt_profiles_dir=dbt_project_path, dbt_target=_default_dbt_target())
        click.echo(str(target))

    @meta_partition_change.command("propagator")
    @click.option("--dbt-project-dir", default="./dbt_project", show_default=True)
    @click.option("--path", "path_", default="dagsterization.yml", show_default=True)
    @click.option("--model", required=True)
    @click.option("--enabled/--disabled", default=False, show_default=True)
    @click.option("--name", default="")
    @click.option("--minimum-interval-seconds", type=int, default=30, show_default=True)
    @click.option("--targets", required=True, help="Comma-separated job names")
    @click.option("--prepare/--no-prepare", default=True, show_default=True)
    @click.option("--parse/--no-parse", default=False, show_default=True)
    def meta_partition_change_propagator(
        dbt_project_dir: str,
        path_: str,
        model: str,
        enabled: bool,
        name: str,
        minimum_interval_seconds: int,
        targets: str,
        prepare: bool,
        parse: bool,
    ) -> None:
        _propagator_impl(
            dbt_project_dir=dbt_project_dir,
            path_=path_,
            model=model,
            enabled=enabled,
            name=name,
            minimum_interval_seconds=minimum_interval_seconds,
            targets=targets,
            prepare=prepare,
            parse=parse,
        )

    @meta.command("validate")
    @click.option("--dbt-project-dir", default="./dbt_project", show_default=True)
    @click.option("--path", "path_", default="dagsterization.yml", show_default=True)
    @click.option("--prepare/--no-prepare", default=True, show_default=True)
    def meta_validate(dbt_project_dir: str, path_: str, prepare: bool) -> None:
        dbt_project_path = resolve_dir_arg(dbt_project_dir)
        if not dbt_project_path.exists():
            raise click.ClickException(f"dbt project dir does not exist: {dbt_project_path}")

        mp = dbt_manifest_path(dbt_project_path)
        if not mp.exists() and not prepare:
            raise click.ClickException(
                f"manifest not found: {mp} (re-run with --prepare to run dbt parse)"
            )

        manifest = load_dbt_manifest(
            dbt_project_dir=dbt_project_path,
            dbt_profiles_dir=dbt_project_path,
            dbt_target=_default_dbt_target(),
            prepare=prepare,
        )

        orch_path = resolve_orchestration_path(dbt_project_dir=dbt_project_path, path_=Path(path_))
        orchestration = load_orch(orch_path)

        issues = validate_orchestration_structure(orchestration=orchestration)
        issues.extend(
            validate_orchestration(
                manifest=manifest,
                orchestration=orchestration,
                require_file_exists=True,
                orchestration_path=orch_path,
            )
        )
        errors = [i for i in issues if i.level == "error"]
        for issue in issues:
            prefix = "ERROR" if issue.level == "error" else "WARN"
            click.echo(f"{prefix}: {issue.message}")
        if errors:
            raise click.ClickException(f"Validation failed with {len(errors)} error(s)")
        click.echo("OK")

    return meta
