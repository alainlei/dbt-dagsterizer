from __future__ import annotations

import re

import dagster as dg


def _sanitized_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", name)


def build_ssrs_report_assets(*, specs: list[dict]) -> list:
    """Build one report asset per spec.

    Each asset depends on its upstream dbt model asset and, when enabled,
    auto-materializes eagerly after the model materializes: it starts the
    SQL Server Agent job backing the pre-defined SSRS subscription, and SSRS
    itself handles report rendering and delivery.
    """

    def make_asset(spec: dict):
        name = spec["name"]
        subscription_description = spec["subscription_description"]
        enabled = bool(spec.get("enabled", False))

        upstream_asset_key = dg.AssetKey(spec["upstream_relation"])
        automation_condition = dg.AutomationCondition.eager() if enabled else None

        @dg.asset(
            key=dg.AssetKey(["ssrs", _sanitized_name(name)]),
            deps=[upstream_asset_key],
            group_name="reports",
            automation_condition=automation_condition,
            required_resource_keys={"ssrs_agent"},
            description=(
                f"Trigger SSRS subscription '{subscription_description}' via its SQL Server Agent job "
                f"after '{spec['model']}' materializes."
            ),
        )
        def _report_asset(context) -> dg.MaterializeResult:
            job_name = context.resources.ssrs_agent.start_subscription_job(
                subscription_description=subscription_description,
            )
            context.log.info(
                "Started SQL Server Agent job '%s' for SSRS subscription '%s'",
                job_name,
                subscription_description,
            )
            return dg.MaterializeResult(
                metadata={
                    "subscription_description": subscription_description,
                    "agent_job_name": job_name,
                }
            )

        return _report_asset

    return [make_asset(spec) for spec in specs]
