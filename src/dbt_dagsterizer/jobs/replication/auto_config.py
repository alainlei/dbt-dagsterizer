"""Auto-configuration for replication jobs.

Reuses the asset specs from ``assets.replication.auto_config`` and wraps them
into job specs suitable for ``factory.py``.
"""
from __future__ import annotations

from ...assets.replication.auto_config import build_auto_replication_specs


def build_auto_replication_job_specs() -> list[dict]:
    """Build replication job specs from dagsterization.yml.

    Returns an empty list when replication is disabled.
    """
    asset_specs = build_auto_replication_specs()
    job_specs: list[dict] = []
    for spec in asset_specs:
        job_specs.append(
            {
                "name": f"{spec['name']}_job",
                "asset_key": spec["name"],
                "partition_type": spec.get("partition_type", "unpartitioned"),
            }
        )
    return job_specs
