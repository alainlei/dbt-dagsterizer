"""dlt-based replication executor: StarRocks -> SQL Server.

All ``dlt`` imports are lazy (inside function bodies) so that the rest of the
codebase works even when the ``dlt`` package is not installed.
"""
from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote_plus

from ...resources.mssql import SqlServerClient
from ...resources.starrocks import StarRocksClient

logger = logging.getLogger(__name__)


def execute_replication(
    *,
    context: Any,
    spec: dict,
    starrocks_client: StarRocksClient,
    mssql_client: SqlServerClient,
) -> None:
    """Execute a single replication via dlt.

    Args:
        context: Dagster asset execution context (used for logging + partition key).
        spec: Replication spec dict from ``auto_config.build_auto_replication_specs``.
        starrocks_client: Source database client.
        mssql_client: Destination database client.
    """
    import dlt
    from dlt.sources.sql_database import sql_database

    source_database = spec["source_database"]
    source_table = spec["source_table"]
    destination_table = spec["destination_table"]
    destination_schema = spec["destination_schema"]
    write_disposition = spec["write_disposition"]
    partition_column = spec.get("partition_column")

    # Extract partition key when the asset is partitioned
    partition_key: str | None = None
    try:
        partition_key = context.partition_key
    except Exception:
        pass

    log = getattr(context, "log", logger)
    log.info(
        "Replication: %s -> MSSQL (table=%s, schema=%s, disposition=%s, partition=%s)",
        source_table,
        destination_table,
        destination_schema,
        write_disposition,
        partition_key,
    )

    # Build the StarRocks source connection string (MySQL protocol via pymysql)
    source_credentials = (
        f"mysql+pymysql://{starrocks_client.user}:{starrocks_client.password}"
        f"@{starrocks_client.host}:{starrocks_client.port}/{source_database}"
    )

    # Build SQL Server connection string for dlt
    # URL-encode credentials to handle special characters
    user_encoded = quote_plus(mssql_client.user)
    password_encoded = quote_plus(mssql_client.password)
    driver_encoded = quote_plus(mssql_client.driver)
    mssql_credentials = (
        f"mssql://{user_encoded}:{password_encoded}"
        f"@{mssql_client.host}:{mssql_client.port}/{mssql_client.database}"
        f"?driver={driver_encoded}&TrustServerCertificate=yes&Encrypt=no"
    )

    # Create dlt pipeline targeting SQL Server
    pipeline = dlt.pipeline(
        pipeline_name=f"replicate_{spec['model']}",
        destination=dlt.destinations.mssql(mssql_credentials),
        dataset_name=destination_schema,
    )

    # Build the source from StarRocks
    source = sql_database(
        credentials=source_credentials,
        table_names=[source_table],
        reflection_level="minimal",
    )

    resource = source.resources[source_table]

    # Tell dlt to rename the source table to the destination table
    resource.apply_hints(
        table_name=destination_table,
    )

    # Apply partition filter when partition-aware
    if partition_column and partition_key:
        resource.apply_hints(
            incremental=dlt.sources.incremental(
                cursor_path=partition_column,
                initial_value=partition_key,
                end_value=partition_key,
            ),
        )
        log.info("Partition-aware replication: filtering %s = %s", partition_column, partition_key)

    # Execute the pipeline
    load_info = pipeline.run(
        resource,
        table_name=destination_table,
        write_disposition=write_disposition,
    )

    log.info("Replication completed: %s rows loaded", load_info)
