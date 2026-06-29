"""Query StarRocks for table row counts after dbt execution."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from ..resources.starrocks import StarRocksClient

logger = logging.getLogger(__name__)


def _get_table_relation(node: dict[str, Any]) -> tuple[str, str, str] | None:
    """Extract database, schema, and table name from a dbt manifest node.
    
    Returns:
        Tuple of (database, schema, table) or None if not available
    """
    database = node.get("database") or node.get("schema")
    schema = node.get("schema")
    identifier = node.get("identifier") or node.get("alias") or node.get("name")
    
    if not database or not schema or not identifier:
        return None
    
    return (str(database), str(schema), str(identifier))


def _build_count_query(database: str, schema: str, table: str) -> str:
    """Build a SQL query to count rows in a StarRocks table."""
    # StarRocks uses standard SQL COUNT
    return f"SELECT COUNT(*) FROM `{database}`.`{schema}`.`{table}`" if database != schema else f"SELECT COUNT(*) FROM `{database}`.`{table}`"


def get_row_counts_from_starrocks(
    client: StarRocksClient,
    node
) -> int:
    """Query StarRocks for row counts of dbt models.
    
    Args:
        client: StarRocksClient instance for database connection
        manifest_path: Path to dbt manifest.json
        target_unique_id: dbt model unique_id
        
    Returns:
        Dictionary mapping unique_id to row count
    """
    row_counts = -1 
    relation = _get_table_relation(node)
    if not relation:
        return -1
    
    database, schema, table = relation
    
    try:
        query = _build_count_query(database, schema, table)
        count = client.query_scalar(query)
        
        if count is not None:
            row_counts = int(count)
    except Exception as e:
        logger.warning(f"Row counts: Failed to query StarRocks for {node.get('unique_id')}: {e}")
        return -1
    
    return row_counts
