from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SqlServerClient:
    """Configuration for connecting to Microsoft SQL Server (replication destination)."""

    host: str
    port: int
    user: str
    password: str
    database: str
    driver: str = "ODBC Driver 18 for SQL Server"

    def connection_string(self) -> str:
        """Build a pyodbc / SQLAlchemy-compatible connection string for SQL Server."""
        driver_encoded = self.driver.replace(" ", "+")
        return (
            f"mssql+pyodbc://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
            f"?driver={driver_encoded}&TrustServerCertificate=yes"
        )


def make_mssql_resource() -> SqlServerClient:
    return SqlServerClient(
        host=os.getenv("SQLSERVER_HOST", "localhost"),
        port=int(os.getenv("SQLSERVER_PORT", "1433")),
        user=os.getenv("SQLSERVER_USER", "sa"),
        password=os.getenv("SQLSERVER_PASSWORD", ""),
        database=os.getenv("SQLSERVER_DATABASE", "master"),
        driver=os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"),
    )
