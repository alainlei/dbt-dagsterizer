from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

_JOB_NAME_LOOKUP_SQL = (
    "SELECT j.name AS SqlAgentJobName "
    "FROM ReportServer.dbo.Catalog c "
    "INNER JOIN ReportServer.dbo.Subscriptions s ON c.ItemID = s.Report_OID "
    "INNER JOIN ReportServer.dbo.ReportSchedule rs ON s.SubscriptionID = rs.SubscriptionID "
    "INNER JOIN msdb.dbo.sysjobs j ON CAST(rs.ScheduleID AS VARCHAR(100)) = j.name "
    "WHERE s.Description = %s"
)


@dataclass(frozen=True)
class SsrsAgentJobClient:
    """Client for firing SSRS subscriptions through their SQL Server Agent jobs.

    Every SSRS subscription is backed by a SQL Server Agent job. The job name
    is resolved by looking up the subscription's description in the
    ReportServer catalog (Subscriptions/ReportSchedule joined against
    msdb.dbo.sysjobs); starting that job makes the report server execute the
    pre-defined subscription (rendering + delivery are handled entirely by
    SSRS).
    """

    host: str
    port: int = 1433
    username: str = ""
    password: str = ""
    database: str = "msdb"
    timeout_seconds: int = 60
    connection_factory: Any = field(default=None, compare=False)

    def _connect(self):
        if self.connection_factory is not None:
            return self.connection_factory()
        try:
            import pymssql
        except ImportError as exc:
            raise RuntimeError(
                "Triggering SSRS subscriptions via SQL Server Agent requires the "
                "'pymssql' package; install it to use ssrs_reports"
            ) from exc
        return pymssql.connect(
            server=self.host,
            port=str(self.port),
            user=self.username or None,
            password=self.password or None,
            database=self.database,
            timeout=self.timeout_seconds,
            login_timeout=self.timeout_seconds,
        )

    def start_subscription_job(self, *, subscription_description: str) -> str:
        """Start the SQL Server Agent job backing the given SSRS subscription.

        The subscription is identified by its description in the ReportServer
        catalog. Returns the Agent job name that was started.
        """
        if not self.host:
            raise ValueError("SSRS_DB_HOST is not configured")
        description = subscription_description.strip()
        if not description:
            raise ValueError("subscription_description must be non-empty")

        connection = self._connect()
        with connection:
            cursor = connection.cursor()
            cursor.execute(_JOB_NAME_LOOKUP_SQL, (description,))
            rows = cursor.fetchall()
            if not rows:
                raise RuntimeError(
                    f"No SQL Server Agent job found for SSRS subscription description "
                    f"'{description}'"
                )
            if len(rows) > 1:
                raise RuntimeError(
                    f"SSRS subscription description '{description}' matches "
                    f"{len(rows)} SQL Server Agent jobs; descriptions must be unique"
                )
            job_name = str(rows[0][0])
            cursor.execute("EXEC msdb.dbo.sp_start_job @job_name = %s", (job_name,))
            connection.commit()
        return job_name


def make_ssrs_agent_resource() -> SsrsAgentJobClient:
    return SsrsAgentJobClient(
        host=os.getenv("SSRS_DB_HOST", ""),
        port=int(os.getenv("SSRS_DB_PORT", "1433")),
        username=os.getenv("SSRS_DB_USERNAME", ""),
        password=os.getenv("SSRS_DB_PASSWORD", ""),
        database=os.getenv("SSRS_DB_DATABASE", "msdb"),
        timeout_seconds=int(os.getenv("SSRS_DB_TIMEOUT_SECONDS", "60")),
    )
