from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Generator


@dataclass(frozen=True)
class StarRocksClient:
    host: str
    port: int
    user: str
    password: str
    connect_timeout: int = 10

    @contextmanager
    def _connection(self) -> Generator[Any, None, None]:
        import pymysql

        connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            connect_timeout=self.connect_timeout,
            read_timeout=self.connect_timeout,
            write_timeout=self.connect_timeout,
            autocommit=True,
        )
        try:
            yield connection
        finally:
            connection.close()

    def query_scalar(self, sql: str) -> Any:
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                row = cursor.fetchone()
                return row[0] if row else None

    def query_first_column(self, sql: str) -> list[Any]:
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                return [row[0] for row in rows]

    def query_rows(self, sql: str) -> list[tuple[Any, ...]]:
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                return list(rows)


def make_starrocks_resource() -> StarRocksClient:
    return StarRocksClient(
        host=os.getenv("STARROCKS_HOST", "localhost"),
        port=int(os.getenv("STARROCKS_PORT", "9030")),
        user=os.getenv("STARROCKS_USER", "root"),
        password=os.getenv("STARROCKS_PASSWORD", ""),
        connect_timeout=int(os.getenv("STARROCKS_CONNECT_TIMEOUT", "10")),
    )
