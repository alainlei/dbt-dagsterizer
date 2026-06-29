from .dbt import make_dbt_resource
from .mssql import make_mssql_resource
from .starrocks import make_starrocks_resource


def get_resources():
    return {
        "dbt": make_dbt_resource(),
        "starrocks": make_starrocks_resource(),
        "mssql": make_mssql_resource(),
    }
