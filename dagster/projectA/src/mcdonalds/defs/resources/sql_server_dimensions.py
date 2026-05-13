from collections.abc import Sequence
from typing import Any, Dict, Optional
from datetime import datetime

import os
import pyodbc
from dagster import resource


class SqlDimServerResource:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        connect_timeout: int,
        command_timeout: int,
        driver: str,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connect_timeout = connect_timeout
        self.command_timeout = command_timeout
        self.driver = driver

    def _get_connection(self) -> pyodbc.Connection:
        conn_str = (
            "DRIVER={"
            + self.driver
            + "};SERVER="
            + self.host
            + ","
            + str(self.port)
            + ";DATABASE="
            + self.database
            + ";UID="
            + self.user
            + ";PWD="
            + self.password
            + ";TrustServerCertificate=yes"
            + ";Connection Timeout="
            + str(self.connect_timeout)
        )
        return pyodbc.connect(conn_str)

    def query(self, query: str) -> Optional[Sequence[Any]]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(query)
                    rows = cursor.fetchall()
                except pyodbc.Error:
                    return None
                if rows is None or len(rows) == 0:
                    return None
                return rows

    def get_basic_options(self) -> Dict[str, Any]:
        url = (
                "jdbc:sqlserver://"
                + self.host
                + ":"
                + str(self.port)
                + ";databaseName="
                + self.database
                + ";encrypt=true;trustServerCertificate=true"
                + ";Connection Timeout="
                + str(self.connect_timeout)
        )
        return {
            "url": url,
            "user": self.user,
            "password": self.password,
            "driver": self.driver,
        }

    def get_watermark(self) -> Optional[datetime]:
        return self._get_max_modified_at(["Sensors"])

    def get_watermark_schedules(self) -> Optional[datetime]:
        return self._get_max_modified_at(
            [
                "GroupSensors",
                "GroupHasSchedules",
                "ProgramSchedules",
                "ProgramProfileTys",
                "ProgramProfiles",
            ]
        )

    def get_watermark_sensor_expected_values(self) -> Optional[datetime]:
        return self._get_max_modified_at(
            [
                "Sensors",
                "SensorTys",
                "Devices",
                "DeviceTys",
                "GroupSensors",
                "GroupHasSchedules",
                "ProgramSchedules",
                "ProgramProfileTys",
                "ProgramProfiles",
            ]
        )

    def _get_max_modified_at(self, tables: list[str]) -> Optional[datetime]:
        conn_str = (
                "DRIVER={ODBC Driver 18 for SQL Server}"
                + ";SERVER="
                + self.host
                + ","
                + str(self.port)
                + ";DATABASE="
                + self.database
                + ";UID="
                + self.user
                + ";PWD="
                + self.password
                + ";TrustServerCertificate=yes"
                + ";Connection Timeout="
                + str(self.connect_timeout)
        )
        conn = pyodbc.connect(conn_str)
        max_modified_at: Optional[datetime] = None

        with conn:
            with conn.cursor() as cursor:
                try:
                    for table_name in tables:
                        cursor.execute(f"SELECT MAX(ModifiedAt) FROM dbo.{table_name};")
                        row = cursor.fetchone()
                        if row is None or row[0] is None:
                            continue
                        if max_modified_at is None or row[0] > max_modified_at:
                            max_modified_at = row[0]
                except pyodbc.Error:
                    return None

        return max_modified_at

@resource
def dim_sqlserver_resource(_):
    host = os.getenv("DIMSQL_HOST", "localhost")
    port = int(os.getenv("DIMSQL_PORT", "1433"))
    database = os.getenv("DIM_DB", "YourDatabaseName")
    user = os.getenv("DIMSQL_USER", "app_ingestion_user")
    password = os.getenv("DIMSQL_PASSWORD", "")
    connect_timeout = int(os.getenv("DIMSQL_CONNECT_TIMEOUT", "300"))
    command_timeout = int(os.getenv("DIMSQL_COMMAND_TIMEOUT", "4500"))
    driver = os.getenv("DIMSQL_DRIVER", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    return SqlDimServerResource(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        connect_timeout=connect_timeout,
        command_timeout=command_timeout,
        driver=driver,
    )
