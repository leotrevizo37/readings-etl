import importlib
import sys
import unittest
import warnings
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

usage_module = importlib.import_module(
    "projectA.defs.transformations.telemetry_user_usage"
)
build_dim_aspnet_users_df = usage_module.build_dim_aspnet_users_df
build_fact_telemetry_user_usage_df = (
    usage_module.build_fact_telemetry_user_usage_df
)

warnings.simplefilter("ignore", ResourceWarning)


class TelemetryUserUsageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("projectA-telemetry-user-usage-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_dimension_projects_only_allowed_columns(self) -> None:
        users_df = self.spark.createDataFrame(
            [
                {
                    "Id": "11111111-1111-1111-1111-111111111111",
                    "UserName": "user",
                    "Name": "Name",
                    "LastName": "Last",
                    "Position": "Position",
                    "Email": "not-selected",
                }
            ]
        )

        result = build_dim_aspnet_users_df(users_df)

        self.assertEqual(
            result.columns,
            ["Id", "UserName", "Name", "LastName", "Position"],
        )

    def test_fact_builds_daily_rows_and_usage_metrics(self) -> None:
        users_schema = StructType(
            [
                StructField("Id", StringType(), False),
                StructField("CreatedAt", TimestampType(), False),
            ]
        )
        logs_schema = StructType(
            [
                StructField("RegisterId", StringType(), False),
                StructField("UserId", StringType(), True),
                StructField("Module", StringType(), True),
                StructField("RequestDate", TimestampType(), False),
                StructField("StatusCode", IntegerType(), True),
                StructField("ExecutionTimeMs", LongType(), True),
            ]
        )
        first_user = "11111111-1111-1111-1111-111111111111"
        second_user = "22222222-2222-2222-2222-222222222222"
        third_user = "33333333-3333-3333-3333-333333333333"
        users_df = self.spark.createDataFrame(
            [
                (first_user, datetime(2026, 6, 4, 8, 0)),
                (second_user, datetime(2026, 6, 4, 8, 0)),
                (third_user, datetime(2026, 6, 6, 12, 0)),
            ],
            users_schema,
        )
        system_logs_df = self.spark.createDataFrame(
            [
                (
                    "00000000-0000-0000-0000-000000000001",
                    first_user,
                    "login",
                    datetime(2026, 6, 5, 9, 0),
                    200,
                    100,
                ),
                (
                    "00000000-0000-0000-0000-000000000002",
                    first_user,
                    "reads",
                    datetime(2026, 6, 5, 9, 5),
                    200,
                    999,
                ),
                (
                    "00000000-0000-0000-0000-000000000003",
                    first_user,
                    "locations",
                    datetime(2026, 6, 5, 9, 10),
                    200,
                    200,
                ),
                (
                    "00000000-0000-0000-0000-000000000004",
                    first_user,
                    "5d5de745-452b-41a3-99cb-08dec5694e18",
                    datetime(2026, 6, 5, 9, 15),
                    200,
                    700,
                ),
                (
                    "00000000-0000-0000-0000-000000000005",
                    first_user,
                    "sensors",
                    datetime(2026, 6, 5, 9, 20),
                    200,
                    300,
                ),
                (
                    "00000000-0000-0000-0000-000000000006",
                    first_user,
                    "devices",
                    datetime(2026, 6, 5, 10, 0),
                    200,
                    400,
                ),
                (
                    "00000000-0000-0000-0000-000000000007",
                    second_user,
                    "reads",
                    datetime(2026, 6, 5, 9, 0),
                    200,
                    500,
                ),
                (
                    "00000000-0000-0000-0000-000000000008",
                    third_user,
                    "locations",
                    datetime(2026, 6, 6, 13, 0),
                    500,
                    500,
                ),
                (
                    "00000000-0000-0000-0000-000000000009",
                    first_user,
                    "locations",
                    datetime(2026, 6, 6, 9, 0),
                    200,
                    100,
                ),
                (
                    "00000000-0000-0000-0000-000000000010",
                    first_user,
                    "sensors",
                    datetime(2026, 6, 6, 9, 0, 10),
                    200,
                    100,
                ),
            ],
            logs_schema,
        )

        result = build_fact_telemetry_user_usage_df(
            users_df,
            system_logs_df,
            datetime(2026, 6, 5),
            datetime(2026, 6, 7),
        )
        rows = {
            (row["UserId"], str(row["Date"])): row
            for row in result.collect()
        }

        self.assertEqual(len(rows), 5)
        first_day = rows[(first_user, "2026-06-05")]
        self.assertTrue(first_day["HasConnected"])
        self.assertTrue(first_day["MadeCompleteInteraction"])
        self.assertEqual(first_day["TimeConnected"], 1200)
        self.assertEqual(
            first_day["AvgLatency"],
            250,
        )
        self.assertEqual(
            float(first_day["Latency95thPercentile"]),
            385.0,
        )

        second_day = rows[(second_user, "2026-06-05")]
        self.assertTrue(second_day["HasConnected"])
        self.assertFalse(second_day["MadeCompleteInteraction"])
        self.assertEqual(second_day["TimeConnected"], 0)
        self.assertEqual(
            second_day["AvgLatency"],
            0,
        )
        self.assertIsNone(second_day["Latency95thPercentile"])

        short_interaction = rows[(first_user, "2026-06-06")]
        self.assertFalse(short_interaction["MadeCompleteInteraction"])
        self.assertEqual(short_interaction["TimeConnected"], 10)

        no_connection = rows[(third_user, "2026-06-06")]
        self.assertFalse(no_connection["HasConnected"])
        self.assertFalse(no_connection["MadeCompleteInteraction"])
        self.assertEqual(no_connection["TimeConnected"], 0)
        self.assertEqual(
            no_connection["AvgLatency"],
            0,
        )
        self.assertIsNone(no_connection["Latency95thPercentile"])


if __name__ == "__main__":
    unittest.main()
