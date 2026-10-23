import sys
import unittest
from datetime import datetime
from importlib import import_module
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

redings_audits = import_module(
    "projectD.defs.transformations.redings_audits"
).redings_audits


class RedingsAuditsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("projectD-redings-audits-tests")
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

    def test_builds_sensor_hour_health_rows(self) -> None:
        first_sensor = "11111111-1111-1111-1111-111111111111"
        second_sensor = "22222222-2222-2222-2222-222222222222"
        third_sensor = "33333333-3333-3333-3333-333333333333"
        dimensions_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": first_sensor,
                    "Active": True,
                    "TimeZone": "America/Denver",
                },
                {
                    "SensorId": second_sensor,
                    "Active": True,
                    "TimeZone": "America/Tijuana",
                },
                {
                    "SensorId": third_sensor,
                    "Active": True,
                    "TimeZone": "America/Chihuahua",
                },
            ]
        )
        readings_df = self.spark.createDataFrame(
            [
                {
                    "ReadId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "SensorId": first_sensor,
                    "TimeSpan": datetime(2026, 7, 30, 16, 5),
                }
            ]
        )
        previous_health_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": second_sensor,
                    "TimeSpan": datetime(2026, 7, 30, 15, 0),
                    "LastReadingAt": datetime(2026, 7, 30, 15, 0),
                }
            ],
            schema=StructType(
                [
                    StructField("SensorId", StringType(), False),
                    StructField("TimeSpan", TimestampType(), False),
                    StructField("LastReadingAt", TimestampType(), True),
                ]
            ),
        )

        result = redings_audits(
            readings_df=readings_df,
            dimensions_df=dimensions_df,
            previous_health_df=previous_health_df,
            from_utc=datetime(2026, 7, 30, 16, 0),
            to_utc=datetime(2026, 7, 30, 16, 0),
            as_of_utc=datetime(2026, 7, 30, 16, 20),
        )
        rows = {
            row["SensorId"]: row
            for row in result.orderBy("SensorId").collect()
        }

        self.assertEqual(
            rows[first_sensor]["TimeSpan"],
            datetime(2026, 7, 30, 16, 0),
        )
        self.assertEqual(
            rows[first_sensor]["LocalTimeSpan"],
            datetime(2026, 7, 30, 10, 0),
        )
        self.assertEqual(
            rows[second_sensor]["LocalTimeSpan"],
            datetime(2026, 7, 30, 9, 0),
        )
        self.assertEqual(
            rows[third_sensor]["LocalTimeSpan"],
            datetime(2026, 7, 30, 10, 0),
        )
        self.assertEqual(rows[first_sensor]["ReadingsCount"], 1)
        self.assertIsNone(rows[first_sensor]["AverageGapMinutes"])
        self.assertIsNone(rows[first_sensor]["MaximumGapMinutes"])
        self.assertIsNone(rows[first_sensor]["MinimumGapMinutes"])
        self.assertEqual(
            float(rows[first_sensor]["ReadingsCoveragePercentage"]),
            16.6667,
        )
        self.assertEqual(rows[first_sensor]["CoveredReadingBlocks"], 1)
        self.assertEqual(rows[first_sensor]["MinutesWithoutReadings"], 15)
        self.assertTrue(rows[first_sensor]["HasLateReadings"])
        self.assertFalse(rows[first_sensor]["IsConnectionLost"])
        self.assertIsNone(rows[first_sensor]["ConnectionLostAt"])
        self.assertEqual(rows[second_sensor]["ReadingsCount"], 0)
        self.assertEqual(
            float(rows[second_sensor]["ReadingsCoveragePercentage"]),
            0.0,
        )
        self.assertEqual(rows[second_sensor]["CoveredReadingBlocks"], 0)
        self.assertEqual(rows[second_sensor]["MinutesWithoutReadings"], 80)
        self.assertTrue(rows[second_sensor]["HasLateReadings"])
        self.assertTrue(rows[second_sensor]["IsConnectionLost"])
        self.assertEqual(
            rows[second_sensor]["ConnectionLostAt"],
            datetime(2026, 7, 30, 16, 0),
        )
        self.assertEqual(rows[third_sensor]["ReadingsCount"], 0)
        self.assertIsNone(rows[third_sensor]["MinutesWithoutReadings"])
        self.assertFalse(rows[third_sensor]["HasLateReadings"])
        self.assertFalse(rows[third_sensor]["IsConnectionLost"])

    def test_marks_a_recovered_gap_as_late(self) -> None:
        sensor_id = "11111111-1111-1111-1111-111111111111"
        dimensions_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": sensor_id,
                    "Active": True,
                    "TimeZone": "America/Denver",
                }
            ]
        )
        readings_df = self.spark.createDataFrame(
            [
                {
                    "ReadId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "SensorId": sensor_id,
                    "TimeSpan": datetime(2026, 7, 30, 16, 20),
                },
                {
                    "ReadId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "SensorId": sensor_id,
                    "TimeSpan": datetime(2026, 7, 30, 16, 20),
                },
            ]
        )
        previous_health_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": sensor_id,
                    "TimeSpan": datetime(2026, 7, 30, 15, 0),
                    "LastReadingAt": datetime(2026, 7, 30, 16, 0),
                }
            ],
            schema=StructType(
                [
                    StructField("SensorId", StringType(), False),
                    StructField("TimeSpan", TimestampType(), False),
                    StructField("LastReadingAt", TimestampType(), True),
                ]
            ),
        )

        result = redings_audits(
            readings_df=readings_df,
            dimensions_df=dimensions_df,
            previous_health_df=previous_health_df,
            from_utc=datetime(2026, 7, 30, 16, 0),
            to_utc=datetime(2026, 7, 30, 16, 0),
            as_of_utc=datetime(2026, 7, 30, 16, 21),
        )
        row = result.collect()[0]

        self.assertEqual(row["ReadingsCount"], 1)
        self.assertEqual(float(row["AverageGapMinutes"]), 20.0)
        self.assertEqual(float(row["MaximumGapMinutes"]), 20.0)
        self.assertEqual(float(row["MinimumGapMinutes"]), 20.0)
        self.assertEqual(float(row["ReadingsCoveragePercentage"]), 16.6667)
        self.assertEqual(row["CoveredReadingBlocks"], 1)
        self.assertEqual(row["MinutesWithoutReadings"], 1)
        self.assertTrue(row["HasLateReadings"])
        self.assertFalse(row["IsConnectionLost"])

    def test_calculates_gap_and_reading_block_coverage_metrics(self) -> None:
        sensor_id = "11111111-1111-1111-1111-111111111111"
        dimensions_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": sensor_id,
                    "Active": True,
                    "TimeZone": "America/Denver",
                }
            ]
        )
        readings_df = self.spark.createDataFrame(
            [
                {
                    "ReadId": f"aaaaaaaa-aaaa-aaaa-aaaa-{index:012d}",
                    "SensorId": sensor_id,
                    "TimeSpan": datetime(2026, 7, 30, 16, minute),
                }
                for index, minute in enumerate([1, 4, 5, 15, 59], start=1)
            ]
        )
        previous_health_df = self.spark.createDataFrame(
            [],
            schema=StructType(
                [
                    StructField("SensorId", StringType(), False),
                    StructField("TimeSpan", TimestampType(), False),
                    StructField("LastReadingAt", TimestampType(), True),
                ]
            ),
        )

        row = redings_audits(
            readings_df=readings_df,
            dimensions_df=dimensions_df,
            previous_health_df=previous_health_df,
            from_utc=datetime(2026, 7, 30, 16, 0),
            to_utc=datetime(2026, 7, 30, 16, 0),
            as_of_utc=datetime(2026, 7, 30, 17, 0),
        ).collect()[0]

        self.assertEqual(row["ReadingsCount"], 5)
        self.assertEqual(float(row["AverageGapMinutes"]), 14.5)
        self.assertEqual(float(row["MaximumGapMinutes"]), 44.0)
        self.assertEqual(float(row["MinimumGapMinutes"]), 1.0)
        self.assertEqual(float(row["ReadingsCoveragePercentage"]), 50.0)
        self.assertEqual(row["CoveredReadingBlocks"], 3)

    def test_keeps_repeated_local_hour_separate_by_utc_time_span(self) -> None:
        sensor_id = "11111111-1111-1111-1111-111111111111"
        dimensions_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": sensor_id,
                    "Active": True,
                    "TimeZone": "America/Denver",
                }
            ]
        )
        readings_df = self.spark.createDataFrame(
            [],
            schema=StructType(
                [
                    StructField("ReadId", StringType(), False),
                    StructField("SensorId", StringType(), False),
                    StructField("TimeSpan", TimestampType(), False),
                ]
            ),
        )
        previous_health_df = self.spark.createDataFrame(
            [],
            schema=StructType(
                [
                    StructField("SensorId", StringType(), False),
                    StructField("TimeSpan", TimestampType(), False),
                    StructField("LastReadingAt", TimestampType(), True),
                ]
            ),
        )

        rows = redings_audits(
            readings_df=readings_df,
            dimensions_df=dimensions_df,
            previous_health_df=previous_health_df,
            from_utc=datetime(2026, 11, 1, 7, 0),
            to_utc=datetime(2026, 11, 1, 8, 0),
            as_of_utc=datetime(2026, 11, 1, 8, 30),
        ).orderBy("TimeSpan").collect()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["TimeSpan"], datetime(2026, 11, 1, 7, 0))
        self.assertEqual(rows[1]["TimeSpan"], datetime(2026, 11, 1, 8, 0))
        self.assertEqual(
            rows[0]["LocalTimeSpan"],
            datetime(2026, 11, 1, 1, 0),
        )
        self.assertEqual(
            rows[1]["LocalTimeSpan"],
            datetime(2026, 11, 1, 1, 0),
        )


if __name__ == "__main__":
    unittest.main()
