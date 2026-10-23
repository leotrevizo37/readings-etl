import sys
import tempfile
import unittest
import warnings
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql.readwriter import DataFrameReader


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from projectA.defs.resources.spark import (
    AUDIENCIA_SCHEMA,
    SparkResource,
)


warnings.simplefilter("ignore", ResourceWarning)


class FactAudienciaParquetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("projectA-factaudiencia-parquet-tests")
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

    def _resource(self) -> SparkResource:
        resource = object.__new__(SparkResource)
        resource._spark = self.spark
        resource.adls_container_name = "projectA"
        resource.adls_account_name = "proddatalake"
        resource.adls_base_path = "reads"
        return resource

    def test_builds_paths_under_audience_for_every_day_in_window(self) -> None:
        resource = self._resource()
        captured_paths = []

        def existing_paths(paths):
            captured_paths.extend(paths)
            return []

        resource._existing_readable_paths = existing_paths

        result = resource.read_audiencia_range(
            datetime(2026, 8, 26, 23, 59),
            datetime(2026, 8, 27, 0, 1),
        )

        self.assertEqual(
            captured_paths,
            [
                "abfss://projectA@proddatalake.dfs.core.windows.net/"
                "audience/2026/8/26//",
                "abfss://projectA@proddatalake.dfs.core.windows.net/"
                "audience/2026/8/27//",
            ],
        )
        self.assertEqual(result.schema, AUDIENCIA_SCHEMA)
        self.assertEqual(result.count(), 0)

    def test_reads_existing_parquet_and_applies_inclusive_utc_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            audience_root = Path(temp_dir) / "audience"
            day_path = audience_root / "2026" / "8" / "26"
            rows = [
                (
                    "00000000-0000-0000-0000-000000000001",
                    "10000000-0000-0000-0000-000000000001",
                    "camera-1",
                    datetime(2026, 8, 26, 2, 0),
                    datetime(2026, 8, 25, 20, 0),
                    20,
                    "adult",
                    False,
                    "female",
                    "Neutral",
                    "neutral",
                    "neutral",
                ),
                (
                    "00000000-0000-0000-0000-000000000002",
                    "10000000-0000-0000-0000-000000000001",
                    "camera-1",
                    datetime(2026, 8, 26, 3, 0),
                    datetime(2026, 8, 25, 21, 0),
                    30,
                    "adult",
                    False,
                    "male",
                    "Happy",
                    "happy",
                    "positive",
                ),
            ]
            self.spark.createDataFrame(rows, AUDIENCIA_SCHEMA).write.parquet(
                str(day_path)
            )
            resource = self._resource()
            resource._build_adls_path = lambda _base_path=None: audience_root.as_uri()

            result = resource.read_audiencia_range(
                datetime(2026, 8, 26, 2, 0),
                datetime(2026, 8, 26, 3, 0),
            )
            event_ids = [
                row["EventId"] for row in result.orderBy("TimeSpan").collect()
            ]

        self.assertEqual(
            event_ids,
            [
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
            ],
        )

    def test_returns_typed_empty_dataframe_when_no_parquet_exists(self) -> None:
        resource = self._resource()
        resource._existing_readable_paths = lambda _paths: []

        result = resource.read_audiencia_range(
            datetime(2026, 8, 26),
            datetime(2026, 8, 26, 23, 59, 59),
        )

        self.assertEqual(result.schema, AUDIENCIA_SCHEMA)
        self.assertEqual(result.count(), 0)

    def test_returns_typed_empty_dataframe_when_parquet_read_fails(self) -> None:
        resource = self._resource()
        resource._existing_readable_paths = lambda paths: paths
        with patch.object(
            DataFrameReader,
            "parquet",
            side_effect=RuntimeError("invalid parquet"),
        ):
            result = resource.read_audiencia_range(
                datetime(2026, 8, 26),
                datetime(2026, 8, 26, 23, 59, 59),
            )

        self.assertEqual(result.schema, AUDIENCIA_SCHEMA)
        self.assertEqual(result.count(), 0)

if __name__ == "__main__":
    unittest.main()

