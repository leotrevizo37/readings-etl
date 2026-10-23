import sys
import unittest
import warnings
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from projectA.defs.resources.spark import (
    AUDIENCIA_SCHEMA,
    SparkResource,
)


REQUIRED_COLUMNS = (
    "EventId",
    "SensorId",
    "TimeSpan",
    "LocalTimeSpan",
    "Age",
    "IsKid",
)
OPTIONAL_COLUMNS = (
    "PhysicalIdentifier",
    "AgeGroup",
    "Gender",
    "EmotionRaw",
    "Emotion",
    "Sentiment",
)
VALID_VALUES = {
    "EventId": "00000000-0000-0000-0000-000000000001",
    "SensorId": "10000000-0000-0000-0000-000000000001",
    "PhysicalIdentifier": "camera-1",
    "TimeSpan": datetime(2026, 8, 26, 2, 0),
    "LocalTimeSpan": datetime(2026, 8, 25, 20, 0),
    "Age": 30,
    "AgeGroup": "adult",
    "IsKid": False,
    "Gender": "female",
    "EmotionRaw": "Neutral",
    "Emotion": "neutral",
    "Sentiment": "neutral",
}


warnings.simplefilter("ignore", ResourceWarning)


class FactAudienciaNullabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("projectA-factaudiencia-nullability-tests")
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

    def _dataframe(self, values, columns=None):
        selected_columns = tuple(columns or AUDIENCIA_SCHEMA.fieldNames())
        schema = StructType(
            [
                StructField(
                    field.name,
                    field.dataType,
                    True,
                )
                for field in AUDIENCIA_SCHEMA
                if field.name in selected_columns
            ]
        )
        row = tuple(values[field.name] for field in schema)
        return self.spark.createDataFrame([row], schema)

    def _resource_with_sql_contract(self):
        resource = object.__new__(SparkResource)

        def write(df, table, chunk_column):
            row = df.first()
            for column in REQUIRED_COLUMNS:
                if column not in df.columns:
                    raise RuntimeError("missing required column " + column)
                value = row[column]
                if value is None:
                    raise RuntimeError("null required column " + column)
                if column in ("EventId", "SensorId") and not value.strip():
                    raise RuntimeError("empty required column " + column)
            self.assertEqual(table, "dwh.factAudiencia")
            self.assertEqual(chunk_column, "EventId")
            return df.count()

        resource._write_jdbc_in_chunks = write
        return resource

    def test_nullable_columns_accept_null_and_empty_values(self) -> None:
        values = dict(VALID_VALUES)
        for column in OPTIONAL_COLUMNS:
            values[column] = None
        values["Gender"] = ""
        resource = self._resource_with_sql_contract()

        row_count = resource.write_fact_audiencia_to_sql(
            self._dataframe(values)
        )

        self.assertEqual(row_count, 1)

    def test_nullable_columns_can_be_absent_from_dataframe(self) -> None:
        resource = self._resource_with_sql_contract()

        row_count = resource.write_fact_audiencia_to_sql(
            self._dataframe(VALID_VALUES, REQUIRED_COLUMNS)
        )

        self.assertEqual(row_count, 1)

    def test_null_required_columns_propagate_jdbc_failure(self) -> None:
        resource = self._resource_with_sql_contract()

        for column in REQUIRED_COLUMNS:
            with self.subTest(column=column):
                values = dict(VALID_VALUES)
                values[column] = None
                with self.assertRaises(RuntimeError):
                    resource.write_fact_audiencia_to_sql(
                        self._dataframe(values)
                    )

    def test_empty_required_identifiers_propagate_jdbc_failure(self) -> None:
        resource = self._resource_with_sql_contract()

        for column in ("EventId", "SensorId"):
            with self.subTest(column=column):
                values = dict(VALID_VALUES)
                values[column] = " "
                with self.assertRaises(RuntimeError):
                    resource.write_fact_audiencia_to_sql(
                        self._dataframe(values)
                    )

    def test_missing_required_columns_propagate_failure(self) -> None:
        resource = self._resource_with_sql_contract()

        for column in REQUIRED_COLUMNS:
            with self.subTest(column=column):
                remaining_columns = [
                    name
                    for name in AUDIENCIA_SCHEMA.fieldNames()
                    if name != column
                ]
                with self.assertRaises(Exception):
                    resource.write_fact_audiencia_to_sql(
                        self._dataframe(VALID_VALUES, remaining_columns)
                    )


if __name__ == "__main__":
    unittest.main()
