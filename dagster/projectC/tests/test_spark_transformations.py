import sys
import unittest
import warnings
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from projectC.defs.transformations.lecturas_time_hours import (  # noqa: E402
    lecturas_time_hours,
)
from projectC.defs.transformations.maquinas_nieve import maquinas_nieve  # noqa: E402
from projectC.defs.transformations.stats_calculator import (  # noqa: E402
    stats_calculator,
)
from projectC.operations_filters import (  # noqa: E402
    limit_operations_rows_to_closed_hours,
)


warnings.simplefilter("ignore", ResourceWarning)


class SparkTransformationsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("projectC-spark-transformations-tests")
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

    def _readings_df(self, rows: list[dict]) -> DataFrame:
        return self.spark.createDataFrame(rows)

    def test_lecturas_time_hours_builds_an_inclusive_hourly_scaffold(self) -> None:
        base = self.spark.createDataFrame(
            [
                {"SensorId": 10, "DeviceId": 1, "SensorTyId": 100, "sensor_type": 1, "device_type": 1, "LocationId": 11},
                {"SensorId": 10, "DeviceId": 1, "SensorTyId": 100, "sensor_type": 1, "device_type": 1, "LocationId": 11},
                {"SensorId": 11, "DeviceId": 1, "SensorTyId": 101, "sensor_type": 2, "device_type": 1, "LocationId": 11},
            ]
        )

        result = lecturas_time_hours(
            base,
            "2026-03-29 10:00:00",
            "2026-03-29 12:00:00",
        )

        rows = result.orderBy("SensorId", "LocalTimeSpan").collect()

        self.assertIsNotNone(result)
        self.assertEqual(len(rows), 6)
        self.assertEqual(
            [row["LocalTimeSpan"] for row in rows[:3]],
            [
                datetime(2026, 3, 29, 10, 0),
                datetime(2026, 3, 29, 11, 0),
                datetime(2026, 3, 29, 12, 0),
            ],
        )

    def test_lecturas_time_hours_returns_none_for_empty_base(self) -> None:
        empty_schema = StructType(
            [
                StructField("SensorId", IntegerType(), True),
                StructField("DeviceId", IntegerType(), True),
                StructField("SensorTyId", IntegerType(), True),
                StructField("device_type", IntegerType(), True),
                StructField("sensor_type", IntegerType(), True),
                StructField("LocationId", IntegerType(), True),
            ]
        )
        base = self.spark.createDataFrame([], schema=empty_schema)

        result = lecturas_time_hours(
            base,
            "2026-03-29 10:00:00",
            "2026-03-29 12:00:00",
        )

        self.assertIsNone(result)

    def test_stats_calculator_aggregates_hourly_readings_and_anomalies(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 3),
                    "Value": 21.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 18),
                    "Value": 24.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 43),
                    "Value": 10.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 55),
                    "Value": 999.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Presion",
            min_expected_value=20,
        )

        row = result.collect()[0]

        self.assertEqual(row["LocalTimeSpan"], datetime(2026, 3, 29, 10, 0))
        self.assertEqual(row["ReadingsCount"], 3)
        self.assertEqual(row["Anomalies"], 1)
        self.assertAlmostEqual(row["MeasurementValue"], 55 / 3)

    def test_stats_calculator_uses_upper_bound_for_status_and_anomalies(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 25.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Presion",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 110.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Presion",
            min_expected_value=20,
            max_expected_value=100,
        )

        row = result.collect()[0]

        self.assertEqual(row["ReadingsCount"], 2)
        self.assertEqual(row["Anomalies"], 1)
        self.assertAlmostEqual(row["MeasurementValue"], 67.5)

    def test_stats_calculator_counts_sales_above_threshold(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 7.0,
                    "device_type": "MÃ¡quina de nieve",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 20),
                    "Value": 7.1,
                    "device_type": "MÃ¡quina de nieve",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 8.0,
                    "device_type": "MÃ¡quina de nieve",
                    "device_name": "A/C Rooftop 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Corriente",
            max_expected_value=13,
            sales_threshold=7,
        )

        row = result.collect()[0]

        self.assertEqual(row["Sales"], 2)

    def test_stats_calculator_returns_none_when_sensor_type_is_absent(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 5.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                }
            ]
        )

        result = stats_calculator(base, sensor_type="Presion", min_expected_value=20)

        self.assertIsNone(result)

    def test_stats_calculator_anomalies_counts_bad_windows_within_the_hour(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 0.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 20),
                    "Value": 0.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 1.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Corriente",
            min_expected_value=0.5,
        )

        row = result.collect()[0]

        self.assertEqual(row["LocalTimeSpan"], datetime(2026, 3, 29, 10, 0))
        self.assertEqual(row["ReadingsCount"], 3)
        self.assertEqual(row["Anomalies"], 1)
        self.assertAlmostEqual(row["MeasurementValue"], 1 / 3)

    def test_stats_calculator_anomalies_treats_missing_windows_as_anomalies(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 1.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 1.0,
                    "device_type": "A/C Rooftop",
                    "device_name": "A/C Rooftop 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Corriente",
            max_expected_value=0.5,
        )

        row = result.collect()[0]

        self.assertEqual(row["ReadingsCount"], 2)
        self.assertEqual(row["Anomalies"], 1)
        self.assertAlmostEqual(row["MeasurementValue"], 1.0)

    def test_maquinas_nieve_builds_one_operation_group_per_device_hour(self) -> None:
        factlecturas_extended_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "device_type": "Máquina de nieve",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 5),
                    "Value": 0.5,
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "device_type": "Máquina de nieve",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 0.6,
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Corriente",
                    "device_type": "Máquina de nieve",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 10),
                    "Value": 21.0,
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Corriente",
                    "device_type": "Máquina de nieve",
                    "LocalTimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 22.0,
                    "device_name": "A/C Rooftop 1",
                },
            ]
        )
        dimensions_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Corriente",
                    "device_type": "Máquina de nieve",
                    "device_name": "A/C Rooftop 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Corriente",
                    "device_type": "Máquina de nieve",
                    "device_name": "A/C Rooftop 1",
                },
            ]
        )

        result = maquinas_nieve(
            factlecturas_extended_df,
            dimensions_df,
            "2026-03-29 10:00:00",
            "2026-03-29 10:00:00",
        )

        rows = result.orderBy("SensorId").collect()

        self.assertEqual(len(rows), 2)
        self.assertEqual({row["SensorId"] for row in rows}, {10, 11})
        self.assertEqual(
            {row["LocalTimeSpan"] for row in rows},
            {datetime(2026, 3, 29, 10, 0)},
        )
        self.assertEqual(len({row["OperationId"] for row in rows}), 2)
        self.assertEqual({row["LocationId"] for row in rows}, {100})
        self.assertEqual(
            {row["sensor_type"] for row in rows},
            {"Corriente"},
        )
        sales_by_sensor = {row["SensorId"]: row["Sales"] for row in rows}
        self.assertEqual(sales_by_sensor[10], 0)
        self.assertEqual(sales_by_sensor[11], 2)


    def test_limit_operations_rows_to_closed_hours_removes_future_rows(self) -> None:
        schema = StructType(
            [
                StructField("DeviceId", IntegerType(), True),
                StructField("SensorId", IntegerType(), True),
                StructField("LocalTimeSpan", TimestampType(), True),
                StructField("sensor_type", StringType(), True),
                StructField("MeasurementValue", DoubleType(), True),
            ]
        )
        base = self.spark.createDataFrame(
            [
                (1, 10, datetime(2026, 3, 29, 10, 0), "Corriente", 1.0),
                (1, 10, datetime(2026, 3, 29, 11, 0), "Corriente", 2.0),
            ],
            schema=schema,
        )

        result = limit_operations_rows_to_closed_hours(
            base,
            datetime(2026, 3, 29, 10, 0),
        )

        rows = result.orderBy("LocalTimeSpan").collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["LocalTimeSpan"], datetime(2026, 3, 29, 10, 0))


if __name__ == "__main__":
    unittest.main()
