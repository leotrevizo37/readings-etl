import json
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

from projectA.defs.transformations.lecturas_time_hours import (  # noqa: E402
    lecturas_time_hours,
)
from projectA.defs.transformations import cisterna  # noqa: E402
from projectA.defs.transformations import (  # noqa: E402
    cuartos_refrigeracion,
)
from projectA.defs.transformations.stats_calculator import (  # noqa: E402
    stats_calculator,
)
from projectA.operations_filters import (  # noqa: E402
    limit_operations_rows_to_closed_hours,
)


warnings.simplefilter("ignore", ResourceWarning)


class SparkTransformationsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("projectA-spark-transformations-tests")
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
                {"SensorId": 10, "DeviceId": 1, "SensorTyId": 100, "sensor_name": "Sensor 10", "sensor_type": 1, "device_type": 1, "LocationId": 11, "TimeZone": "America/Chihuahua"},
                {"SensorId": 10, "DeviceId": 1, "SensorTyId": 100, "sensor_name": "Sensor 10", "sensor_type": 1, "device_type": 1, "LocationId": 11, "TimeZone": "America/Chihuahua"},
                {"SensorId": 11, "DeviceId": 1, "SensorTyId": 101, "sensor_name": "Sensor 11", "sensor_type": 2, "device_type": 1, "LocationId": 11, "TimeZone": "America/Denver"},
            ]
        )

        result = lecturas_time_hours(
            base,
            "2026-03-29 10:00:00",
            "2026-03-29 12:00:00",
        )

        rows = result.orderBy("SensorId", "TimeSpan").collect()

        self.assertIsNotNone(result)
        self.assertEqual(len(rows), 6)
        self.assertEqual(
            [row["TimeSpan"] for row in rows[:3]],
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
                StructField("sensor_name", StringType(), True),
                StructField("device_type", IntegerType(), True),
                StructField("sensor_type", IntegerType(), True),
                StructField("LocationId", IntegerType(), True),
                StructField("TimeZone", StringType(), True),
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
                    "TimeSpan": datetime(2026, 3, 29, 10, 3),
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
                    "TimeSpan": datetime(2026, 3, 29, 10, 18),
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
                    "TimeSpan": datetime(2026, 3, 29, 10, 43),
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
                    "TimeSpan": datetime(2026, 3, 29, 10, 55),
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

        self.assertEqual(row["TimeSpan"], datetime(2026, 3, 29, 10, 0))
        self.assertEqual(row["ReadingsCount"], 3)
        self.assertEqual(row["Anomalies"], 1)
        self.assertTrue(row["HasAnomalies"])
        self.assertEqual(row["Min"], 10.0)
        self.assertEqual(row["Max"], 24.0)
        self.assertEqual(
            [reading["value"] for reading in json.loads(row["CorrectReadingsJson"])],
            [21.0, 24.0],
        )
        self.assertEqual(
            [reading["value"] for reading in json.loads(row["AnomalousReadingsJson"])],
            [10.0],
        )
        self.assertEqual(
            set(json.loads(row["AnomalousReadingsJson"])[0]),
            {"timestamp", "value"},
        )
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
                    "TimeSpan": datetime(2026, 3, 29, 10, 0),
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
                    "TimeSpan": datetime(2026, 3, 29, 10, 25),
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

    def test_stats_calculator_returns_none_when_sensor_type_is_absent(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura",
                    "TimeSpan": datetime(2026, 3, 29, 10, 0),
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
                    "TimeSpan": datetime(2026, 3, 29, 10, 0),
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
                    "TimeSpan": datetime(2026, 3, 29, 10, 20),
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
                    "TimeSpan": datetime(2026, 3, 29, 10, 40),
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

        self.assertEqual(row["TimeSpan"], datetime(2026, 3, 29, 10, 0))
        self.assertEqual(row["ReadingsCount"], 3)
        self.assertEqual(row["Anomalies"], 2)
        self.assertTrue(row["HasAnomalies"])
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
                    "TimeSpan": datetime(2026, 3, 29, 10, 0),
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
                    "TimeSpan": datetime(2026, 3, 29, 10, 40),
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
        self.assertEqual(row["Anomalies"], 2)
        self.assertAlmostEqual(row["MeasurementValue"], 1.0)

    def test_cisterna_uses_projectA_sensor_name_from_dimensions(self) -> None:
        factlecturas_extended_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "nivel de cisterna porcentaje",
                    "device_type": "Nivel de cisterna ",
                    "TimeSpan": datetime(2026, 3, 29, 10, 5),
                    "Value": 55.0,
                    "device_name": "Cisterna 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "nivel de cisterna porcentaje",
                    "device_type": "Nivel de cisterna ",
                    "TimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 65.0,
                    "device_name": "Cisterna 1",
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
                    "sensor_type": "nivel de cisterna porcentaje",
                    "device_type": "Nivel de cisterna ",
                    "device_name": "Cisterna 1",
                    "sensor_name": "Nivel de cisterna",
                    "TimeZone": "America/Chihuahua",
                },
            ]
        )

        result = cisterna(
            factlecturas_extended_df,
            dimensions_df,
            "2026-03-29 10:00:00",
            "2026-03-29 10:00:00",
        )

        rows = result.collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ReadingsCount"], 2)
        self.assertEqual(rows[0]["sensor_name"], "Nivel de cisterna")
        self.assertAlmostEqual(rows[0]["MeasurementValue"], 60.0)
        self.assertEqual(rows[0]["LocalTimeSpan"], datetime(2026, 3, 29, 4, 0))

    def test_cuartos_refrigeracion_uses_projectA_temperature_f_sensor(self) -> None:
        factlecturas_extended_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura F",
                    "device_type": "C\u00e1mara fr\u00eda",
                    "TimeSpan": datetime(2026, 3, 29, 10, 5),
                    "Value": 35.0,
                    "device_name": "C\u00e1mara fr\u00eda 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Humedad",
                    "device_type": "C\u00e1mara fr\u00eda",
                    "TimeSpan": datetime(2026, 3, 29, 10, 10),
                    "Value": 50.0,
                    "device_name": "C\u00e1mara fr\u00eda 1",
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
                    "sensor_type": "Temperatura F",
                    "device_type": "C\u00e1mara fr\u00eda",
                    "device_name": "C\u00e1mara fr\u00eda 1",
                    "sensor_name": "Temperatura",
                    "TimeZone": "America/Tijuana",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Humedad",
                    "device_type": "C\u00e1mara fr\u00eda",
                    "device_name": "C\u00e1mara fr\u00eda 1",
                    "sensor_name": "Humedad",
                    "TimeZone": "America/Tijuana",
                },
            ]
        )

        result = cuartos_refrigeracion(
            factlecturas_extended_df,
            dimensions_df,
            "2026-03-29 10:00:00",
            "2026-03-29 10:00:00",
        )

        rows_by_sensor = {
            row["sensor_type"]: row
            for row in result.collect()
        }

        self.assertEqual(set(rows_by_sensor), {"Temperatura F", "Humedad"})
        self.assertEqual(rows_by_sensor["Temperatura F"]["ReadingsCount"], 1)
        self.assertEqual(rows_by_sensor["Humedad"]["ReadingsCount"], 1)
        self.assertAlmostEqual(
            rows_by_sensor["Temperatura F"]["MeasurementValue"],
            35.0,
        )
        self.assertAlmostEqual(rows_by_sensor["Humedad"]["MeasurementValue"], 50.0)

    def test_cuartos_refrigeracion_preserves_repeated_local_hour_in_all_paths(self) -> None:
        dimensions_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura F",
                    "device_type": "C\u00e1mara fr\u00eda",
                    "sensor_name": "Temperatura",
                    "TimeZone": "America/Denver",
                }
            ]
        )
        scenarios = [
            (
                "with_readings",
                [
                    {
                        "DeviceId": 1,
                        "SensorId": 10,
                        "LocationId": 100,
                        "SensorTyId": 200,
                        "sensor_type": "Temperatura F",
                        "device_type": "C\u00e1mara fr\u00eda",
                        "TimeSpan": datetime(2026, 11, 1, 7, 5),
                        "Value": 35.0,
                        "device_name": "C\u00e1mara fr\u00eda 1",
                    },
                    {
                        "DeviceId": 1,
                        "SensorId": 10,
                        "LocationId": 100,
                        "SensorTyId": 200,
                        "sensor_type": "Temperatura F",
                        "device_type": "C\u00e1mara fr\u00eda",
                        "TimeSpan": datetime(2026, 11, 1, 8, 5),
                        "Value": 36.0,
                        "device_name": "C\u00e1mara fr\u00eda 1",
                    },
                ],
                [1, 1],
            ),
            (
                "without_readings",
                [{"device_type": "Oficina"}],
                [0, 0],
            ),
        ]
        operation_ids_by_scenario = {}

        for scenario, fact_rows, expected_readings_count in scenarios:
            with self.subTest(scenario=scenario):
                result = cuartos_refrigeracion(
                    self._readings_df(fact_rows),
                    dimensions_df,
                    "2026-11-01 07:00:00",
                    "2026-11-01 08:00:00",
                )

                rows = result.orderBy("TimeSpan").collect()

                self.assertEqual(
                    [row["TimeSpan"] for row in rows],
                    [
                        datetime(2026, 11, 1, 7, 0),
                        datetime(2026, 11, 1, 8, 0),
                    ],
                )
                self.assertEqual(
                    [row["LocalTimeSpan"] for row in rows],
                    [
                        datetime(2026, 11, 1, 1, 0),
                        datetime(2026, 11, 1, 1, 0),
                    ],
                )
                self.assertEqual(
                    [row["ReadingsCount"] for row in rows],
                    expected_readings_count,
                )
                self.assertEqual(len({row["OperationId"] for row in rows}), 2)
                self.assertEqual(
                    result.dropDuplicates(["OperationId"]).count(),
                    2,
                )
                operation_ids_by_scenario[scenario] = {
                    row["TimeSpan"]: row["OperationId"]
                    for row in rows
                }

        self.assertEqual(
            operation_ids_by_scenario["with_readings"],
            operation_ids_by_scenario["without_readings"],
        )

    def test_cuartos_refrigeracion_preserves_utc_hours_across_skipped_local_hour(self) -> None:
        factlecturas_extended_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura F",
                    "device_type": "C\u00e1mara fr\u00eda",
                    "TimeSpan": datetime(2026, 3, 8, 8, 5),
                    "Value": 35.0,
                    "device_name": "C\u00e1mara fr\u00eda 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura F",
                    "device_type": "C\u00e1mara fr\u00eda",
                    "TimeSpan": datetime(2026, 3, 8, 9, 5),
                    "Value": 36.0,
                    "device_name": "C\u00e1mara fr\u00eda 1",
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
                    "sensor_type": "Temperatura F",
                    "device_type": "C\u00e1mara fr\u00eda",
                    "sensor_name": "Temperatura",
                    "TimeZone": "America/Denver",
                }
            ]
        )

        result = cuartos_refrigeracion(
            factlecturas_extended_df,
            dimensions_df,
            "2026-03-08 08:00:00",
            "2026-03-08 09:00:00",
        )

        rows = result.orderBy("TimeSpan").collect()

        self.assertEqual(
            [row["TimeSpan"] for row in rows],
            [
                datetime(2026, 3, 8, 8, 0),
                datetime(2026, 3, 8, 9, 0),
            ],
        )
        self.assertEqual(
            [row["LocalTimeSpan"] for row in rows],
            [
                datetime(2026, 3, 8, 1, 0),
                datetime(2026, 3, 8, 3, 0),
            ],
        )
        self.assertEqual([row["ReadingsCount"] for row in rows], [1, 1])
        self.assertEqual(len({row["OperationId"] for row in rows}), 2)
        self.assertEqual(result.dropDuplicates(["OperationId"]).count(), 2)

    def test_operations_excludes_presence_and_preserves_continuous_zero_one_readings(self) -> None:
        from types import SimpleNamespace
        from unittest.mock import Mock
        from projectA.defs.assets.operations_assets import build_devices_df_op
        from projectA.defs.resources.transformation_ops import TransformationOps

        dimensions = [
            {
                "DeviceId": 1,
                "SensorId": sensor_id,
                "LocationId": 100,
                "SensorTyId": sensor_id + 100,
                "sensor_type": sensor_type,
                "sensor_name": sensor_type,
                "device_type": "Cuarto refrigeracion",
                "device_name": "Refrigeracion 1",
                "TimeZone": "America/Denver",
            }
            for sensor_id, sensor_type in ((10, "Sensor PRESENCIA 1"), (11, "Temperatura F"))
        ]
        readings = [
            {**dimension, "TimeSpan": datetime(2026, 3, 29, 10, minute), "Value": value}
            for dimension in dimensions
            for minute, value in ((5, 0.0), (25, 1.0))
        ]
        context = SimpleNamespace(
            resources=SimpleNamespace(transformation_ops=TransformationOps()),
            log=Mock(),
        )
        rows = build_devices_df_op.compute_fn.decorated_fn(
            context,
            {"fetch_from": datetime(2026, 3, 29, 10), "max_utc_hour": datetime(2026, 3, 29, 11)},
            self.spark.createDataFrame(dimensions),
            self.spark.createDataFrame(readings),
        ).collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["SensorId"], 11)
        self.assertEqual(rows[0]["ReadingsCount"], 2)
        self.assertEqual(rows[0]["MeasurementValue"], 0.5)
        self.assertFalse(hasattr(TransformationOps(), "presencia"))

    def test_limit_operations_rows_to_closed_hours_removes_future_rows(self) -> None:
        schema = StructType(
            [
                StructField("DeviceId", IntegerType(), True),
                StructField("SensorId", IntegerType(), True),
                StructField("TimeSpan", TimestampType(), True),
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

        rows = result.orderBy("TimeSpan").collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["TimeSpan"], datetime(2026, 3, 29, 10, 0))

    def test_factreadingsmeasurement_processes_utc_timespan(self) -> None:
        operations_path = PROJECT_SRC / "projectA" / "defs" / "assets" / "operations_assets.py"
        datalake_path = PROJECT_SRC / "projectA" / "defs" / "transformations" / "datalake_readings_extended.py"
        operations_source = operations_path.read_text(encoding="utf-8")
        datalake_source = datalake_path.read_text(encoding="utf-8")

        self.assertIn("read_lecturas_range(", datalake_source)
        self.assertIn("read_lecturas_local_range", datalake_source)
        self.assertIn("use_utc_range=True", operations_source)
        self.assertIn('col("sensor_name").alias("SensorName")', operations_source)
        self.assertIn('spark_max("TimeSpan")', operations_source)
        self.assertIn('get_max_time_span(devices_df, "TimeSpan")', operations_source)
        self.assertNotIn('spark_max("LocalTimeSpan")', operations_source)

    def test_factreadingsmeasurement_uses_stage_merge_without_window_cleanup(self) -> None:
        operations_path = PROJECT_SRC / "projectA" / "defs" / "assets" / "operations_assets.py"
        source = operations_path.read_text(encoding="utf-8")
        write_section = source.split("def write_factreadingsmeasurement_op", 1)[1].split("@op(", 1)[0]
        graph_section = source.split("def operations_assets", 1)[1]

        self.assertIn("write_fact_operations_stage(", write_section)
        self.assertIn("merge_fact_readings_measurement(", write_section)
        self.assertNotIn("delete_range(", write_section)
        self.assertNotIn("window_cleanup(fetch_from_ts)", graph_section)


if __name__ == "__main__":
    unittest.main()
