import ast
import sys
import unittest
import warnings
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from projectB.defs.transformations.cuarto_congelacion import cuarto_congelacion  # noqa: E402
from projectB.defs.transformations.cuarto_residuos import cuarto_residuos  # noqa: E402
from projectB.defs.transformations.cuartos_frios import cuartos_frios  # noqa: E402


warnings.simplefilter("ignore", ResourceWarning)


class SparkTransformationsTests(unittest.TestCase):
    DEVICE_TRANSFORMATION_CASES = [
        (
            "cuartos_frios",
            cuartos_frios,
            "Cuarto Frio",
            "Cuarto frio 3",
        ),
        (
            "cuarto_congelacion",
            cuarto_congelacion,
            "Cuarto Congelacion",
            "Cuarto de Congelaci\u00f3n",
        ),
        (
            "cuarto_residuos",
            cuarto_residuos,
            "Cuarto Residuos",
            "Cuarto de Residuos",
        ),
    ]

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("projectB-spark-transformations-tests")
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

    def _projectB_room_inputs(
        self,
        method_name: str,
        device_type: str,
        device_name: str,
    ) -> tuple[DataFrame, DataFrame, set[str]]:
        reading_rows = []
        dimension_rows = []
        device_id = method_name + "-device"
        location_id = method_name + "-location"
        sensors = [
            ("temperature", "Temperatura", "Air Temperature", [5.0, 6.0]),
            ("humidity", "Humedad", "Relative Humidity", [50.0, 51.0]),
            ("fase-1", "Medicion de corriente", "Current 1", [4.0, 5.0]),
            ("fase-2", "Medicion de corriente", "Current 2", [6.0, 7.0]),
            ("fase-3", "Medicion de corriente", "Current 3", [8.0, 9.0]),
            ("door", "Estado de Puerta", "Door Status", [1.0, 0.0]),
        ]

        for sensor_idx, (suffix, sensor_type, sensor_name, values) in enumerate(
            sensors,
            start=1,
        ):
            sensor_id = method_name + "-" + suffix
            sensor_ty_id = method_name + "-sensor-ty-" + str(sensor_idx)
            dimension_rows.append(
                {
                    "DeviceId": device_id,
                    "SensorId": sensor_id,
                    "LocationId": location_id,
                    "SensorTyId": sensor_ty_id,
                    "sensor_type": sensor_type,
                    "sensor_name": sensor_name,
                    "device_type": device_type,
                    "device_name": device_name,
                    "TimeZone": "America/Chihuahua",
                }
            )
            for read_idx, value in enumerate(values):
                reading_rows.append(
                    {
                        "ReadId": method_name + "-read-" + str(sensor_idx) + "-" + str(read_idx),
                        "DeviceId": device_id,
                        "SensorId": sensor_id,
                        "LocationId": location_id,
                        "SensorTyId": sensor_ty_id,
                        "sensor_type": sensor_type,
                        "sensor_name": sensor_name,
                        "device_type": device_type,
                        "device_name": device_name,
                        "TimeSpan": datetime(2026, 3, 29, 10, 5 + read_idx * 20),
                        "Value": float(value),
                    }
                )

        dimension_rows.append(
            {
                "DeviceId": device_id,
                "SensorId": method_name + "-unsupported",
                "LocationId": location_id,
                "SensorTyId": method_name + "-unsupported-ty",
                "sensor_type": "Nivel",
                "sensor_name": "Nivel",
                "device_type": device_type,
                "device_name": device_name,
                "TimeZone": "America/Chihuahua",
            }
        )
        reading_rows.append(
            {
                "ReadId": method_name + "-unsupported-read",
                "DeviceId": device_id,
                "SensorId": method_name + "-unsupported",
                "LocationId": location_id,
                "SensorTyId": method_name + "-unsupported-ty",
                "sensor_type": "Nivel",
                "sensor_name": "Nivel",
                "device_type": device_type,
                "device_name": device_name,
                "TimeSpan": datetime(2026, 3, 29, 10, 15),
                "Value": 999.0,
            }
        )

        return (
            self._readings_df(reading_rows),
            self._readings_df(dimension_rows),
            {method_name + "-" + sensor[0] for sensor in sensors if sensor[1] != "Estado de Puerta"},
        )

    def test_projectB_room_transformations_materialize_configured_sensors(self) -> None:
        for method_name, transform_fn, device_type, device_name in self.DEVICE_TRANSFORMATION_CASES:
            with self.subTest(method=method_name):
                readings_df, dimensions_df, expected_sensor_ids = self._projectB_room_inputs(
                    method_name,
                    device_type,
                    device_name,
                )

                result = transform_fn(
                    readings_df,
                    dimensions_df,
                    "2026-03-29 10:00:00",
                    "2026-03-29 10:00:00",
                )

                self.assertIsNotNone(result)
                rows = result.orderBy("SensorId").collect()

                self.assertEqual(len(rows), 5)
                self.assertEqual({row["SensorId"] for row in rows}, expected_sensor_ids)
                self.assertEqual({row["device_type"] for row in rows}, {device_type})
                self.assertEqual(
                    {row["TimeSpan"] for row in rows},
                    {datetime(2026, 3, 29, 10, 0)},
                )
                self.assertEqual(
                    {row["LocalTimeSpan"] for row in rows},
                    {datetime(2026, 3, 29, 4, 0)},
                )
                self.assertEqual(len({row["OperationId"] for row in rows}), 5)
                self.assertEqual(
                    sorted(row["sensor_type"] for row in rows),
                    [
                        "Humedad",
                        "Medicion de corriente",
                        "Medicion de corriente",
                        "Medicion de corriente",
                        "Temperatura",
                    ],
                )
                self.assertTrue(all(row["ReadingsCount"] == 2 for row in rows))

    def test_cuartos_frios_scaffolds_configured_sensors_without_readings(self) -> None:
        readings_schema = StructType(
            [
                StructField("ReadId", StringType(), True),
                StructField("DeviceId", StringType(), True),
                StructField("SensorId", StringType(), True),
                StructField("LocationId", StringType(), True),
                StructField("SensorTyId", StringType(), True),
                StructField("sensor_type", StringType(), True),
                StructField("sensor_name", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("device_name", StringType(), True),
                StructField("TimeSpan", TimestampType(), True),
                StructField("Value", DoubleType(), True),
            ]
        )
        factlecturas_extended_df = self.spark.createDataFrame([], readings_schema)
        _readings_df, dimensions_df, expected_sensor_ids = self._projectB_room_inputs(
            "cuartos_frios",
            "Cuarto Frio",
            "Cuarto Frio 2",
        )

        result = cuartos_frios(
            factlecturas_extended_df,
            dimensions_df,
            "2026-03-29 10:00:00",
            "2026-03-29 10:00:00",
        )

        self.assertIsNotNone(result)
        rows = result.orderBy("SensorId").collect()

        self.assertEqual(len(rows), 5)
        self.assertEqual({row["SensorId"] for row in rows}, expected_sensor_ids)
        self.assertEqual({row["ReadingsCount"] for row in rows}, {0})
        self.assertEqual({row["Anomalies"] for row in rows}, {0})
        self.assertEqual({row["HasAnomalies"] for row in rows}, {False})
        self.assertTrue(all(row["MeasurementValue"] is None for row in rows))

    def test_operations_device_transformations_are_wired_and_covered(self) -> None:
        operations_path = (
            PROJECT_SRC
            / "projectB"
            / "defs"
            / "assets"
            / "operations_assets.py"
        )
        transformation_ops_path = (
            PROJECT_SRC
            / "projectB"
            / "defs"
            / "resources"
            / "transformation_ops.py"
        )

        operations_tree = ast.parse(operations_path.read_text())
        methods = None
        for node in ast.walk(operations_tree):
            if isinstance(node, ast.FunctionDef) and node.name == "build_devices_df_op":
                for child in ast.walk(node):
                    if not isinstance(child, ast.Assign):
                        continue
                    if not any(
                        isinstance(target, ast.Name) and target.id == "methods"
                        for target in child.targets
                    ):
                        continue
                    methods = [
                        item.value
                        for item in child.value.elts
                        if isinstance(item, ast.Constant)
                    ]
                    break

        self.assertEqual(methods, ["cuartos_frios", "cuarto_congelacion", "cuarto_residuos"])

        transformation_ops_tree = ast.parse(transformation_ops_path.read_text())
        wrapper_methods = set()
        for node in ast.walk(transformation_ops_tree):
            if isinstance(node, ast.ClassDef) and node.name == "TransformationOps":
                wrapper_methods = {
                    child.name
                    for child in node.body
                    if isinstance(child, ast.FunctionDef)
                }
                break

        self.assertTrue(set(methods).issubset(wrapper_methods))

    def test_device_transformations_use_rlike_patterns(self) -> None:
        for module_name in ("cuartos_frios", "cuarto_congelacion", "cuarto_residuos"):
            with self.subTest(module=module_name):
                source = (
                    PROJECT_SRC
                    / "projectB"
                    / "defs"
                    / "transformations"
                    / (module_name + ".py")
                ).read_text(encoding="utf-8")

                self.assertIn(".rlike(", source)
                self.assertNotIn(".isin(\"Cuarto Frio 2\"", source)

    def test_factreadingsmeasurement_processes_utc_timespan(self) -> None:
        operations_path = PROJECT_SRC / "projectB" / "defs" / "assets" / "operations_assets.py"
        datalake_path = PROJECT_SRC / "projectB" / "defs" / "transformations" / "datalake_readings_extended.py"
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
        operations_path = PROJECT_SRC / "projectB" / "defs" / "assets" / "operations_assets.py"
        source = operations_path.read_text(encoding="utf-8")
        write_section = source.split("def write_factreadingsmeasurement_op", 1)[1].split("@op(", 1)[0]
        graph_section = source.split("def operations_assets", 1)[1]

        self.assertIn("write_fact_operations_stage(", write_section)
        self.assertIn("merge_fact_readings_measurement(", write_section)
        self.assertNotIn("delete_range(", write_section)
        self.assertNotIn("window_cleanup(fetch_from_ts)", graph_section)


if __name__ == "__main__":
    unittest.main()
