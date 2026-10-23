import sys
import json
import unittest
import warnings
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

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

from projectD.defs.transformations.lecturas_time_hours import (
    lecturas_time_hours,
)
from projectD.defs.transformations import gabinete
from projectD.defs.transformations.stats_calculator import (
    stats_calculator,
)
from projectD.operations_filters import (
    limit_operations_rows_to_closed_hours,
)


warnings.simplefilter("ignore", ResourceWarning)


class SparkTransformationsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("gabinete-spark-transformations-tests")
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

    def test_raw_dimensions_preserves_sensor_model_hub_and_chirpstack_identity(self) -> None:
        from projectD.defs.transformations.raw_dimensions import raw_dimensions

        tables = [
            self.spark.createDataFrame(
                [(10, 100, 1, 2, 3, "0000000000000001", False, True, "Sensor 10"),
                 (11, 100, 1, 2, 3, None, True, True, "Sensor 11")],
                ["SensorId", "SensorTyId", "DeviceId", "ModelId", "SensorHubId", "PhysicalIdentifier", "PerEvent", "Active", "Name"],
            ),
            self.spark.createDataFrame([(2, "Sensor model"), (4, "Hub model")], ["ModelId", "Description"]),
            self.spark.createDataFrame([(3, 4, "Hub")], ["SensorHubId", "ModelId", "Name"]),
            self.spark.createDataFrame([(100, "Temperatura")], ["SensorTyId", "Name"]),
            self.spark.createDataFrame([(1, 5, 6, "Device")], ["DeviceId", "DeviceTyId", "SubLocationId", "Name"]),
            self.spark.createDataFrame([(5, "Gabinete")], ["DeviceTyId", "Name"]),
            self.spark.createDataFrame([(6, 7, 8, "Sublocation")], ["SubLocationId", "SubLocationTyId", "LocationId", "Name"]),
            self.spark.createDataFrame([(7, "Room")], ["SubLocationTyId", "Name"]),
            self.spark.createDataFrame([(8, 9, "Location", "America/Ciudad_Juarez")], ["LocationId", "CityId", "Name", "TimeZone"]),
            self.spark.createDataFrame([(9, "City")], ["CityId", "Name"]),
        ]
        reader = Mock()
        reader.format.return_value = reader
        reader.option.return_value = reader
        reader.options.return_value = reader
        reader.load.side_effect = tables
        spark_res = SimpleNamespace(get_spark=lambda: SimpleNamespace(read=reader))
        dim_sqlserver = SimpleNamespace(get_basic_options=lambda: {})

        rows = raw_dimensions(spark_res, dim_sqlserver).orderBy("SensorId").collect()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["PhysicalIdentifier"], "0000000000000001")
        self.assertIsNone(rows[1]["PhysicalIdentifier"])
        self.assertFalse(rows[0]["PerEvent"])
        self.assertTrue(rows[1]["PerEvent"])
        self.assertEqual(rows[0]["model_name"], "Sensor model")
        self.assertEqual(rows[0]["hub_model_name"], "Hub model")
        self.assertEqual(rows[0]["hub_name"], "Hub")
        self.assertEqual(rows[0]["device_type"], "Gabinete")
        self.assertEqual(rows[0]["TimeZone"], "America/Ciudad_Juarez")
        self.assertEqual(reader.load.call_count, 10)

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
                    "sensor_type": "Temperatura",
                    "TimeSpan": datetime(2026, 3, 29, 10, 3),
                    "Value": 21.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura",
                    "TimeSpan": datetime(2026, 3, 29, 10, 18),
                    "Value": 24.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura",
                    "TimeSpan": datetime(2026, 3, 29, 10, 43),
                    "Value": 10.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "TimeSpan": datetime(2026, 3, 29, 10, 55),
                    "Value": 999.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Temperatura",
            min_expected_value=20,
        )

        row = result.collect()[0]

        self.assertEqual(row["TimeSpan"], datetime(2026, 3, 29, 10, 0))
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
                    "sensor_type": "Temperatura",
                    "TimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 25.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Temperatura",
                    "TimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 110.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Temperatura",
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
                    "device_type": "Maquina nieve",
                    "device_name": "Gabinete 1",
                }
            ]
        )

        result = stats_calculator(base, sensor_type="Humedad", min_expected_value=20)

        self.assertIsNone(result)

    def test_stats_calculator_anomalies_counts_bad_windows_within_the_hour(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "TimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 0.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "TimeSpan": datetime(2026, 3, 29, 10, 20),
                    "Value": 0.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "TimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 1.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Humedad",
            min_expected_value=0.5,
        )

        row = result.collect()[0]

        self.assertEqual(row["TimeSpan"], datetime(2026, 3, 29, 10, 0))
        self.assertEqual(row["ReadingsCount"], 3)
        self.assertEqual(row["Anomalies"], 2)
        self.assertAlmostEqual(row["MeasurementValue"], 1 / 3)

    def test_stats_calculator_anomalies_treats_missing_windows_as_anomalies(self) -> None:
        base = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "TimeSpan": datetime(2026, 3, 29, 10, 0),
                    "Value": 1.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "TimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 1.0,
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                },
            ]
        )

        result = stats_calculator(
            base,
            sensor_type="Humedad",
            max_expected_value=0.5,
        )

        row = result.collect()[0]

        self.assertEqual(row["ReadingsCount"], 2)
        self.assertEqual(row["Anomalies"], 2)
        self.assertAlmostEqual(row["MeasurementValue"], 1.0)

    def test_gabinete_builds_one_operation_group_per_device_hour(self) -> None:
        factlecturas_extended_df = self._readings_df(
            [
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "device_type": "Gabinete",
                    "TimeSpan": datetime(2026, 3, 29, 10, 5),
                    "Value": 0.5,
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "LocationId": 100,
                    "SensorTyId": 200,
                    "sensor_type": "Humedad",
                    "device_type": "Gabinete",
                    "TimeSpan": datetime(2026, 3, 29, 10, 25),
                    "Value": 0.6,
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Humedad",
                    "device_type": "Gabinete",
                    "TimeSpan": datetime(2026, 3, 29, 10, 10),
                    "Value": 21.0,
                    "device_name": "Gabinete 1",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Humedad",
                    "device_type": "Gabinete",
                    "TimeSpan": datetime(2026, 3, 29, 10, 40),
                    "Value": 22.0,
                    "device_name": "Gabinete 1",
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
                    "sensor_type": "Humedad",
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                    "sensor_name": "Humedad 1",
                    "TimeZone": "America/Chihuahua",
                },
                {
                    "DeviceId": 1,
                    "SensorId": 11,
                    "LocationId": 100,
                    "SensorTyId": 201,
                    "sensor_type": "Humedad",
                    "device_type": "Gabinete",
                    "device_name": "Gabinete 1",
                    "sensor_name": "Humedad 2",
                    "TimeZone": "America/Chihuahua",
                },
            ]
        )

        result = gabinete(
            factlecturas_extended_df,
            dimensions_df,
            "2026-03-29 10:00:00",
            "2026-03-29 10:00:00",
        )

        rows = result.orderBy("SensorId").collect()

        self.assertEqual(len(rows), 2)
        self.assertEqual({row["SensorId"] for row in rows}, {10, 11})
        self.assertEqual(
            {row["TimeSpan"] for row in rows},
            {datetime(2026, 3, 29, 10, 0)},
        )
        self.assertEqual(
            {row["LocalTimeSpan"] for row in rows},
            {datetime(2026, 3, 29, 4, 0)},
        )
        self.assertEqual(len({row["OperationId"] for row in rows}), 2)
        self.assertEqual([row["ReadingsCount"] for row in rows], [2, 2])
        self.assertAlmostEqual(rows[0]["MeasurementValue"], 0.55)
        self.assertEqual(rows[0]["Anomalies"], 2)
        self.assertEqual(rows[1]["MeasurementValue"], 21.5)
        self.assertEqual(rows[1]["Anomalies"], 0)
        self.assertEqual({row["LocationId"] for row in rows}, {100})
        self.assertEqual(
            {row["sensor_type"] for row in rows},
            {"Humedad"},
        )


    def test_projectD_device_types_materialize_all_sensor_channels(self) -> None:
        from projectD.defs.assets.operations_assets import build_devices_df_op
        from projectD.defs.resources.transformation_ops import TransformationOps

        sensor_groups = (
            ("Gabinete", ("Temperatura", "Humedad")),
            ("CISTERNAS", ("EM500-SWL-Submersible Water Level Sensor",) * 2),
            ("Sensor fase ", ("CORRIENTE ELECTRICA",) * 3),
            ("TEMPERATURA Y HUMEDAD", ("Temperatura",)),
            ("ACOMETIDA", ("CORRIENTE ELECTRICA",) * 3),
            ("CONTROL DE LUCES", ("CONTROL DE LUCES WS558",) * 7),
            (" Weather Station", (
                "Velocidad del viento ",
                "Lluvia acumulada",
                "Dirección del viento",
                "Temperatura",
                "WTS506 - IoT Weather Station",
                "Humedad",
                "Porcentaje de Bateria",
            )),
        )
        dimensions = []
        readings = []
        for device_index, (device_type, sensor_types) in enumerate(sensor_groups, 1):
            for sensor_type in sensor_types:
                sensor_index = len(dimensions) + 1
                dimension = {
                    "DeviceId": f"10000000-0000-0000-0000-{device_index:012d}",
                    "SensorId": f"00000000-0000-0000-0000-{sensor_index:012d}",
                    "SensorTyId": f"20000000-0000-0000-0000-{sensor_index:012d}",
                    "LocationId": "30000000-0000-0000-0000-000000000001",
                    "sensor_type": sensor_type,
                    "device_type": device_type,
                    "device_name": f"Equipo {device_index}",
                    "sensor_name": f"Sensor {sensor_index}",
                    "TimeZone": "America/Chihuahua",
                }
                dimensions.append(dimension)
                for minute, value in ((5, 10.0), (25, 30.0)):
                    readings.append({
                        **dimension,
                        "TimeSpan": datetime(2026, 3, 29, 10, minute),
                        "Value": float(minute == 25) if device_type == "CONTROL DE LUCES" else value,
                    })

        context = SimpleNamespace(
            resources=SimpleNamespace(transformation_ops=TransformationOps()),
            log=Mock(),
        )
        result = build_devices_df_op.compute_fn.decorated_fn(
            context,
            {"fetch_from": datetime(2026, 3, 29, 10), "max_utc_hour": datetime(2026, 3, 29, 11)},
            self.spark.createDataFrame(dimensions),
            self.spark.createDataFrame(readings),
        )
        rows = result.collect()
        self.assertEqual(len(rows), 18)
        self.assertEqual({row["SensorId"] for row in rows}, {row["SensorId"] for row in dimensions if row["device_type"] != "CONTROL DE LUCES"})
        self.assertEqual(len({row["OperationId"] for row in rows}), 18)
        for row in rows:
            self.assertEqual(row["ReadingsCount"], 2)
            self.assertEqual(row["MeasurementValue"], 20.0)
            self.assertEqual(row["Min"], 10.0)
            self.assertEqual(row["Max"], 30.0)
            self.assertEqual(row["Anomalies"], 0)
            self.assertFalse(row["HasAnomalies"])
            self.assertEqual(row["AnomalousReadingsJson"], "[]")
            self.assertEqual(len(json.loads(row["CorrectReadingsJson"])), 2)
            self.assertEqual(row["TimeSpan"], datetime(2026, 3, 29, 10))
            self.assertEqual(row["LocalTimeSpan"], datetime(2026, 3, 29, 4))

    def test_projectD_new_types_accept_all_values_and_scope_empty_hours(self) -> None:
        from projectD.defs.resources.transformation_ops import TransformationOps

        transformations = TransformationOps()
        for method, device_type, sensor_type in (
            ("cisternas", "CISTERNAS", "EM500-SWL-Submersible Water Level Sensor"),
            ("sensor_fase", "Sensor fase ", "CORRIENTE ELECTRICA"),
            ("temperatura_humedad", "TEMPERATURA Y HUMEDAD", "Temperatura"),
            ("acometida", "ACOMETIDA", "CORRIENTE ELECTRICA"),
            ("weather_station", " Weather Station", "Velocidad del viento "),
        ):
            with self.subTest(method=method):
                dimension = {
                    "DeviceId": 1,
                    "SensorId": 10,
                    "SensorTyId": 100,
                    "LocationId": 1000,
                    "sensor_type": sensor_type,
                    "device_type": device_type,
                    "device_name": "Equipo de prueba",
                    "sensor_name": "Sensor de prueba",
                    "TimeZone": "America/Chihuahua",
                }
                dimensions = self.spark.createDataFrame([
                    dimension,
                    {**dimension, "SensorId": 11},
                    {**dimension, "SensorId": 12, "device_type": "Otro tipo"},
                    {**dimension, "SensorId": 13, "sensor_type": "Otro sensor"},
                ])
                readings = self.spark.createDataFrame([
                    {**dimension, "TimeSpan": datetime(2026, 3, 29, 10, 5), "Value": -1000.0},
                    {**dimension, "TimeSpan": datetime(2026, 3, 29, 10, 25), "Value": 1000.0},
                ])
                transform = getattr(transformations, method)
                rows = transform(readings, dimensions, "2026-03-29 10:00:00", "2026-03-29 11:00:00").collect()
                self.assertEqual(len(rows), 4)
                self.assertEqual({row["SensorId"] for row in rows}, {10, 11})
                self.assertEqual(len({row["OperationId"] for row in rows}), 4)
                for row in rows:
                    self.assertEqual(row["Anomalies"], 0)
                    self.assertFalse(row["HasAnomalies"])
                    self.assertEqual(row["AnomalousReadingsJson"], "[]")
                    if row["ReadingsCount"]:
                        self.assertEqual(row["MeasurementValue"], 0.0)
                        self.assertEqual(row["Min"], -1000.0)
                        self.assertEqual(row["Max"], 1000.0)
                        self.assertEqual(len(json.loads(row["CorrectReadingsJson"])), 2)
                    else:
                        self.assertIsNone(row["MeasurementValue"])
                        self.assertEqual(row["CorrectReadingsJson"], "[]")

                empty = transform(readings.limit(0), dimensions, "2026-03-29 10:00:00", "2026-03-29 10:00:00").collect()
                self.assertEqual(len(empty), 2)
                self.assertTrue(all(row["ReadingsCount"] == 0 and row["MeasurementValue"] is None for row in empty))
                self.assertIsNone(transform(readings, dimensions.filter("device_type = 'Otro tipo'"), "2026-03-29 10:00:00", "2026-03-29 10:00:00"))

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
                (1, 10, datetime(2026, 3, 29, 10, 0), "Humedad", 1.0),
                (1, 10, datetime(2026, 3, 29, 11, 0), "Humedad", 2.0),
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
        operations_path = PROJECT_SRC / "projectD" / "defs" / "assets" / "operations_assets.py"
        datalake_path = PROJECT_SRC / "projectD" / "defs" / "transformations" / "datalake_readings_extended.py"
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
        operations_path = PROJECT_SRC / "projectD" / "defs" / "assets" / "operations_assets.py"
        source = operations_path.read_text(encoding="utf-8")
        write_section = source.split("def write_factreadingsmeasurement_op", 1)[1].split("@op(", 1)[0]
        graph_section = source.split("def operations_assets", 1)[1]

        self.assertIn("write_fact_operations_stage(", write_section)
        self.assertIn("merge_fact_readings_measurement(", write_section)
        self.assertNotIn("delete_range(", write_section)
        self.assertNotIn("window_cleanup(fetch_from_ts)", graph_section)


if __name__ == "__main__":
    unittest.main()
