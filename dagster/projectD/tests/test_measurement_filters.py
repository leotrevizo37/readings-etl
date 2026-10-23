import sys
import unittest
from datetime import datetime
from importlib import import_module
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


CASES = [
    ["gabinete",["Gabinete","Cabinet","Armário"],[["Humedad","Humidity","Umidade"]]],
    ["cisternas",["CISTERNAS","Cisterns","Cisternas"],[["EM500-SWL-Submersible Water Level Sensor","EM500-SWL-Submersible Water Level Sensor","EM500-SWL-Submersible Water Level Sensor"]]],
    ["sensor_fase",["Sensor fase","Phase Sensor","Sensor de Fase"],[["CORRIENTE ELECTRICA","Electrical Current","Corrente Elétrica"]]],
    ["temperatura_humedad",["TEMPERATURA Y HUMEDAD","Temperature and Humidity","Temperatura e Umidade"],[["Temperatura","Temperature","Temperatura"]]],
    ["acometida",["ACOMETIDA","Service Entrance","Entrada de Energia"],[["CORRIENTE ELECTRICA","Electrical Current","Corrente Elétrica"]]],
    ["weather_station",["Weather Station","Weather Station","Estação Meteorológica"],[["Velocidad del viento","Wind Speed","Velocidade do Vento"],["Lluvia acumulada","Accumulated Rainfall","Chuva acumulada"],["Dirección del viento","Wind Direction","Direção do vento"],["Temperatura","Temperature","Temperatura"],["WTS506 - IoT Weather Station","WTS506 - IoT Weather Station","WTS506 - IoT Weather Station"],["Humedad","Humidity","Umidade"],["Porcentaje de Bateria","Battery Percentage","Porcentagem da bateria"]]],
]


class MeasurementFiltersTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("projectD-measurement-filters")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_translations_preserve_labels_and_measurements(self):
        for method, devices, sensors in CASES:
            transform = getattr(import_module("projectD.defs.transformations." + method), method)
            baseline = None
            for language in range(3):
                with self.subTest(method=method, language=language):
                    dimension_rows = []
                    reading_rows = []
                    for index, labels in enumerate(sensors):
                        sensor_name = ("Fase 1", "Phase 1", "Fase 1")[language] if labels[0] == "Medicion de corriente" else labels[language]
                        dimension = {
                            "DeviceId": "device", "SensorId": str(index), "SensorTyId": str(index),
                            "LocationId": "location", "TimeZone": "UTC",
                            "device_type": devices[language], "device_name": devices[language],
                            "sensor_type": labels[language], "sensor_name": sensor_name,
                        }
                        if "projectD" == "projectD" and method != "gabinete":
                            dimension["device_type"] = " " + dimension["device_type"] + " "
                            dimension["sensor_type"] = " " + dimension["sensor_type"] + " "
                        dimension_rows.append(dimension)
                        reading_rows.extend(
                            dict(dimension, ReadId=f"{index}-{minute}", TimeSpan=datetime(2026, 10, 19, 10, minute), Value=value)
                            for minute, value in ((5, 20.0), (25, 40.0))
                        )
                    readings = self.spark.createDataFrame(reading_rows)
                    dimensions = self.spark.createDataFrame(dimension_rows)
                    result = transform(readings, dimensions, "2026-10-19 10:00:00", "2026-10-19 10:00:00")
                    self.assertIsNotNone(result)
                    actual = result.orderBy("SensorId").collect()
                    self.assertEqual(len(actual), len(sensors))
                    numeric_rows = []
                    for index, row in enumerate(actual):
                        self.assertEqual(row["ReadingsCount"], 2)
                        self.assertEqual(row["sensor_type"], dimension_rows[index]["sensor_type"])
                        self.assertEqual(row["device_type"], dimension_rows[index]["device_type"])
                        self.assertEqual(row["sensor_name"], dimension_rows[index]["sensor_name"])
                        numeric_rows.append({
                            key: value for key, value in row.asDict().items()
                            if key not in ("sensor_type", "device_type", "sensor_name", "device_name")
                        })
                    if language == 0:
                        baseline = numeric_rows
                        baseline_schema = result.schema
                    else:
                        self.assertEqual(numeric_rows, baseline)
                        self.assertEqual(result.schema, baseline_schema)
                    self.assertEqual(readings.collect(), self.spark.createDataFrame(reading_rows).collect())
                    self.assertEqual(dimensions.collect(), self.spark.createDataFrame(dimension_rows).collect())

    def test_filter_preserves_exact_fallback_and_excludes_nulls(self):
        module = import_module("projectD.defs.transformations.stats_calculator")
        frame = self.spark.createDataFrame(
            [("Temperatura",), ("Temperature",), ("Temperature F",), ("Unknown",), (None,)],
            "sensor_type string",
        )
        expected = ["Temperature", "Temperatura"] if "projectD" != "projectA" else ["Temperatura"]
        selected = frame.filter(module.sensor_type_filter("Temperatura"))
        self.assertCountEqual([row.sensor_type for row in selected.collect()], expected)
        self.assertEqual(
            [row.sensor_type for row in frame.filter(module.sensor_type_filter("Unknown")).collect()],
            ["Unknown"],
        )
        self.assertEqual(frame.limit(0).filter(module.sensor_type_filter("Unknown")).count(), 0)
        self.assertEqual(frame.count(), 5)
