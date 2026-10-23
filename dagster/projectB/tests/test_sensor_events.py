import sys
import unittest
import warnings
from datetime import datetime, timedelta
from hashlib import md5
from importlib import import_module
from pathlib import Path
from typing import Optional
from uuid import UUID

from pyspark.sql import DataFrame, SparkSession

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

sensor_events_module = import_module("projectB.defs.transformations.sensor_events")
temperature_events_module = import_module("projectB.defs.transformations.temperature_events")
sensor_events = sensor_events_module.sensor_events
resolve_sensor_events_from = sensor_events_module.resolve_sensor_events_from
temperature_events = temperature_events_module.temperature_events


warnings.simplefilter("ignore", ResourceWarning)


class SensorEventsTransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("sensor-events-tests")
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

    def _expected_values_df(self, rows: list[dict]) -> DataFrame:
        return self.spark.createDataFrame(rows)

    def _row(
        self,
        read_id: str,
        sensor_id: str,
        local_time: datetime,
        value: float,
        device_type: str = "Cuarto Frio",
        device_name: str = "Cuarto Frio 2",
        sensor_type: str = "Estado de Puerta",
        time_span: Optional[datetime] = None,
        time_zone: str = "UTC",
    ) -> dict:
        return {
            "ReadId": read_id,
            "SensorId": sensor_id,
            "DeviceId": "d-" + sensor_id,
            "LocationId": "l1",
            "sublocation_name": "Local 1",
            "device_type": device_type,
            "device_name": device_name,
            "sensor_type": sensor_type,
            "sensor_name": "sensor-" + sensor_id,
            "TimeSpan": time_span if time_span is not None else local_time,
            "LocalTimeSpan": local_time,
            "TimeZone": time_zone,
            "Value": value,
        }

    def _event_tuples(self, result: DataFrame) -> list[tuple[str, float, float, bool]]:
        return [
            (
                row["ReadId"],
                float(row["Value"]),
                round(float(row["EventMinutes"]), 6),
                bool(row["IsEventCompleted"]),
            )
            for row in result.orderBy("SensorId", "LocalTimeSpan").collect()
        ]

    def test_returns_empty_dataframe_when_no_door_state_readings(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "r1",
                    "s1",
                    datetime(2026, 3, 29, 10, 0),
                    1.0,
                    device_type="Gabinete",
                    sensor_type="Temperatura",
                ),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertEqual(result.count(), 0)

    def test_detects_state_changes_for_door(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 0.0),
                self._row("r2", "s1", datetime(2026, 3, 29, 10, 5), 0.0),
                self._row("r3", "s1", datetime(2026, 3, 29, 10, 10), 1.0),
                self._row("r4", "s1", datetime(2026, 3, 29, 10, 15), 1.0),
                self._row("r5", "s1", datetime(2026, 3, 29, 10, 20), 0.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
        rows = result.orderBy("LocalTimeSpan").collect()
        self.assertEqual([row["ReadId"] for row in rows], ["r1", "r3", "r5"])
        self.assertEqual([row["Value"] for row in rows], [0.0, 1.0, 0.0])
        self.assertAlmostEqual(rows[0]["EventMinutes"], 10.0)
        self.assertAlmostEqual(rows[1]["EventMinutes"], 10.0)
        self.assertAlmostEqual(rows[2]["EventMinutes"], 60.0)
        self.assertTrue(rows[2]["IsEventCompleted"])

    def test_reprocess_start_uses_open_event_when_older_than_control(self) -> None:
        control_from = datetime(2026, 3, 29, 11, 0)
        open_event_from = datetime(2026, 3, 29, 10, 0)

        result = resolve_sensor_events_from(control_from, open_event_from)

        self.assertEqual(result, open_event_from)

    def test_reprocess_start_uses_control_when_no_open_event_exists(self) -> None:
        control_from = datetime(2026, 3, 29, 11, 0)

        result = resolve_sensor_events_from(control_from, None)

        self.assertEqual(result, control_from)

    def test_calculates_event_minutes_for_open_then_close(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 1.0),
                self._row("r2", "s1", datetime(2026, 3, 29, 10, 30), 0.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
        rows = result.orderBy("LocalTimeSpan").collect()
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(rows[0]["EventMinutes"], 30.0)
        self.assertTrue(rows[0]["IsEventCompleted"])
        self.assertAlmostEqual(rows[1]["EventMinutes"], 60.0)
        self.assertTrue(rows[1]["IsEventCompleted"])

    def test_event_closes_at_communication_threshold(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "r1",
                    "s1",
                    datetime(2026, 3, 29, 10, 0),
                    1.0,
                    device_type="Cuarto Residuos",
                    device_name="Cuarto de Residuos",
                ),
            ]
        )

        result = sensor_events(df, "2026-03-29 11:00:00")

        self.assertIsNotNone(result)
        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["EventMinutes"], 60.0)
        self.assertTrue(rows[0]["IsEventCompleted"])

    def test_event_closes_at_communication_threshold_using_utc_time_span(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "r1",
                    "s1",
                    datetime(2026, 3, 29, 10, 0),
                    1.0,
                    time_span=datetime(2026, 3, 29, 16, 0),
                ),
            ]
        )

        result = sensor_events(df, "2026-03-29 17:00:00")

        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["EventMinutes"], 60.0)
        self.assertTrue(rows[0]["IsEventCompleted"])

    def test_same_state_readings_extend_event_until_communication_threshold(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 1.0),
                self._row("r2", "s1", datetime(2026, 3, 29, 10, 30), 1.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["EventMinutes"], 90.0)
        self.assertTrue(rows[0]["IsEventCompleted"])

    def test_same_state_at_threshold_starts_new_event(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 1.0),
                self._row("r2", "s1", datetime(2026, 3, 29, 10, 30), 1.0),
                self._row("r3", "s1", datetime(2026, 3, 29, 11, 30), 1.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        rows = result.orderBy("LocalTimeSpan").collect()
        self.assertEqual([row["ReadId"] for row in rows], ["r1", "r3"])
        self.assertAlmostEqual(rows[0]["EventMinutes"], 90.0)
        self.assertTrue(rows[0]["IsEventCompleted"])
        self.assertAlmostEqual(rows[1]["EventMinutes"], 30.0)
        self.assertFalse(rows[1]["IsEventCompleted"])

    def test_ignores_temperature_out_of_range_when_under_two_hours(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "t1",
                    "temp1",
                    datetime(2026, 3, 30, 10, 0),
                    50.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t2",
                    "temp1",
                    datetime(2026, 3, 30, 11, 30),
                    40.0,
                    sensor_type="Temperatura",
                ),
            ]
        )
        expected_df = self._expected_values_df(
            [
                {
                    "SensorId": "temp1",
                    "DeviceId": "d-temp1",
                    "min_expected_value": 30.0,
                    "max_expected_value": 40.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1",
                },
            ]
        )

        result = temperature_events(df, expected_df, "2026-03-30 12:00:00")

        self.assertEqual(result.collect(), [])

    def test_detects_completed_temperature_event_after_two_hours(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "t1",
                    "temp1",
                    datetime(2026, 3, 30, 10, 0),
                    50.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t2",
                    "temp1",
                    datetime(2026, 3, 30, 10, 30),
                    51.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t3",
                    "temp1",
                    datetime(2026, 3, 30, 11, 0),
                    52.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t4",
                    "temp1",
                    datetime(2026, 3, 30, 11, 30),
                    53.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t5",
                    "temp1",
                    datetime(2026, 3, 30, 12, 0),
                    40.0,
                    sensor_type="Temperatura",
                ),
            ]
        )
        expected_df = self._expected_values_df(
            [
                {
                    "SensorId": "temp1",
                    "DeviceId": "d-temp1",
                    "min_expected_value": 30.0,
                    "max_expected_value": 40.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1",
                },
            ]
        )

        result = temperature_events(df, expected_df, "2026-03-30 12:30:00")

        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertNotEqual(rows[0]["EventId"], "t1")
        self.assertEqual(rows[0]["ReadId"], "t1")
        self.assertEqual(rows[0]["SensorType"], "Temperatura")
        self.assertAlmostEqual(rows[0]["Value"], 50.0)
        self.assertAlmostEqual(rows[0]["EventMinutes"], 120.0)
        self.assertTrue(rows[0]["IsEventCompleted"])

    def test_detects_open_temperature_event_after_two_hours(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "t1",
                    "temp1",
                    datetime(2026, 3, 30, 10, 0),
                    50.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t2",
                    "temp1",
                    datetime(2026, 3, 30, 10, 30),
                    51.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t3",
                    "temp1",
                    datetime(2026, 3, 30, 11, 0),
                    52.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t4",
                    "temp1",
                    datetime(2026, 3, 30, 11, 30),
                    53.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t5",
                    "temp1",
                    datetime(2026, 3, 30, 12, 0),
                    54.0,
                    sensor_type="Temperatura",
                ),
            ]
        )
        expected_df = self._expected_values_df(
            [
                {
                    "SensorId": "temp1",
                    "DeviceId": "d-temp1",
                    "min_expected_value": 30.0,
                    "max_expected_value": 40.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1",
                },
            ]
        )

        result = temperature_events(df, expected_df, "2026-03-30 12:30:00")

        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ReadId"], "t1")
        self.assertAlmostEqual(rows[0]["EventMinutes"], 150.0)
        self.assertFalse(rows[0]["IsEventCompleted"])

    def test_temperature_event_closes_after_communication_threshold(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "t1",
                    "temp1",
                    datetime(2026, 3, 30, 10, 0),
                    50.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t2",
                    "temp1",
                    datetime(2026, 3, 30, 10, 30),
                    51.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t3",
                    "temp1",
                    datetime(2026, 3, 30, 11, 0),
                    52.0,
                    sensor_type="Temperatura",
                ),
            ]
        )
        expected_df = self._expected_values_df(
            [
                {
                    "SensorId": "temp1",
                    "DeviceId": "d-temp1",
                    "min_expected_value": 30.0,
                    "max_expected_value": 40.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1",
                },
            ]
        )

        result = temperature_events(df, expected_df, "2026-03-30 13:00:00")

        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["EventMinutes"], 120.0)
        self.assertTrue(rows[0]["IsEventCompleted"])

    def test_temperature_reading_after_gap_does_not_extend_previous_event(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "t1",
                    "temp1",
                    datetime(2026, 3, 30, 10, 0),
                    50.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t2",
                    "temp1",
                    datetime(2026, 3, 30, 10, 30),
                    51.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t3",
                    "temp1",
                    datetime(2026, 3, 30, 11, 0),
                    52.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "t4",
                    "temp1",
                    datetime(2026, 3, 30, 13, 0),
                    53.0,
                    sensor_type="Temperatura",
                ),
            ]
        )
        expected_df = self._expected_values_df(
            [
                {
                    "SensorId": "temp1",
                    "DeviceId": "d-temp1",
                    "min_expected_value": 30.0,
                    "max_expected_value": 40.0,
                    "StartTime": "00:00:00",
                    "EndTime": "23:59:59",
                    "scheduledDays": "1",
                },
            ]
        )

        result = temperature_events(df, expected_df, "2026-03-30 13:30:00")

        rows = result.collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ReadId"], "t1")
        self.assertAlmostEqual(rows[0]["EventMinutes"], 120.0)
        self.assertTrue(rows[0]["IsEventCompleted"])

    def test_ignores_temperature_readings_outside_schedule(self) -> None:
        df = self._readings_df(
            [
                self._row(
                    "out-time",
                    "temp1",
                    datetime(2026, 3, 30, 10, 0),
                    50.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "out-day",
                    "temp1",
                    datetime(2026, 3, 29, 12, 30),
                    51.0,
                    sensor_type="Temperatura",
                ),
            ]
        )
        expected_df = self._expected_values_df(
            [
                {
                    "SensorId": "temp1",
                    "DeviceId": "d-temp1",
                    "min_expected_value": 30.0,
                    "max_expected_value": 40.0,
                    "StartTime": "12:00:00",
                    "EndTime": "13:00:00",
                    "scheduledDays": "1",
                },
            ]
        )

        result = temperature_events(df, expected_df, "2026-03-30 14:00:00")

        self.assertEqual(result.collect(), [])

    def test_long_event_closes_before_new_event_after_outage(self) -> None:
        start = datetime(2026, 7, 26, 9, 0)
        rows = [
            self._row("r" + str(index), "s1", start + timedelta(minutes=index * 30), 0.0)
            for index in range(43)
        ]
        rows.extend(
            [
                self._row("r-last", "s1", start + timedelta(minutes=1275), 0.0),
                self._row("r-return", "s1", datetime(2026, 8, 12, 16, 0), 0.0),
            ]
        )

        events = sensor_events(
            self._readings_df(rows),
            "2026-08-12 16:30:00",
        ).orderBy("TimeSpan").collect()

        self.assertEqual(
            [row["TimeSpan"] for row in events],
            [datetime(2026, 7, 26, 9, 0), datetime(2026, 8, 12, 16, 0)],
        )
        self.assertAlmostEqual(events[0]["EventMinutes"], 1335.0)
        self.assertTrue(events[0]["IsEventCompleted"])
        self.assertAlmostEqual(events[1]["EventMinutes"], 30.0)
        self.assertFalse(events[1]["IsEventCompleted"])

    def test_identical_duplicate_readings_use_namespaced_event_id(self) -> None:
        first_result = sensor_events(
            self._readings_df(
                [
                    self._row("z-start", "s1", datetime(2026, 7, 26, 9, 0), 0.0),
                    self._row("a-start", "s1", datetime(2026, 7, 26, 9, 0), 0.0),
                    self._row("z-last", "s1", datetime(2026, 7, 26, 9, 30), 0.0),
                    self._row("a-last", "s1", datetime(2026, 7, 26, 9, 30), 0.0),
                ]
            ),
            "2026-07-26 10:30:00",
        ).collect()
        second_result = sensor_events(
            self._readings_df(
                [
                    self._row("another-start", "s1", datetime(2026, 7, 26, 9, 0), 0.0),
                    self._row("another-last", "s1", datetime(2026, 7, 26, 9, 30), 0.0),
                ]
            ),
            "2026-07-26 10:30:00",
        ).collect()
        event_hash = md5(b"events12026-07-26 09:00:00").hexdigest()

        self.assertEqual(len(first_result), 1)
        self.assertEqual(first_result[0]["EventId"], str(UUID(event_hash)))
        self.assertEqual(first_result[0]["EventId"], second_result[0]["EventId"])
        self.assertEqual(first_result[0]["ReadId"], "a-start")
        self.assertEqual(second_result[0]["ReadId"], "another-start")

    def test_conflicting_duplicate_readings_are_discarded(self) -> None:
        events = sensor_events(
            self._readings_df(
                [
                    self._row("r1", "s1", datetime(2026, 7, 26, 9, 0), 0.0),
                    self._row("r2", "s1", datetime(2026, 7, 26, 9, 0), 1.0),
                    self._row("r3", "s1", datetime(2026, 7, 26, 9, 5), 1.0),
                ]
            ),
            "2026-07-26 09:30:00",
        ).collect()

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["TimeSpan"], datetime(2026, 7, 26, 9, 5))
        self.assertAlmostEqual(events[0]["EventMinutes"], 25.0)
        self.assertFalse(events[0]["IsEventCompleted"])


    def test_discards_first_event_contained_in_previous_completed_event(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 8, 20, 14, 49, 41), 0.0),
                self._row("r2", "s1", datetime(2026, 8, 20, 14, 51, 47), 1.0),
            ]
        )
        previous_events_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "PreviousTimeSpan": datetime(2026, 8, 20, 14, 47, 19),
                    "PreviousValue": 0.0,
                    "PreviousEndTime": datetime(2026, 8, 20, 14, 51, 47),
                }
            ]
        )

        events = sensor_events(
            df,
            "2026-08-20 14:52:00",
            previous_events_df,
        ).collect()

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["TimeSpan"], datetime(2026, 8, 20, 14, 51, 47))
        self.assertEqual(events[0]["Value"], 1.0)

    def test_keeps_first_event_after_previous_completed_interval(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 8, 20, 14, 49, 41), 0.0),
                self._row("r2", "s1", datetime(2026, 8, 20, 14, 51, 47), 1.0),
            ]
        )
        previous_events_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "PreviousTimeSpan": datetime(2026, 8, 20, 14, 47, 19),
                    "PreviousValue": 0.0,
                    "PreviousEndTime": datetime(2026, 8, 20, 14, 49, 0),
                }
            ]
        )

        events = sensor_events(
            df,
            "2026-08-20 14:52:00",
            previous_events_df,
        ).orderBy("TimeSpan").collect()

        self.assertEqual(
            [event["TimeSpan"] for event in events],
            [
                datetime(2026, 8, 20, 14, 49, 41),
                datetime(2026, 8, 20, 14, 51, 47),
            ],
        )

    def test_keeps_first_event_when_completed_interval_end_differs(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 8, 20, 14, 49, 41), 0.0),
                self._row("r2", "s1", datetime(2026, 8, 20, 14, 52), 1.0),
            ]
        )
        previous_events_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "PreviousTimeSpan": datetime(2026, 8, 20, 14, 47, 19),
                    "PreviousValue": 0.0,
                    "PreviousEndTime": datetime(2026, 8, 20, 14, 51, 47),
                }
            ]
        )

        events = sensor_events(
            df,
            "2026-08-20 14:53:00",
            previous_events_df,
        ).orderBy("TimeSpan").collect()

        self.assertEqual(
            [event["TimeSpan"] for event in events],
            [
                datetime(2026, 8, 20, 14, 49, 41),
                datetime(2026, 8, 20, 14, 52),
            ],
        )

    def test_keeps_first_event_when_previous_completed_value_differs(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 8, 20, 14, 49, 41), 0.0),
                self._row("r2", "s1", datetime(2026, 8, 20, 14, 51, 47), 1.0),
            ]
        )
        previous_events_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "PreviousTimeSpan": datetime(2026, 8, 20, 14, 47, 19),
                    "PreviousValue": 1.0,
                    "PreviousEndTime": datetime(2026, 8, 20, 14, 51, 47),
                }
            ]
        )

        events = sensor_events(
            df,
            "2026-08-20 14:52:00",
            previous_events_df,
        ).orderBy("TimeSpan").collect()

        self.assertEqual(
            [event["TimeSpan"] for event in events],
            [
                datetime(2026, 8, 20, 14, 49, 41),
                datetime(2026, 8, 20, 14, 51, 47),
            ],
        )

    def test_previous_completed_event_validation_is_per_sensor(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 8, 20, 14, 49, 41), 0.0),
                self._row("r2", "s1", datetime(2026, 8, 20, 14, 51, 47), 1.0),
                self._row("r3", "s2", datetime(2026, 8, 20, 14, 49, 41), 0.0),
                self._row("r4", "s2", datetime(2026, 8, 20, 14, 51, 47), 1.0),
            ]
        )
        previous_events_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "PreviousTimeSpan": datetime(2026, 8, 20, 14, 47, 19),
                    "PreviousValue": 0.0,
                    "PreviousEndTime": datetime(2026, 8, 20, 14, 51, 47),
                }
            ]
        )

        events = sensor_events(
            df,
            "2026-08-20 14:52:00",
            previous_events_df,
        ).orderBy("SensorId", "TimeSpan").collect()

        self.assertEqual(
            [(event["SensorId"], event["TimeSpan"]) for event in events],
            [
                ("s1", datetime(2026, 8, 20, 14, 51, 47)),
                ("s2", datetime(2026, 8, 20, 14, 49, 41)),
                ("s2", datetime(2026, 8, 20, 14, 51, 47)),
            ],
        )

    def test_uses_latest_containing_previous_completed_event(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 8, 20, 14, 49, 41), 0.0),
                self._row("r2", "s1", datetime(2026, 8, 20, 14, 51, 47), 1.0),
            ]
        )
        previous_events_df = self.spark.createDataFrame(
            [
                {
                    "SensorId": "s1",
                    "PreviousTimeSpan": datetime(2026, 8, 20, 14, 40),
                    "PreviousValue": 0.0,
                    "PreviousEndTime": datetime(2026, 8, 20, 15),
                },
                {
                    "SensorId": "s1",
                    "PreviousTimeSpan": datetime(2026, 8, 20, 14, 47, 19),
                    "PreviousValue": 1.0,
                    "PreviousEndTime": datetime(2026, 8, 20, 14, 51, 47),
                },
            ]
        )

        events = sensor_events(
            df,
            "2026-08-20 14:52:00",
            previous_events_df,
        ).orderBy("TimeSpan").collect()

        self.assertEqual(
            [event["TimeSpan"] for event in events],
            [
                datetime(2026, 8, 20, 14, 49, 41),
                datetime(2026, 8, 20, 14, 51, 47),
            ],
        )

    def test_multiple_sensors_compute_independently(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 1.0),
                self._row("r2", "s1", datetime(2026, 3, 29, 10, 10), 0.0),
                self._row("r3", "s2", datetime(2026, 3, 29, 10, 5), 1.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
        rows = result.orderBy("SensorId", "LocalTimeSpan").collect()
        s1_rows = [row for row in rows if row["SensorId"] == "s1"]
        s2_rows = [row for row in rows if row["SensorId"] == "s2"]
        self.assertEqual(len(s1_rows), 2)
        self.assertEqual(len(s2_rows), 1)
        self.assertAlmostEqual(s1_rows[0]["EventMinutes"], 10.0)
        self.assertAlmostEqual(s1_rows[1]["EventMinutes"], 60.0)
        self.assertTrue(s1_rows[1]["IsEventCompleted"])
        self.assertAlmostEqual(s2_rows[0]["EventMinutes"], 60.0)
        self.assertTrue(s2_rows[0]["IsEventCompleted"])

    def test_long_sequence_fills_minutes_for_every_state_change(self) -> None:
        df = self._readings_df(
            [
                self._row("r00", "s1", datetime(2026, 3, 29, 8, 0), 0.0),
                self._row("r01", "s1", datetime(2026, 3, 29, 8, 5), 0.0),
                self._row("r02", "s1", datetime(2026, 3, 29, 8, 15), 1.0),
                self._row("r03", "s1", datetime(2026, 3, 29, 8, 20), 1.0),
                self._row("r04", "s1", datetime(2026, 3, 29, 8, 45), 0.0),
                self._row("r05", "s1", datetime(2026, 3, 29, 9, 10), 1.0),
                self._row("r06", "s1", datetime(2026, 3, 29, 9, 25), 0.0),
                self._row("r07", "s1", datetime(2026, 3, 29, 9, 50), 0.0),
                self._row("r08", "s1", datetime(2026, 3, 29, 10, 10), 1.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 11:00:00")

        self.assertIsNotNone(result)
        self.assertEqual(
            self._event_tuples(result),
            [
                ("r00", 0.0, 15.0, True),
                ("r02", 1.0, 30.0, True),
                ("r04", 0.0, 25.0, True),
                ("r05", 1.0, 15.0, True),
                ("r06", 0.0, 45.0, True),
                ("r08", 1.0, 50.0, False),
            ],
        )

    def test_interleaved_sensors_keep_independent_timelines(self) -> None:
        df = self._readings_df(
            [
                self._row("s1-r1", "s1", datetime(2026, 3, 29, 10, 0), 0.0),
                self._row("s2-r1", "s2", datetime(2026, 3, 29, 10, 5), 1.0),
                self._row("s1-r2", "s1", datetime(2026, 3, 29, 10, 20), 1.0),
                self._row("s2-r2", "s2", datetime(2026, 3, 29, 10, 25), 0.0),
                self._row("s1-r3", "s1", datetime(2026, 3, 29, 10, 30), 0.0),
                self._row("s2-r3", "s2", datetime(2026, 3, 29, 10, 50), 1.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 11:00:00")

        self.assertIsNotNone(result)
        self.assertEqual(
            self._event_tuples(result),
            [
                ("s1-r1", 0.0, 20.0, True),
                ("s1-r2", 1.0, 10.0, True),
                ("s1-r3", 0.0, 30.0, False),
                ("s2-r1", 1.0, 20.0, True),
                ("s2-r2", 0.0, 25.0, True),
                ("s2-r3", 1.0, 10.0, False),
            ],
        )

    def test_ignores_non_estado_non_door_and_invalid_values(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 0.0),
                self._row(
                    "noise-temp",
                    "s1",
                    datetime(2026, 3, 29, 10, 5),
                    1.0,
                    sensor_type="Temperatura",
                ),
                self._row(
                    "noise-device",
                    "s2",
                    datetime(2026, 3, 29, 10, 10),
                    1.0,
                    device_type="Gabinete",
                ),
                self._row("noise-value", "s1", datetime(2026, 3, 29, 10, 15), 2.0),
                self._row("r2", "s1", datetime(2026, 3, 29, 10, 20), 1.0),
                self._row("r3", "s1", datetime(2026, 3, 29, 10, 35), 0.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 11:00:00")

        self.assertIsNotNone(result)
        self.assertEqual(
            self._event_tuples(result),
            [
                ("r1", 0.0, 20.0, True),
                ("r2", 1.0, 15.0, True),
                ("r3", 0.0, 25.0, False),
            ],
        )

    def test_output_columns_match_sql_table_schema(self) -> None:
        df = self._readings_df(
            [
                self._row("r1", "s1", datetime(2026, 3, 29, 10, 0), 1.0),
            ]
        )

        result = sensor_events(df, "2026-03-29 12:00:00")

        self.assertIsNotNone(result)
        self.assertEqual(
            result.columns,
            [
                "EventId",
                "ReadId",
                "SensorId",
                "DeviceId",
                "LocationId",
                "LocationName",
                "DeviceType",
                "SensorType",
                "SensorName",
                "TimeSpan",
                "LocalTimeSpan",
                "Value",
                "EventMinutes",
                "IsEventCompleted",
            ],
        )


if __name__ == "__main__":
    unittest.main()
