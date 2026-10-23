import sys
import unittest
from datetime import datetime
from importlib import import_module
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

factlecturas_backfill = import_module("projectB.factlecturas_backfill")
build_backfill_window = factlecturas_backfill.build_backfill_window
build_daily_backfill_window = factlecturas_backfill.build_daily_backfill_window
build_sensor_backfill_request = factlecturas_backfill.build_sensor_backfill_request
normalize_sensor_ids = factlecturas_backfill.normalize_sensor_ids
parse_utc_datetime = factlecturas_backfill.parse_utc_datetime


class FactLecturasBackfillTests(unittest.TestCase):
    def test_parse_utc_datetime_normalizes_timezone_and_microseconds(self) -> None:
        parsed = parse_utc_datetime("2026-05-01T01:02:03.456789+00:00")

        self.assertEqual(parsed, datetime(2026, 5, 1, 1, 2, 3))

    def test_backfill_window_rejects_invalid_range(self) -> None:
        with self.assertRaises(ValueError):
            build_backfill_window(
                "2026-05-02T00:00:00",
                "2026-05-01T00:00:00",
            )

    def test_normalize_sensor_ids_validates_and_deduplicates(self) -> None:
        sensor_ids = normalize_sensor_ids(
            [
                "aaaaaaaa-0000-0000-0000-000000000001",
                "AAAAAAAA-0000-0000-0000-000000000001",
            ]
        )

        self.assertEqual(sensor_ids, ["AAAAAAAA-0000-0000-0000-000000000001"])

    def test_daily_backfill_window_uses_one_utc_day(self) -> None:
        window = build_daily_backfill_window("2026-05-01")

        self.assertEqual(window["from_utc"], datetime(2026, 5, 1))
        self.assertEqual(window["to_utc"], datetime(2026, 5, 2))

    def test_sensor_backfill_request_is_append_only(self) -> None:
        request = build_sensor_backfill_request(
            "2026-05-01T00:00:00",
            "2026-05-02T00:00:00",
            ["00000000-0000-0000-0000-000000000001"],
        )

        self.assertFalse(request["delete_before_load"])


if __name__ == "__main__":
    unittest.main()
