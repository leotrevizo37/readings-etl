import sys
import unittest
from datetime import datetime
from importlib import import_module
from pathlib import Path
from unittest.mock import patch


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

factaudiencia_backfill = import_module("projectA.factaudiencia_backfill")
build_backfill_window = factaudiencia_backfill.build_backfill_window
build_daily_backfill_window = factaudiencia_backfill.build_daily_backfill_window
get_factaudiencia_backfill_start_datetime = (
    factaudiencia_backfill.get_factaudiencia_backfill_start_datetime
)
parse_utc_datetime = factaudiencia_backfill.parse_utc_datetime


class FactAudienciaBackfillTests(unittest.TestCase):
    def test_parse_utc_datetime_normalizes_timezone_and_microseconds(self) -> None:
        parsed = parse_utc_datetime("2026-08-26T02:05:07.456789+00:00")

        self.assertEqual(parsed, datetime(2026, 8, 26, 2, 5, 7))

    def test_backfill_window_rejects_invalid_range(self) -> None:
        with self.assertRaises(ValueError):
            build_backfill_window(
                "2026-08-27T00:00:00",
                "2026-08-26T00:00:00",
            )

    def test_daily_backfill_window_uses_one_utc_day(self) -> None:
        window = build_daily_backfill_window("2026-08-26")

        self.assertEqual(window["from_utc"], datetime(2026, 8, 26))
        self.assertEqual(window["to_utc"], datetime(2026, 8, 27))

    def test_backfill_start_date_uses_factaudiencia_environment_name(self) -> None:
        with patch.dict(
            "os.environ",
            {"FACTAUDIENCIA_BACKFILL_START_DATE": "2026-08-01"},
        ):
            start = get_factaudiencia_backfill_start_datetime()

        self.assertEqual(start, datetime(2026, 8, 1))

    def test_backfill_contract_does_not_expose_sensor_filtering(self) -> None:
        job_source = (
            PROJECT_SRC
            / "projectA"
            / "defs"
            / "jobs"
            / "factaudiencia_backfill_job.py"
        ).read_text()

        self.assertNotIn("sensor_ids", job_source)
        self.assertIn('df.filter(col("TimeSpan") < to_utc)', job_source)


if __name__ == "__main__":
    unittest.main()
