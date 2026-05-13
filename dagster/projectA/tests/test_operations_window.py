import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from projectA.operations_window import (
    DEFAULT_OPERATIONS_BOOTSTRAP_START,
    OPERATIONS_TIMEZONE,
    build_operations_window,
    current_operational_hour_start,
    resolve_operations_watermark,
)


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

class BuildOperationsWindowTests(unittest.TestCase):
    def setUp(self) -> None:
        self._original_overlap = os.environ.pop("FACTREADINGS_OVERLAP_HOURS", None)

    def tearDown(self) -> None:
        if self._original_overlap is not None:
            os.environ["FACTREADINGS_OVERLAP_HOURS"] = self._original_overlap
        else:
            os.environ.pop("FACTREADINGS_OVERLAP_HOURS", None)

    def test_uses_local_hour_boundary_for_operational_window(self) -> None:
        now = datetime(2026, 3, 29, 16, 35, tzinfo=OPERATIONS_TIMEZONE)
        last_loaded_at = datetime(2026, 3, 29, 14, 17)

        window = build_operations_window(last_loaded_at, now=now)

        self.assertEqual(window["max_local_hour"], datetime(2026, 3, 29, 16, 0))
        self.assertEqual(window["fetch_from"], datetime(2026, 3, 29, 3, 0))
        self.assertEqual(
            window["delete_to_exclusive"],
            datetime(2026, 3, 29, 17, 0),
        )
        self.assertFalse(window["watermark_in_future"])

    def test_future_watermark_is_clamped_for_processing_and_cleaned_up(self) -> None:
        now = datetime(2026, 3, 29, 16, 35, tzinfo=OPERATIONS_TIMEZONE)
        last_loaded_at = datetime(2026, 3, 29, 22, 5)

        window = build_operations_window(last_loaded_at, now=now)

        self.assertTrue(window["watermark_in_future"])
        self.assertEqual(window["max_local_hour"], datetime(2026, 3, 29, 16, 0))
        self.assertEqual(window["fetch_from"], datetime(2026, 3, 29, 5, 0))
        self.assertEqual(
            window["delete_to_exclusive"],
            datetime(2026, 3, 29, 23, 5),
        )

    def test_bootstrap_window_uses_default_start(self) -> None:
        now = datetime(2026, 3, 29, 16, 35, tzinfo=OPERATIONS_TIMEZONE)

        window = build_operations_window(None, now=now)

        self.assertEqual(
            window["original_last_loaded_at"],
            DEFAULT_OPERATIONS_BOOTSTRAP_START,
        )
        self.assertEqual(
            window["fetch_from"],
            datetime(2025, 9, 30, 13, 0),
        )

    def test_uses_configured_factreadings_overlap_hours(self) -> None:
        os.environ["FACTREADINGS_OVERLAP_HOURS"] = "3"
        now = datetime(2026, 3, 29, 16, 35, tzinfo=OPERATIONS_TIMEZONE)
        last_loaded_at = datetime(2026, 3, 29, 14, 17)

        window = build_operations_window(last_loaded_at, now=now)

        self.assertEqual(window["fetch_from"], datetime(2026, 3, 29, 12, 0))


class CurrentOperationalHourStartTests(unittest.TestCase):
    def test_converts_aware_utc_now_into_local_hour_start(self) -> None:
        utc_now = datetime(2026, 1, 15, 18, 45, 12, tzinfo=timezone.utc)

        hour_start = current_operational_hour_start(now=utc_now)
        expected = (
            utc_now.astimezone(OPERATIONS_TIMEZONE)
            .replace(tzinfo=None, minute=0, second=0, microsecond=0)
        )

        self.assertEqual(hour_start, expected)


class ResolveOperationsWatermarkTests(unittest.TestCase):
    def test_failed_run_keeps_original_watermark(self) -> None:
        original = datetime(2026, 3, 29, 9, 0)

        resolved = resolve_operations_watermark(
            succeeded=False,
            original_last_loaded_at=original,
            max_time_span=datetime(2026, 3, 29, 12, 0),
            max_local_hour=datetime(2026, 3, 29, 13, 0),
        )

        self.assertEqual(resolved, original)

    def test_empty_successful_run_advances_to_current_local_hour(self) -> None:
        resolved = resolve_operations_watermark(
            succeeded=True,
            original_last_loaded_at=datetime(2026, 3, 29, 9, 0),
            max_time_span=None,
            max_local_hour=datetime(2026, 3, 29, 13, 0),
        )

        self.assertEqual(resolved, datetime(2026, 3, 29, 13, 0))

    def test_successful_run_prefers_max_time_span(self) -> None:
        resolved = resolve_operations_watermark(
            succeeded=True,
            original_last_loaded_at=datetime(2026, 3, 29, 9, 0),
            max_time_span=datetime(2026, 3, 29, 12, 0),
            max_local_hour=datetime(2026, 3, 29, 13, 0),
        )

        self.assertEqual(resolved, datetime(2026, 3, 29, 12, 0))


if __name__ == "__main__":
    unittest.main()
