import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dagster import AssetCheckSeverity
from projectB.defs.asset_checks import (
    _build_factlecturas_original_lte_to_utc_result,
    _build_factlecturas_row_count_result,
    _build_utc_window_order_result,
    _build_operations_succeeded_result,
    _build_original_lte_to_utc_result,
    _build_real_readings_ratio_result,
    _build_to_utc_gte_max_time_span_result,
)


class AssetChecksTests(unittest.TestCase):
    def test_factreadings_real_readings_ratio_warns_before_66_percent(self):
        passing = _build_real_readings_ratio_result(
            {"row_count": 10, "rows_with_readings_count_gt_0": 7}
        )
        warning = _build_real_readings_ratio_result(
            {"row_count": 10, "rows_with_readings_count_gt_0": 6}
        )
        failing = _build_real_readings_ratio_result(
            {"row_count": 10, "rows_with_readings_count_gt_0": 5}
        )

        self.assertTrue(passing.passed)
        self.assertFalse(warning.passed)
        self.assertEqual(warning.severity, AssetCheckSeverity.WARN)
        self.assertFalse(failing.passed)
        self.assertEqual(failing.severity, AssetCheckSeverity.ERROR)

    def test_factreadings_utc_window_requires_to_utc_after_from_utc(self):
        passing = _build_utc_window_order_result(
            {
                "from_utc": "2026-05-01 10:00:00",
                "to_utc": "2026-05-01 11:00:00",
            }
        )
        failing = _build_utc_window_order_result(
            {
                "from_utc": "2026-05-01 10:00:00",
                "to_utc": "2026-05-01 10:00:00",
            }
        )

        self.assertTrue(passing.passed)
        self.assertEqual(passing.severity, AssetCheckSeverity.WARN)
        self.assertFalse(failing.passed)
        self.assertEqual(failing.severity, AssetCheckSeverity.WARN)

    def test_operations_control_validates_success_and_watermarks(self):
        metadata = {
            "succeeded": True,
            "original_last_loaded_at": "2026-05-01 09:00:00",
            "max_time_span": "2026-05-01 10:30:00",
            "to_utc": "2026-05-01 11:00:00",
        }

        succeeded = _build_operations_succeeded_result(metadata)
        to_utc_check = _build_to_utc_gte_max_time_span_result(metadata)
        original_check = _build_original_lte_to_utc_result(metadata)
        self.assertTrue(succeeded.passed)
        self.assertTrue(to_utc_check.passed)
        self.assertTrue(original_check.passed)
        self.assertEqual(succeeded.severity, AssetCheckSeverity.ERROR)
        self.assertEqual(to_utc_check.severity, AssetCheckSeverity.WARN)
        self.assertEqual(original_check.severity, AssetCheckSeverity.WARN)

        self.assertFalse(
            _build_operations_succeeded_result({**metadata, "succeeded": False}).passed
        )
        future_check = _build_to_utc_gte_max_time_span_result(
            {**metadata, "max_time_span": "2026-05-01 12:00:00"}
        )
        self.assertFalse(future_check.passed)
        self.assertEqual(future_check.severity, AssetCheckSeverity.ERROR)
        one_hour_check = _build_to_utc_gte_max_time_span_result(
            {**metadata, "max_time_span": "2026-05-01 10:00:00"}
        )
        two_hours_check = _build_to_utc_gte_max_time_span_result(
            {**metadata, "max_time_span": "2026-05-01 09:00:00"}
        )
        over_two_hours_check = _build_to_utc_gte_max_time_span_result(
            {**metadata, "max_time_span": "2026-05-01 08:59:59"}
        )
        self.assertFalse(one_hour_check.passed)
        self.assertEqual(one_hour_check.severity, AssetCheckSeverity.WARN)
        self.assertFalse(two_hours_check.passed)
        self.assertEqual(two_hours_check.severity, AssetCheckSeverity.WARN)
        self.assertFalse(over_two_hours_check.passed)
        self.assertEqual(over_two_hours_check.severity, AssetCheckSeverity.ERROR)
        self.assertTrue(
            _build_to_utc_gte_max_time_span_result(
                {**metadata, "max_time_span": None}
            ).passed
        )
        self.assertFalse(
            _build_original_lte_to_utc_result(
                {**metadata, "original_last_loaded_at": "2026-05-01 12:00:00"}
            ).passed
        )

    def test_factlecturas_validates_row_count_and_original_before_to_utc(self):
        metadata = {
            "row_count": 1,
            "original_last_loaded_at": "2026-05-01 09:00:00",
            "to_utc": "2026-05-01 10:00:00",
        }

        row_count_check = _build_factlecturas_row_count_result(metadata)
        original_check = _build_factlecturas_original_lte_to_utc_result(metadata)
        self.assertTrue(row_count_check.passed)
        self.assertTrue(original_check.passed)
        self.assertEqual(row_count_check.severity, AssetCheckSeverity.WARN)
        self.assertEqual(original_check.severity, AssetCheckSeverity.WARN)

        self.assertFalse(
            _build_factlecturas_row_count_result({**metadata, "row_count": 0}).passed
        )
        self.assertFalse(
            _build_factlecturas_original_lte_to_utc_result(
                {**metadata, "original_last_loaded_at": "2026-05-01 11:00:00"}
            ).passed
        )


if __name__ == "__main__":
    unittest.main()
